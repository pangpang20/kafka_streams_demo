package com.kafka.consumer.stream;

import com.kafka.consumer.config.AppConfigLoader;
import com.kafka.consumer.config.ConnectionConfigLoader;
import com.kafka.consumer.config.TableSchemaConfigLoader;
import com.kafka.consumer.model.ProcessingResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 可配置 OceanBase 异议数据写入器
 *
 * 将质量检查失败的数据写入 OceanBase 数据库
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableInvalidDataWriter {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableInvalidDataWriter.class);

    private final HikariDataSource dataSource;
    private final String tableName;
    private final List<String> fieldNames;

    private final List<ProcessingResult> buffer = new ArrayList<>();
    private static final int BATCH_SIZE = 10;

    /**
     * 构造函数
     *
     * @param connectionConfig 连接配置
     * @param tableSchemaConfig 表结构配置
     * @param appConfig 应用配置
     */
    public ConfigurableInvalidDataWriter(ConnectionConfigLoader connectionConfig,
                                          TableSchemaConfigLoader tableSchemaConfig,
                                          AppConfigLoader appConfig) {
        this.tableName = appConfig.getInvalidDataTableName();
        this.fieldNames = extractFieldNames(tableSchemaConfig, appConfig.getSourceTableName());
        this.dataSource = createDataSource(connectionConfig.getOceanBaseConfig());

        // 如果配置了自动建表，创建目标表
        if (appConfig.isAutoCreateTable()) {
            createTableIfNotExists(connectionConfig, tableSchemaConfig, appConfig);
        }

        log.info("ConfigurableInvalidDataWriter 初始化完成，表名：{}", tableName);
    }

    /**
     * 提取字段名列表
     */
    private List<String> extractFieldNames(TableSchemaConfigLoader schemaConfig, String sourceTableName) {
        List<String> names = new ArrayList<>();
        TableSchemaConfigLoader.TableConfig table = schemaConfig.getTableByName(sourceTableName);
        if (table != null && table.getFields() != null) {
            for (TableSchemaConfigLoader.FieldConfig field : table.getFields()) {
                names.add(field.getName());
            }
        }
        return names;
    }

    /**
     * 创建数据源
     */
    private HikariDataSource createDataSource(ConnectionConfigLoader.OceanBaseConfig config) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(String.format(
            "jdbc:oceanbase://%s:%d/%s?useSSL=false&useUnicode=true&characterEncoding=utf8" +
            "&connectTimeout=%d&socketTimeout=%d&allowPublicKeyRetrieval=true",
            config.getHost(), config.getPort(), config.getDatabase(),
            config.getPool() != null ? config.getPool().getConnectionTimeoutMs() : 30000,
            config.getPool() != null ? config.getPool().getIdleTimeoutMs() : 60000
        ));
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.setDriverClassName("com.oceanbase.jdbc.Driver");

        if (config.getPool() != null) {
            hc.setMaximumPoolSize(config.getPool().getMaxPoolSize());
            hc.setMinimumIdle(config.getPool().getMinIdle());
            hc.setConnectionTimeout(config.getPool().getConnectionTimeoutMs());
            hc.setIdleTimeout(config.getPool().getIdleTimeoutMs());
            hc.setMaxLifetime(config.getPool().getMaxLifetimeMs());
        }

        try {
            HikariDataSource ds = new HikariDataSource(hc);
            try (Connection conn = ds.getConnection()) {
                log.info("OceanBase 连接测试成功：{}", conn.getCatalog());
            }
            return ds;
        } catch (Exception e) {
            log.error("OceanBase 连接池初始化失败：{}", e.getMessage(), e);
            throw new RuntimeException("OceanBase 初始化失败", e);
        }
    }

    /**
     * 创建异议数据表
     */
    private void createTableIfNotExists(ConnectionConfigLoader connectionConfig,
                                         TableSchemaConfigLoader tableSchemaConfig,
                                         AppConfigLoader appConfig) {
        String sql = tableSchemaConfig.generateInvalidTableSQL(
            appConfig.getSourceTableName(),
            connectionConfig.getOceanBaseConfig().getDatabase(),
            appConfig.getSource().getInvalidTablePrefix()
        );

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.execute();
            log.info("异议数据表创建成功：{}", tableName);
        } catch (SQLException e) {
            log.error("创建异议数据表失败：{} - {}", tableName, e.getMessage());
        }
    }

    /**
     * 写入异议数据
     */
    public void logInvalidData(ProcessingResult result) {
        if (result == null || result.isSuccess()) {
            return;
        }

        synchronized (buffer) {
            buffer.add(result);
            if (buffer.size() >= BATCH_SIZE) {
                flushBuffer();
            }
        }
    }

    /**
     * 刷新缓冲区
     */
    public void flushBuffer() {
        if (buffer.isEmpty()) {
            return;
        }

        List<ProcessingResult> toFlush;
        synchronized (buffer) {
            toFlush = new ArrayList<>(buffer);
            buffer.clear();
        }

        log.info("准备批量写入 {} 条记录到 OceanBase (invalid_data)", toFlush.size());

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tableName).append(" (");
        sqlBuilder.append("record_key, record_timestamp, opcode, ");
        for (String field : fieldNames) {
            sqlBuilder.append(field).append(", ");
        }
        sqlBuilder.append("failure_summary, error_fields, error_details, raw_data, log_timestamp) VALUES (");
        for (int i = 0; i < 7 + fieldNames.size(); i++) {
            sqlBuilder.append("?");
            if (i < 7 + fieldNames.size() - 1) {
                sqlBuilder.append(", ");
            }
        }
        sqlBuilder.append(")");

        String sql = sqlBuilder.toString();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (ProcessingResult result : toFlush) {
                addBatch(pstmt, result);
            }

            pstmt.executeBatch();
            conn.commit();
            log.info("批量写入 OceanBase 成功：{} 条记录 (invalid_data)", toFlush.size());

        } catch (SQLException e) {
            log.error("批量写入 OceanBase 失败 (invalid_data)：{}", e.getMessage(), e);
            synchronized (buffer) {
                buffer.addAll(0, toFlush);
            }
        }
    }

    /**
     * 添加单条记录到批处理
     */
    private void addBatch(PreparedStatement pstmt, ProcessingResult result) throws SQLException {
        int idx = 1;

        pstmt.setString(idx++, result.getRecordKey());
        // 使用毫秒精度时间戳，避免 OceanBase 不支持纳秒精度的问题
        String recordTimestamp = String.format("%tF %<tH:%<tM:%<tS.%1$tL", System.currentTimeMillis());
        pstmt.setString(idx++, recordTimestamp);
        pstmt.setString(idx++, result.getOpcode());

        // 设置业务字段
        Map<String, Object> fieldValues = extractFieldValues(result);
        for (String field : fieldNames) {
            pstmt.setObject(idx++, fieldValues.get(field));
        }

        // 设置质量检查结果
        ProcessingResult.ValidationSummary summary = result.getValidationSummary();
        pstmt.setString(idx++, summary != null ? summary.getErrorSummary() : null);
        pstmt.setString(idx++, summary != null ? summary.getErrorFields() : null);
        pstmt.setString(idx++, summary != null ? summary.getErrorDetails() : null);

        pstmt.setString(idx++, result.getRawJson());
        // 使用毫秒精度时间戳，避免 OceanBase 不支持纳秒精度的问题
        pstmt.setTimestamp(idx++, new Timestamp(System.currentTimeMillis()));

        pstmt.addBatch();
    }

    /**
     * 提取字段值
     */
    private Map<String, Object> extractFieldValues(ProcessingResult result) {
        Map<String, Object> values = new java.util.HashMap<>();
        if (result.getRecord() != null && result.getRecord().getAllfields() != null
                && result.getRecord().getAllfields().getAfterData() != null) {
            for (Map.Entry<String, com.kafka.consumer.model.CdcEvent.FieldData> entry :
                    result.getRecord().getAllfields().getAfterData().entrySet()) {
                if (entry.getValue() != null && entry.getValue().getValue() != null) {
                    values.put(entry.getKey(), entry.getValue().getValue());
                }
            }
        }
        return values;
    }

    /**
     * 关闭资源
     */
    public void close() {
        flushBuffer();
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("OceanBase 连接池已关闭 (InvalidData)");
        }
    }
}
