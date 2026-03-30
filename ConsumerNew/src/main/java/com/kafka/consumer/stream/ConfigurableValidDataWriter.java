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
 * 可配置 OceanBase 写入器
 *
 * 将正常数据写入 OceanBase 数据库
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableValidDataWriter {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableValidDataWriter.class);

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
    public ConfigurableValidDataWriter(ConnectionConfigLoader connectionConfig,
                                        TableSchemaConfigLoader tableSchemaConfig,
                                        AppConfigLoader appConfig) {
        this.tableName = appConfig.getSourceTableName();
        this.fieldNames = extractFieldNames(tableSchemaConfig, this.tableName);
        this.dataSource = createDataSource(connectionConfig.getOceanBaseConfig());

        // 如果配置了自动建表，创建目标表
        if (appConfig.isAutoCreateTable()) {
            createTableIfNotExists(connectionConfig, tableSchemaConfig);
        }

        log.info("ConfigurableValidDataWriter 初始化完成，表名：{}, 业务字段：{}", tableName, fieldNames);
    }

    /**
     * 提取字段名列表
     */
    private List<String> extractFieldNames(TableSchemaConfigLoader schemaConfig, String tableName) {
        List<String> names = new ArrayList<>();
        TableSchemaConfigLoader.TableConfig table = schemaConfig.getTableByName(tableName);
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
            // 测试连接
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
     * 创建表（只包含业务字段）
     */
    private void createTableIfNotExists(ConnectionConfigLoader connectionConfig,
                                         TableSchemaConfigLoader tableSchemaConfig) {
        // 生成只包含业务字段的建表 SQL
        String sql = generateBusinessTableSQL(tableSchemaConfig, tableName,
                connectionConfig.getOceanBaseConfig().getDatabase());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.execute();
            log.info("表创建成功：{} (只包含业务字段)", tableName);
        } catch (SQLException e) {
            log.error("创建表失败：{} - {}", tableName, e.getMessage());
        }
    }

    /**
     * 生成只包含业务字段的建表 SQL
     */
    private String generateBusinessTableSQL(TableSchemaConfigLoader schemaConfig, String tableName, String database) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(database).append(".").append(tableName).append(" (\n");

        TableSchemaConfigLoader.TableConfig table = schemaConfig.getTableByName(tableName);
        List<String> primaryKeys = new ArrayList<>();

        if (table != null && table.getFields() != null) {
            List<String> fieldDefs = new ArrayList<>();
            for (TableSchemaConfigLoader.FieldConfig field : table.getFields()) {
                StringBuilder fieldSql = new StringBuilder();
                fieldSql.append("  ").append(field.getName());
                fieldSql.append(" ").append(field.getType());

                if (!field.isNullable()) {
                    fieldSql.append(" NOT NULL");
                }

                if (field.isPrimaryKey()) {
                    primaryKeys.add(field.getName());
                }

                fieldDefs.add(fieldSql.toString());
            }

            sql.append(String.join(",\n", fieldDefs));

            // 添加主键约束
            if (!primaryKeys.isEmpty()) {
                sql.append(",\n  PRIMARY KEY (").append(String.join(", ", primaryKeys)).append(")");
            }
        }

        sql.append("\n)");
        return sql.toString();
    }

    /**
     * 写入正常数据（只包含业务字段）
     */
    public void logValidData(ProcessingResult result) {
        if (result == null || !result.isSuccess()) {
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

        log.info("准备批量写入 {} 条记录到 OceanBase (valid_data)", toFlush.size());

        // 只包含业务字段的 SQL
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tableName).append(" (");

        // 只添加业务字段
        for (int i = 0; i < fieldNames.size(); i++) {
            sqlBuilder.append(fieldNames.get(i));
            if (i < fieldNames.size() - 1) {
                sqlBuilder.append(", ");
            }
        }

        sqlBuilder.append(") VALUES (");
        for (int i = 0; i < fieldNames.size(); i++) {
            sqlBuilder.append("?");
            if (i < fieldNames.size() - 1) {
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
            log.info("批量写入 OceanBase 成功：{} 条记录 (valid_data)", toFlush.size());

        } catch (SQLException e) {
            log.error("批量写入 OceanBase 失败 (valid_data)：{}", e.getMessage(), e);
            synchronized (buffer) {
                buffer.addAll(0, toFlush);
            }
        }
    }

    /**
     * 添加单条记录到批处理（只包含业务字段）
     */
    private void addBatch(PreparedStatement pstmt, ProcessingResult result) throws SQLException {
        // 只设置业务字段值
        Map<String, Object> fieldValues = extractFieldValues(result);
        for (String field : fieldNames) {
            pstmt.setObject(fieldNames.indexOf(field) + 1, fieldValues.get(field));
        }
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
            log.info("OceanBase 连接池已关闭 (ValidData)");
        }
    }
}
