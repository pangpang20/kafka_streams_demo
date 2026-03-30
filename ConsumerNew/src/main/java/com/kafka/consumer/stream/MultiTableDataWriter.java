package com.kafka.consumer.stream;

import com.kafka.consumer.config.*;
import com.kafka.consumer.model.ProcessingResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 多表 OceanBase 写入器
 *
 * 支持将多个表的数据写入对应的目标表
 * 正常数据写入：表名
 * 异议数据写入：前缀 + 表名（如 e_baseinfo）
 *
 * @author Kafka Demo
 * @version 2.0.0
 */
public class MultiTableDataWriter {

    private static final Logger log = LoggerFactory.getLogger(MultiTableDataWriter.class);

    private final HikariDataSource dataSource;
    private final String databaseName;
    private final String validDatabaseName;
    private final String invalidDatabaseName;
    private final String invalidTablePrefix;
    private final boolean writeToOceanbase;
    private final TableSchemaConfigLoader schemaConfig;

    // 表名 -> 字段列表缓存
    private final Map<String, List<String>> tableFieldCache = new ConcurrentHashMap<>();

    // 表名 -> 数据缓冲区
    private final Map<String, List<ProcessingResult>> validBuffers = new ConcurrentHashMap<>();
    private final Map<String, List<ProcessingResult>> invalidBuffers = new ConcurrentHashMap<>();

    private static final int BATCH_SIZE = 10;

    /**
     * 构造函数
     *
     * @param connectionConfig 连接配置
     * @param schemaConfig 表结构配置
     * @param appConfig 应用配置
     */
    public MultiTableDataWriter(ConnectionConfigLoader connectionConfig,
                                 TableSchemaConfigLoader schemaConfig,
                                 AppConfigLoader appConfig) {
        // 从配置中获取数据库名，支持双数据库配置
        ConnectionConfigLoader.DatabaseConfig dbConfig = connectionConfig.getDatabaseConfig();
        String baseDbName = connectionConfig.getOceanBaseConfig().getDatabase();

        if (dbConfig != null && dbConfig.getValidDatabase() != null) {
            this.validDatabaseName = dbConfig.getValidDatabase();
        } else {
            this.validDatabaseName = baseDbName;
        }

        if (dbConfig != null && dbConfig.getInvalidDatabase() != null) {
            this.invalidDatabaseName = dbConfig.getInvalidDatabase();
        } else {
            this.invalidDatabaseName = baseDbName;
        }

        // 默认使用 validDatabaseName 作为 databaseName（兼容旧代码）
        this.databaseName = this.validDatabaseName;

        this.invalidTablePrefix = appConfig.getSource() != null ? appConfig.getSource().getInvalidTablePrefix() : "e_";
        this.writeToOceanbase = appConfig.isWriteToOceanbase();
        this.schemaConfig = schemaConfig;

        // 初始化数据源
        this.dataSource = createDataSource(connectionConfig.getOceanBaseConfig());

        // 预加载表结构缓存
        if (schemaConfig.getTables() != null) {
            for (TableSchemaConfigLoader.TableConfig table : schemaConfig.getTables()) {
                List<String> fieldNames = new ArrayList<>();
                if (table.getFields() != null) {
                    for (TableSchemaConfigLoader.FieldConfig field : table.getFields()) {
                        fieldNames.add(field.getName());
                    }
                }
                tableFieldCache.put(table.getName(), fieldNames);

                // 初始化缓冲区
                validBuffers.put(table.getName(), Collections.synchronizedList(new ArrayList<>()));
                invalidBuffers.put(table.getName(), Collections.synchronizedList(new ArrayList<>()));

                // 如果配置了自动建表，创建目标表
                if (appConfig.isAutoCreateTable()) {
                    createTablesIfNotExists(connectionConfig, table.getName());
                }
            }
        }

        log.info("MultiTableDataWriter 初始化完成，数据库：{}, 已知表：{}",
                databaseName, tableFieldCache.size());
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
     * 创建表（正常数据表和异议数据表）
     */
    private void createTablesIfNotExists(ConnectionConfigLoader connectionConfig, String tableName) {
        // 创建正常数据表（使用 validDatabaseName）
        String validSql = schemaConfig.generateCreateTableSQL(
            tableName, validDatabaseName
        );
        executeSql(validSql);
        log.info("创建正常数据表：{}.{}", validDatabaseName, tableName);

        // 创建异议数据表（使用 invalidDatabaseName）
        String invalidSql = schemaConfig.generateInvalidTableSQL(
            tableName, invalidDatabaseName, invalidTablePrefix
        );
        executeSql(invalidSql);
        log.info("创建异议数据表：{}.{}{}", invalidDatabaseName, invalidTablePrefix, tableName);
    }

    /**
     * 执行 SQL
     */
    private void executeSql(String sql) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.execute();
            log.info("SQL 执行成功：{}", sql.substring(0, Math.min(100, sql.length())));
        } catch (SQLException e) {
            log.error("SQL 执行失败：{} - {}", sql.substring(0, Math.min(50, sql.length())), e.getMessage());
        }
    }

    /**
     * 注册新表（动态添加）
     */
    public void registerTable(String tableName, List<String> fieldNames) {
        tableFieldCache.put(tableName, fieldNames);
        validBuffers.put(tableName, Collections.synchronizedList(new ArrayList<>()));
        invalidBuffers.put(tableName, Collections.synchronizedList(new ArrayList<>()));
        log.info("注册新表：{}, 字段数：{}", tableName, fieldNames.size());
    }

    /**
     * 写入正常数据
     */
    public void logValidData(ProcessingResult result) {
        if (result == null || !result.isSuccess()) {
            return;
        }

        String tableName = result.getTableName();
        if (tableName == null) {
            log.warn("正常数据表名为空，跳过");
            return;
        }

        // 确保缓冲区存在
        validBuffers.computeIfAbsent(tableName, k -> Collections.synchronizedList(new ArrayList<>()));

        synchronized (validBuffers.get(tableName)) {
            validBuffers.get(tableName).add(result);
            if (validBuffers.get(tableName).size() >= BATCH_SIZE) {
                flushValidBuffer(tableName);
            }
        }
    }

    /**
     * 写入异议数据
     */
    public void logInvalidData(ProcessingResult result) {
        if (result == null || result.isSuccess()) {
            return;
        }

        String tableName = result.getTableName();
        if (tableName == null) {
            log.warn("异议数据表名为空，跳过");
            return;
        }

        // 确保缓冲区存在
        invalidBuffers.computeIfAbsent(tableName, k -> Collections.synchronizedList(new ArrayList<>()));

        synchronized (invalidBuffers.get(tableName)) {
            invalidBuffers.get(tableName).add(result);
            if (invalidBuffers.get(tableName).size() >= BATCH_SIZE) {
                flushInvalidBuffer(tableName);
            }
        }
    }

    /**
     * 刷新正常数据缓冲区
     */
    private void flushValidBuffer(String tableName) {
        List<ProcessingResult> toFlush;
        synchronized (validBuffers.get(tableName)) {
            if (validBuffers.get(tableName).isEmpty()) {
                return;
            }
            toFlush = new ArrayList<>(validBuffers.get(tableName));
            validBuffers.get(tableName).clear();
        }

        List<String> fields = tableFieldCache.get(tableName);
        if (fields == null) {
            log.error("表 [{}] 结构未定义，无法写入", tableName);
            return;
        }

        log.info("准备批量写入 {} 条记录到 OceanBase ({} 表)", toFlush.size(), tableName);

        String sql = buildInsertSql(tableName, fields, false);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (ProcessingResult result : toFlush) {
                addValidBatch(pstmt, result, fields);
            }

            pstmt.executeBatch();
            conn.commit();
            log.info("批量写入 OceanBase 成功：{} 条记录 ({})", toFlush.size(), tableName);

        } catch (SQLException e) {
            log.error("批量写入 OceanBase 失败 ({}): {}", tableName, e.getMessage(), e);
            synchronized (validBuffers.get(tableName)) {
                validBuffers.get(tableName).addAll(0, toFlush);
            }
        }
    }

    /**
     * 刷新异议数据缓冲区
     */
    private void flushInvalidBuffer(String tableName) {
        List<ProcessingResult> toFlush;
        synchronized (invalidBuffers.get(tableName)) {
            if (invalidBuffers.get(tableName).isEmpty()) {
                return;
            }
            toFlush = new ArrayList<>(invalidBuffers.get(tableName));
            invalidBuffers.get(tableName).clear();
        }

        List<String> fields = tableFieldCache.get(tableName);
        if (fields == null) {
            log.error("表 [{}] 结构未定义，无法写入", tableName);
            return;
        }

        String actualTableName = invalidTablePrefix + tableName;
        log.info("准备批量写入 {} 条记录到 OceanBase ({} 表)", toFlush.size(), actualTableName);

        String sql = buildInsertSql(actualTableName, fields, true);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            for (ProcessingResult result : toFlush) {
                addInvalidBatch(pstmt, result, fields);
            }

            pstmt.executeBatch();
            conn.commit();
            log.info("批量写入 OceanBase 成功：{} 条记录 ({})", toFlush.size(), actualTableName);

        } catch (SQLException e) {
            log.error("批量写入 OceanBase 失败 ({}): {}", actualTableName, e.getMessage(), e);
            synchronized (invalidBuffers.get(tableName)) {
                invalidBuffers.get(tableName).addAll(0, toFlush);
            }
        }
    }

    /**
     * 构建 INSERT SQL
     */
    private String buildInsertSql(String tableName, List<String> fields, boolean isInvalid) {
        StringBuilder sql = new StringBuilder();
        // 根据是否为异议数据选择数据库
        String dbName = isInvalid ? invalidDatabaseName : validDatabaseName;
        sql.append("INSERT INTO ").append(dbName).append(".").append(tableName).append(" (");

        if (isInvalid) {
            // 异议数据：业务字段 + 管理字段
            for (String field : fields) {
                sql.append(field).append(", ");
            }
            sql.append("record_id, dw_insert_time, dispute_settlement_time, rule_description, ");
            sql.append("dispute_ticket_no, inspection_job_id, source, source_dept_id, ");
            sql.append("log_timestamp) VALUES (");
            int paramCount = fields.size() + 9;
            for (int i = 0; i < paramCount; i++) {
                sql.append("?");
                if (i < paramCount - 1) {
                    sql.append(", ");
                }
            }
        } else {
            // 正常数据：只保存业务字段
            for (String field : fields) {
                sql.append(field).append(", ");
            }
            // 移除最后的逗号和空格，添加右括号
            sql.setLength(sql.length() - 2);
            sql.append(") VALUES (");
            int paramCount = fields.size();
            for (int i = 0; i < paramCount; i++) {
                sql.append("?");
                if (i < paramCount - 1) {
                    sql.append(", ");
                }
            }
        }
        sql.append(")");

        return sql.toString();
    }

    /**
     * 添加正常数据到批处理
     */
    private void addValidBatch(PreparedStatement pstmt, ProcessingResult result, List<String> fields) throws SQLException {
        // 只设置业务字段值
        Map<String, Object> fieldValues = extractFieldValues(result);
        int idx = 1;
        for (String field : fields) {
            pstmt.setObject(idx++, fieldValues.get(field));
        }

        pstmt.addBatch();
    }

    /**
     * 添加异议数据到批处理
     */
    private void addInvalidBatch(PreparedStatement pstmt, ProcessingResult result, List<String> fields) throws SQLException {
        int idx = 1;

        // 设置业务字段
        Map<String, Object> fieldValues = extractFieldValues(result);
        for (String field : fields) {
            pstmt.setObject(idx++, fieldValues.get(field));
        }

        // 异议数据管理字段
        pstmt.setNull(idx++, java.sql.Types.BIGINT);  // record_id (自增，设为 NULL)
        // 使用毫秒精度时间戳字符串，避免 OceanBase 不支持 setTimestamp 的问题
        String dwInsertTime = String.format("%tF %<tH:%<tM:%<tS.%1$tL", System.currentTimeMillis());
        pstmt.setString(idx++, dwInsertTime);  // dw_insert_time
        pstmt.setNull(idx++, java.sql.Types.TIMESTAMP);  // dispute_settlement_time (核销时填充)
        pstmt.setString(idx++, buildRuleDescription(result));  // rule_description
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);  // dispute_ticket_no (处理时生成)
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);  // inspection_job_id (可扩展)
        pstmt.setString(idx++, "数据治理");  // source
        pstmt.setNull(idx++, java.sql.Types.VARCHAR);  // source_dept_id (可扩展)
        String logTimestamp = String.format("%tF %<tH:%<tM:%<tS.%1$tL", System.currentTimeMillis());
        pstmt.setString(idx++, logTimestamp);  // log_timestamp

        pstmt.addBatch();
    }

    /**
     * 构建规则描述
     */
    private String buildRuleDescription(ProcessingResult result) {
        ProcessingResult.ValidationSummary summary = result.getValidationSummary();
        if (summary == null) return null;
        // 格式：规则类型 + 规则表达式
        String errorFields = summary.getErrorFields();
        if (errorFields != null && !errorFields.isEmpty()) {
            return "字段验证失败：[" + errorFields + "]";
        }
        return summary.getErrorSummary();
    }

    /**
     * 提取字段值
     */
    private Map<String, Object> extractFieldValues(ProcessingResult result) {
        Map<String, Object> values = new HashMap<>();
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
        // 刷新所有缓冲区
        for (String tableName : validBuffers.keySet()) {
            flushValidBuffer(tableName);
            flushInvalidBuffer(tableName);
        }

        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("OceanBase 连接池已关闭");
        }
    }

    /**
     * 获取已知表数量
     */
    public int getTableCount() {
        return tableFieldCache.size();
    }
}
