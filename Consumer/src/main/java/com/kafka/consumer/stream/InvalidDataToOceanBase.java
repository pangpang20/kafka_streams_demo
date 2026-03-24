package com.kafka.consumer.stream;

import com.kafka.consumer.config.ConsumerConfig;
import com.kafka.consumer.model.QualityCheckResult;
import com.kafka.consumer.processor.ProcessingResult;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * OceanBase 异议数据写入器
 *
 * 将质量检查失败的数据写入 OceanBase 数据库，便于后续分析和追溯。
 * 使用 HikariCP 连接池提高性能。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class InvalidDataToOceanBase {

    private static final Logger log = LoggerFactory.getLogger(InvalidDataToOceanBase.class);

    /**
     * 数据库连接池
     */
    private HikariDataSource dataSource;

    /**
     * 批量写入缓冲区
     */
    private final List<ProcessingResult> buffer = new ArrayList<>();

    /**
     * 批量写入阈值
     */
    private static final int BATCH_SIZE = 10;

    /**
     * 构造函数
     */
    public InvalidDataToOceanBase() {
        initializeDataSource();
    }

    /**
     * 初始化数据源
     */
    private void initializeDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(ConsumerConfig.OCEANBASE_JDBC_URL);
        config.setUsername(ConsumerConfig.OCEANBASE_USERNAME);
        config.setPassword(ConsumerConfig.OCEANBASE_PASSWORD);

        // 显式指定驱动类
        config.setDriverClassName("com.oceanbase.jdbc.Driver");

        // 连接池配置
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(2);
        config.setConnectionTimeout(30000);
        config.setIdleTimeout(600000);
        config.setMaxLifetime(1800000);

        // OceanBase 特定配置
        config.addDataSourceProperty("useSSL", "false");
        config.addDataSourceProperty("allowPublicKeyRetrieval", "true");
        config.addDataSourceProperty("useUnicode", "true");
        config.addDataSourceProperty("characterEncoding", "utf8");
        config.addDataSourceProperty("connectTimeout", "10000");
        config.addDataSourceProperty("socketTimeout", "30000");

        try {
            dataSource = new HikariDataSource(config);
            log.info("OceanBase 连接池初始化成功");

            // 测试连接
            try (Connection conn = dataSource.getConnection()) {
                log.info("OceanBase 连接测试成功：{}", conn.getCatalog());
            }
        } catch (Exception e) {
            log.error("OceanBase 连接池初始化失败：{}", e.getMessage(), e);
            throw new RuntimeException("OceanBase 初始化失败", e);
        }
    }

    /**
     * 写入异议数据
     *
     * @param result 质量检查结果
     */
    public void logInvalidData(ProcessingResult result) {
        log.info("logInvalidData called: key={}, success={}",
                 result.getRecordKey(), result.isSuccess());

        if (result == null || result.isSuccess()) {
            return;
        }

        synchronized (buffer) {
            buffer.add(result);
            log.info("Buffer size: {}", buffer.size());

            // 达到批量写入阈值时提交
            if (buffer.size() >= BATCH_SIZE) {
                log.info("触发批量写入，buffer size: {}", buffer.size());
                flushBuffer();
            }
        }
    }

    /**
     * 刷新缓冲区（批量写入）
     */
    public void flushBuffer() {
        if (buffer.isEmpty()) {
            log.debug("Buffer is empty, skipping flush");
            return;
        }

        List<ProcessingResult> toFlush;
        synchronized (buffer) {
            toFlush = new ArrayList<>(buffer);
            buffer.clear();
        }

        log.info("准备批量写入 {} 条记录到 OceanBase", toFlush.size());

        String sql = "INSERT INTO kafka_quality_check.invalid_data " +
                "(record_key, record_timestamp, database_name, table_name, opcode, data_type, " +
                "personid, idcard, name, sex, age, telephone, bloodtype, creditscore, housingareas, " +
                "failure_summary, error_fields, error_fields_json, error_details, log_timestamp, raw_data) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            log.info("获取数据库连接，autoCommit: {}", conn.getAutoCommit());

            for (ProcessingResult result : toFlush) {
                addBatch(pstmt, result);
            }

            pstmt.executeBatch();
            conn.commit();
            log.info("批量写入 OceanBase 成功：{} 条记录，已 commit", toFlush.size());
            log.info("===== 统计：invalid = {} 条 ===== (写入 OceanBase)", toFlush.size());

        } catch (SQLException e) {
            log.error("批量写入 OceanBase 失败：{}", e.getMessage(), e);
            // 写入失败时，将数据添加到缓冲区尾部，避免丢失
            synchronized (buffer) {
                buffer.addAll(0, toFlush);
            }
        }
    }

    /**
     * 添加单条记录到批处理
     */
    private void addBatch(PreparedStatement pstmt, ProcessingResult result) throws SQLException {
        QualityCheckResult qualityResult = result.getQualityResult();
        String recordKey = result.getRecordKey();

        // 设置主键
        pstmt.setString(1, recordKey);
        pstmt.setString(2, LocalDateTime.now().toString());

        // 设置元数据
        if (result.getRecord() != null) {
            pstmt.setString(3, result.getRecord().getDatabasename());
            pstmt.setString(4, result.getRecord().getTablename());
            pstmt.setString(5, result.getRecord().getOpcode());
            pstmt.setString(6, result.getRecord().getType());
        } else {
            pstmt.setString(3, null);
            pstmt.setString(4, null);
            pstmt.setString(5, null);
            pstmt.setString(6, null);
        }

        // 设置业务字段
        Object personid = getFieldValue(result, "personid");
        Object idcard = getFieldValue(result, "idcard");
        Object name = getFieldValue(result, "name");
        Object sex = getFieldValue(result, "sex");
        Object age = getFieldValue(result, "age");
        Object telephone = getFieldValue(result, "telephone");
        Object bloodtype = getFieldValue(result, "bloodtype");
        Object creditscore = getFieldValue(result, "creditscore");
        Object housingareas = getFieldValue(result, "housingareas");

        pstmt.setObject(7, personid != null ? Long.valueOf(personid.toString()) : null);
        pstmt.setString(8, idcard != null ? idcard.toString() : null);
        pstmt.setString(9, name != null ? name.toString() : null);
        pstmt.setString(10, sex != null ? sex.toString() : null);
        pstmt.setObject(11, age != null ? Integer.valueOf(age.toString()) : null);
        pstmt.setString(12, telephone != null ? telephone.toString() : null);
        pstmt.setString(13, bloodtype != null ? bloodtype.toString() : null);
        pstmt.setObject(14, creditscore != null ? Double.valueOf(creditscore.toString()) : null);
        pstmt.setObject(15, housingareas != null ? Double.valueOf(housingareas.toString()) : null);

        // 设置质量检查结果
        pstmt.setString(16, qualityResult != null ? qualityResult.getFailureSummary() : null);
        pstmt.setString(17, qualityResult != null ? String.join(",", qualityResult.getErrorFields()) : null);
        pstmt.setString(18, qualityResult != null ? qualityResult.getErrorFieldsJson() : null);
        pstmt.setString(19, qualityResult != null ? qualityResult.getErrorDetailsJson() : null);
        pstmt.setTimestamp(20, Timestamp.valueOf(LocalDateTime.now()));

        // 设置原始数据
        pstmt.setString(21, result.getRawJson());
    }

    /**
     * 获取字段值
     */
    private Object getFieldValue(ProcessingResult result, String fieldName) {
        if (result.getRecord() == null || result.getRecord().getAllfields() == null
                || result.getRecord().getAllfields().getAfterData() == null) {
            return null;
        }

        var fieldData = result.getRecord().getAllfields().getAfterData().get(fieldName);
        if (fieldData == null || fieldData.getValue() == null) {
            return null;
        }

        return fieldData.getValue();
    }

    /**
     * 关闭资源
     */
    public void close() {
        // 刷新缓冲区
        flushBuffer();

        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            log.info("OceanBase 连接池已关闭");
        }
    }
}
