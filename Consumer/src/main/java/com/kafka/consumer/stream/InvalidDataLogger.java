package com.kafka.consumer.stream;

import com.kafka.consumer.model.QualityCheckResult;
import com.kafka.consumer.processor.ProcessingResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 异议数据日志记录器
 *
 * 将质量检查失败的异常数据输出到独立的日志文件，
 * 便于后续审计和问题追踪。
 *
 * 日志文件格式：JSON Lines (每行一条 JSON 记录)
 * 日志文件位置：logs/invalid-data-yyyy-MM-dd.log
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class InvalidDataLogger {

    private static final Logger log = LoggerFactory.getLogger(InvalidDataLogger.class);

    /**
     * 日志文件目录
     */
    private static final String LOG_DIR = "logs";

    /**
     * 日志文件名前缀
     */
    private static final String LOG_FILE_PREFIX = "invalid-data-";

    /**
     * 日志文件名后缀
     */
    private static final String LOG_FILE_SUFFIX = ".log";

    /**
     * 日期格式化器
     */
    private static final DateTimeFormatter DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * 时间戳格式化器（用于日志中的时间字段）
     */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * JSON 序列化器
     */
    private final ObjectMapper objectMapper;

    /**
     * 当前日志文件
     */
    private File currentLogFile;

    /**
     * 当前日志文件的写入器
     */
    private BufferedWriter writer;

    /**
     * 当前日期（用于判断是否需要切换日志文件）
     */
    private String currentDate;

    /**
     * 构造函数
     */
    public InvalidDataLogger() {
        this.objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

        // 创建日志目录
        File logDir = new File(LOG_DIR);
        if (!logDir.exists()) {
            if (logDir.mkdirs()) {
                log.info("创建日志目录：{}", logDir.getAbsolutePath());
            }
        }

        // 初始化日志文件
        initLogFile();
    }

    /**
     * 初始化日志文件
     */
    private void initLogFile() {
        currentDate = LocalDateTime.now().format(DATE_FORMATTER);
        String logFileName = LOG_DIR + File.separator + LOG_FILE_PREFIX + currentDate + LOG_FILE_SUFFIX;
        currentLogFile = new File(logFileName);

        try {
            if (writer != null) {
                writer.close();
            }
            writer = new BufferedWriter(new FileWriter(currentLogFile, true));
            log.info("初始化异议数据日志文件：{}", currentLogFile.getAbsolutePath());
        } catch (IOException e) {
            log.error("创建日志文件失败：{}", e.getMessage(), e);
        }
    }

    /**
     * 记录异议数据
     *
     * @param result 处理结果（包含质量检查失败信息）
     */
    public void logInvalidData(ProcessingResult result) {
        // 检查是否需要切换日志文件（跨天时）
        checkAndRotateLogFile();

        if (writer == null) {
            log.warn("日志写入器未初始化，跳过记录");
            return;
        }

        try {
            // 构建日志条目
            InvalidDataLogEntry entry = new InvalidDataLogEntry(result);
            String json = objectMapper.writeValueAsString(entry);

            // 写入日志文件（JSON Lines 格式，每行一条）
            synchronized (writer) {
                writer.write(json);
                writer.newLine();
                writer.flush();
            }

            log.debug("已记录异议数据到日志文件：key={}", result.getRecordKey());

        } catch (IOException e) {
            log.error("写入日志文件失败：{}", e.getMessage(), e);
        }
    }

    /**
     * 检查并切换日志文件（用于跨天场景）
     */
    private void checkAndRotateLogFile() {
        String newDate = LocalDateTime.now().format(DATE_FORMATTER);
        if (!newDate.equals(currentDate)) {
            log.info("检测到日期变化，切换日志文件");
            initLogFile();
        }
    }

    /**
     * 批量记录异议数据
     *
     * @param results 处理结果列表
     */
    public void logInvalidDataBatch(java.util.List<ProcessingResult> results) {
        for (ProcessingResult result : results) {
            logInvalidData(result);
        }
    }

    /**
     * 关闭日志记录器
     */
    public void close() {
        if (writer != null) {
            try {
                writer.close();
                log.info("异议数据日志记录器已关闭");
            } catch (IOException e) {
                log.error("关闭日志文件失败：{}", e.getMessage(), e);
            }
        }
    }

    /**
     * 获取日志文件路径
     *
     * @return 当前日志文件路径
     */
    public String getLogFilePath() {
        return currentLogFile != null ? currentLogFile.getAbsolutePath() : "未知";
    }

    /**
     * 异议数据日志条目
     *
     * 封装完整的日志信息，包括处理结果、时间戳等。
     */
    public static class InvalidDataLogEntry {
        /**
         * 记录键
         */
        public String recordKey;

        /**
         * 记录时间戳
         */
        public String logTimestamp;

        /**
         * CDC 记录数据
         */
        public Object record;

        /**
         * 质量检查结果
         */
        public QualityCheckResult qualityResult;

        /**
         * 失败原因摘要
         */
        public String failureSummary;

        /**
         * 错误字段列表
         */
        public java.util.List<String> errorFields;

        public InvalidDataLogEntry() {}

        public InvalidDataLogEntry(ProcessingResult result) {
            this.recordKey = result.getRecordKey();
            this.logTimestamp = LocalDateTime.now().format(TIMESTAMP_FORMATTER);

            if (result.getRecord() != null) {
                this.record = result.getRecord();
            }

            this.qualityResult = result.getQualityResult();
            this.failureSummary = result.getMessage();

            // 提取错误字段列表
            if (result.getQualityResult() != null) {
                this.errorFields = new java.util.ArrayList<>();
                for (QualityCheckResult.ValidationError error : result.getQualityResult().getErrors()) {
                    this.errorFields.add(error.getField());
                }
            }
        }
    }
}
