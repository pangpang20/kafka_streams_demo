package com.kafka.producer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Producer 应用配置加载器
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ProducerAppConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ProducerAppConfigLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/app-config.yaml";

    private AppConfig app;
    private SendConfig send;
    private OperationConfig operation;
    private DataQualityConfig dataQuality;
    private SourceConfig source;

    public static class AppConfig {
        private String name;
        private String logDir;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getLogDir() { return logDir; }
        public void setLogDir(String logDir) { this.logDir = logDir; }
    }

    public static class SendConfig {
        private int intervalMs;
        private int durationSeconds;
        private int logInterval;

        public int getIntervalMs() { return intervalMs; }
        public void setIntervalMs(int intervalMs) { this.intervalMs = intervalMs; }
        public int getDurationSeconds() { return durationSeconds; }
        public void setDurationSeconds(int durationSeconds) { this.durationSeconds = durationSeconds; }
        public int getLogInterval() { return logInterval; }
        public void setLogInterval(int logInterval) { this.logInterval = logInterval; }
    }

    public static class OperationConfig {
        private int insertProbability;
        private int updateProbability;
        private int deleteProbability;

        public int getInsertProbability() { return insertProbability; }
        public void setInsertProbability(int insertProbability) { this.insertProbability = insertProbability; }
        public int getUpdateProbability() { return updateProbability; }
        public void setUpdateProbability(int updateProbability) { this.updateProbability = updateProbability; }
        public int getDeleteProbability() { return deleteProbability; }
        public void setDeleteProbability(int deleteProbability) { this.deleteProbability = deleteProbability; }
    }

    public static class DataQualityConfig {
        private int badDataProbability;

        public int getBadDataProbability() { return badDataProbability; }
        public void setBadDataProbability(int badDataProbability) { this.badDataProbability = badDataProbability; }
    }

    public static class SourceConfig {
        private String tableName;
        private String topic;
        private String databaseName;
        private String databaseType;

        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public String getDatabaseName() { return databaseName; }
        public void setDatabaseName(String databaseName) { this.databaseName = databaseName; }
        public String getDatabaseType() { return databaseType; }
        public void setDatabaseType(String databaseType) { this.databaseType = databaseType; }
    }

    public ProducerAppConfigLoader() {
        this(DEFAULT_CONFIG_PATH);
    }

    public ProducerAppConfigLoader(String configPath) {
        loadConfig(configPath);
    }

    public void loadConfig(String configPath) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                throw new RuntimeException("配置文件不存在：" + configPath);
            }

            ProducerAppConfigLoader loaded = mapper.readValue(configFile, ProducerAppConfigLoader.class);
            this.app = loaded.app;
            this.send = loaded.send;
            this.operation = loaded.operation;
            this.dataQuality = loaded.dataQuality;
            this.source = loaded.source;

            log.info("应用配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载应用配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载应用配置失败", e);
        }
    }

    public AppConfig getApp() { return app; }
    public SendConfig getSend() { return send; }
    public OperationConfig getOperation() { return operation; }
    public DataQualityConfig getDataQuality() { return dataQuality; }
    public SourceConfig getSource() { return source; }

    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("Producer 应用配置");
        System.out.println("=====================================");
        System.out.println("应用名称：" + app.getName());
        System.out.println("日志目录：" + app.getLogDir());
        System.out.println("-------------------------------------");
        System.out.println("发送配置:");
        System.out.println("  发送间隔：" + send.getIntervalMs() + "ms");
        System.out.println("  运行时长：" + (send.getDurationSeconds() == 0 ? "无限" : send.getDurationSeconds() + "秒"));
        System.out.println("  统计间隔：" + send.getLogInterval() + "条");
        System.out.println("-------------------------------------");
        System.out.println("操作概率:");
        System.out.println("  插入：" + operation.getInsertProbability() + "%");
        System.out.println("  更新：" + operation.getUpdateProbability() + "%");
        System.out.println("  删除：" + operation.getDeleteProbability() + "%");
        System.out.println("-------------------------------------");
        System.out.println("数据质量:");
        System.out.println("  问题数据概率：" + dataQuality.getBadDataProbability() + "%");
        System.out.println("-------------------------------------");
        System.out.println("源表配置:");
        System.out.println("  表名：" + source.getTableName());
        System.out.println("  Topic: " + source.getTopic());
        System.out.println("  数据库：" + source.getDatabaseName() + " (" + source.getDatabaseType() + ")");
        System.out.println("=====================================");
    }
}
