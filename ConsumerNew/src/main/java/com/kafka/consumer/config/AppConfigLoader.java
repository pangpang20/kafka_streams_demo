package com.kafka.consumer.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Properties;

/**
 * 应用配置加载器
 *
 * 从 YAML 配置文件加载应用级配置
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AppConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(AppConfigLoader.class);

    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/app-config.yaml";

    private App app;
    private LoggingConfig logging;
    private List<TableConfig> tables;

    /**
     * 应用配置
     */
    public static class App {
        private String name;
        private String stateDir;
        private String autoOffsetReset;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getStateDir() { return stateDir; }
        public void setStateDir(String stateDir) { this.stateDir = stateDir; }
        public String getAutoOffsetReset() { return autoOffsetReset; }
        public void setAutoOffsetReset(String autoOffsetReset) { this.autoOffsetReset = autoOffsetReset; }
    }

    /**
     * 表配置
     */
    public static class TableConfig {
        private String tableName;
        private String topic;
        private boolean enabled;
        private boolean skipValidation;
        private boolean autoCreateTable;
        private boolean writeToTopic;
        private String validDataTopic;
        private String invalidDataTopic;
        private boolean writeToOceanbase;
        private String invalidTablePrefix;

        public String getTableName() { return tableName; }
        public void setTableName(String tableName) { this.tableName = tableName; }
        public String getTopic() { return topic; }
        public void setTopic(String topic) { this.topic = topic; }
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public boolean isSkipValidation() { return skipValidation; }
        public void setSkipValidation(boolean skipValidation) { this.skipValidation = skipValidation; }
        public boolean isAutoCreateTable() { return autoCreateTable; }
        public void setAutoCreateTable(boolean autoCreateTable) { this.autoCreateTable = autoCreateTable; }
        public boolean isWriteToTopic() { return writeToTopic; }
        public void setWriteToTopic(boolean writeToTopic) { this.writeToTopic = writeToTopic; }
        public String getValidDataTopic() { return validDataTopic; }
        public void setValidDataTopic(String validDataTopic) { this.validDataTopic = validDataTopic; }
        public String getInvalidDataTopic() { return invalidDataTopic; }
        public void setInvalidDataTopic(String invalidDataTopic) { this.invalidDataTopic = invalidDataTopic; }
        public boolean isWriteToOceanbase() { return writeToOceanbase; }
        public void setWriteToOceanbase(boolean writeToOceanbase) { this.writeToOceanbase = writeToOceanbase; }
        public String getInvalidTablePrefix() { return invalidTablePrefix; }
        public void setInvalidTablePrefix(String invalidTablePrefix) { this.invalidTablePrefix = invalidTablePrefix; }
    }

    /**
     * 日志配置
     */
    public static class LoggingConfig {
        private String dir;
        private boolean verbose;

        public String getDir() { return dir; }
        public void setDir(String dir) { this.dir = dir; }
        public boolean isVerbose() { return verbose; }
        public void setVerbose(boolean verbose) { this.verbose = verbose; }
    }

    /**
     * 默认构造函数，从默认路径加载配置
     */
    public AppConfigLoader() {
        this(DEFAULT_CONFIG_PATH);
    }

    /**
     * 从指定路径加载配置
     *
     * @param configPath 配置文件路径
     */
    public AppConfigLoader(String configPath) {
        loadConfig(configPath);
    }

    /**
     * 加载配置文件
     *
     * @param configPath 配置文件路径
     */
    public void loadConfig(String configPath) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.setPropertyNamingStrategy(com.fasterxml.jackson.databind.PropertyNamingStrategies.SNAKE_CASE);
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                throw new RuntimeException("配置文件不存在：" + configPath);
            }

            // 使用内部类来避免递归调用构造函数
            InternalConfig internal = mapper.readValue(configFile, InternalConfig.class);
            this.app = internal.app;
            this.logging = internal.logging;
            this.tables = internal.tables;

            log.info("应用配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载应用配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载应用配置失败", e);
        }
    }

    // 内部类，用于 Jackson 反序列化
    private static class InternalConfig {
        private App app;
        private LoggingConfig logging;
        private List<TableConfig> tables;

        public App getApp() { return app; }
        public void setApp(App app) { this.app = app; }
        public LoggingConfig getLogging() { return logging; }
        public void setLogging(LoggingConfig logging) { this.logging = logging; }
        public List<TableConfig> getTables() { return tables; }
        public void setTables(List<TableConfig> tables) { this.tables = tables; }
    }

    /**
     * 获取应用配置
     */
    public App getApp() {
        return app;
    }

    /**
     * 获取日志配置
     */
    public LoggingConfig getLogging() {
        return logging;
    }

    /**
     * 获取表配置列表
     */
    public List<TableConfig> getTables() {
        return tables;
    }

    /**
     * 获取源表配置（兼容旧代码，返回第一个表）
     */
    public TableConfig getSource() {
        return (tables != null && !tables.isEmpty()) ? tables.get(0) : null;
    }

    /**
     * 构建 Kafka Streams Properties
     */
    public Properties getStreamsProperties(Properties kafkaProps) {
        Properties props = new Properties();
        props.putAll(kafkaProps);

        if (app != null) {
            props.put("application.id", app.getName());
            props.put("state.dir", app.getStateDir());
            props.put("auto.offset.reset", app.getAutoOffsetReset());
        }

        // 序列化配置
        props.put("default.key.serde",
                "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde",
                "org.apache.kafka.common.serialization.Serdes$StringSerde");

        // 处理保证
        props.put("processing.guarantee", "at_least_once");
        props.put("commit.interval.ms", "1000");

        return props;
    }

    /**
     * 获取应用名称
     */
    public String getAppName() {
        return app != null ? app.getName() : "unknown";
    }

    /**
     * 获取源表名（第一个表）
     */
    public String getSourceTableName() {
        return getSource() != null ? getSource().getTableName() : null;
    }

    /**
     * 获取源 Topic
     */
    public String getSourceTopic() {
        return getSource() != null ? getSource().getTopic() : null;
    }

    /**
     * 是否写入 Topic
     */
    public boolean isWriteToTopic() {
        TableConfig source = getSource();
        return source != null && source.isWriteToTopic();
    }

    /**
     * 是否自动建表
     */
    public boolean isAutoCreateTable() {
        TableConfig source = getSource();
        return source != null && source.isAutoCreateTable();
    }

    /**
     * 是否写入 OceanBase
     */
    public boolean isWriteToOceanbase() {
        TableConfig source = getSource();
        return source != null && source.isWriteToOceanbase();
    }

    /**
     * 获取异议数据表名
     */
    public String getInvalidDataTableName() {
        TableConfig source = getSource();
        if (source == null) return null;
        return source.getInvalidTablePrefix() + source.getTableName();
    }

    /**
     * 打印配置信息
     */
    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("应用配置信息");
        System.out.println("=====================================");
        if (app != null) {
            System.out.println("应用名称：" + app.getName());
            System.out.println("状态目录：" + app.getStateDir());
            System.out.println("Offset 重置：" + app.getAutoOffsetReset());
        }
        System.out.println("-------------------------------------");
        System.out.println("表配置:");
        if (tables != null) {
            for (TableConfig table : tables) {
                System.out.println("  表名：" + table.getTableName());
                System.out.println("  Topic: " + table.getTopic());
                System.out.println("  启用：" + (table.isEnabled() ? "是" : "否"));
                System.out.println("  跳过质检：" + (table.isSkipValidation() ? "是" : "否"));
            }
        }
        System.out.println("-------------------------------------");
        if (logging != null) {
            System.out.println("日志配置:");
            System.out.println("  日志目录：" + logging.getDir());
            System.out.println("  详细日志：" + (logging.isVerbose() ? "是" : "否"));
        }
        System.out.println("=====================================");
    }
}
