package com.kafka.consumer.config;

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
public class AppConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(AppConfigLoader.class);

    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/app-config.yaml";

    private AppConfig app;
    private SourceConfig source;
    private LoggingConfig logging;

    /**
     * 应用配置
     */
    public static class AppConfig {
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
     * 源表配置
     */
    public static class SourceConfig {
        private String tableName;
        private String topic;
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
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                log.warn("配置文件不存在：{}, 尝试从 classpath 加载", configPath);
                if (getClass().getClassLoader().getResourceAsStream("app-config.yaml") != null) {
                    AppConfigLoader loaded = mapper.readValue(
                        getClass().getClassLoader().getResourceAsStream("app-config.yaml"),
                        AppConfigLoader.class
                    );
                    this.app = loaded.app;
                    this.source = loaded.source;
                    this.logging = loaded.logging;
                } else {
                    throw new RuntimeException("无法找到配置文件 app-config.yaml");
                }
            } else {
                AppConfigLoader loaded = mapper.readValue(configFile, AppConfigLoader.class);
                this.app = loaded.app;
                this.source = loaded.source;
                this.logging = loaded.logging;
            }

            log.info("应用配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载应用配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载应用配置失败", e);
        }
    }

    /**
     * 获取应用配置
     */
    public AppConfig getApp() {
        return app;
    }

    /**
     * 获取源表配置
     */
    public SourceConfig getSource() {
        return source;
    }

    /**
     * 获取日志配置
     */
    public LoggingConfig getLogging() {
        return logging;
    }

    /**
     * 构建 Kafka Streams Properties
     */
    public Properties getStreamsProperties(Properties kafkaProps) {
        Properties props = new Properties();
        props.putAll(kafkaProps);

        props.put("application.id", app.getName());
        props.put("state.dir", app.getStateDir());
        props.put("auto.offset.reset", app.getAutoOffsetReset());

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
     * 获取异议数据表名
     */
    public String getInvalidDataTableName() {
        return source.getInvalidTablePrefix() + source.getTableName();
    }

    /**
     * 打印配置信息
     */
    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("应用配置信息");
        System.out.println("=====================================");
        System.out.println("应用名称：" + app.getName());
        System.out.println("状态目录：" + app.getStateDir());
        System.out.println("Offset 重置：" + app.getAutoOffsetReset());
        System.out.println("-------------------------------------");
        System.out.println("源表配置:");
        System.out.println("  表名：" + source.getTableName());
        System.out.println("  源 Topic: " + source.getTopic());
        System.out.println("  自动建表：" + (source.isAutoCreateTable() ? "是" : "否"));
        System.out.println("  写入 Topic: " + (source.isWriteToTopic() ? "是" : "否"));
        if (source.isWriteToTopic()) {
            System.out.println("    正常数据 Topic: " + source.getValidDataTopic());
            System.out.println("    异议数据 Topic: " + source.getInvalidDataTopic());
        }
        System.out.println("  写入 OceanBase: " + (source.isWriteToOceanbase() ? "是" : "否"));
        if (source.isWriteToOceanbase()) {
            System.out.println("    异议数据表：" + getInvalidDataTableName());
        }
        System.out.println("-------------------------------------");
        System.out.println("日志配置:");
        System.out.println("  日志目录：" + logging.getDir());
        System.out.println("  详细日志：" + (logging.isVerbose() ? "是" : "否"));
        System.out.println("=====================================");
    }

    /**
     * 获取应用名称
     */
    public String getAppName() {
        return app.getName();
    }

    /**
     * 获取源表名
     */
    public String getSourceTableName() {
        return source.getTableName();
    }

    /**
     * 获取源 Topic
     */
    public String getSourceTopic() {
        return source.getTopic();
    }

    /**
     * 是否写入 Topic
     */
    public boolean isWriteToTopic() {
        return source.isWriteToTopic();
    }

    /**
     * 是否自动建表
     */
    public boolean isAutoCreateTable() {
        return source.isAutoCreateTable();
    }

    /**
     * 是否写入 OceanBase
     */
    public boolean isWriteToOceanbase() {
        return source.isWriteToOceanbase();
    }
}
