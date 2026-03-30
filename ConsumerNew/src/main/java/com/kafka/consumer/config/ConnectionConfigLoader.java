package com.kafka.consumer.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * 连接配置加载器
 *
 * 从 YAML 配置文件加载 Kafka 和 OceanBase 连接配置
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectionConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConnectionConfigLoader.class);

    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/connection-config.yaml";

    private KafkaConfig kafka;
    private OceanBaseConfig oceanbase;
    private DatabaseConfig database;

    /**
     * Kafka 配置
     */
    public static class KafkaConfig {
        private String bootstrapServers;
        private SecurityConfig security;

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public SecurityConfig getSecurity() { return security; }
        public void setSecurity(SecurityConfig security) { this.security = security; }
    }

    /**
     * Kafka 安全配置
     */
    public static class SecurityConfig {
        private boolean enabled;
        private String protocol;
        private String mechanism;
        private String username;
        private String password;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getProtocol() { return protocol; }
        public void setProtocol(String protocol) { this.protocol = protocol; }
        public String getMechanism() { return mechanism; }
        public void setMechanism(String mechanism) { this.mechanism = mechanism; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }

    /**
     * OceanBase 配置
     */
    public static class OceanBaseConfig {
        private String host;
        private int port;
        private String username;
        private String password;
        private String database;
        private PoolConfig pool;

        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
        public PoolConfig getPool() { return pool; }
        public void setPool(PoolConfig pool) { this.pool = pool; }
    }

    /**
     * 连接池配置
     */
    public static class PoolConfig {
        private int maxPoolSize;
        private int minIdle;
        private int connectionTimeoutMs;
        private int idleTimeoutMs;
        private int maxLifetimeMs;

        public int getMaxPoolSize() { return maxPoolSize; }
        public void setMaxPoolSize(int maxPoolSize) { this.maxPoolSize = maxPoolSize; }
        public int getMinIdle() { return minIdle; }
        public void setMinIdle(int minIdle) { this.minIdle = minIdle; }
        public int getConnectionTimeoutMs() { return connectionTimeoutMs; }
        public void setConnectionTimeoutMs(int connectionTimeoutMs) { this.connectionTimeoutMs = connectionTimeoutMs; }
        public int getIdleTimeoutMs() { return idleTimeoutMs; }
        public void setIdleTimeoutMs(int idleTimeoutMs) { this.idleTimeoutMs = idleTimeoutMs; }
        public int getMaxLifetimeMs() { return maxLifetimeMs; }
        public void setMaxLifetimeMs(int maxLifetimeMs) { this.maxLifetimeMs = maxLifetimeMs; }
    }

    /**
     * 数据库表配置（可选）
     */
    public static class DatabaseConfig {
        private String validTablePrefix;
        private String invalidTablePrefix;
        private Boolean autoCreateTables;

        public String getValidTablePrefix() { return validTablePrefix; }
        public void setValidTablePrefix(String validTablePrefix) { this.validTablePrefix = validTablePrefix; }
        public String getInvalidTablePrefix() { return invalidTablePrefix; }
        public void setInvalidTablePrefix(String invalidTablePrefix) { this.invalidTablePrefix = invalidTablePrefix; }
        public Boolean getAutoCreateTables() { return autoCreateTables; }
        public void setAutoCreateTables(Boolean autoCreateTables) { this.autoCreateTables = autoCreateTables; }
    }

    /**
     * 从指定路径加载配置
     *
     * @param configPath 配置文件路径
     */
    public ConnectionConfigLoader(String configPath) {
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
            this.kafka = internal.kafka;
            this.oceanbase = internal.oceanbase;
            this.database = internal.database;

            log.info("连接配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载连接配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载连接配置失败", e);
        }
    }

    // 内部类，用于 Jackson 反序列化
    private static class InternalConfig {
        private KafkaConfig kafka;
        private OceanBaseConfig oceanbase;
        private DatabaseConfig database;

        public KafkaConfig getKafka() { return kafka; }
        public void setKafka(KafkaConfig kafka) { this.kafka = kafka; }
        public OceanBaseConfig getOceanbase() { return oceanbase; }
        public void setOceanbase(OceanBaseConfig oceanbase) { this.oceanbase = oceanbase; }
        public DatabaseConfig getDatabase() { return database; }
        public void setDatabase(DatabaseConfig database) { this.database = database; }
    }

    /**
     * 获取 Kafka 配置
     */
    public KafkaConfig getKafkaConfig() {
        return kafka;
    }

    /**
     * 获取 OceanBase 配置
     */
    public OceanBaseConfig getOceanBaseConfig() {
        return oceanbase;
    }

    /**
     * 获取 Database 配置
     */
    public DatabaseConfig getDatabaseConfig() {
        return database;
    }

    /**
     * 构建 Kafka Properties
     */
    public Properties getKafkaProperties() {
        Properties props = new Properties();
        if (kafka != null) {
            props.put("bootstrap.servers", kafka.getBootstrapServers());

            if (kafka.getSecurity() != null && kafka.getSecurity().isEnabled()) {
                SecurityConfig sec = kafka.getSecurity();
                props.put("security.protocol", sec.getProtocol());
                props.put("sasl.mechanism", sec.getMechanism());

                String jaasConfig = String.format(
                    "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                    sec.getUsername(), sec.getPassword()
                );
                props.put("sasl.jaas.config", jaasConfig);
            }
        }

        return props;
    }

    /**
     * 构建 OceanBase JDBC URL
     */
    public String getOceanBaseJdbcUrl() {
        if (oceanbase == null) return null;
        OceanBaseConfig ob = oceanbase;
        return String.format(
            "jdbc:mysql://%s:%d/%s?useSSL=false&allowPublicKeyRetrieval=true",
            ob.getHost(), ob.getPort(), ob.getDatabase()
        );
    }

    /**
     * 打印配置信息
     */
    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("连接配置信息");
        System.out.println("=====================================");
        if (kafka != null) {
            System.out.println("Kafka Bootstrap Servers: " + kafka.getBootstrapServers());
            if (kafka.getSecurity() != null && kafka.getSecurity().isEnabled()) {
                System.out.println("Kafka SASL 认证：已启用");
                System.out.println("  安全协议：" + kafka.getSecurity().getProtocol());
                System.out.println("  认证机制：" + kafka.getSecurity().getMechanism());
                System.out.println("  用户名：" + kafka.getSecurity().getUsername());
            } else {
                System.out.println("Kafka SASL 认证：未启用");
            }
        }
        System.out.println("-------------------------------------");
        if (oceanbase != null) {
            System.out.println("OceanBase 连接:");
            System.out.println("  Host: " + oceanbase.getHost() + ":" + oceanbase.getPort());
            System.out.println("  用户名：" + oceanbase.getUsername());
            System.out.println("  数据库：" + oceanbase.getDatabase());
            if (oceanbase.getPool() != null) {
                System.out.println("  连接池配置:");
                System.out.println("    最大连接数：" + oceanbase.getPool().getMaxPoolSize());
                System.out.println("    最小空闲：" + oceanbase.getPool().getMinIdle());
            }
        }
        if (database != null) {
            System.out.println("-------------------------------------");
            System.out.println("Database 表配置:");
            System.out.println("  正常数据表前缀：" + (database.getValidTablePrefix() != null ? database.getValidTablePrefix() : "(空)"));
            System.out.println("  异议数据表前缀：" + (database.getInvalidTablePrefix() != null ? database.getInvalidTablePrefix() : "(空)"));
            System.out.println("  自动建表：" + (database.getAutoCreateTables() != null ? database.getAutoCreateTables() : "false"));
        }
        System.out.println("=====================================");
    }
}
