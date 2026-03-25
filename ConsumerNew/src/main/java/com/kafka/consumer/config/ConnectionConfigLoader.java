package com.kafka.consumer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * 连接配置加载器
 *
 * 从 YAML 配置文件加载 Kafka 和 OceanBase 连接配置
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConnectionConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConnectionConfigLoader.class);

    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/connection-config.yaml";

    private KafkaConfig kafkaConfig;
    private OceanBaseConfig oceanBaseConfig;

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
     * 默认构造函数，从默认路径加载配置
     */
    public ConnectionConfigLoader() {
        this(DEFAULT_CONFIG_PATH);
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
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                log.warn("配置文件不存在：{}, 尝试从 classpath 加载", configPath);
                InputStream is = getClass().getClassLoader().getResourceAsStream("connection-config.yaml");
                if (is != null) {
                    ConnectionConfigLoader loaded = mapper.readValue(is, ConnectionConfigLoader.class);
                    this.kafkaConfig = loaded.kafkaConfig;
                    this.oceanBaseConfig = loaded.oceanBaseConfig;
                } else {
                    throw new RuntimeException("无法找到配置文件 connection-config.yaml");
                }
            } else {
                ConnectionConfigLoader loaded = mapper.readValue(configFile, ConnectionConfigLoader.class);
                this.kafkaConfig = loaded.kafkaConfig;
                this.oceanBaseConfig = loaded.oceanBaseConfig;
            }

            log.info("连接配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载连接配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载连接配置失败", e);
        }
    }

    /**
     * 获取 Kafka 配置
     */
    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    /**
     * 获取 OceanBase 配置
     */
    public OceanBaseConfig getOceanBaseConfig() {
        return oceanBaseConfig;
    }

    /**
     * 构建 Kafka Properties
     */
    public Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaConfig.getBootstrapServers());

        if (kafkaConfig.getSecurity() != null && kafkaConfig.getSecurity().isEnabled()) {
            SecurityConfig sec = kafkaConfig.getSecurity();
            props.put("security.protocol", sec.getProtocol());
            props.put("sasl.mechanism", sec.getMechanism());

            String jaasConfig = String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                sec.getUsername(), sec.getPassword()
            );
            props.put("sasl.jaas.config", jaasConfig);
        }

        return props;
    }

    /**
     * 构建 OceanBase JDBC URL
     */
    public String getOceanBaseJdbcUrl() {
        OceanBaseConfig ob = oceanBaseConfig;
        return String.format(
            "jdbc:oceanbase://%s:%d/%s?useSSL=false&useUnicode=true&characterEncoding=utf8" +
            "&connectTimeout=%d&socketTimeout=%d&allowPublicKeyRetrieval=true",
            ob.getHost(), ob.getPort(), ob.getDatabase(),
            ob.getPool() != null ? ob.getPool().getConnectionTimeoutMs() : 30000,
            ob.getPool() != null ? ob.getPool().getIdleTimeoutMs() : 60000
        );
    }

    /**
     * 打印配置信息
     */
    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("连接配置信息");
        System.out.println("=====================================");
        System.out.println("Kafka Bootstrap Servers: " + kafkaConfig.getBootstrapServers());
        if (kafkaConfig.getSecurity() != null && kafkaConfig.getSecurity().isEnabled()) {
            System.out.println("Kafka SASL 认证：已启用");
            System.out.println("  安全协议：" + kafkaConfig.getSecurity().getProtocol());
            System.out.println("  认证机制：" + kafkaConfig.getSecurity().getMechanism());
            System.out.println("  用户名：" + kafkaConfig.getSecurity().getUsername());
        } else {
            System.out.println("Kafka SASL 认证：未启用");
        }
        System.out.println("-------------------------------------");
        System.out.println("OceanBase 连接:");
        System.out.println("  Host: " + oceanBaseConfig.getHost() + ":" + oceanBaseConfig.getPort());
        System.out.println("  用户名：" + oceanBaseConfig.getUsername());
        System.out.println("  数据库：" + oceanBaseConfig.getDatabase());
        if (oceanBaseConfig.getPool() != null) {
            System.out.println("  连接池配置:");
            System.out.println("    最大连接数：" + oceanBaseConfig.getPool().getMaxPoolSize());
            System.out.println("    最小空闲：" + oceanBaseConfig.getPool().getMinIdle());
        }
        System.out.println("=====================================");
    }
}
