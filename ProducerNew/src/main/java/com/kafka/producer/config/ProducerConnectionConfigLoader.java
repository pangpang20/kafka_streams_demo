package com.kafka.producer.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

/**
 * Producer 连接配置加载器
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ProducerConnectionConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ProducerConnectionConfigLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "src/main/resources/connection-config.yaml";

    private KafkaConfig kafka;
    private ProducerConfig producer;

    public static class KafkaConfig {
        private String bootstrapServers;
        private SecurityConfig security;

        public String getBootstrapServers() { return bootstrapServers; }
        public void setBootstrapServers(String bootstrapServers) { this.bootstrapServers = bootstrapServers; }
        public SecurityConfig getSecurity() { return security; }
        public void setSecurity(SecurityConfig security) { this.security = security; }
    }

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

    public static class ProducerConfig {
        private int retries;
        private int batchSize;
        private int lingerMs;
        private long bufferMemory;
        private String acks;

        public int getRetries() { return retries; }
        public void setRetries(int retries) { this.retries = retries; }
        public int getBatchSize() { return batchSize; }
        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
        public int getLingerMs() { return lingerMs; }
        public void setLingerMs(int lingerMs) { this.lingerMs = lingerMs; }
        public long getBufferMemory() { return bufferMemory; }
        public void setBufferMemory(long bufferMemory) { this.bufferMemory = bufferMemory; }
        public String getAcks() { return acks; }
        public void setAcks(String acks) { this.acks = acks; }
    }

    public ProducerConnectionConfigLoader() {
        this(DEFAULT_CONFIG_PATH);
    }

    public ProducerConnectionConfigLoader(String configPath) {
        loadConfig(configPath);
    }

    public void loadConfig(String configPath) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            File configFile = new File(configPath);

            if (!configFile.exists()) {
                throw new RuntimeException("配置文件不存在：" + configPath);
            }

            ProducerConnectionConfigLoader loaded = mapper.readValue(configFile, ProducerConnectionConfigLoader.class);
            this.kafka = loaded.kafka;
            this.producer = loaded.producer;

            log.info("连接配置加载成功：{}", configPath);
        } catch (Exception e) {
            log.error("加载连接配置失败：{}", e.getMessage(), e);
            throw new RuntimeException("加载连接配置失败", e);
        }
    }

    public KafkaConfig getKafka() { return kafka; }
    public ProducerConfig getProducer() { return producer; }

    public Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("acks", producer.getAcks());
        props.put("retries", producer.getRetries());
        props.put("batch.size", producer.getBatchSize());
        props.put("linger.ms", producer.getLingerMs());
        props.put("buffer.memory", producer.getBufferMemory());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        if (kafka.getSecurity() != null && kafka.getSecurity().isEnabled()) {
            var sec = kafka.getSecurity();
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

    public void printConfig() {
        System.out.println("=====================================");
        System.out.println("Producer 连接配置");
        System.out.println("=====================================");
        System.out.println("Bootstrap Servers: " + kafka.getBootstrapServers());
        if (kafka.getSecurity() != null && kafka.getSecurity().isEnabled()) {
            System.out.println("SASL 认证：已启用");
            System.out.println("  安全协议：" + kafka.getSecurity().getProtocol());
            System.out.println("  认证机制：" + kafka.getSecurity().getMechanism());
            System.out.println("  用户名：" + kafka.getSecurity().getUsername());
        } else {
            System.out.println("SASL 认证：未启用");
        }
        System.out.println("-------------------------------------");
        System.out.println("Producer 性能配置:");
        System.out.println("  ACK 模式：" + producer.getAcks());
        System.out.println("  重试次数：" + producer.getRetries());
        System.out.println("  批量大小：" + producer.getBatchSize());
        System.out.println("  Linger 时间：" + producer.getLingerMs() + "ms");
        System.out.println("  缓冲区大小：" + producer.getBufferMemory());
        System.out.println("=====================================");
    }
}
