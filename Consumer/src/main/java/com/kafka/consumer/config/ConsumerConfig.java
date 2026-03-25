package com.kafka.consumer.config;

import java.util.Properties;

/**
 * Kafka Streams Consumer 配置类
 *
 * 包含 Kafka Streams 应用的所有配置项，支持 SASL 认证。
 * 配置项说明：
 * - 连接配置：Bootstrap Servers、Topic 名称
 * - 安全配置：SASL 认证参数
 * - Streams 配置：Application ID、状态目录等
 * - 消费配置：自动重置策略、反序列化器
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConsumerConfig {

    // ==================== Kafka 连接配置 ====================

    /**
     * Kafka Bootstrap Servers 地址
     * 三节点高可用集群
     */
    public static final String BOOTSTRAP_SERVERS = "localhost:19091,localhost:19092,localhost:19093";

    /**
     * 源 Topic 名称（生产者发送的数据）
     */
    public static final String SOURCE_TOPIC = "mytopic";

    /**
     * 正常数据输出 Topic
     */
    public static final String VALID_DATA_TOPIC = "mytopic-valid";

    /**
     * 异常数据输出 Topic
     */
    public static final String INVALID_DATA_TOPIC = "mytopic-invalid";

    // ==================== Kafka Streams 应用配置 ====================

    /**
     * Streams Application ID
     * 必须是唯一的，用于标识这个 Streams 应用
     */
    public static final String APPLICATION_ID = "quality-check-app";

    /**
     * 状态存储目录
     * 用于存储 Kafka Streams 的状态（如聚合状态、表状态等）
     */
    public static final String STATE_DIR = "/tmp/kafka-streams-quality-check";

    // ==================== SASL 认证配置 ====================

    /**
     * 是否启用 SASL 认证
     * 如需启用认证，将此值改为 true
     */
    public static final boolean ENABLE_SASL = true;

    /**
     * SASL 安全协议
     */
    public static final String SECURITY_PROTOCOL = "SASL_PLAINTEXT";

    /**
     * SASL 认证机制
     */
    public static final String SASL_MECHANISM = "PLAIN";

    /**
     * SASL JAAS 配置
     * 用户名：admin
     * 密码：Audaque@123
     */
    public static final String SASL_JAAS_CONFIG =
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"admin\" " +
            "password=\"Audaque@123\";";

    // ==================== 消费配置 ====================

    /**
     * 自动 offset 重置策略
     * earliest: 从最早的消息开始消费
     * latest: 从最新的消息开始消费
     */
    public static final String AUTO_OFFSET_RESET = "earliest";

    /**
     * Key 反序列化器
     */
    public static final String KEY_DESERIALIZER =
            "org.apache.kafka.common.serialization.StringDeserializer";

    /**
     * Value 反序列化器
     */
    public static final String VALUE_DESERIALIZER =
            "org.apache.kafka.common.serialization.StringDeserializer";

    // ==================== 数据质量检查规则配置 ====================

    /**
     * 是否检查性别字段
     */
    public static final boolean CHECK_SEX = true;

    /**
     * 合法的性别值
     */
    public static final String[] VALID_SEX_VALUES = {"男", "女"};

    /**
     * 是否检查年龄字段
     */
    public static final boolean CHECK_AGE = true;

    /**
     * 年龄最小值
     */
    public static final int MIN_AGE = 0;

    /**
     * 年龄最大值
     */
    public static final int MAX_AGE = 120;

    /**
     * 是否检查电话号码
     */
    public static final boolean CHECK_TELEPHONE = true;

    /**
     * 电话号码正则表达式
     */
    public static final String TELEPHONE_REGEX = "^1[3-9]\\d{9}$";

    /**
     * 是否检查血型
     */
    public static final boolean CHECK_BLOOD_TYPE = true;

    /**
     * 合法的血型值
     */
    public static final String[] VALID_BLOOD_TYPES = {"A", "B", "O", "AB"};

    /**
     * 是否检查信用分
     */
    public static final boolean CHECK_CREDIT_SCORE = true;

    /**
     * 信用分最小值
     */
    public static final double MIN_CREDIT_SCORE = 0.0;

    /**
     * 信用分最大值
     */
    public static final double MAX_CREDIT_SCORE = 1000.0;

    /**
     * 是否检查住房面积
     */
    public static final boolean CHECK_HOUSING_AREA = true;

    /**
     * 住房面积最小值
     */
    public static final double MIN_HOUSING_AREA = 0.0;

    // ==================== OceanBase 配置 ====================

    /**
     * OceanBase JDBC URL
     * OceanBase 使用专用的 JDBC 驱动和 URL 格式
     * Consumer 在宿主机运行，使用 localhost:2881 连接
     */
    public static final String OCEANBASE_JDBC_URL = "jdbc:oceanbase://localhost:2881/kafka_quality_check?useSSL=false&useUnicode=true&characterEncoding=utf8&connectTimeout=30000&socketTimeout=60000&allowPublicKeyRetrieval=true";

    /**
     * OceanBase 用户名
     * OceanBase 需要使用租户名格式：user@tenant
     */
    public static final String OCEANBASE_USERNAME = "root@test";

    /**
     * OceanBase 密码
     */
    public static final String OCEANBASE_PASSWORD = "Audaque@123";

    // ==================== 获取 Streams Properties ====================

    /**
     * 获取 Kafka Streams 配置
     *
     * @return Properties 对象
     */
    public static Properties getStreamsProperties() {
        Properties props = new Properties();

        // 基础连接配置
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("application.id", APPLICATION_ID);

        // 状态存储配置
        props.put("state.dir", STATE_DIR);

        // 序列化配置
        props.put("default.key.serde",
                "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde",
                "org.apache.kafka.common.serialization.Serdes$StringSerde");

        // 消费配置
        props.put("auto.offset.reset", AUTO_OFFSET_RESET);

        // SASL 认证配置（如果启用）
        if (ENABLE_SASL) {
            props.put("security.protocol", SECURITY_PROTOCOL);
            props.put("sasl.mechanism", SASL_MECHANISM);
            props.put("sasl.jaas.config", SASL_JAAS_CONFIG);
        }

        // 处理保证
        props.put("processing.guarantee", "at_least_once");

        // 提交间隔
        props.put("commit.interval.ms", "1000");

        return props;
    }

    /**
     * 打印当前配置信息
     */
    public static void printConfig() {
        System.out.println("=====================================");
        System.out.println("Kafka Streams Consumer 配置信息");
        System.out.println("=====================================");
        System.out.println("Bootstrap Servers: " + BOOTSTRAP_SERVERS);
        System.out.println("Source Topic: " + SOURCE_TOPIC);
        System.out.println("Valid Data Topic: " + VALID_DATA_TOPIC);
        System.out.println("Invalid Data Topic: " + INVALID_DATA_TOPIC);
        System.out.println("Application ID: " + APPLICATION_ID);
        System.out.println("SASL 认证：" + (ENABLE_SASL ? "已启用" : "未启用"));
        System.out.println("Offset 重置：" + AUTO_OFFSET_RESET);
        System.out.println("-------------------------------------");
        System.out.println("数据质量检查规则:");
        System.out.println("  性别检查：" + (CHECK_SEX ? "开启 (允许：" + String.join(",", VALID_SEX_VALUES) + ")" : "关闭"));
        System.out.println("  年龄检查：" + (CHECK_AGE ? "开启 [" + MIN_AGE + "-" + MAX_AGE + "]" : "关闭"));
        System.out.println("  电话检查：" + (CHECK_TELEPHONE ? "开启 (正则：" + TELEPHONE_REGEX + ")" : "关闭"));
        System.out.println("  血型检查：" + (CHECK_BLOOD_TYPE ? "开启 (允许：" + String.join(",", VALID_BLOOD_TYPES) + ")" : "关闭"));
        System.out.println("  信用分检查：" + (CHECK_CREDIT_SCORE ? "开启 [" + MIN_CREDIT_SCORE + "-" + MAX_CREDIT_SCORE + "]" : "关闭"));
        System.out.println("  住房面积检查：" + (CHECK_HOUSING_AREA ? "开启 (最小值：" + MIN_HOUSING_AREA + ")" : "关闭"));
        System.out.println("=====================================");
    }
}
