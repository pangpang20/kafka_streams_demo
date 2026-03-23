package com.kafka.producer.config;

import java.util.Properties;

/**
 * Kafka Producer 配置类
 *
 * 包含 Kafka Producer 的所有配置项，支持 SASL 认证。
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ProducerConfig {

    // ==================== Kafka 连接配置 ====================

    /**
     * Kafka Bootstrap Servers 地址
     * 格式：host1:port1,host2:port2,host3:port3
     */
    public static final String BOOTSTRAP_SERVERS = "localhost:19091,localhost:19092,localhost:19093";

    /**
     * 目标 Topic 名称
     */
    public static final String TOPIC_NAME = "mytopic";

    // ==================== SASL 认证配置 ====================

    /**
     * 是否启用 SASL 认证
     * 如需启用认证，将此值改为 true
     */
    public static final boolean ENABLE_SASL = false;

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

    /**
     * SASL 登录回调处理类
     */
    public static final String SASL_LOGIN_CALLBACK_HANDLER_CLASS =
            "org.apache.kafka.common.security.plain.PlainLoginModule";

    // ==================== Producer 性能配置 ====================

    /**
     * 重试次数
     */
    public static final int RETRIES = 3;

    /**
     * 批量发送大小（字节）
     */
    public static final int BATCH_SIZE = 16384;

    /**
     *  linger 时间（毫秒）
     * 等待更多消息加入 batch 的时间
     */
    public static final int LINGER_MS = 1;

    /**
     * 缓冲区内存大小（字节）
     */
    public static final long BUFFER_MEMORY = 33554432;

    /**
     * 序列化器 - Key
     */
    public static final String KEY_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";

    /**
     * 序列化器 - Value
     */
    public static final String VALUE_SERIALIZER =
            "org.apache.kafka.common.serialization.StringSerializer";

    // ==================== 数据生成配置 ====================

    /**
     * 发送间隔（毫秒）
     */
    public static final int SEND_INTERVAL_MS = 1000;

    /**
     * 插入操作概率（50%）
     */
    public static final int INSERT_PROBABILITY = 50;

    /**
     * 更新操作概率（45%）
     */
    public static final int UPDATE_PROBABILITY = 45;

    /**
     * 删除操作概率（5%）
     */
    public static final int DELETE_PROBABILITY = 5;

    /**
     * 问题数据概率（10%）
     * 用于测试 Kafka Streams 的质量检查功能
     */
    public static final int BAD_DATA_PROBABILITY = 10;

    // ==================== 已存在数据池（用于模拟更新/删除） ====================

    /**
     * 存储已生成的数据，用于模拟更新和删除操作
     */
    private static final java.util.Map<Long, java.util.Map<String, Object>> EXISTING_DATA_POOL =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * 获取或创建模拟的已有数据
     */
    public static java.util.Map<String, Object> getOrCreateExistingData(Long personId) {
        if (personId != null && EXISTING_DATA_POOL.containsKey(personId)) {
            return EXISTING_DATA_POOL.get(personId);
        }

        // 创建新的模拟数据
        java.util.Map<String, Object> data = new java.util.HashMap<>();
        long newPersonId = personId != null ? personId : System.currentTimeMillis() % 1000000;

        data.put("personid", newPersonId);
        data.put("idcard", generateIdCard(newPersonId));
        data.put("name", generateName());
        data.put("sex", newPersonId % 2 == 0 ? "男" : "女");
        data.put("age", 20 + (int)(newPersonId % 50));
        data.put("telephone", generatePhone());
        data.put("address", "北京");
        data.put("creditscore", 500.0 + (newPersonId % 200));
        data.put("bloodtype", new String[]{"A", "B", "O", "AB"}[(int)(newPersonId % 4)]);

        if (personId == null) {
            personId = newPersonId;
        }
        EXISTING_DATA_POOL.put(personId, data);

        return data;
    }

    /**
     * 更新已有数据
     */
    public static void updateExistingData(Long personId, java.util.Map<String, Object> newData) {
        if (personId != null) {
            EXISTING_DATA_POOL.put(personId, newData);
        }
    }

    /**
     * 删除已有数据
     */
    public static void removeExistingData(Long personId) {
        if (personId != null) {
            EXISTING_DATA_POOL.remove(personId);
        }
    }

    /**
     * 获取随机一个已有数据的 personId
     */
    public static Long getRandomExistingPersonId() {
        if (EXISTING_DATA_POOL.isEmpty()) {
            return null;
        }
        java.util.List<Long> ids = new java.util.ArrayList<>(EXISTING_DATA_POOL.keySet());
        return ids.get(new java.util.Random().nextInt(ids.size()));
    }

    // ==================== 辅助方法 ====================

    private static final java.util.Random RANDOM = new java.util.Random();
    private static final String[] SURNAMES = {"张", "王", "李", "赵", "刘"};
    private static final String[] NAMES = {"伟", "芳", "娜", "敏", "强"};
    private static final String[] ID_PREFIXES = {"110", "310", "440", "320"};

    private static String generateIdCard(long personId) {
        String prefix = ID_PREFIXES[RANDOM.nextInt(ID_PREFIXES.length)];
        String year = String.format("%04d", 1980 + RANDOM.nextInt(30));
        String month = String.format("%02d", RANDOM.nextInt(12) + 1);
        String day = String.format("%02d", RANDOM.nextInt(28) + 1);
        String suffix = String.format("%04d", personId % 10000);
        return prefix + year + month + day + suffix;
    }

    private static String generateName() {
        return SURNAMES[RANDOM.nextInt(SURNAMES.length)] +
               NAMES[RANDOM.nextInt(NAMES.length)];
    }

    private static String generatePhone() {
        String[] prefixes = {"138", "139", "188", "187"};
        return prefixes[RANDOM.nextInt(prefixes.length)] +
               String.format("%08d", RANDOM.nextInt(99999999));
    }

    // ==================== 获取 Producer Properties ====================

    /**
     * 获取 Kafka Producer 配置
     *
     * @return Properties 对象
     */
    public static Properties getProducerProperties() {
        Properties props = new Properties();

        // 基础连接配置
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("acks", "all");  // 所有副本确认
        props.put("retries", RETRIES);
        props.put("batch.size", BATCH_SIZE);
        props.put("linger.ms", LINGER_MS);
        props.put("buffer.memory", BUFFER_MEMORY);

        // 序列化配置
        props.put("key.serializer", KEY_SERIALIZER);
        props.put("value.serializer", VALUE_SERIALIZER);

        // SASL 认证配置（如果启用）
        if (ENABLE_SASL) {
            props.put("security.protocol", SECURITY_PROTOCOL);
            props.put("sasl.mechanism", SASL_MECHANISM);
            props.put("sasl.jaas.config", SASL_JAAS_CONFIG);
            props.put("sasl.login.callback.handler.class", SASL_LOGIN_CALLBACK_HANDLER_CLASS);
        }

        return props;
    }

    /**
     * 打印当前配置信息
     */
    public static void printConfig() {
        System.out.println("=====================================");
        System.out.println("Kafka Producer 配置信息");
        System.out.println("=====================================");
        System.out.println("Bootstrap Servers: " + BOOTSTRAP_SERVERS);
        System.out.println("Topic: " + TOPIC_NAME);
        System.out.println("SASL 认证：" + (ENABLE_SASL ? "已启用" : "未启用"));
        if (ENABLE_SASL) {
            System.out.println("  Security Protocol: " + SECURITY_PROTOCOL);
            System.out.println("  SASL Mechanism: " + SASL_MECHANISM);
        }
        System.out.println("发送间隔：" + SEND_INTERVAL_MS + "ms");
        System.out.println("操作概率：插入=" + INSERT_PROBABILITY + "%, " +
                          "更新=" + UPDATE_PROBABILITY + "%, " +
                          "删除=" + DELETE_PROBABILITY + "%");
        System.out.println("问题数据概率：" + BAD_DATA_PROBABILITY + "%");
        System.out.println("=====================================");
    }
}
