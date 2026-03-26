package com.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kafka.producer.config.ProducerAppConfigLoader;
import com.kafka.producer.config.ProducerConnectionConfigLoader;
import com.kafka.producer.config.ProducerTableConfigLoader;
import com.kafka.producer.generator.ConfigurableDataGenerator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 可配置 Kafka Producer - 主程序
 * <p>
 * 支持多表多 Topic 配置，自动创建 Topic
 *
 * @author Kafka Demo
 * @version 2.0.0
 */
public class ConfigurableProducer {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableProducer.class);

    private final ProducerConnectionConfigLoader connectionConfig;
    private final ProducerAppConfigLoader appConfig;
    private final ProducerTableConfigLoader tableConfig;
    private final KafkaProducer<String, String> producer;
    private final AdminClient adminClient;
    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    // 多表支持
    private final List<TableContext> tableContexts = new ArrayList<>();

    /**
     * 表上下文，包含表相关的所有信息
     */
    public static class TableContext {
        private final String tableName;
        private final String topic;
        private final String databaseName;
        private final String databaseType;
        private final int intervalMs;
        private ConfigurableDataGenerator generator;

        public TableContext(String tableName, String topic, String databaseName, String databaseType, int intervalMs) {
            this.tableName = tableName;
            this.topic = topic;
            this.databaseName = databaseName;
            this.databaseType = databaseType;
            this.intervalMs = intervalMs;
        }

        public String getTableName() { return tableName; }
        public String getTopic() { return topic; }
        public String getDatabaseName() { return databaseName; }
        public String getDatabaseType() { return databaseType; }
        public int getIntervalMs() { return intervalMs; }
        public ConfigurableDataGenerator getGenerator() { return generator; }
        public void setGenerator(ConfigurableDataGenerator generator) { this.generator = generator; }
    }

    public ConfigurableProducer(String configPath) {
        this.connectionConfig = new ProducerConnectionConfigLoader(configPath + "/connection-config.yaml");
        this.appConfig = new ProducerAppConfigLoader(configPath + "/app-config.yaml");
        this.tableConfig = new ProducerTableConfigLoader(configPath + "/table-schema.yaml");

        this.producer = new KafkaProducer<>(connectionConfig.getKafkaProperties());
        this.adminClient = AdminClient.create(connectionConfig.getKafkaProperties());

        // 初始化表上下文
        initTableContexts();

        log.info("ConfigurableProducer 初始化完成，共配置 {} 个表", tableContexts.size());
    }

    /**
     * 初始化表上下文
     */
    private void initTableContexts() {
        List<ProducerAppConfigLoader.TableSourceConfig> tables = appConfig.getTables();

        if (tables != null && !tables.isEmpty()) {
            // 多表模式
            for (ProducerAppConfigLoader.TableSourceConfig table : tables) {
                if (table.isEnabled()) {
                    int intervalMs = table.getIntervalMs() != null ? table.getIntervalMs() : appConfig.getSend().getIntervalMs();
                    TableContext ctx = new TableContext(
                            table.getTableName(),
                            table.getTopic(),
                            table.getDatabaseName(),
                            table.getDatabaseType(),
                            intervalMs
                    );
                    ctx.setGenerator(new ConfigurableDataGenerator(tableConfig, appConfig, table.getTableName()));
                    tableContexts.add(ctx);
                    log.info("添加表配置：{} -> {}", table.getTableName(), table.getTopic());
                }
            }
        } else {
            // 单表模式（向后兼容）
            ProducerAppConfigLoader.SourceConfig source = appConfig.getSource();
            TableContext ctx = new TableContext(
                    source.getTableName(),
                    source.getTopic(),
                    source.getDatabaseName(),
                    source.getDatabaseType(),
                    appConfig.getSend().getIntervalMs()
            );
            ctx.setGenerator(new ConfigurableDataGenerator(tableConfig, appConfig, source.getTableName()));
            tableContexts.add(ctx);
            log.info("添加表配置 (单表模式): {} -> {}", source.getTableName(), source.getTopic());
        }

        // 自动创建 Topic
        autoCreateTopics();
    }

    /**
     * 自动创建 Topic
     */
    private void autoCreateTopics() {
        ProducerAppConfigLoader.TopicConfig topicConfig = appConfig.getTopic();
        if (topicConfig == null || !topicConfig.isAutoCreate()) {
            log.info("Topic 自动创建未启用");
            return;
        }

        int partitions = topicConfig.getPartitions();
        int replicationFactor = topicConfig.getReplicationFactor();

        Set<String> topicsToCreate = new HashSet<>();
        for (TableContext ctx : tableContexts) {
            topicsToCreate.add(ctx.getTopic());
        }

        for (String topicName : topicsToCreate) {
            createTopicIfNotExists(topicName, partitions, replicationFactor);
        }
    }

    /**
     * 检查并创建 Topic
     */
    private void createTopicIfNotExists(String topicName, int partitions, int replicationFactor) {
        try {
            // 检查 Topic 是否已存在
            Set<String> existingTopics = adminClient.listTopics().names().get(30, TimeUnit.SECONDS);
            if (existingTopics.contains(topicName)) {
                log.info("Topic 已存在：{}", topicName);
                return;
            }

            // 创建 Topic
            NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicationFactor);
            adminClient.createTopics(Collections.singletonList(newTopic))
                    .all().get(30, TimeUnit.SECONDS);
            log.info("Topic 创建成功：{} (partitions={}, replicationFactor={})",
                    topicName, partitions, replicationFactor);
        } catch (Exception e) {
            // 检查 Topic 是否已存在（通过错误消息判断）
            if (e.getMessage() != null && e.getMessage().contains("already exists")) {
                log.info("Topic 已存在：{}", topicName);
                return;
            }
            log.error("创建 Topic 失败：{} - {}", topicName, e.getMessage(), e);
        }
    }

    /**
     * 启动数据生成
     */
    public void start() {
        log.info("开始生成 CDC 数据，运行时长：{} 秒",
                appConfig.getSend().getDurationSeconds() == 0 ? "无限" : appConfig.getSend().getDurationSeconds());
        log.info("启用的表数量：{}", tableContexts.size());

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();
        long totalRecordCount = 0;
        Map<String, Long> tableRecordCounts = new HashMap<>();

        try {
            while (true) {
                // 检查运行时间
                int durationSeconds = appConfig.getSend().getDurationSeconds();
                if (durationSeconds > 0) {
                    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
                    if (elapsed >= durationSeconds) {
                        log.info("已达到运行时长，停止生成");
                        break;
                    }
                }

                // 轮询每个表生成数据
                for (TableContext ctx : tableContexts) {
                    long count = generateAndSendRecord(ctx);
                    totalRecordCount += count;
                    tableRecordCounts.put(ctx.getTableName(),
                            tableRecordCounts.getOrDefault(ctx.getTableName(), 0L) + count);

                    // 使用该表的发送间隔
                    try {
                        Thread.sleep(ctx.getIntervalMs());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }

                // 每 N 条打印统计
                if (totalRecordCount % appConfig.getSend().getLogInterval() == 0) {
                    log.info("已发送 {} 条记录", totalRecordCount);
                    for (TableContext ctx : tableContexts) {
                        Long count = tableRecordCounts.get(ctx.getTableName());
                        log.info("  表 {}:{} 条，数据池大小：{}",
                                ctx.getTableName(), count != null ? count : 0,
                                ctx.getGenerator().getDataPoolSize());
                    }
                }
            }
        } catch (Exception e) {
            log.error("数据生成过程中发生错误", e);
        } finally {
            shutdown();
            log.info("生产者已关闭，共发送 {} 条记录", totalRecordCount);
        }

        try {
            shutdownLatch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 生成并发送单条记录
     */
    private long generateAndSendRecord(TableContext ctx) {
        // 1. 决定操作类型
        String opcode = determineOperationType();

        // 2. 获取或生成人员 ID
        Map<String, Object> existingData = null;
        if ("U".equals(opcode) || "D".equals(opcode)) {
            existingData = ctx.getGenerator().getExistingData();
            if (existingData == null) {
                // 如果没有已有数据，降级为插入操作
                opcode = "I";
            }
        }

        // 3. 生成 CDC 记录
        Map<String, Object> recordData = ctx.getGenerator().generateRecord(opcode, existingData);

        // 4. 构建 CDC 事件
        Map<String, Object> cdcEvent = buildCdcEvent(opcode, recordData, ctx);

        // 5. 构建消息 key
        String keyField = tableConfig.getTableByName(ctx.getTableName()).getKeyFields();
        String key = recordData.getOrDefault(keyField, UUID.randomUUID().toString()).toString() +
                ":" + opcode + ":" + System.currentTimeMillis();

        // 6. 发送到 Kafka
        sendRecord(ctx.getTopic(), key, cdcEvent, opcode, recordData);
        return 1;
    }

    /**
     * 构建 CDC 事件
     */
    private Map<String, Object> buildCdcEvent(String opcode, Map<String, Object> recordData, TableContext ctx) {
        Map<String, Object> allFields = new HashMap<>();
        Map<String, Object> afterData = new HashMap<>();

        // 构建字段数据
        for (Map.Entry<String, Object> entry : recordData.entrySet()) {
            Map<String, Object> fieldData = new HashMap<>();
            fieldData.put("type", getTypeForField(entry.getKey(), entry.getValue()));
            fieldData.put("value", entry.getValue());
            afterData.put(entry.getKey(), fieldData);
        }

        allFields.put("afterData", afterData);

        Map<String, Object> dataRecord = new HashMap<>();
        dataRecord.put("databasename", ctx.getDatabaseName());
        dataRecord.put("tablename", ctx.getTableName());
        dataRecord.put("opcode", opcode);
        dataRecord.put("type", ctx.getDatabaseType());
        dataRecord.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")));
        dataRecord.put("pos", "bin." + String.format("%06d", new Random().nextInt(9999)) + "," + System.nanoTime());
        dataRecord.put("keyfields", tableConfig.getTableByName(ctx.getTableName()).getKeyFields());
        dataRecord.put("allfields", allFields);

        Map<String, Object> event = new HashMap<>();
        event.put("data", Collections.singletonList(dataRecord));

        return event;
    }

    /**
     * 获取字段类型
     */
    private String getTypeForField(String fieldName, Object value) {
        if (value == null) return "null:STRING";
        if (value instanceof Long) return "null:INT64";
        if (value instanceof Integer) return "null:INT32";
        if (value instanceof Double) return "null:FLOAT64";
        if (value instanceof BigDecimal) return "org.apache.kafka.connect.data.Decimal:BYTES";
        if (value instanceof LocalDateTime) return "io.debezium.time.Timestamp:INT64";
        return "null:STRING";
    }

    /**
     * 发送记录到 Kafka
     */
    private void sendRecord(String topic, String key, Map<String, Object> event, String opcode, Map<String, Object> recordData) {
        String value;
        try {
            value = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            log.error("JSON 序列化失败", e);
            return;
        }

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

        producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("发送失败：topic={}, opcode={}, key={}, error={}", topic, opcode, key, exception.getMessage());
            } else {
                log.debug("发送成功：topic={}, opcode={}, key={}, partition={}, offset={}",
                        topic, opcode, key, metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * 决定操作类型
     */
    private String determineOperationType() {
        int rand = new Random().nextInt(100);
        int insertProb = appConfig.getOperation().getInsertProbability();
        int updateProb = appConfig.getOperation().getUpdateProbability();

        if (rand < insertProb) {
            return "I";
        } else if (rand < insertProb + updateProb) {
            return "U";
        } else {
            return "D";
        }
    }

    /**
     * 关闭生产者
     */
    public void shutdown() {
        if (producer != null) {
            producer.flush();
            producer.close();
            log.info("Kafka Producer 已关闭");
        }
        if (adminClient != null) {
            adminClient.close();
            log.info("AdminClient 已关闭");
        }
    }

    /**
     * 主程序入口
     */
    public static void main(String[] args) {
        System.out.println("=====================================");
        System.out.println("可配置 Kafka Producer (多表多 Topic 支持)");
        System.out.println("=====================================");

        String configPath = "src/main/resources";
        if (args.length > 0) {
            configPath = args[0];
        }

        System.out.println("配置文件路径：" + configPath);
        System.out.println();

        try {
            // 加载并打印配置
            ProducerConnectionConfigLoader connConfig = new ProducerConnectionConfigLoader(configPath + "/connection-config.yaml");
            ProducerAppConfigLoader appConfig = new ProducerAppConfigLoader(configPath + "/app-config.yaml");
            ProducerTableConfigLoader tableConfig = new ProducerTableConfigLoader(configPath + "/table-schema.yaml");

            connConfig.printConfig();
            appConfig.printConfig();
            tableConfig.printConfig();

            // 创建并启动生产者
            ConfigurableProducer producer = new ConfigurableProducer(configPath);

            // 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n收到关闭信号，正在停止...");
                producer.shutdown();
            }));

            // 启动生成
            producer.start();

            System.out.println("生产者程序结束");

        } catch (Exception e) {
            System.err.println("启动失败：" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
