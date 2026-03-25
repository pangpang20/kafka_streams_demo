package com.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.kafka.producer.config.ProducerAppConfigLoader;
import com.kafka.producer.config.ProducerConnectionConfigLoader;
import com.kafka.producer.config.ProducerTableConfigLoader;
import com.kafka.producer.generator.ConfigurableDataGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableProducer {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableProducer.class);

    private final ProducerConnectionConfigLoader connectionConfig;
    private final ProducerAppConfigLoader appConfig;
    private final ProducerTableConfigLoader tableConfig;
    private final ConfigurableDataGenerator generator;
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final String topic;
    private final String tableName;
    private final String databaseName;
    private final String databaseType;

    public ConfigurableProducer(String configPath) {
        this.connectionConfig = new ProducerConnectionConfigLoader(configPath + "/connection-config.yaml");
        this.appConfig = new ProducerAppConfigLoader(configPath + "/app-config.yaml");
        this.tableConfig = new ProducerTableConfigLoader(configPath + "/table-schema.yaml");
        this.generator = new ConfigurableDataGenerator(tableConfig, appConfig);

        this.topic = appConfig.getSource().getTopic();
        this.tableName = appConfig.getSource().getTableName();
        this.databaseName = appConfig.getSource().getDatabaseName();
        this.databaseType = appConfig.getSource().getDatabaseType();

        this.producer = new KafkaProducer<>(connectionConfig.getKafkaProperties());
        log.info("ConfigurableProducer 初始化完成，Topic: {}", topic);
    }

    /**
     * 启动数据生成
     */
    public void start() {
        log.info("开始生成 CDC 数据，运行时长：{} 秒",
                appConfig.getSend().getDurationSeconds() == 0 ? "无限" : appConfig.getSend().getDurationSeconds());

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();
        long recordCount = 0;

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

                // 生成并发送记录
                generateAndSendRecord();
                recordCount++;

                // 间隔等待
                try {
                    Thread.sleep(appConfig.getSend().getIntervalMs());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }

                // 每 N 条打印统计
                if (recordCount % appConfig.getSend().getLogInterval() == 0) {
                    log.info("已发送 {} 条记录，数据池大小：{}", recordCount, generator.getDataPoolSize());
                }
            }
        } catch (Exception e) {
            log.error("数据生成过程中发生错误", e);
        } finally {
            shutdown();
            log.info("生产者已关闭，共发送 {} 条记录", recordCount);
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
    private void generateAndSendRecord() {
        // 1. 决定操作类型
        String opcode = determineOperationType();

        // 2. 获取或生成人员 ID
        Map<String, Object> existingData = null;
        if ("U".equals(opcode) || "D".equals(opcode)) {
            existingData = generator.getExistingData();
            if (existingData == null) {
                // 如果没有已有数据，降级为插入操作
                opcode = "I";
            }
        }

        // 3. 生成 CDC 记录
        Map<String, Object> recordData = generator.generateRecord(opcode, existingData);

        // 4. 构建 CDC 事件
        Map<String, Object> cdcEvent = buildCdcEvent(opcode, recordData);

        // 5. 构建消息 key
        String keyField = tableConfig.getTableByName(tableName).getKeyFields();
        String key = recordData.getOrDefault(keyField, UUID.randomUUID().toString()).toString() +
                ":" + opcode + ":" + System.currentTimeMillis();

        // 6. 发送到 Kafka
        sendRecord(key, cdcEvent, opcode, recordData);
    }

    /**
     * 构建 CDC 事件
     */
    private Map<String, Object> buildCdcEvent(String opcode, Map<String, Object> recordData) {
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
        dataRecord.put("databasename", databaseName);
        dataRecord.put("tablename", tableName);
        dataRecord.put("opcode", opcode);
        dataRecord.put("type", databaseType);
        dataRecord.put("timestamp", LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S")));
        dataRecord.put("pos", "bin." + String.format("%06d", new Random().nextInt(9999)) + "," + System.nanoTime());
        dataRecord.put("keyfields", tableConfig.getTableByName(tableName).getKeyFields());
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
    private void sendRecord(String key, Map<String, Object> event, String opcode, Map<String, Object> recordData) {
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
                log.error("发送失败：opcode={}, key={}, error={}", opcode, key, exception.getMessage());
            } else {
                log.debug("发送成功：opcode={}, key={}, partition={}, offset={}",
                        opcode, key, metadata.partition(), metadata.offset());
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
    }

    /**
     * 主程序入口
     */
    public static void main(String[] args) {
        System.out.println("=====================================");
        System.out.println("可配置 Kafka Producer");
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
