package com.kafka.producer;

import com.kafka.producer.config.ProducerConfig;
import com.kafka.producer.generator.RandomDataGenerator;
import com.kafka.producer.model.CdcEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * CDC 数据生成器 - Kafka Producer 主程序
 *
 * 功能说明:
 * 1. 每秒生成一条模拟的数据库变更记录（CDC 数据）
 * 2. 支持三种操作类型：插入 (50%)、更新 (45%)、删除 (5%)
 * 3. 可配置生成问题数据（10% 概率），用于测试消费者端的数据质量检查
 * 4. 数据发送到指定的 Kafka Topic
 *
 * 数据格式：
 * - 符合 CDC 变更捕获格式
 * - 包含操作类型、表信息、字段数据（before/after）
 * - 支持 baseinfo 表的完整字段结构
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class CdcDataProducer {

    private static final Logger log = LoggerFactory.getLogger(CdcDataProducer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Random random = new Random();

    // 存储已生成的数据，用于模拟更新和删除操作
    private final Map<Long, Map<String, Object>> existingDataPool = new HashMap<>();

    /**
     * 构造函数
     *
     * @param topic 目标 Kafka Topic
     */
    public CdcDataProducer(String topic) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(ProducerConfig.getProducerProperties());
        log.info("Kafka Producer 初始化完成，Topic: {}", topic);
    }

    /**
     * 启动数据生成
     *
     * @param durationSeconds 运行时长（秒），0 表示无限运行
     */
    public void start(int durationSeconds) {
        log.info("开始生成 CDC 数据，运行时长：{} 秒", durationSeconds == 0 ? "无限" : durationSeconds);

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        long startTime = System.currentTimeMillis();
        long recordCount = 0;

        try {
            while (true) {
                // 检查运行时间
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
                    Thread.sleep(ProducerConfig.SEND_INTERVAL_MS);
                } catch (InterruptedException e) {
                    log.warn("发送间隔被中断", e);
                    Thread.currentThread().interrupt();
                    break;
                }

                // 每 100 条打印统计
                if (recordCount % 100 == 0) {
                    log.info("已发送 {} 条记录", recordCount);
                }
            }
        } catch (Exception e) {
            log.error("数据生成过程中发生错误", e);
        } finally {
            shutdown();
            log.info("生产者可关闭，共发送 {} 条记录", recordCount);
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

        // 2. 决定是否生成问题数据
        boolean includeBadData = random.nextInt(100) < ProducerConfig.BAD_DATA_PROBABILITY;

        // 3. 获取或生成人员 ID
        Long personId = null;
        Map<String, Object> existingData = null;

        if ("U".equals(opcode) || "D".equals(opcode)) {
            // 更新和删除操作需要已有数据
            personId = ProducerConfig.getRandomExistingPersonId();
            if (personId != null) {
                existingData = ProducerConfig.getOrCreateExistingData(personId);
            } else {
                // 如果没有已有数据，降级为插入操作
                opcode = "I";
                log.debug("无已有数据，将操作降级为插入");
            }
        }

        // 4. 生成 CDC 记录
        CdcEvent.DataRecord record = RandomDataGenerator.generateBaseinfoRecord(
                opcode, personId, existingData, includeBadData);

        // 5. 更新数据池
        if ("I".equals(opcode) || "U".equals(opcode)) {
            // 插入或更新后，保存/更新数据到池中
            Long newPersonId = extractPersonId(record);
            if (newPersonId != null) {
                Map<String, Object> newData = extractAfterData(record);
                ProducerConfig.updateExistingData(newPersonId, newData);
                existingDataPool.put(newPersonId, newData);
            }
        } else if ("D".equals(opcode)) {
            // 删除后，从池中移除
            if (personId != null) {
                ProducerConfig.removeExistingData(personId);
                existingDataPool.remove(personId);
            }
        }

        // 6. 构建消息
        CdcEvent event = new CdcEvent();
        event.setData(new CdcEvent.DataRecord[]{record});

        String key = record.getKeyfields() + ":" + record.getOpcode() + ":" + System.currentTimeMillis();
        String value;

        try {
            value = objectMapper.writeValueAsString(event);
        } catch (Exception e) {
            log.error("JSON 序列化失败", e);
            return;
        }

        // 7. 发送到 Kafka
        sendRecord(key, value, record);
    }

    /**
     * 发送记录到 Kafka
     */
    private void sendRecord(String key, String value, CdcEvent.DataRecord record) {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topic, key, value);

        producer.send(producerRecord, (RecordMetadata metadata, Exception exception) -> {
            if (exception != null) {
                log.error("发送失败：opcode={}, key={}, error={}",
                        record.getOpcode(), key, exception.getMessage());
            } else {
                log.debug("发送成功：opcode={}, key={}, partition={}, offset={}",
                        record.getOpcode(), key, metadata.partition(), metadata.offset());
            }
        });
    }

    /**
     * 决定操作类型
     *
     * @return 操作类型：I=插入，U=更新，D=删除
     */
    private String determineOperationType() {
        int rand = random.nextInt(100);

        if (rand < ProducerConfig.INSERT_PROBABILITY) {
            return "I";  // 50% 插入
        } else if (rand < ProducerConfig.INSERT_PROBABILITY + ProducerConfig.UPDATE_PROBABILITY) {
            return "U";  // 45% 更新
        } else {
            return "D";  // 5% 删除
        }
    }

    /**
     * 从记录中提取人员 ID
     */
    private Long extractPersonId(CdcEvent.DataRecord record) {
        if (record.getAllfields() != null && record.getAllfields().getAfterData() != null) {
            CdcEvent.FieldData personIdData = record.getAllfields().getAfterData().get("personid");
            if (personIdData != null && personIdData.getValue() != null) {
                try {
                    return Long.valueOf(personIdData.getValue().toString());
                } catch (NumberFormatException e) {
                    log.warn("解析 personid 失败", e);
                }
            }
        }
        return null;
    }

    /**
     * 从记录中提取 after_data 数据
     */
    private Map<String, Object> extractAfterData(CdcEvent.DataRecord record) {
        Map<String, Object> result = new HashMap<>();

        if (record.getAllfields() != null && record.getAllfields().getAfterData() != null) {
            for (Map.Entry<String, CdcEvent.FieldData> entry :
                    record.getAllfields().getAfterData().entrySet()) {

                if (entry.getValue() != null && entry.getValue().getValue() != null) {
                    result.put(entry.getKey(), entry.getValue().getValue());
                }
            }
        }

        return result;
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
     *
     * @param args 命令行参数
     *             args[0]: 运行时长（秒），可选，默认无限运行
     */
    public static void main(String[] args) {
        // 打印配置信息
        ProducerConfig.printConfig();

        // 解析运行时长
        int durationSeconds = 0;  // 0 表示无限运行
        if (args.length > 0) {
            try {
                durationSeconds = Integer.parseInt(args[0]);
                System.out.println("运行时长：" + durationSeconds + " 秒");
            } catch (NumberFormatException e) {
                System.out.println("无效的运行时长参数，使用默认值（无限运行）");
            }
        }

        // 创建并启动生产者
        CdcDataProducer producer = new CdcDataProducer(ProducerConfig.TOPIC_NAME);

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n收到关闭信号，正在停止...");
            producer.shutdown();
        }));

        // 启动生成
        producer.start(durationSeconds);

        System.out.println("生产者程序结束");
    }
}
