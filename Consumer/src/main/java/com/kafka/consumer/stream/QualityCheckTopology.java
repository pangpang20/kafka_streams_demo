package com.kafka.consumer.stream;

import com.kafka.consumer.config.ConsumerConfig;
import com.kafka.consumer.model.QualityCheckResult;
import com.kafka.consumer.processor.DataProcessor;
import com.kafka.consumer.processor.ProcessingResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Kafka Streams 拓扑构建器
 *
 * 构建数据处理流程：
 * 1. 从源 Topic 读取数据
 * 2. 使用 DataProcessor 进行处理（认证→解析→验证）
 * 3. 根据处理结果分发到不同 Topic：
 *    - 正常数据 -> mytopic-valid
 *    - 异常数据 -> mytopic-invalid
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class QualityCheckTopology {

    private static final Logger log = LoggerFactory.getLogger(QualityCheckTopology.class);

    /**
     * 数据处理器
     */
    private final DataProcessor processor;

    /**
     * JSON 序列化器
     */
    private final ObjectMapper objectMapper;

    /**
     * 异议数据日志记录器
     */
    private final InvalidDataLogger invalidDataLogger;

    /**
     * OceanBase 异议数据写入器
     */
    private final InvalidDataToOceanBase invalidDataToOceanBase;

    /**
     * OceanBase 正常数据写入器
     */
    private final ValidDataToOceanBase validDataToOceanBase;

    /**
     * Kafka Streams 实例
     */
    private KafkaStreams streams;

    /**
     * 构造函数
     */
    public QualityCheckTopology() {
        this.processor = new DataProcessor();
        this.objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        this.invalidDataLogger = new InvalidDataLogger();
        this.invalidDataToOceanBase = new InvalidDataToOceanBase();
        this.validDataToOceanBase = new ValidDataToOceanBase();
        log.info("QualityCheckTopology 初始化完成");
    }

    /**
     * 构建 Streams 拓扑
     *
     * 拓扑结构说明:
     *
     *                    ┌─────────────────────┐
     *                    │   Source: mytopic   │
     *                    └──────────┬──────────┘
     *                               │
     *                               ▼
     *                    ┌─────────────────────┐
     *                    │  DataProcessor      │
     *                    │  1. 认证 (Auth)     │
     *                    │  2. 解析 (Parse)    │
     *                    │  3. 验证 (Validate) │
     *                    └──────────┬──────────┘
     *                               │
     *                               ▼
     *                    ┌─────────────────────┐
     *                    │   质量检查结果      │
     *                    └──────────┬──────────┘
     *                               │
     *              ┌────────────────┼────────────────┐
     *              │                │                │
     *              ▼                ▼                ▼
     *     ┌────────────────┐ ┌─────────────────┐ ┌──────────────┐
     *     │ mytopic-valid  │ │ mytopic-invalid │ │  日志/监控   │
     *     │ (正常数据)      │ │ (异常数据)       │ │ (打印统计)   │
     *     └────────────────┘ └─────────────────┘ └──────────────┘
     *
     * @return StreamsBuilder
     */
    public StreamsBuilder buildTopology() {
        log.info("开始构建 Kafka Streams 拓扑...");

        StreamsBuilder builder = new StreamsBuilder();

        // ============================================
        // 步骤 1: 定义输入流
        // ============================================
        // 从源 Topic 读取数据，key 和 value 都是 String 类型
        KStream<String, String> sourceStream = builder.stream(
                ConsumerConfig.SOURCE_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        log.info("已定义输入流：Topic={}", ConsumerConfig.SOURCE_TOPIC);

        // ============================================
        // 步骤 2: 数据处理（认证→解析→验证）
        // ============================================
        // 使用 mapValues 对每条消息进行处理
        KStream<String, ProcessingResult> processedStream = sourceStream.mapValues(value -> {
            try {
                // 使用 DataProcessor 处理数据
                // Processor 内部执行：认证 → 解析 → 验证
                return processor.process(null, value);
            } catch (Exception e) {
                log.error("数据处理异常：{}", e.getMessage(), e);
                // 异常时返回失败结果，避免消息丢失
                ProcessingResult errorResult = new ProcessingResult();
                errorResult.setSuccess(false);
                errorResult.setMessage("处理异常：" + e.getMessage());
                return errorResult;
            }
        });

        log.info("已定义处理流：DataProcessor");

        // ============================================
        // 步骤 3: 打印处理日志和统计
        // ============================================
        // 使用 peek 进行日志记录（不影响流）
        processedStream.peek((key, result) -> {
            if (result.isSuccess()) {
                log.info("[PASS] key={}, opcode={}, table={}",
                        key,
                        result.getRecord() != null ? result.getRecord().getOpcode() : "N/A",
                        result.getRecord() != null ? result.getRecord().getTablename() : "N/A");
                // 写入正常数据到 OceanBase 数据库
                validDataToOceanBase.logValidData(result);
            } else {
                log.warn("[FAIL] key={}, reason={}", key, result.getMessage());
                // 记录异议数据到日志文件
                invalidDataLogger.logInvalidData(result);
                // 同时写入 OceanBase 数据库
                invalidDataToOceanBase.logInvalidData(result);
            }
        });

        // ============================================
        // 步骤 4: 分支处理 - 根据结果分发到不同 Topic
        // ============================================
        // 正常数据 -> mytopic-valid
        processedStream
                .filter((key, result) -> result.isSuccess())
                .mapValues(this::resultToJson)
                .to(ConsumerConfig.VALID_DATA_TOPIC,
                        Produced.with(Serdes.String(), Serdes.String()));

        log.info("已定义正常数据输出：Topic={}", ConsumerConfig.VALID_DATA_TOPIC);

        // 异常数据 -> mytopic-invalid
        processedStream
                .filter((key, result) -> !result.isSuccess())
                .mapValues(this::resultToJson)
                .to(ConsumerConfig.INVALID_DATA_TOPIC,
                        Produced.with(Serdes.String(), Serdes.String()));

        log.info("已定义异常数据输出：Topic={}", ConsumerConfig.INVALID_DATA_TOPIC);

        // ============================================
        // 步骤 5: 添加全局统计（可选）
        // ============================================
        // 使用 Grouped 进行计数统计
        // 注意：先用 mapValues 将 ProcessingResult 转换为 String，避免 Serde 类型不匹配问题
        processedStream
                .mapValues(result -> result.isSuccess() ? "valid" : "invalid")
                .groupBy((key, validOrInvalid) -> validOrInvalid,
                        Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("quality-check-stats"))
                .toStream()
                .peek((key, count) ->
                        log.info("===== 统计：{} = {} 条 =====", key, count));

        log.info("已定义统计流：quality-check-stats");

        log.info("Kafka Streams 拓扑构建完成");
        log.info("-------------------------------------");
        log.info("拓扑流程:");
        log.info("  {} -> [DataProcessor] -> {} / {}",
                ConsumerConfig.SOURCE_TOPIC,
                ConsumerConfig.VALID_DATA_TOPIC,
                ConsumerConfig.INVALID_DATA_TOPIC);
        log.info("-------------------------------------");

        return builder;
    }

    /**
     * 将处理结果转换为 JSON 字符串
     */
    private String resultToJson(ProcessingResult result) {
        try {
            return objectMapper.writeValueAsString(result);
        } catch (Exception e) {
            log.error("JSON 序列化失败：{}", e.getMessage());
            return "{\"error\": \"序列化失败\"}";
        }
    }

    /**
     * 启动 Streams 应用
     *
     * @param props Streams 配置
     */
    public void start(Properties props) {
        log.info("启动 Kafka Streams 应用...");

        // 构建拓扑
        StreamsBuilder builder = buildTopology();

        // 创建 Streams 实例
        streams = new KafkaStreams(builder.build(), new StreamsConfig(props));

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到关闭信号，正在停止 Streams 应用...");
            streams.close();
            invalidDataLogger.close();
            invalidDataToOceanBase.close();
            validDataToOceanBase.close();
            log.info("Streams 应用已停止");
        }));

        // 启动
        streams.start();
        log.info("Kafka Streams 应用已启动");
        log.info("Application ID: {}", ConsumerConfig.APPLICATION_ID);
        log.info("State Directory: {}", ConsumerConfig.STATE_DIR);

        // 打印运行状态
        printRuntimeInfo();
    }

    /**
     * 打印运行时信息
     */
    private void printRuntimeInfo() {
        new Thread(() -> {
            try {
                Thread.sleep(5000); // 等待 5 秒后开始打印
                while (streams != null && streams.state() != KafkaStreams.State.NOT_RUNNING) {
                    KafkaStreams.State state = streams.state();
                    log.info("当前状态：{}", state);
                    Thread.sleep(30000); // 每 30 秒打印一次
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "RuntimeInfo-Printer").start();
    }

    /**
     * 停止 Streams 应用
     */
    public void stop() {
        if (streams != null) {
            streams.close();
            log.info("Kafka Streams 应用已停止");
        }
    }

    /**
     * 获取 Streams 实例（用于监控）
     */
    public KafkaStreams getStreams() {
        return streams;
    }
}
