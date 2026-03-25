package com.kafka.consumer.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.config.*;
import com.kafka.consumer.model.ProcessingResult;
import com.kafka.consumer.processor.ConfigurableDataProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 可配置 Kafka Streams 拓扑构建器
 *
 * 根据配置文件构建数据处理流程
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableTopology {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableTopology.class);

    private final AppConfigLoader appConfig;
    private final ConfigurableDataProcessor processor;
    private final ObjectMapper objectMapper;
    private final ConfigurableInvalidDataWriter invalidDataWriter;
    private final ConfigurableValidDataWriter validDataWriter;

    private KafkaStreams streams;

    /**
     * 构造函数
     */
    public ConfigurableTopology(AppConfigLoader appConfig,
                                 ValidationRuleConfigLoader ruleConfig,
                                 ConnectionConfigLoader connectionConfig,
                                 TableSchemaConfigLoader schemaConfig) {
        this.appConfig = appConfig;
        this.processor = new ConfigurableDataProcessor(appConfig.getSourceTableName(), ruleConfig);

        // 根据配置决定是否初始化写入器
        if (appConfig.isWriteToOceanbase()) {
            this.validDataWriter = new ConfigurableValidDataWriter(connectionConfig, schemaConfig, appConfig);
            this.invalidDataWriter = new ConfigurableInvalidDataWriter(connectionConfig, schemaConfig, appConfig);
        } else {
            this.validDataWriter = null;
            this.invalidDataWriter = null;
        }

        this.objectMapper = new ObjectMapper();

        log.info("ConfigurableTopology 初始化完成");
        log.info("  源表名：{}", appConfig.getSourceTableName());
        log.info("  源 Topic: {}", appConfig.getSourceTopic());
        log.info("  写入 Topic: {}", appConfig.isWriteToTopic());
        log.info("  写入 OceanBase: {}", appConfig.isWriteToOceanbase());
    }

    /**
     * 构建 Streams 拓扑
     */
    public StreamsBuilder buildTopology() {
        log.info("开始构建 Kafka Streams 拓扑...");

        StreamsBuilder builder = new StreamsBuilder();

        // 定义输入流
        KStream<String, String> sourceStream = builder.stream(
                appConfig.getSourceTopic(),
                Consumed.with(Serdes.String(), Serdes.String())
        );

        log.info("已定义输入流：Topic={}", appConfig.getSourceTopic());

        // 数据处理
        KStream<String, ProcessingResult> processedStream = sourceStream.mapValues(value -> {
            try {
                return processor.process(null, value);
            } catch (Exception e) {
                log.error("数据处理异常：{}", e.getMessage(), e);
                ProcessingResult errorResult = new ProcessingResult();
                errorResult.setSuccess(false);
                errorResult.setMessage("处理异常：" + e.getMessage());
                return errorResult;
            }
        });

        log.info("已定义处理流：ConfigurableDataProcessor");

        // 日志记录和数据库写入
        processedStream.peek((key, result) -> {
            if (result == null) {
                // 表名不匹配，跳过
                return;
            }
            if (result.isSuccess()) {
                log.info("[PASS] key={}, opcode={}, table={}",
                        key,
                        result.getRecord() != null ? result.getRecord().getOpcode() : "N/A",
                        result.getRecord() != null ? result.getRecord().getTablename() : "N/A");
                // 写入正常数据到 OceanBase
                if (validDataWriter != null) {
                    validDataWriter.logValidData(result);
                }
            } else {
                log.warn("[FAIL] key={}, reason={}", key, result.getMessage());
                // 写入异议数据到 OceanBase
                if (invalidDataWriter != null) {
                    invalidDataWriter.logInvalidData(result);
                }
            }
        });

        // 根据配置决定是否写入 Topic
        if (appConfig.isWriteToTopic()) {
            // 正常数据 -> valid topic
            processedStream
                    .filter((key, result) -> result != null && result.isSuccess())
                    .mapValues(this::resultToJson)
                    .to(appConfig.getSource().getValidDataTopic(),
                            Produced.with(Serdes.String(), Serdes.String()));

            log.info("已定义正常数据输出：Topic={}", appConfig.getSource().getValidDataTopic());

            // 异常数据 -> invalid topic
            processedStream
                    .filter((key, result) -> result != null && !result.isSuccess())
                    .mapValues(this::resultToJson)
                    .to(appConfig.getSource().getInvalidDataTopic(),
                            Produced.with(Serdes.String(), Serdes.String()));

            log.info("已定义异常数据输出：Topic={}", appConfig.getSource().getInvalidDataTopic());
        }

        // 统计
        processedStream
                .filter((key, result) -> result != null)
                .mapValues(result -> result.isSuccess() ? "valid" : "invalid")
                .groupBy((key, validOrInvalid) -> validOrInvalid,
                        Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as(appConfig.getAppName() + "-stats"))
                .toStream()
                .peek((key, count) ->
                        log.info("===== 统计：{} = {} 条 =====", key, count));

        log.info("Kafka Streams 拓扑构建完成");
        log.info("-------------------------------------");
        log.info("拓扑流程:");
        log.info("  {} -> [ConfigurableDataProcessor] -> {} / {}",
                appConfig.getSourceTopic(),
                appConfig.isWriteToTopic() ? appConfig.getSource().getValidDataTopic() : "(不写入 Topic)",
                appConfig.isWriteToTopic() ? appConfig.getSource().getInvalidDataTopic() : "(不写入 Topic)");
        log.info("-------------------------------------");

        return builder;
    }

    /**
     * 将处理结果转换为 JSON
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
     */
    public void start(Properties props) {
        log.info("启动 Kafka Streams 应用...");

        StreamsBuilder builder = buildTopology();
        streams = new KafkaStreams(builder.build(), new StreamsConfig(props));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("收到关闭信号，正在停止 Streams 应用...");
            streams.close();
            if (validDataWriter != null) {
                validDataWriter.close();
            }
            if (invalidDataWriter != null) {
                invalidDataWriter.close();
            }
            log.info("Streams 应用已停止");
        }));

        streams.start();
        log.info("Kafka Streams 应用已启动");
        log.info("Application ID: {}", appConfig.getAppName());
        log.info("State Directory: {}", appConfig.getApp().getStateDir());

        printRuntimeInfo();
    }

    /**
     * 打印运行时信息
     */
    private void printRuntimeInfo() {
        new Thread(() -> {
            try {
                Thread.sleep(5000);
                while (streams != null && streams.state() != KafkaStreams.State.NOT_RUNNING) {
                    KafkaStreams.State state = streams.state();
                    log.info("当前状态：{}", state);
                    Thread.sleep(30000);
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
     * 获取 Streams 实例
     */
    public KafkaStreams getStreams() {
        return streams;
    }
}
