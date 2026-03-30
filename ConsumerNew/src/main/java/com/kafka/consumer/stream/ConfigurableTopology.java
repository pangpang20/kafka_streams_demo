package com.kafka.consumer.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.config.*;
import com.kafka.consumer.model.ProcessingResult;
import com.kafka.consumer.processor.MultiTableDataProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.streams.KeyValue;

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

    /**
     * 任务状态枚举
     */
    public enum TaskState {
        INIT("初始化"),
        STARTING("启动中"),
        RUNNING("运行中"),
        REBALANCING("重平衡中"),
        ERROR("错误"),
        STOPPING("停止中"),
        STOPPED("已停止");

        private final String description;

        TaskState(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * 任务运行时状态（线程安全）
     */
    public static class TaskStatus {
        private final String taskId;
        private final AtomicReference<TaskState> state;
        private final long startTime;
        private final AtomicLong endTime;
        private final AtomicReference<String> errorMessage;
        private final AtomicLong processedCount;
        private final AtomicLong validCount;
        private final AtomicLong invalidCount;

        public TaskStatus(String taskId) {
            this.taskId = taskId;
            this.state = new AtomicReference<>(TaskState.INIT);
            this.startTime = System.currentTimeMillis();
            this.endTime = new AtomicLong(0);
            this.errorMessage = new AtomicReference<>(null);
            this.processedCount = new AtomicLong(0);
            this.validCount = new AtomicLong(0);
            this.invalidCount = new AtomicLong(0);
        }

        public String getTaskId() {
            return taskId;
        }

        public TaskState getState() {
            return state.get();
        }

        public void setState(TaskState state) {
            this.state.set(state);
        }

        public long getStartTime() {
            return startTime;
        }

        public long getEndTime() {
            return endTime.get();
        }

        public void setEndTime(long endTime) {
            this.endTime.set(endTime);
        }

        public String getErrorMessage() {
            return errorMessage.get();
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage.set(errorMessage);
        }

        public long getProcessedCount() {
            return processedCount.get();
        }

        public void incrementProcessedCount() {
            processedCount.incrementAndGet();
        }

        public long getValidCount() {
            return validCount.get();
        }

        public void incrementValidCount() {
            validCount.incrementAndGet();
        }

        public long getInvalidCount() {
            return invalidCount.get();
        }

        public void incrementInvalidCount() {
            invalidCount.incrementAndGet();
        }

        public long getRunningTime() {
            long end = endTime.get();
            if (end > 0) {
                return end - startTime;
            }
            return System.currentTimeMillis() - startTime;
        }
    }

    private final AppConfigLoader appConfig;
    private final MultiTableDataProcessor processor;
    private final ObjectMapper objectMapper;
    private final MultiTableDataWriter dataWriter;

    private KafkaStreams streams;
    private TaskStatus taskStatus;

    /**
     * 构造函数
     */
    public ConfigurableTopology(AppConfigLoader appConfig,
                                 ValidationRuleConfigLoader ruleConfig,
                                 ConnectionConfigLoader connectionConfig,
                                 TableSchemaConfigLoader schemaConfig) {
        this(appConfig, ruleConfig, connectionConfig, schemaConfig, UUID.randomUUID().toString());
    }

    /**
     * 构造函数（带 taskId）
     */
    public ConfigurableTopology(AppConfigLoader appConfig,
                                 ValidationRuleConfigLoader ruleConfig,
                                 ConnectionConfigLoader connectionConfig,
                                 TableSchemaConfigLoader schemaConfig,
                                 String taskId) {
        this.appConfig = appConfig;
        this.processor = new MultiTableDataProcessor(ruleConfig);

        // 初始化多表写入器
        if (appConfig.isWriteToOceanbase()) {
            this.dataWriter = new MultiTableDataWriter(connectionConfig, schemaConfig, appConfig);
        } else {
            this.dataWriter = null;
        }

        this.objectMapper = new ObjectMapper();
        this.taskStatus = new TaskStatus(taskId);

        // 注册已知表
        if (schemaConfig.getTables() != null) {
            for (TableSchemaConfigLoader.TableConfig table : schemaConfig.getTables()) {
                processor.registerTables(java.util.Collections.singletonList(table.getName()));
            }
        }

        log.info("ConfigurableTopology 初始化完成（多表模式）");
        log.info("  Task ID: {}", taskId);
        log.info("  源 Topic: {}", appConfig.getSourceTopic());
        log.info("  写入 Topic: {}", appConfig.isWriteToTopic());
        log.info("  写入 OceanBase: {}", appConfig.isWriteToOceanbase());
        log.info("  已知表数量：{}", processor.getKnownTables().size());
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

        // 数据处理（使用 map 而不是 mapValues，以便访问 key）
        KStream<String, ProcessingResult> processedStream = sourceStream.map((key, value) -> {
            try {
                ProcessingResult result = processor.process(key, value);
                return KeyValue.pair(key, result);
            } catch (Exception e) {
                log.error("数据处理异常：{}", e.getMessage(), e);
                ProcessingResult errorResult = new ProcessingResult();
                errorResult.setSuccess(false);
                errorResult.setMessage("处理异常：" + e.getMessage());
                return KeyValue.pair(key, errorResult);
            }
        });

        log.info("已定义处理流：MultiTableDataProcessor（多表模式）");

        // 日志记录和数据库写入
        processedStream.peek((key, result) -> {
            if (result == null) {
                return;
            }
            // 更新统计计数
            taskStatus.incrementProcessedCount();
            if (result.isSuccess()) {
                taskStatus.incrementValidCount();
            } else {
                taskStatus.incrementInvalidCount();
            }

            String tableName = result.getTableName() != null ? result.getTableName() : "unknown";
            if (result.isSuccess()) {
                log.info("[PASS] key={}, opcode={}, table={}",
                        key,
                        result.getOpcode(),
                        tableName);
                // 写入正常数据到 OceanBase
                if (dataWriter != null) {
                    dataWriter.logValidData(result);
                }
            } else {
                log.warn("[FAIL] key={}, table={}, reason={}", key, tableName, result.getMessage());
                // 写入异议数据到 OceanBase
                if (dataWriter != null) {
                    dataWriter.logInvalidData(result);
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
        log.info("  {} -> [MultiTableDataProcessor] -> {} / {}",
                appConfig.getSourceTopic(),
                appConfig.isWriteToTopic() ? appConfig.getSource().getValidDataTopic() : "(不写入 Topic)",
                appConfig.isWriteToTopic() ? appConfig.getSource().getInvalidDataTopic() : "(不写入 Topic)");
        log.info("已知表：{}", String.join(", ", processor.getKnownTables()));
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
        log.info("Task ID: {}", taskStatus.getTaskId());

        taskStatus.setState(TaskState.STARTING);

        try {
            StreamsBuilder builder = buildTopology();
            streams = new KafkaStreams(builder.build(), new StreamsConfig(props));

            // 添加状态监听
            streams.setStateListener((newState, oldState) -> {
                log.info("状态变化：{} -> {}", oldState, newState);
                if (newState == KafkaStreams.State.RUNNING) {
                    taskStatus.setState(TaskState.RUNNING);
                } else if (newState == KafkaStreams.State.REBALANCING) {
                    taskStatus.setState(TaskState.REBALANCING);
                } else if (newState == KafkaStreams.State.NOT_RUNNING) {
                    taskStatus.setState(TaskState.STOPPED);
                    taskStatus.setEndTime(System.currentTimeMillis());
                }
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("收到关闭信号，正在停止 Streams 应用...");
                taskStatus.setState(TaskState.STOPPING);
                stop();
                taskStatus.setState(TaskState.STOPPED);
                taskStatus.setEndTime(System.currentTimeMillis());
                if (dataWriter != null) {
                    dataWriter.close();
                }
                log.info("Streams 应用已停止");
            }));

            streams.start();
            taskStatus.setState(TaskState.RUNNING);
            log.info("Kafka Streams 应用已启动");
            log.info("Task ID: {}", taskStatus.getTaskId());
            log.info("Application ID: {}", appConfig.getAppName());
            log.info("State Directory: {}", appConfig.getApp().getStateDir());

            printRuntimeInfo();

        } catch (Exception e) {
            log.error("启动失败：{}", e.getMessage(), e);
            taskStatus.setState(TaskState.ERROR);
            taskStatus.setErrorMessage(e.getMessage());
            taskStatus.setEndTime(System.currentTimeMillis());
            throw new RuntimeException("Kafka Streams 启动失败", e);
        }
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
                    log.info("当前状态：{} | Task ID: {} | 运行时长：{} 秒",
                            state, taskStatus.getTaskId(), taskStatus.getRunningTime() / 1000);
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

    /**
     * 获取任务状态
     */
    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    /**
     * 获取 Task ID
     */
    public String getTaskId() {
        return taskStatus != null ? taskStatus.getTaskId() : null;
    }

    /**
     * 将任务状态转换为 JSON
     */
    public String getTaskStatusJson() {
        if (taskStatus == null) {
            return "{\"error\": \"任务未初始化\"}";
        }
        return String.format(
            "{\"taskId\":\"%s\",\"state\":\"%s\",\"stateDesc\":\"%s\",\"startTime\":%d,\"runningTime\":%d,\"processedCount\":%d,\"validCount\":%d,\"invalidCount\":%d,\"errorMessage\":%s}",
            taskStatus.getTaskId(),
            taskStatus.getState().name(),
            taskStatus.getState().getDescription(),
            taskStatus.getStartTime(),
            taskStatus.getRunningTime(),
            taskStatus.getProcessedCount(),
            taskStatus.getValidCount(),
            taskStatus.getInvalidCount(),
            taskStatus.getErrorMessage() != null ? "\"" + taskStatus.getErrorMessage() + "\"" : "null"
        );
    }
}
