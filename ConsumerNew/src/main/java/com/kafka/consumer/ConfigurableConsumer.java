package com.kafka.consumer;

import com.kafka.consumer.config.*;
import com.kafka.consumer.stream.ConfigurableTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * 可配置 Kafka Streams Consumer - 主程序
 *
 * ================================================
 * 程序说明
 * ================================================
 *
 * 本程序实现了一个完全可配置的 Kafka Streams 数据处理应用，
 * 所有配置通过 YAML 文件管理，无需修改代码即可适应不同的表结构。
 *
 * 配置说明：
 * 1. 修改 src/main/resources 目录下的配置文件
 * 2. 重新编译并运行
 *
 * 配置文件：
 * - connection-config.yaml: Kafka 和 OceanBase 连接配置
 * - app-config.yaml: 应用级配置（表名、Topic、写入开关等）
 * - table-schema.yaml: 表结构定义
 * - validation-rules.yaml: 数据验证规则
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class ConfigurableConsumer {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableConsumer.class);

    /**
     * 主程序入口
     *
     * @param args 命令行参数
     *             args[0]: 配置文件路径 (可选，默认 src/main/resources)
     */
    public static void main(String[] args) {
        System.out.println("=====================================");
        System.out.println("可配置 Kafka Streams Consumer");
        System.out.println("=====================================");

        // 确定配置文件路径
        String configPath = "src/main/resources";
        if (args.length > 0) {
            configPath = args[0];
        }

        // 检查配置文件是否存在
        File configDir = new File(configPath);
        if (!configDir.exists()) {
            System.err.println("配置文件目录不存在：" + configPath);
            System.exit(1);
        }

        System.out.println("配置文件路径：" + configPath);
        System.out.println();

        try {
            // 1. 加载配置
            System.out.println("正在加载配置文件...");
            ConnectionConfigLoader connectionConfig = new ConnectionConfigLoader(configPath + "/connection-config.yaml");
            AppConfigLoader appConfig = new AppConfigLoader(configPath + "/app-config.yaml");

            // 支持目录加载：优先从 schemas/rules 目录加载，否则使用单文件
            String schemaConfigPath = configPath + "/schemas";
            if (!new File(schemaConfigPath).exists()) {
                schemaConfigPath = configPath + "/table-schema.yaml";
            }
            TableSchemaConfigLoader schemaConfig = new TableSchemaConfigLoader(schemaConfigPath);

            String ruleConfigPath = configPath + "/rules";
            if (!new File(ruleConfigPath).exists()) {
                ruleConfigPath = configPath + "/validation-rules.yaml";
            }
            ValidationRuleConfigLoader ruleConfig = new ValidationRuleConfigLoader(ruleConfigPath);

            // 启动文件监听（动态刷新）
            schemaConfig.startWatching();
            ruleConfig.startWatching();

            // 打印配置信息
            connectionConfig.printConfig();
            appConfig.printConfig();
            schemaConfig.printConfig();
            ruleConfig.printConfig();

            // 2. 构建拓扑
            ConfigurableTopology topology = new ConfigurableTopology(
                    appConfig, ruleConfig, connectionConfig, schemaConfig
            );

            // 3. 获取 Kafka 配置
            Properties kafkaProps = connectionConfig.getKafkaProperties();

            // 4. 获取 Streams 配置
            Properties streamsProps = appConfig.getStreamsProperties(kafkaProps);

            // 5. 使用 CountDownLatch 等待关闭信号
            CountDownLatch shutdownLatch = new CountDownLatch(1);

            // 6. 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n收到关闭信号，正在停止应用...");
                topology.stop();
                shutdownLatch.countDown();
            }));

            // 7. 启动 Streams 应用
            System.out.println("\n启动 Kafka Streams 应用...");
            topology.start(streamsProps);

            // 8. 等待关闭信号
            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("主线程被中断", e);
            }

            System.out.println("应用已停止");

        } catch (Exception e) {
            System.err.println("启动失败：" + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
