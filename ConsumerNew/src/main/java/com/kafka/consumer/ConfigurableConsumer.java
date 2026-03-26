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
 * 支持命令行参数指定 Topic 和配置文件。
 *
 * 使用方式:
 * 1. 指定配置目录（自动加载 schemas/ 和 rules/）
 *    java -jar consumer-new.jar --config-dir /path/to/config
 *
 * 2. 分别指定 schema 和 rules 文件
 *    java -jar consumer-new.jar --schema /path/to/schema.yaml --rules /path/to/rules.yaml
 *
 * 3. 同时指定 topic 和配置目录
 *    java -jar consumer-new.jar --topic mytopic --config-dir /path/to/config
 *
 * 命令行参数:
 * --topic <topic_name>       : 指定 Kafka Topic（一个 topic 对应一个表）
 * --config-dir <dir_path>    : 指定配置目录，自动加载 schema 和 rules
 * --schema <file_path>       : 指定表结构配置文件
 * --rules <file_path>        : 指定验证规则配置文件
 * --help                     : 显示帮助信息
 *
 * @author Kafka Demo
 * @version 2.0.0
 */
public class ConfigurableConsumer {

    private static final Logger log = LoggerFactory.getLogger(ConfigurableConsumer.class);

    /**
     * 命令行参数
     */
    private static class Args {
        String topic;
        String configDir;
        String schemaFile;
        String rulesFile;
        boolean help;

        boolean hasConfig() {
            return configDir != null || schemaFile != null || rulesFile != null;
        }

        boolean useConfigDir() {
            return configDir != null;
        }

        boolean useSeparateFiles() {
            return schemaFile != null || rulesFile != null;
        }
    }

    /**
     * 主程序入口
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) {
        System.out.println("=====================================");
        System.out.println("可配置 Kafka Streams Consumer");
        System.out.println("=====================================");

        // 1. 解析命令行参数
        Args parsedArgs = parseArgs(args);

        if (parsedArgs.help) {
            printHelp();
            return;
        }

        // 2. 确定配置文件路径
        String schemaConfigPath;
        String ruleConfigPath;

        if (parsedArgs.useConfigDir()) {
            // 从配置目录加载
            String configDir = parsedArgs.configDir;
            File dir = new File(configDir);
            if (!dir.exists()) {
                System.err.println("配置目录不存在：" + configDir);
                System.exit(1);
            }

            // 优先从 schemas/rules 子目录加载
            String schemasDir = configDir + "/schemas";
            String rulesDir = configDir + "/rules";

            if (new File(schemasDir).exists()) {
                schemaConfigPath = schemasDir;
            } else {
                // 查找第一个 YAML 文件作为 schema
                File[] yamlFiles = dir.listFiles((d, name) ->
                    name.endsWith(".yaml") || name.endsWith(".yml"));
                if (yamlFiles != null && yamlFiles.length > 0) {
                    schemaConfigPath = yamlFiles[0].getAbsolutePath();
                } else {
                    System.err.println("配置目录中未找到 YAML 配置文件：" + configDir);
                    System.exit(1);
                    return;
                }
            }

            if (new File(rulesDir).exists()) {
                ruleConfigPath = rulesDir;
            } else {
                // 查找包含 rule 的 YAML 文件
                File[] yamlFiles = dir.listFiles((d, name) ->
                    (name.endsWith(".yaml") || name.endsWith(".yml")) &&
                    name.toLowerCase().contains("rule"));
                if (yamlFiles != null && yamlFiles.length > 0) {
                    ruleConfigPath = yamlFiles[0].getAbsolutePath();
                } else {
                    System.err.println("配置目录中未找到规则文件：" + configDir);
                    System.exit(1);
                    return;
                }
            }

            System.out.println("配置目录：" + configDir);
            System.out.println("Schema 配置：" + schemaConfigPath);
            System.out.println("Rules 配置：" + ruleConfigPath);

        } else if (parsedArgs.useSeparateFiles()) {
            // 分别指定文件
            schemaConfigPath = parsedArgs.schemaFile;
            ruleConfigPath = parsedArgs.rulesFile;

            if (schemaConfigPath != null && !new File(schemaConfigPath).exists()) {
                System.err.println("Schema 配置文件不存在：" + schemaConfigPath);
                System.exit(1);
            }
            if (ruleConfigPath != null && !new File(ruleConfigPath).exists()) {
                System.err.println("Rules 配置文件不存在：" + ruleConfigPath);
                System.exit(1);
            }

            System.out.println("Schema 配置：" + schemaConfigPath);
            System.out.println("Rules 配置：" + ruleConfigPath);

        } else {
            // 使用默认路径
            schemaConfigPath = "src/main/resources/schemas";
            if (!new File(schemaConfigPath).exists()) {
                schemaConfigPath = "src/main/resources/table-schema.yaml";
            }
            ruleConfigPath = "src/main/resources/rules";
            if (!new File(ruleConfigPath).exists()) {
                ruleConfigPath = "src/main/resources/validation-rules.yaml";
            }

            System.out.println("使用默认配置路径");
            System.out.println("Schema 配置：" + schemaConfigPath);
            System.out.println("Rules 配置：" + ruleConfigPath);
        }

        System.out.println();

        try {
            // 3. 加载配置
            System.out.println("正在加载配置文件...");
            ConnectionConfigLoader connectionConfig = new ConnectionConfigLoader("src/main/resources/connection-config.yaml");
            AppConfigLoader appConfig = new AppConfigLoader("src/main/resources/app-config.yaml");

            // 如果命令行指定了 topic，覆盖配置
            if (parsedArgs.topic != null) {
                appConfig.getSource().setTopic(parsedArgs.topic);
                System.out.println("使用命令行指定的 Topic: " + parsedArgs.topic);
            }

            // 加载 schema 和 rules
            TableSchemaConfigLoader schemaConfig = new TableSchemaConfigLoader(schemaConfigPath);
            ValidationRuleConfigLoader ruleConfig = new ValidationRuleConfigLoader(ruleConfigPath);

            // 启动文件监听（动态刷新）
            schemaConfig.startWatching();
            ruleConfig.startWatching();

            // 打印配置信息
            connectionConfig.printConfig();
            appConfig.printConfig();
            schemaConfig.printConfig();
            ruleConfig.printConfig();

            // 4. 构建拓扑
            ConfigurableTopology topology = new ConfigurableTopology(
                    appConfig, ruleConfig, connectionConfig, schemaConfig
            );

            // 5. 获取 Kafka 配置
            Properties kafkaProps = connectionConfig.getKafkaProperties();

            // 6. 获取 Streams 配置
            Properties streamsProps = appConfig.getStreamsProperties(kafkaProps);

            // 7. 使用 CountDownLatch 等待关闭信号
            CountDownLatch shutdownLatch = new CountDownLatch(1);

            // 8. 添加关闭钩子
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n收到关闭信号，正在停止应用...");
                topology.stop();
                shutdownLatch.countDown();
            }));

            // 9. 启动 Streams 应用
            System.out.println("\n启动 Kafka Streams 应用...");
            topology.start(streamsProps);

            // 10. 等待关闭信号
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

    /**
     * 解析命令行参数
     */
    private static Args parseArgs(String[] args) {
        Args parsedArgs = new Args();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "--topic":
                case "-t":
                    if (i + 1 < args.length) {
                        parsedArgs.topic = args[++i];
                    } else {
                        System.err.println("错误：--topic 需要参数");
                        System.exit(1);
                    }
                    break;

                case "--config-dir":
                case "-c":
                    if (i + 1 < args.length) {
                        parsedArgs.configDir = args[++i];
                    } else {
                        System.err.println("错误：--config-dir 需要参数");
                        System.exit(1);
                    }
                    break;

                case "--schema":
                case "-s":
                    if (i + 1 < args.length) {
                        parsedArgs.schemaFile = args[++i];
                    } else {
                        System.err.println("错误：--schema 需要参数");
                        System.exit(1);
                    }
                    break;

                case "--rules":
                case "-r":
                    if (i + 1 < args.length) {
                        parsedArgs.rulesFile = args[++i];
                    } else {
                        System.err.println("错误：--rules 需要参数");
                        System.exit(1);
                    }
                    break;

                case "--help":
                case "-h":
                    parsedArgs.help = true;
                    break;

                default:
                    if (arg.startsWith("-")) {
                        System.err.println("未知参数：" + arg);
                        System.err.println("使用 --help 查看帮助");
                        System.exit(1);
                    }
                    break;
            }
        }

        return parsedArgs;
    }

    /**
     * 打印帮助信息
     */
    private static void printHelp() {
        System.out.println();
        System.out.println("用法:");
        System.out.println("  java -jar consumer-new.jar [选项]");
        System.out.println();
        System.out.println("选项:");
        System.out.println("  --topic, -t <topic_name>     指定 Kafka Topic（一个 topic 对应一个表）");
        System.out.println("  --config-dir, -c <dir_path>  指定配置目录，自动加载 schemas/ 和 rules/");
        System.out.println("  --schema, -s <file_path>     指定表结构配置文件");
        System.out.println("  --rules, -r <file_path>      指定验证规则配置文件");
        System.out.println("  --help, -h                   显示帮助信息");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  # 方式 1：指定配置目录（推荐）");
        System.out.println("  java -jar consumer-new.jar --config-dir /path/to/config");
        System.out.println();
        System.out.println("  # 方式 2：指定 topic 和配置目录");
        System.out.println("  java -jar consumer-new.jar --topic mytopic --config-dir /path/to/config");
        System.out.println();
        System.out.println("  # 方式 3：分别指定 schema 和 rules 文件");
        System.out.println("  java -jar consumer-new.jar --schema baseinfo.yaml --rules baseinfo-rules.yaml");
        System.out.println();
        System.out.println("配置目录结构:");
        System.out.println("  config/");
        System.out.println("  ├── schemas/              # 表结构配置目录");
        System.out.println("  │   └── baseinfo.yaml");
        System.out.println("  └── rules/                # 验证规则配置目录");
        System.out.println("      └── baseinfo-rules.yaml");
        System.out.println();
    }
}
