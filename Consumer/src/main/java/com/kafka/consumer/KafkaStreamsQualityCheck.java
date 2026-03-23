package com.kafka.consumer;

import com.kafka.consumer.config.ConsumerConfig;
import com.kafka.consumer.stream.QualityCheckTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Kafka Streams 数据质量检查应用 - 主程序
 *
 * ================================================
 * 程序说明
 * ================================================
 *
 * 本程序实现了一个结构化的 Kafka Streams 数据处理应用，
 * 用于对 CDC（Change Data Capture）数据进行质量检查。
 *
 * 处理流程:
 * 1. 从 Kafka Topic (mytopic) 读取 CDC 数据
 * 2. 执行数据处理流程:
 *    - 认证 (Authentication): 验证数据源合法性
 *    - 解析 (Parsing): 将 JSON 解析为对象
 *    - 验证 (Validation): 执行业务规则检查
 * 3. 根据检查结果分发数据:
 *    - 正常数据 -> mytopic-valid
 *    - 异常数据 -> mytopic-invalid
 *
 * 质量检查规则:
 * - 性别：只能是"男"或"女"
 * - 年龄：0-120 岁
 * - 电话：符合中国手机号格式
 * - 血型：A/B/O/AB
 * - 信用分：0-1000
 * - 住房面积：非负数
 *
 * ================================================
 * 开发指南
 * ================================================
 *
 * 1. 添加新的验证规则:
 *    - 在 ConsumerConfig 中添加配置项
 *    - 在 DataValidator 中添加验证方法
 *
 * 2. 修改处理流程:
 *    - 在 DataProcessor 中调整处理顺序
 *    - 在 QualityCheckTopology 中修改拓扑结构
 *
 * 3. 扩展输出目标:
 *    - 在 ConsumerConfig 中添加新 Topic
 *    - 在 QualityCheckTopology 中添加新的分支
 *
 * @author Kafka Demo
 * @version 1.0.0
 */
public class KafkaStreamsQualityCheck {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsQualityCheck.class);

    /**
     * 主程序入口
     *
     * @param args 命令行参数（暂不使用）
     */
    public static void main(String[] args) {
        System.out.println("=====================================");
        System.out.println("Kafka Streams 数据质量检查应用");
        System.out.println("=====================================");

        // 打印配置信息
        ConsumerConfig.printConfig();

        // 获取配置
        Properties props = ConsumerConfig.getStreamsProperties();

        // 创建拓扑构建器
        QualityCheckTopology topology = new QualityCheckTopology();

        // 使用 CountDownLatch 等待关闭信号
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n收到关闭信号，正在停止应用...");
            topology.stop();
            shutdownLatch.countDown();
        }));

        // 启动 Streams 应用
        System.out.println("\n启动 Kafka Streams 应用...");
        topology.start(props);

        // 等待关闭信号
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("主线程被中断", e);
        }

        System.out.println("应用已停止");
    }
}
