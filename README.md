# Kafka Streams 数据清洗示例

基于 Kafka 2.8.2 和 Kafka Streams 的数据清洗加工示例项目。

## 项目结构

```
kafka_streams_demo/
├── docker/                     # Docker 部署配置
│   ├── docker-compose.yml      # Kafka 三节点集群配置
│   ├── docker-compose-sasl.yml # SASL 认证版本配置
│   ├── start.sh                # 集群启动脚本
│   ├── start-sasl.sh           # SASL 版本启动脚本
│   ├── stop.sh                 # 集群停止脚本
│   └── stop-sasl.sh            # SASL 版本停止脚本
├── Producer/                   # Kafka 生产者示例
│   ├── pom.xml
│   └── src/main/java/com/kafka/producer/
│       ├── CdcDataProducer.java    # CDC 数据生产者
│       ├── config/
│       │   └── ProducerConfig.java # 生产者配置
│       ├── model/
│       │   └── CdcEvent.java       # CDC 事件模型
│       └── generator/
│           └── RandomDataGenerator.java # 随机数据生成器
└── Consumer/                   # Kafka Streams 消费者示例
    ├── pom.xml
    └── src/main/java/com/kafka/consumer/
        ├── config/
        │   └── ConsumerConfig.java # Streams 配置
        └── model/
            ├── CdcEvent.java           # CDC 事件模型
            └── QualityCheckResult.java # 质检结果模型
```

## 快速启动

### 1. 启动 Kafka 集群

```bash
# 启动标准版本（无认证）
cd docker && ./start.sh

# 或启动 SASL 认证版本
cd docker && ./start-sasl.sh
```

启动后可访问 Kafka UI 管理界面：http://localhost:28080

### 2. 配置说明

#### Producer 配置 (ProducerConfig.java)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| BOOTSTRAP_SERVERS | localhost:19091,localhost:19092,localhost:19093 | Kafka 集群地址 |
| TOPIC_NAME | mytopic | 目标 Topic |
| ENABLE_SASL | false | 是否启用 SASL 认证 |
| SEND_INTERVAL_MS | 1000 | 发送间隔（毫秒） |
| BAD_DATA_PROBABILITY | 10 | 问题数据概率（%） |

#### Consumer 配置 (ConsumerConfig.java)

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| BOOTSTRAP_SERVERS | localhost:19091,localhost:19092,localhost:19093 | Kafka 集群地址 |
| SOURCE_TOPIC | mytopic | 源 Topic |
| VALID_DATA_TOPIC | mytopic-valid | 正常数据输出 Topic |
| INVALID_DATA_TOPIC | mytopic-invalid | 异常数据输出 Topic |
| APPLICATION_ID | quality-check-app | Streams 应用 ID |
| ENABLE_SASL | false | 是否启用 SASL 认证 |

### 3. 数据质量检查规则

Consumer 内置以下质检规则（可配置）：

| 规则 | 检查项 | 默认值 |
|------|--------|--------|
| 性别检查 | CHECK_SEX | 男/女 |
| 年龄检查 | CHECK_AGE | 0-120 |
| 电话检查 | CHECK_TELEPHONE | ^1[3-9]\d{9}$ |
| 血型检查 | CHECK_BLOOD_TYPE | A/B/O/AB |
| 信用分检查 | CHECK_CREDIT_SCORE | 0-1000 |
| 住房面积检查 | CHECK_HOUSING_AREA | >= 0 |

## SASL 认证配置

如需启用 SASL 认证：

1. 使用 SASL 版本的启动脚本：
   ```bash
   ./start-sasl.sh
   ```

2. 修改 Producer/Consumer 配置：
   ```java
   // ProducerConfig.java 或 ConsumerConfig.java
   public static final boolean ENABLE_SASL = true;
   ```

3. SASL 认证信息：
   - 用户名：`admin`
   - 密码：`Audaque@123`
   - 安全协议：`SASL_PLAINTEXT`
   - 认证机制：`PLAIN`

## 常用命令

```bash
# 查看 Topic 列表
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9091 --list

# 创建 Topic
docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:9091 \
  --create --topic my-topic --partitions 3 --replication-factor 3

# 发送消息
docker exec -it kafka-1 kafka-console-producer \
  --bootstrap-server kafka-1:9091 --topic my-topic

# 消费消息
docker exec -it kafka-1 kafka-console-consumer \
  --bootstrap-server kafka-1:9091 --topic my-topic --from-beginning

# 停止集群
cd docker && ./stop.sh

# 停止 SASL 集群
cd docker && ./stop-sasl.sh
```

## 技术栈

- Kafka 2.8.2 (Confluent Platform 6.2.0)
- Kafka Streams
- Zookeeper 3 节点集群
- Kafka 3 节点集群（3 副本配置）
- Kafka UI (provectuslabs/kafka-ui)

## 许可证

MIT License
