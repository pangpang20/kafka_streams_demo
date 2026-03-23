# CDC 数据生成器 (Producer)

模拟数据库 CDC（Change Data Capture）变更数据生成器，用于向 Kafka Topic 发送模拟的数据库变更记录。

## 功能特性

- **三种操作类型**: 插入 (50%)、更新 (45%)、删除 (5%)
- **问题数据生成**: 10% 概率生成问题数据，用于测试质量检查
- **定时发送**: 每秒发送一条记录
- **可配置**: 支持 SASL 认证、操作概率等配置

## 数据结构

```json
{
  "data": [{
    "keyfields": "idcard",
    "pos": "bin.001327,218472920#218473551",
    "databasename": "test",
    "allfields": {
      "after_data": {
        "字段名": {"type": "类型", "value": "值"}
      },
      "before_data": {}
    },
    "tablename": "baseinfo",
    "opcode": "I",
    "type": "mysql",
    "timestamp": "2025-11-19 11:34:43.0"
  }]
}
```

## 快速开始

### 1. 启动 Kafka 集群

```bash
cd ../docker
./start.sh
```

### 2. 创建 Topic

```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic --partitions 3 --replication-factor 3
```

### 3. 启动生产者

```bash
./start.sh          # 无限运行
./start.sh 60       # 运行 60 秒
```

## 配置说明

编辑 `src/main/java/com/kafka/producer/config/ProducerConfig.java`:

```java
// Kafka 连接配置
public static final String BOOTSTRAP_SERVERS = "localhost:19091,localhost:19092,localhost:19093";
public static final String TOPIC_NAME = "mytopic";

// SASL 认证配置
public static final boolean ENABLE_SASL = false;  // 改为 true 启用认证

// 数据生成配置
public static final int SEND_INTERVAL_MS = 1000;      // 发送间隔
public static final int INSERT_PROBABILITY = 50;      // 插入概率
public static final int UPDATE_PROBABILITY = 45;      // 更新概率
public static final int DELETE_PROBABILITY = 5;       // 删除概率
public static final int BAD_DATA_PROBABILITY = 10;    // 问题数据概率
```

## 问题数据类型

生产者会生成以下类型的问题数据（用于测试质量检查）：

| 字段 | 问题类型 | 示例 |
|------|----------|------|
| sex | 非法枚举值 | M, F, 未知，male |
| age | 超出范围 | -5, 150+ |
| telephone | 格式错误 | 138123, 138abc12345 |
| bloodtype | 非法枚举值 | A 型，C, Rh |
| creditscore | 超出范围 | -100, 1500+ |
| housingareas | 负数 | -100 |

## 构建

```bash
mvn clean package
```

生成的 jar 包：`target/kafka-producer-1.0.0-jar-with-dependencies.jar`
