# ProducerNew - 可配置 Kafka Producer（多表多 Topic 支持）

基于配置文件实现的通用 Kafka 数据生成器，支持模拟 CDC 数据，**支持多表多 Topic 配置和 Topic 自动创建**。

## 特性

- **完全可配置**：所有配置通过 YAML 文件管理
- **多表多 Topic 支持**：通过配置支持多个表，每个表发送到不同的 Topic
- **Topic 自动创建**：启动时自动检查并创建不存在的 Topic
- **数据生成器**：支持多种内置生成器（序列、身份证、姓名、电话等）
- **问题数据注入**：可配置概率生成问题数据用于测试
- **操作类型**：支持插入、更新、删除操作，概率可配
- **每表独立配置**：每个表可以有自己的发送间隔
- **正常/异常数据分离**：每个表可配置正常数据 topic 和异常数据 topic

## 快速开始

### 编译

```bash
cd /opt/kafka_streams_demo/ProducerNew
mvn clean package
```

### 启动

```bash
# 使用默认配置路径
./start.sh

# 指定配置文件路径（config 目录）
./start.sh config

# 运行指定时长后自动停止（秒）
./start.sh config 60
```

### 停止

```bash
# Ctrl+C 或
kill <pid>
```

## 配置文件说明

ProducerNew 有三个配置文件，都在 `config/` 目录下：

### 1. connection-config.yaml - 连接配置

配置 Kafka 连接信息和 Producer 性能参数。

```yaml
kafka:
  bootstrap_servers: "localhost:19091,localhost:19092,localhost:19093"
  security:
    enabled: true                    # 是否启用 SASL 认证
    protocol: "SASL_PLAINTEXT"
    mechanism: "PLAIN"
    username: "admin"
    password: "Audaque@123"

producer:
  retries: 3
  batch_size: 16384
  linger_ms: 1
  buffer_memory: 33554432
  acks: "all"                        # all/-1: 所有 ISR 副本确认; 1: leader 确认; 0: 不确认
```

### 2. app-config.yaml - 应用配置

配置应用行为、多表映射关系和 Topic 自动创建参数。

```yaml
app:
  name: "configurable-producer-app"
  log_dir: "./logs"

# 发送配置
send:
  interval_ms: 1000          # 默认发送间隔（毫秒）
  duration_seconds: 0        # 运行时长 (0=无限运行)
  log_interval: 100          # 每发送多少条打印一次统计

# 操作概率配置（总和应为 100）
operation:
  insert_probability: 50     # 插入概率%
  update_probability: 45     # 更新概率%
  delete_probability: 5      # 删除概率%

# 数据质量配置
data_quality:
  bad_data_probability: 10   # 问题数据概率%

# Topic 自动创建配置
topic:
  auto_create: true          # 是否自动创建 Topic
  partitions: 3              # 默认分区数
  replication_factor: 3      # 默认副本数

# 多表配置（推荐方式）
# 配置后，每个表的数据会发送到不同的 Topic
# 每个表可以配置三个 Topic：
# - topic: 主 Topic（原始数据，包含正常和异常数据）
# - valid_data_topic: 正常数据 Topic（可选）
# - invalid_data_topic: 异常数据 Topic（可选）
tables:
  - table_name: "baseinfo"
    topic: "baseinfo-topic"
    valid_data_topic: "baseinfo-valid"
    invalid_data_topic: "baseinfo-invalid"
    database_name: "test"
    database_type: "mysql"
    enabled: true
    # 可选：指定该表的发送间隔，不指定则使用全局 send.interval_ms
    # interval_ms: 2000

  - table_name: "orderinfo"
    topic: "orderinfo-topic"
    valid_data_topic: "orderinfo-valid"
    invalid_data_topic: "orderinfo-invalid"
    database_name: "test"
    database_type: "mysql"
    enabled: true
    interval_ms: 1500

# 单表模式配置（当 tables 为空时使用，向后兼容）
source:
  table_name: "baseinfo"
  topic: "mytopic"
  database_name: "test"
  database_type: "mysql"
```

### 3. table-schema.yaml - 表结构配置

定义表的字段、数据类型和数据生成规则。

```yaml
tables:
  # 表 1: baseinfo 人员基本信息表
  - name: "baseinfo"
    description: "人员基本信息表"
    primary_key: "idcard"
    key_fields: "idcard"
    fields:
      - name: "personid"
        type: "BIGINT"
        generator: "sequence"
        sequence_name: "personid_seq"
      - name: "idcard"
        type: "STRING"
        generator: "idcard"
      - name: "name"
        type: "STRING"
        generator: "name"
        values_map:
          surnames: ["张", "王", "李"]
          names: ["伟", "芳", "娜"]
      - name: "sex"
        type: "STRING"
        generator: "random"
        values: ["男", "女"]
        bad_values: ["M", "F", "未知"]
        bad_probability: 10

  # 表 2: orderinfo 订单信息表
  - name: "orderinfo"
    description: "订单信息表"
    primary_key: "orderid"
    key_fields: "orderid"
    fields:
      - name: "orderid"
        type: "BIGINT"
        generator: "sequence"
        sequence_name: "orderid_seq"
      - name: "userid"
        type: "STRING"
        generator: "uuid"
      - name: "productname"
        type: "STRING"
        generator: "random_select"
        values: ["iPhone 15", "MacBook Pro", "iPad Air"]
```

## 多表多 Topic 配置示例

### 场景：同时生成 baseinfo 和 orderinfo 两个表的数据

**app-config.yaml**:

```yaml
send:
  interval_ms: 1000  # 默认间隔
  duration_seconds: 0
  log_interval: 100

topic:
  auto_create: true
  partitions: 3
  replication_factor: 3

tables:
  # 表 1: baseinfo
  - table_name: "baseinfo"
    topic: "baseinfo-topic"           # 主 Topic（原始数据）
    valid_data_topic: "baseinfo-valid"     # 正常数据 Topic
    invalid_data_topic: "baseinfo-invalid" # 异常数据 Topic
    database_name: "test"
    database_type: "mysql"
    enabled: true
    interval_ms: 1000  # 每秒 1 条

  # 表 2: orderinfo
  - table_name: "orderinfo"
    topic: "orderinfo-topic"
    valid_data_topic: "orderinfo-valid"
    invalid_data_topic: "orderinfo-invalid"
    database_name: "test"
    database_type: "mysql"
    enabled: true
    interval_ms: 500   # 每秒 2 条
```

### Topic 说明

| Topic 类型 | 说明 | 数据内容 |
|------------|------|----------|
| `{table}-topic` | 主 Topic | 包含所有原始数据（正常 + 异常） |
| `{table}-valid` | 正常数据 Topic | 仅包含验证通过的正常数据 |
| `{table}-invalid` | 异常数据 Topic | 仅包含验证失败的异常数据 |

### 执行流程

1. ProducerNew 启动时读取 `tables` 配置
2. 检查每个表对应的 Topic 是否存在
3. 如果 Topic 不存在且 `topic.auto_create=true`，则自动创建
4. 轮询每个表，按照配置的 `interval_ms` 发送数据
5. 每个表有独立的数据池，更新/删除操作只在各自表的数据池中选择

## Topic 自动创建

### 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `topic.auto_create` | `true` | 是否自动创建 Topic |
| `topic.partitions` | `3` | Topic 分区数 |
| `topic.replication_factor` | `3` | Topic 副本数 |

### 工作原理

1. 启动时读取所有启用的表配置
2. 收集所有需要使用的 Topic 名称（去重）
3. 通过 Kafka AdminClient 检查每个 Topic 是否存在
4. 对不存在的 Topic 执行创建操作
5. 如果 Topic 已存在，跳过创建

### 关闭自动创建

如果不想自动创建 Topic，可以设置为 `false`：

```yaml
topic:
  auto_create: false
```

此时需要手动创建 Topic：

```bash
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic --partitions 3 --replication-factor 3
```

## 数据生成器类型

| 生成器 | 说明 | 配置参数 |
|--------|------|----------|
| `sequence` | 序列生成 | `sequence_name` |
| `idcard` | 身份证号 | - |
| `name` | 姓名 | `values.surnames`, `values.names` |
| `random` / `random_select` | 随机选择 | `values` |
| `random_int` | 随机整数 | `min`, `max` |
| `random_double` | 随机小数 | `minDouble`, `maxDouble` |
| `birthdate` | 出生日期 | - |
| `phone` | 电话号码 | - |
| `current_timestamp` | 当前时间戳 | - |
| `recent_timestamp` | 近期时间戳 | `daysBack` |
| `constant` | 常量值 | `value` |
| `empty` | 空值 | - |

## 问题数据配置

在字段配置中设置 `bad_values` 和 `bad_probability`：

```yaml
- name: "sex"
  type: "STRING"
  generator: "random"
  values: ["男", "女"]
  bad_values: ["M", "F", "未知", "male", "female"]
  bad_probability: 10  # 10% 概率生成问题数据
```

### 常见问题数据类型

| 字段 | 问题类型 | 示例配置 |
|------|----------|----------|
| sex | 非法枚举值 | `bad_values: ["M", "F", "未知", "male"]` |
| age | 超出范围 | `bad_values: [-5, 150, 200]` |
| telephone | 格式错误 | `bad_values: ["138123", "138abc12345"]` |
| bloodtype | 非法枚举值 | `bad_values: ["A 型", "C", "Rh"]` |
| creditscore | 超出范围 | `bad_values: [-100.0, 1500.0]` |
| housingareas | 负数 | `bad_values: [-100.0]` |

## 操作类型概率

默认配置：
- 插入 (I): 50%
- 更新 (U): 45%
- 删除 (D): 5%

**注意**：更新和删除操作需要从已生成的数据池中选择数据，如果数据池为空，会自动降级为插入操作。

## CDC 数据格式

生成的 CDC 数据符合 Debezium 格式：

```json
{
  "data": [{
    "databasename": "test",
    "tablename": "baseinfo",
    "opcode": "I",
    "type": "mysql",
    "timestamp": "2024-01-01 12:00:00.123",
    "pos": "bin.123456,1234567890123456789",
    "keyfields": "idcard",
    "allfields": {
      "afterData": {
        "idcard": {"type": "null:STRING", "value": "110101199001011234"},
        "name": {"type": "null:STRING", "value": "张伟"},
        "sex": {"type": "null:STRING", "value": "男"}
      }
    }
  }]
}
```

### 字段类型映射

| Java 类型 | Kafka Connect 类型 |
|-----------|-------------------|
| null | `null:STRING` |
| Long | `null:INT64` |
| Integer | `null:INT32` |
| Double | `null:FLOAT64` |
| BigDecimal | `org.apache.kafka.connect.data.Decimal:BYTES` |
| LocalDateTime | `io.debezium.time.Timestamp:INT64` |
| String | `null:STRING` |

## 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `configPath` | 配置文件路径 | `src/main/resources` |

### 使用示例

```bash
# 使用默认配置
./start.sh

# 使用外部配置目录
./start.sh /etc/kafka-producer/config

# 运行 60 秒后自动停止
java -jar target/kafka-producer-new-1.0.0-jar-with-dependencies.jar src/main/resources
```

## 日志说明

### 启动日志

```
=====================================
可配置 Kafka Producer (多表多 Topic 支持)
=====================================
配置文件路径：src/main/resources

=====================================
Producer 连接配置
=====================================
Bootstrap Servers: localhost:19091,localhost:19092,localhost:19093
SASL 认证：已启用
...

=====================================
Producer 应用配置
=====================================
...
Topic 自动创建:
  自动创建：是
  分区数：3
  副本数：3
-------------------------------------
多表配置 (共 2 个表):
  - 表名：baseinfo, Topic: mytopic, 数据库：test (mysql)
  - 表名：orderinfo, Topic: order-topic, 数据库：test (mysql)
```

### Topic 创建日志

```
INFO  Topic 已存在：mytopic
INFO  Topic 创建成功：order-topic (partitions=3, replicationFactor=3)
```

### 运行时日志

```
INFO  已发送 100 条记录
  表 baseinfo:50 条，数据池大小：45
  表 orderinfo:50 条，数据池大小：48
```

## 常见问题

### Q: 如何只生成一个表的数据？

使用单表模式，在 `app-config.yaml` 中不配置 `tables`，只配置 `source`：

```yaml
# 不配置 tables
# tables: []

source:
  table_name: "baseinfo"
  topic: "mytopic"
  database_name: "test"
  database_type: "mysql"
```

### Q: 如何让某个表停止发送数据？

将对应表的 `enabled` 设置为 `false`：

```yaml
tables:
  - table_name: "baseinfo"
    topic: "mytopic"
    enabled: false  # 暂停发送
```

### Q: Topic 创建失败怎么办？

检查以下几点：
1. Kafka 集群是否正常
2. SASL 认证配置是否正确
3. `replication_factor` 不能超过 Kafka 节点数
4. 查看日志中的具体错误信息

### Q: 如何验证 Topic 是否创建成功？

```bash
# 列出所有 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 --list

# 查看 Topic 详情
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --describe --topic mytopic
```

## 配置文件完整示例

参考 `src/main/resources/` 目录下的示例配置文件：
- `connection-config.yaml` - 连接配置示例
- `app-config.yaml` - 应用配置示例（支持多表）
- `table-schema.yaml` - 表结构配置示例
