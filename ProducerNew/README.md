# ProducerNew - 可配置 Kafka Producer

基于配置文件实现的通用 Kafka 数据生成器，支持模拟 CDC 数据。

## 特性

- **完全可配置**：所有配置通过 YAML 文件管理
- **多表支持**：通过配置支持不同表结构的数据生成
- **数据生成器**：支持多种内置生成器（序列、身份证、姓名、电话等）
- **问题数据注入**：可配置概率生成问题数据用于测试
- **操作类型**：支持插入、更新、删除操作，概率可配

## 配置文件说明

### 1. connection-config.yaml - 连接配置

```yaml
kafka:
  bootstrap_servers: "localhost:19091,localhost:19092,localhost:19093"
  security:
    enabled: true
    protocol: "SASL_PLAINTEXT"
    mechanism: "PLAIN"
    username: "admin"
    password: "Audaque@123"

producer:
  retries: 3
  batch_size: 16384
  linger_ms: 1
  buffer_memory: 33554432
  acks: "all"
```

### 2. app-config.yaml - 应用配置

```yaml
app:
  name: "configurable-producer-app"
  log_dir: "./logs"

send:
  interval_ms: 1000          # 发送间隔
  duration_seconds: 0        # 运行时长 (0=无限)
  log_interval: 100          # 统计打印间隔

operation:
  insert_probability: 50     # 插入概率%
  update_probability: 45     # 更新概率%
  delete_probability: 5      # 删除概率%

data_quality:
  bad_data_probability: 10   # 问题数据概率%

source:
  table_name: "baseinfo"
  topic: "mytopic"
  database_name: "test"
  database_type: "mysql"
```

### 3. table-schema.yaml - 表结构配置

```yaml
tables:
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
        values:
          surnames: ["张", "王", "李"]
          names: ["伟", "芳", "娜"]
      - name: "sex"
        type: "STRING"
        generator: "random"
        values: ["男", "女"]
        bad_values: ["M", "F", "未知"]
        bad_probability: 10
      - name: "age"
        type: "INT"
        generator: "random_int"
        min: 18
        max: 80
        bad_values: [-5, 150]
        bad_probability: 10
```

## 数据生成器类型

| 生成器 | 说明 | 配置参数 |
|--------|------|----------|
| sequence | 序列生成 | sequence_name |
| idcard | 身份证号 | - |
| name | 姓名 | values.surnames, values.names |
| random | 随机选择 | values |
| random_select | 随机选择 | values |
| random_int | 随机整数 | min, max |
| random_double | 随机小数 | minDouble, maxDouble |
| birthdate | 出生日期 | - |
| phone | 电话号码 | - |
| current_timestamp | 当前时间戳 | - |
| recent_timestamp | 近期时间戳 | daysBack |
| constant | 常量值 | value |
| empty | 空值 | - |

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

# 运行 60 秒
./start.sh
# Ctrl+C 停止
```

### 停止

```bash
./stop.sh
```

## 问题数据

在字段配置中设置 `bad_values` 和 `bad_probability`：

```yaml
- name: "sex"
  type: "STRING"
  generator: "random"
  values: ["男", "女"]
  bad_values: ["M", "F", "未知", "male"]
  bad_probability: 10  # 10% 概率生成问题数据
```

## 操作类型概率

默认配置：
- 插入 (I): 50%
- 更新 (U): 45%
- 删除 (D): 5%

更新和删除操作会从已生成的数据池中随机选择一条已有数据。

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
