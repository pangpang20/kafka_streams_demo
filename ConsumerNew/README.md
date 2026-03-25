# ConsumerNew - 可配置 Kafka Streams Consumer

基于配置文件实现的通用 Kafka Streams 数据质量检查系统，支持动态规则刷新。

## 特性

- **完全可配置**：所有配置通过 YAML 文件管理，无需修改代码
- **动态规则刷新**：验证规则修改后自动生效，无需重启应用
- **多表支持**：通过配置支持不同表结构的数据处理
- **灵活输出**：可选择是否写入 Kafka Topic 和 OceanBase 数据库
- **自动建表**：支持自动在 OceanBase 中创建目标表

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

oceanbase:
  host: "localhost"
  port: 2881
  username: "root@test"
  password: "Audaque@123"
  database: "kafka_quality_check"
```

### 2. app-config.yaml - 应用配置

```yaml
app:
  name: "configurable-consumer-app"
  state_dir: "/tmp/kafka-streams-consumer"
  auto_offset_reset: "earliest"

source:
  table_name: "baseinfo"       # 源表名
  topic: "mytopic"             # 源 Topic
  auto_create_table: true      # 是否自动建表
  write_to_topic: true         # 是否写入 Topic
  valid_data_topic: "mytopic-valid"
  invalid_data_topic: "mytopic-invalid"
  write_to_oceanbase: true     # 是否写入 OceanBase
  invalid_table_prefix: "e_"   # 异议数据表前缀
```

### 3. table-schema.yaml - 表结构配置

```yaml
tables:
  - name: "baseinfo"
    description: "人员基本信息表"
    fields:
      - name: "personid"
        type: "BIGINT"
        nullable: true
      - name: "idcard"
        type: "STRING"
        nullable: false
        is_primary_key: true
      - name: "sex"
        type: "STRING"
        nullable: true
      # ... 更多字段
```

### 4. validation-rules.yaml - 验证规则配置（支持动态刷新）

```yaml
tables:
  - name: "baseinfo"
    rules:
      - field: "sex"
        type: "enum"
        enabled: true
        values: ["男", "女"]
      - field: "age"
        type: "range"
        enabled: true
        min_value: 0
        max_value: 120
      - field: "telephone"
        type: "regex"
        enabled: true
        pattern: "^1[3-9]\\d{9}$"
```

## 快速开始

### 编译

```bash
cd /opt/kafka_streams_demo/ConsumerNew
mvn clean package
```

### 启动

```bash
# 使用默认配置路径 (src/main/resources)
./start.sh

# 使用自定义配置路径
./start.sh /path/to/config
```

### 停止

```bash
./stop.sh
```

## 动态规则刷新

修改 `validation-rules.yaml` 后，应用会在 5 秒内自动检测到变化并重新加载规则，无需重启。

规则变化日志：
```
INFO  检测到配置文件变化，重新加载规则...
INFO  验证规则已更新，共 1 个表配置
```

## 输出说明

### 正常数据
- 写入 Topic: `valid_data_topic` (如果 `write_to_topic=true`)
- 写入表：`baseinfo` (如果 `write_to_oceanbase=true`)

### 异议数据
- 写入 Topic: `invalid_data_topic` (如果 `write_to_topic=true`)
- 写入表：`e_baseinfo` (如果 `write_to_oceanbase=true`)

## 验证规则类型

| 类型 | 说明 | 配置参数 |
|------|------|----------|
| enum | 枚举值检查 | values: [允许值列表] |
| range | 范围检查 | min_value, max_value |
| regex | 正则表达式检查 | pattern |
| length | 长度检查 | min_length, max_length |

## 与旧版本对比

| 特性 | 旧版本 (Consumer) | 新版本 (ConsumerNew) |
|------|------------------|---------------------|
| 配置方式 | Java 硬编码 | YAML 配置文件 |
| 规则刷新 | 需要重启 | 自动刷新 |
| 多表支持 | 单表 | 多表配置 |
| 建表方式 | 手动 | 自动/手动可选 |
