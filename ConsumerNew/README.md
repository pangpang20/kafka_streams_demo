# ConsumerNew - 可配置 Kafka Streams Consumer（多表支持）

基于配置文件实现的通用 Kafka Streams 数据质量检查系统，支持**动态规则刷新**、**多表自动识别**和**目录式配置加载**。

**一个 Topic 对应一个表**，通过命令行参数指定 Topic 和配置文件。

## 特性

- **完全可配置**：所有配置通过 YAML 文件管理
- **动态规则刷新**：规则修改后自动生效，无需重启应用（5 秒内检测）
- **多表自动识别**：支持同一个 Topic 中多个表的数据，自动识别并路由
- **目录式配置加载**：支持从 `schemas/` 和 `rules/` 目录自动加载所有表的配置
- **灵活输出**：可选择是否写入 Kafka Topic 和 OceanBase 数据库
- **自动建表**：支持自动在 OceanBase 中创建目标表
- **命令行参数**：支持启动时指定 Topic 和配置文件路径
- **任务跟踪**：支持 UUID 任务 ID，用于跟踪任务状态

## 快速开始

### 编译

```bash
cd /opt/kafka_streams_demo/ConsumerNew
mvn clean package
```

### 启动方式

#### 方式 1：指定配置目录（推荐）

```bash
# 使用默认资源目录中的配置
./start.sh --topic mytopic --config-dir src/main/resources

# 或者使用外部配置目录
./start.sh --topic mytopic --config-dir /path/to/config
```

配置目录结构：
```
config/
├── schemas/              # 表结构配置目录（自动加载所有 YAML 文件）
│   └── baseinfo.yaml
└── rules/                # 验证规则配置目录（自动加载所有 YAML 文件）
    └── baseinfo-rules.yaml
```

#### 方式 2：分别指定 schema 和 rules 文件

```bash
./start.sh --schema /path/to/baseinfo.yaml --rules /path/to/baseinfo-rules.yaml
```

#### 方式 3：使用默认配置

```bash
./start.sh
```

### 命令行参数

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--topic <name>` | `-t` | 指定 Kafka Topic（一个 topic 对应一个表） | 配置文件的值 |
| `--config-dir <path>` | `-c` | 指定配置目录，自动加载 `schemas/` 和 `rules/` | - |
| `--schema <file>` | `-s` | 指定表结构配置文件 | - |
| `--rules <file>` | `-r` | 指定验证规则配置文件 | - |
| `--task-id <id>` | - | 指定任务 ID（用于跟踪任务状态） | 自动生成 UUID |
| `--help` | `-h` | 显示帮助信息 | - |

### 使用示例

```bash
# 启动 baseinfo 表的消费者
./start.sh --topic mytopic-baseinfo --config-dir config/baseinfo

# 启动 orderinfo 表的消费者
./start.sh --topic mytopic-orderinfo --config-dir config/orderinfo

# 使用外部配置目录
./start.sh -t mytopic -c /etc/kafka-consumer/config

# 查看帮助
./start.sh --help
```

### 停止

```bash
./stop.sh
# 或 Ctrl+C
```

### 查看任务状态

```bash
# 查看当前运行任务状态
./status.sh

# 查看指定任务状态
./status.sh <task-id>

# 列出所有任务
./status.sh --list

# 清理已停止的任务
./status.sh --clean
```

**任务状态说明：**

| 状态 | 说明 |
|------|------|
| INIT | 初始化 |
| STARTING | 启动中 |
| RUNNING | 运行中 |
| REBALANCING | 重平衡中 |
| ERROR | 错误 |
| STOPPING | 停止中 |
| STOPPED | 已停止 |

## 配置文件说明

ConsumerNew 有四个配置文件，都在 `src/main/resources/` 目录下：

### 1. connection-config.yaml - 连接配置

配置 Kafka 和 OceanBase 连接信息。

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
  url: "jdbc:mysql://localhost:2881/kafka_quality_check?useSSL=false&allowPublicKeyRetrieval=true"
  username: "root@test"
  password: "Audaque@123"
  driver_class: "com.mysql.cj.jdbc.Driver"

# 数据库表配置
database:
  # 正常数据表名前缀（空表示不写入）
  valid_table_prefix: ""
  # 异议数据表名前缀
  invalid_table_prefix: "e_"
  # 是否自动创建表
  auto_create_tables: true
```

### 2. app-config.yaml - 应用配置

配置应用行为和 Streams 参数。

```yaml
app:
  name: "configurable-consumer-app"
  log_dir: "./logs"

# Kafka Streams 配置
streams:
  application_id: "quality-check-app"
  state_dir: "/tmp/kafka-streams-quality-check"
  num_stream_threads: 1
  auto_offset_reset: latest
  commit_interval_ms: 1000
  cache_max_bytes_buffering: 0

# 源和目标配置
source:
  topic: "mytopic"

valid_data:
  topic: "mytopic-valid"
  write_to_topic: true
  write_to_db: true

invalid_data:
  topic: "mytopic-invalid"
  write_to_topic: true
  write_to_db: true
```

### 3. schemas/baseinfo.yaml - 表结构配置

定义表的字段和类型。

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
        isPrimaryKey: true
      - name: "name"
        type: "STRING"
        nullable: true
      - name: "sex"
        type: "STRING"
        nullable: true
      - name: "age"
        type: "INT"
        nullable: true
```

### 4. rules/baseinfo-rules.yaml - 验证规则配置

定义数据验证规则。

```yaml
tables:
  - name: "baseinfo"
    rules:
      - field: "sex"
        type: "enum"
        enabled: true
        values: ["男", "女"]
        message: "性别值 '{value}' 不在允许范围内"

      - field: "age"
        type: "range"
        enabled: true
        min_value: 0
        max_value: 120
        message: "年龄值 {value} 超出有效范围 [0, 120]"

      - field: "telephone"
        type: "regex"
        enabled: true
        pattern: "^1[3-9]\\d{9}$"
        message: "电话号码格式不正确"
```

## 多表自动识别

### 场景说明

当同一个 Topic 中包含多个表的数据时（如 A、B、C 表），Consumer 会自动识别并路由：

1. **已知表**：启动时从配置文件加载的表，直接创建验证器
2. **新表**：运行时检测到新表名，自动注册并创建验证器

### 配置目录结构

```
config/
├── schemas/                  # 表结构配置目录
│   ├── baseinfo.yaml         # baseinfo 表结构
│   ├── orderinfo.yaml        # orderinfo 表结构
│   └── dinfo.yaml            # dinfo 表结构
└── rules/                    # 验证规则配置目录
    ├── baseinfo-rules.yaml   # baseinfo 表验证规则
    ├── orderinfo-rules.yaml  # orderinfo 表验证规则
    └── dinfo-rules.yaml      # dinfo 表验证规则
```

### 新增表 D 的流程

1. 在 `schemas/` 目录添加 `dinfo.yaml`
2. 在 `rules/` 目录添加 `dinfo-rules.yaml`
3. Producer 开始发送表 D 的数据
4. Consumer 自动检测到新表，加载配置并创建验证器
5. **无需重启应用**（表结构配置和规则配置都支持动态刷新）

### 日志示例

```
INFO  检测到新表：dinfo, 自动注册并创建验证器
INFO  检测到表结构配置文件变化，重新加载...
INFO  从目录加载规则完成：3 个表配置
INFO  注册新表：dinfo, 字段数：8
```

## 动态规则刷新

修改任意规则文件后，应用会在 5 秒内自动检测到变化并重新加载规则，无需重启。

### 规则变化日志

```
INFO  检测到配置文件变化，重新加载规则...
INFO  验证规则已更新，共 3 个表配置
```

### 刷新机制

- 后台线程每 5 秒检测一次配置文件的最后修改时间
- 检测到变化后重新加载配置文件
- 更新验证器映射，新数据使用新规则验证
- 不影响正在处理的数据

## 验证规则类型

| 类型 | 说明 | 配置参数 | 示例 |
|------|------|----------|------|
| `enum` | 枚举值检查 | `values` | `values: ["男", "女"]` |
| `range` | 范围检查 | `min_value`, `max_value` | `min_value: 0, max_value: 120` |
| `regex` | 正则表达式检查 | `pattern` | `pattern: "^1[3-9]\\d{9}$"` |
| `length` | 长度检查 | `min_length`, `max_length` | `min_length: 1, max_length: 50` |
| `not_null` | 非空检查 | - | - |
| `custom` | 自定义检查 | `class` | `class: "com.example.CustomValidator"` |

### 规则配置示例

```yaml
tables:
  - name: "baseinfo"
    rules:
      # 枚举检查
      - field: "sex"
        type: "enum"
        enabled: true
        values: ["男", "女"]
        message: "性别值 '{value}' 不在允许范围内"

      # 范围检查
      - field: "age"
        type: "range"
        enabled: true
        min_value: 0
        max_value: 120
        message: "年龄值 {value} 超出有效范围"

      # 正则检查
      - field: "telephone"
        type: "regex"
        enabled: true
        pattern: "^1[3-9]\\d{9}$"
        message: "电话号码格式不正确"

      # 长度检查
      - field: "name"
        type: "length"
        enabled: true
        min_length: 1
        max_length: 50
        message: "姓名长度超出限制"

      # 非空检查
      - field: "idcard"
        type: "not_null"
        enabled: true
        message: "身份证号不能为空"
```

## 输出说明

### 正常数据

| 输出目标 | 配置项 | 说明 |
|----------|--------|------|
| Kafka Topic | `valid_data.write_to_topic=true` | 写入 `valid_data.topic` 指定的 Topic |
| OceanBase 表 | `valid_data.write_to_db=true` | 写入表名本身 (如 `baseinfo`, `orderinfo`) |

### 异议数据

| 输出目标 | 配置项 | 说明 |
|----------|--------|------|
| Kafka Topic | `invalid_data.write_to_topic=true` | 写入 `invalid_data.topic` 指定的 Topic |
| OceanBase 表 | `invalid_data.write_to_db=true` | 写入 `invalid_table_prefix` + 表名 (如 `e_baseinfo`) |

### 问题数据表结构

```sql
CREATE TABLE e_baseinfo (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    record_key VARCHAR(500),
    database_name VARCHAR(100),
    table_name VARCHAR(100),
    opcode VARCHAR(10),
    failure_summary TEXT,
    error_fields TEXT,
    error_details JSON,
    raw_data JSON,
    log_timestamp DATETIME
);
```

## 完整启动流程

### 1. 启动 Kafka 和 OceanBase

```bash
cd /opt/kafka_streams_demo/docker
./start-sasl.sh
```

### 2. 创建 Topic

```bash
# 源 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic --partitions 3 --replication-factor 3

# 正常数据 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-valid --partitions 3 --replication-factor 3

# 异议数据 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-invalid --partitions 3 --replication-factor 3
```

### 3. 启动 ConsumerNew

```bash
cd /opt/kafka_streams_demo/ConsumerNew
./start.sh --topic mytopic --config-dir src/main/resources
```

### 4. 启动 ProducerNew

```bash
cd /opt/kafka_streams_demo/ProducerNew
./start.sh
```

### 5. 查看结果

```bash
# 查看 Topic 消息量
docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9091 --topic mytopic-valid,mytopic-invalid

# 查看 OceanBase 中的问题数据
docker exec oceanbase obclient -h 127.0.0.1 -P 2881 -u root@test -p'Audaque@123' kafka_quality_check \
  -e "SELECT * FROM e_baseinfo ORDER BY log_timestamp DESC LIMIT 10;"
```

## 启动日志解析

```
=====================================
可配置 Kafka Streams Consumer
=====================================
配置目录：src/main/resources
Schema 配置：src/main/resources/schemas
Rules 配置：src/main/resources/rules
使用命令行指定的 Topic: mytopic

=====================================
Kafka 连接配置
=====================================
Bootstrap Servers: localhost:19091,localhost:19092,localhost:19093
...

=====================================
表结构配置
=====================================
表名：baseinfo
描述：人员基本信息表
字段数：15
...

启动 Kafka Streams 应用...
INFO  StreamsConfig 加载完成
INFO  拓扑构建完成
INFO  Kafka Streams 应用启动成功
```

## 与旧版本对比

| 特性 | 旧版本 (Consumer) | 新版本 (ConsumerNew) |
|------|------------------|---------------------|
| 配置方式 | Java 硬编码 | YAML 配置文件 |
| 规则刷新 | 需要重启 | 自动刷新 (5 秒检测) |
| 多表支持 | 单表 | 多表自动识别 |
| 配置加载 | 单文件 | 目录自动加载 |
| 建表方式 | 手动 | 自动/手动可选 |
| 新表识别 | 不支持 | 运行时自动注册 |
| 命令行参数 | 无 | 支持 |

## 常见问题

### Q: 如何查看 Consumer Group 状态？

```bash
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --describe --group quality-check-app
```

### Q: 如何重置消费 offset？

```bash
# 方法 1: 删除 Consumer Group
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --delete --group quality-check-app

# 方法 2: 重置到最新 offset
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --group quality-check-app --topic mytopic --reset-offsets --to-latest --execute

# 方法 3: 重置到开头
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --group quality-check-app --topic mytopic --reset-offsets --to-earliest --execute
```

### Q: 如何清理本地状态？

```bash
rm -rf /tmp/kafka-streams-quality-check/*
```

### Q: 规则修改后多久生效？

规则文件修改后，最多 5 秒内自动检测并加载。

### Q: 如何查看应用日志？

```bash
# 查看最新日志
tail -f ConsumerNew/logs/app-*.log

# 查看错误日志
grep ERROR ConsumerNew/logs/app-*.log
```

## 配置文件完整示例

参考 `src/main/resources/` 目录下的示例配置文件：
- `connection-config.yaml` - 连接配置示例
- `app-config.yaml` - 应用配置示例
- `schemas/` - 表结构配置目录
- `rules/` - 验证规则配置目录

## 技术架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Source Topic (mytopic)                 │
│                   (包含多个表的数据：A, B, C, D...)           │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   MultiTableDataProcessor                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │ Validator A │  │ Validator B │  │ Validator C │  ...    │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
│         ▲                ▲                ▲                 │
│         │                │                │                 │
│  ┌──────┴────────┐ ┌────┴───────┐ ┌─────┴──────┐          │
│  │ schemas/A.yaml│ │schemas/B.yaml│ │schemas/C.yaml│       │
│  │ rules/A.yaml  │ │rules/B.yaml │ │rules/C.yaml │        │
│  └───────────────┘ └────────────┘ └────────────┘          │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
     ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
     │ Valid Data  │ │Invalid Data │ │   Logging   │
     │ + OceanBase │ │ + OceanBase │ │  + Stats    │
     └─────────────┘ └─────────────┘ └─────────────┘
```
