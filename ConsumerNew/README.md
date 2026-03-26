# ConsumerNew - 可配置 Kafka Streams Consumer（多表支持）

基于配置文件实现的通用 Kafka Streams 数据质量检查系统，支持动态规则刷新和多表自动识别。

## 特性

- **完全可配置**：所有配置通过 YAML 文件管理，无需修改代码
- **动态规则刷新**：验证规则修改后自动生效，无需重启应用（5 秒内检测）
- **多表自动识别**：支持同一个 Topic 中多个表的数据，自动识别并路由
- **目录式配置加载**：支持从目录自动加载所有表的配置和规则
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
  topic: "mytopic"             # 源 Topic（包含多个表的数据）
  auto_create_table: true      # 是否自动建表
  write_to_topic: true         # 是否写入 Topic
  valid_data_topic: "mytopic-valid"
  invalid_data_topic: "mytopic-invalid"
  write_to_oceanbase: true     # 是否写入 OceanBase
  invalid_table_prefix: "e_"   # 异议数据表前缀
```

### 3. 表结构配置（支持目录加载）

**方式一：单文件模式**
```bash
src/main/resources/table-schema.yaml
```

**方式二：目录模式（推荐）**
```bash
src/main/resources/schemas/
├── baseinfo.yaml      # baseinfo 表结构
├── orderinfo.yaml     # orderinfo 表结构
└── ...                # 更多表配置
```

每个表配置文件示例：
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
```

### 4. 验证规则配置（支持目录加载 + 动态刷新）

**方式一：单文件模式**
```bash
src/main/resources/validation-rules.yaml
```

**方式二：目录模式（推荐）**
```bash
src/main/resources/rules/
├── baseinfo-rules.yaml    # baseinfo 表规则
├── orderinfo-rules.yaml   # orderinfo 表规则
└── ...                    # 更多表规则
```

每个规则文件示例：
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
# 自动检测 schemas/ 和 rules/ 目录，如果不存在则使用单文件
./start.sh

# 使用自定义配置路径
./start.sh /path/to/config
```

### 停止

```bash
./stop.sh
```

## 多表自动识别

### 场景说明

当同一个 Topic 中包含多个表的数据时（如 A、B、C 表），Consumer 会自动识别并路由：

1. **已知表**：启动时从配置文件加载的表，直接创建验证器
2. **新表**：运行时检测到新表名，自动注册并创建验证器

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

规则变化日志：
```
INFO  检测到配置文件变化，重新加载规则...
INFO  验证规则已更新，共 3 个表配置
```

## 输出说明

### 正常数据
- 写入 Topic: `valid_data_topic` (如果 `write_to_topic=true`)
- 写入表：表名本身 (如 `baseinfo`, `orderinfo`)

### 异议数据
- 写入 Topic: `invalid_data_topic` (如果 `write_to_topic=true`)
- 写入表：前缀 + 表名 (如 `e_baseinfo`, `e_orderinfo`)

## 验证规则类型

| 类型 | 说明 | 配置参数 |
|------|------|----------|
| enum | 枚举值检查 | values: [允许值列表] |
| range | 范围检查 | min_value, max_value |
| regex | 正则表达式检查 | pattern |
| length | 长度检查 | min_length, max_length |

## 配置目录结构示例

```
src/main/resources/
├── connection-config.yaml       # Kafka 和 OceanBase 连接配置
├── app-config.yaml              # 应用级配置
├── schemas/                     # 表结构配置目录
│   ├── baseinfo.yaml
│   ├── orderinfo.yaml
│   └── dinfo.yaml
└── rules/                       # 验证规则配置目录
    ├── baseinfo-rules.yaml
    ├── orderinfo-rules.yaml
    └── dinfo-rules.yaml
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
│  │ rules/A.yaml  │ │rules/B.yaml│ │rules/C.yaml│          │
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
