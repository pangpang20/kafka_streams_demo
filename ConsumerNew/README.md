# ConsumerNew - 可配置 Kafka Streams Consumer（多表支持）

基于配置文件实现的通用 Kafka Streams 数据质量检查系统，支持动态规则刷新和多表自动识别。

**一个 Topic 对应一个表**，通过命令行参数指定 Topic 和配置文件。

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

| 参数 | 简写 | 说明 |
|------|------|------|
| `--topic <name>` | `-t` | 指定 Kafka Topic（一个 topic 对应一个表） |
| `--config-dir <path>` | `-c` | 指定配置目录，自动加载 `schemas/` 和 `rules/` |
| `--schema <file>` | `-s` | 指定表结构配置文件 |
| `--rules <file>` | `-r` | 指定验证规则配置文件 |
| `--help` | `-h` | 显示帮助信息 |

### 使用示例

```bash
# 启动 baseinfo 表的消费者
./start.sh --topic mytopic-baseinfo --config-dir config/baseinfo

# 启动 orderinfo 表的消费者
./start.sh --topic mytopic-orderinfo --config-dir config/orderinfo
```

### 停止

```bash
./stop.sh
# 或 Ctrl+C
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
