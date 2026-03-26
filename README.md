# Kafka Streams 数据质量检查 Demo 项目

基于 Kafka Streams 实现的 CDC 数据质量检查系统，支持实时数据验证和异议数据记录。
问题数据同时输出到 Kafka Topic (mytopic-invalid)、日志文件和 OceanBase 数据库。

## 项目结构

```
/opt/kafka_streams_demo/
├── docker/                     # Docker 部署
│   ├── docker-compose.yml      # Kafka 集群配置 (3 节点) + OceanBase
│   ├── init-ob.sql             # OceanBase 初始化 SQL 脚本
│   ├── start-sasl.sh           # 启动脚本 (SASL 认证)
│   └── stop-sasl.sh            # 停止脚本 (SASL 认证)
├── Producer/                   # 数据生产者 (硬编码版本)
│   ├── src/main/java/...       # 源代码
│   ├── pom.xml                 # Maven 配置
│   ├── start.sh                # 启动脚本
│   └── README.md               # 使用说明
├── ProducerNew/                # 可配置数据生产者 (新增)
│   ├── src/main/java/...       # 源代码
│   ├── src/main/resources/     # 配置文件
│   ├── pom.xml                 # Maven 配置
│   ├── start.sh                # 启动脚本
│   └── README.md               # 使用说明
├── Consumer/                   # 数据消费者 (硬编码版本)
│   ├── src/main/java/...       # 源代码
│   ├── pom.xml                 # Maven 配置
│   ├── start.sh                # 启动脚本
│   └── README.md               # 使用说明
├── ConsumerNew/                # 可配置数据消费者 (新增)
│   ├── src/main/java/...       # 源代码
│   ├── src/main/resources/     # 配置文件
│   ├── pom.xml                 # Maven 配置
│   ├── start.sh                # 启动脚本
│   └── README.md               # 使用说明
├── oceanbase/                  # OceanBase 工具
│   ├── ob_monitor.py           # 数据监控程序
│   └── start.sh                # 启动脚本
└── README.md                   # 本文件
```

---

## 版本对比

| 特性 | 旧版本 (Producer/Consumer) | 新版本 (ProducerNew/ConsumerNew) |
|------|---------------------------|----------------------------------|
| 配置方式 | Java 硬编码 | YAML 配置文件 |
| 规则刷新 | 需要重启 | 自动刷新 (ConsumerNew, 5 秒检测) |
| 多表支持 | 单表 | 多表自动识别 + 动态扩展 |
| 配置加载 | 单文件 | 目录自动加载 (schemas/, rules/) |
| 建表方式 | 手动 | 自动/手动可选 |
| 适用场景 | 固定表结构 | 通用、多变的表结构 |

**推荐使用**: 新项目建议使用 ProducerNew/ConsumerNew，具有更好的灵活性和可维护性。

### ConsumerNew 新特性

- **多表自动识别**: 同一个 Topic 中包含多个表 (A, B, C...) 的数据时，自动识别并路由到对应的验证器
- **动态表扩展**: Producer 新增表 D 时，只需在配置目录添加表 D 的 schema 和 rules 文件，Consumer 自动检测并加载
- **目录式配置**: 支持从 `schemas/` 和 `rules/` 目录自动加载所有表的配置，每个表一个独立文件
- **双动态刷新**: 表结构配置和验证规则都支持动态刷新，无需重启应用

---

## 一、环境要求

- **JDK**: 1.8+
- **Maven**: 3.6+
- **Docker**: 20.10+
- **Docker Compose**: 1.29+
- **磁盘空间**: 至少 10GB (Kafka 数据目录 + OceanBase 数据目录)
- **内存**: 至少 12GB (Kafka 集群 + OceanBase + 应用)

---

## 二、启动 Docker 集群 (Kafka + OceanBase)

### 2.1 启动集群（SASL 认证）

```bash
cd /opt/kafka_streams_demo/docker
./start-sasl.sh
```

**SASL 认证配置**:
- 用户名：`admin`
- 密码：`Audaque@123`
- 安全协议：`SASL_PLAINTEXT`

**启动流程说明**:
1. 启动 Zookeeper 集群 (3 节点) - 约 5 秒
2. 启动 Kafka 集群 (3 节点) - 约 15 秒
3. 启动 OceanBase 数据库 - 约 60-90 秒

**启动后验证**：

```bash
# 检查容器状态
docker-compose ps

# 预期输出：
#       Name                     Command               State                              Ports
# -----------------------------------------------------------------------------------------------------------------------------
# zookeeper-1         /etc/confluent/docker/run      Up      0.0.0.0:2181->2181/tcp
# zookeeper-2         /etc/confluent/docker/run      Up      0.0.0.0:2182->2181/tcp
# zookeeper-3         /etc/confluent/docker/run      Up      0.0.0.0:2183->2181/tcp
# kafka-1             /etc/confluent/docker/run      Up      0.0.0.0:19091->19091/tcp
# kafka-2             /etc/confluent/docker/run      Up      0.0.0.0:19092->19092/tcp
# kafka-3             /etc/confluent/docker/run      Up      0.0.0.0:19093->19093/tcp
# kafka-ui            /bin/sh -c kafka-ui          Up      0.0.0.0:28080->8080/tcp
# oceanbase           /root/entrypoint.sh          Up      0.0.0.0:2881->2881/tcp
```

### 2.2 停止集群

```bash
cd /opt/kafka_streams_demo/docker
./stop-sasl.sh

# 或者
docker-compose -f docker-compose-sasl.yml down
```

### 2.3 清理所有数据（重置环境）

```bash
docker-compose -f docker-compose-sasl.yml down -v  # -v 选项删除所有卷
```

### 2.4 集群配置详情

| 组件 | 容器名 | 端口 | 说明 |
|------|--------|------|------|
| Zookeeper 1 | zookeeper-1 | 2181 | 集群协调 |
| Zookeeper 2 | zookeeper-2 | 2182 | 集群协调 |
| Zookeeper 3 | zookeeper-3 | 2183 | 集群协调 |
| Kafka Broker 1 | kafka-1 | 19091 | 外部访问 |
| Kafka Broker 2 | kafka-2 | 19092 | 外部访问 |
| Kafka Broker 3 | kafka-3 | 19093 | 外部访问 |
| Kafka UI | kafka-ui | 28080 | Web 管理界面 |
| OceanBase | oceanbase | 2881 | MySQL 兼容模式 |

**访问 Kafka UI**: http://localhost:28080

**连接 OceanBase**:
- Docker 网络内：`oceanbase:2881`
- 宿主机访问：`localhost:2881`
- 用户名：`root@test`
- 密码：`Audaque@123`
- 数据库：`kafka_quality_check`

**使用 obclient 连接**:
```bash
# 无密码方式（已设置密码）
docker exec oceanbase obclient -h 127.0.0.1 -P 2881 -u root@test -p'Audaque@123' kafka_quality_check
```

---

## 三、创建 Kafka Topic

### 3.1 创建必需 Topic

```bash
# 源 Topic（Producer 发送，Consumer 消费）
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic --partitions 3 --replication-factor 3

# 正常数据输出 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-valid --partitions 3 --replication-factor 3

# 异常数据输出 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-invalid --partitions 3 --replication-factor 3
```

### 3.2 验证 Topic 创建

```bash
# 列出所有 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 --list

# 查看 Topic 详情
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --describe --topic mytopic
```

### 3.3 验证 OceanBase 表结构

```bash
# 连接 OceanBase (使用 obclient)
docker exec oceanbase obclient -h 127.0.0.1 -P 2881 -u root@test -p'Audaque@123' kafka_quality_check

# 查看数据库
SHOW DATABASES;
USE kafka_quality_check;
SHOW TABLES;

# 查看表结构
DESC invalid_data;
DESC quality_stats;
```

---

## 四、启动 Consumer (Kafka Streams 应用)

### 4.1 Consumer 启动流程

Consumer 使用 Kafka Streams API，启动时会：
1. 构建处理拓扑 (Topology)
2. 连接 Kafka 集群
3. 加入消费者组
4. 分配分区
5. 开始消费和处理消息

### 4.2 启动 Consumer

编辑 `src/main/java/com/kafka/consumer/config/ConsumerConfig.java`:
```java
public static final boolean ENABLE_SASL = true;  // 启用 SASL
```
然后重新编译并启动：
```bash
cd /opt/kafka_streams_demo/Consumer

# 清理之前的状态（避免 offset 冲突）
rm -rf /tmp/kafka-streams-quality-check/*

# 启动应用
./start.sh
```

### 4.3 Consumer 启动日志解析

```
=====================================
Kafka Streams 数据质量检查应用
=====================================
Bootstrap Servers: localhost:19091,localhost:19092,localhost:19093
Source Topic: mytopic
Valid Data Topic: mytopic-valid
Invalid Data Topic: mytopic-invalid
Application ID: quality-check-app
State Directory: /tmp/kafka-streams-quality-check
-------------------------------------
数据质量检查规则:
  性别检查：开启 (允许：男，女)
  年龄检查：开启 [0-120]
  电话检查：开启 (正则：^1[3-9]\d{9}$)
  血型检查：开启 (允许：A,B,O,AB)
  信用分检查：开启 [0.0-1000.0]
  住房面积检查：开启 (最小值：0.0)
```

### 4.4 Kafka Streams 拓扑结构

```
                    ┌─────────────────────┐
                    │   Source: mytopic   │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │  DataProcessor      │
                    │  1. 认证 (Auth)     │
                    │  2. 解析 (Parse)    │
                    │  3. 验证 (Validate) │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
     ┌────────────────┐ ┌─────────────────┐ ┌──────────────┐
     │ mytopic-valid  │ │ mytopic-invalid │ │  日志/统计   │
     │ (正常数据)      │ │ (异常数据)       │ │ (打印统计)   │
     └────────────────┘ └─────────────────┘ └──────────────┘
```

### 4.5 Consumer 进程信息

```bash
# 查看 Consumer 进程
ps aux | grep kafka-streams-consumer

# 查看 Consumer Group
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --describe --group quality-check-app
```

---

## 五、启动 Producer

### 5.1 启动方式

编辑 `src/main/java/com/kafka/producer/config/ProducerConfig.java`:
```java
public static final boolean ENABLE_SASL = true;  // 启用 SASL
```
然后重新编译并启动：
```bash
cd /opt/kafka_streams_demo/Producer

# 无限运行 (Ctrl+C 停止)
./start.sh 60    # 运行 60 秒
```

### 5.2 Producer 配置说明

| 参数 | 默认值 | 说明 |
|------|--------|------|
| SEND_INTERVAL_MS | 1000 | 发送间隔 (毫秒) |
| INSERT_PROBABILITY | 50 | 插入操作概率 (%) |
| UPDATE_PROBABILITY | 45 | 更新操作概率 (%) |
| DELETE_PROBABILITY | 5 | 删除操作概率 (%) |
| BAD_DATA_PROBABILITY | 10 | 问题数据概率 (%) |

### 5.3 问题数据类型

| 字段 | 问题类型 | 示例 |
|------|----------|------|
| sex | 非法枚举值 | M, F, 未知，male, 1 |
| age | 超出范围 | -5, 150+ |
| telephone | 格式错误 | 138123, 138abc12345 |
| bloodtype | 非法枚举值 | A 型，C, Rh |
| creditscore | 超出范围 | -100, 1500+ |
| housingareas | 负数 | -100 |

---

## 六、监控和观察

### 6.1 Web UI 监控

访问 **Kafka UI**: http://localhost:28080

- 查看 Topic 消息量
- 查看 Consumer Group 状态
- 浏览 Topic 消息内容

### 6.2 命令行监控

```bash
# 查看 Topic 消息量
docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9091 --topic mytopic

# 查看 Consumer Group 消费情况
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --describe --group quality-check-app

# 实时消费 mytopic-valid
docker exec kafka-1 kafka-console-consumer --bootstrap-server localhost:9091 \
  --topic mytopic-valid --from-beginning

# 实时消费 mytopic-invalid
docker exec kafka-1 kafka-console-consumer --bootstrap-server localhost:9091 \
  --topic mytopic-invalid --from-beginning
```

### 6.3 异议数据日志

```bash
# 查看异议数据日志文件
ls -la /opt/kafka_streams_demo/Consumer/logs/

# 查看最新日志内容
tail -f /opt/kafka_streams_demo/Consumer/logs/invalid-data-*.log

# 统计错误类型
cat /opt/kafka_streams_demo/Consumer/logs/invalid-data-*.log | \
  grep -o '"failureSummary" : "[^"]*"' | sort | uniq -c | sort -rn
```

### 6.4 查询 OceanBase 中的问题数据

```bash
# 连接 OceanBase (使用 obclient)
docker exec oceanbase obclient -h 127.0.0.1 -P 2881 -u root@test -p'Audaque@123' kafka_quality_check

# 查看最新的问题数据
SELECT record_key, table_name, opcode, failure_summary, log_timestamp
FROM invalid_data
ORDER BY log_timestamp DESC
LIMIT 10;

# 统计错误类型分布
SELECT
    CASE
        WHEN failure_summary LIKE '%sex%' THEN '性别错误'
        WHEN failure_summary LIKE '%age%' THEN '年龄错误'
        WHEN failure_summary LIKE '%telephone%' THEN '电话错误'
        WHEN failure_summary LIKE '%bloodtype%' THEN '血型错误'
        WHEN failure_summary LIKE '%creditscore%' THEN '信用分错误'
        WHEN failure_summary LIKE '%housingareas%' THEN '住房面积错误'
        ELSE '其他错误'
    END AS error_type,
    COUNT(*) AS error_count
FROM invalid_data
GROUP BY error_type
ORDER BY error_count DESC;

# 查看每日问题数据统计
SELECT
    DATE(log_timestamp) AS stat_date,
    COUNT(*) AS invalid_count
FROM invalid_data
GROUP BY DATE(log_timestamp)
ORDER BY stat_date DESC;
```

---

## 七、OceanBase 数据监控程序

### 7.1 启动监控程序

```bash
cd /opt/kafka_streams_demo/oceanbase
./start.sh
```

### 7.2 功能特性

- **实时监控面板** - 显示数据库状态、表列表、数据统计
- **问题数据统计** - 按操作类型、表名、错误类型统计
- **数据质量统计** - 显示质量检查统计数据
- **最近问题数据** - 查看最新的问题数据记录
- **交互模式** - 支持命令行交互操作
- **批处理模式** - 一次性输出所有统计信息

### 7.3 命令说明

| 命令 | 说明 |
|------|------|
| `dashboard` / `d` | 显示完整监控面板 |
| `stats` / `s` | 显示数据统计 |
| `recent` / `r [n]` | 显示最近 n 条问题数据 |
| `tables` / `t` | 显示表列表 |
| `query` / `q` | 执行自定义 SQL 查询 |
| `refresh` / `f` | 刷新当前视图 |
| `help` / `h` | 显示帮助信息 |
| `quit` / `exit` | 退出程序 |

### 7.4 使用示例

```bash
# 交互模式（默认）
./start.sh

# 批处理模式（一次性输出）
./start.sh -b

# 查看帮助
./start.sh -h
```

### 6.5 控制台日志解读

```
[PASS] key=idcard:I:1234567890, opcode=I, table=baseinfo
  → 数据通过验证

[FAIL] key=idcard:U:1234567891, reason=数据质量检查失败：[sex] 性别值'M'不在允许范围内
  → 数据验证失败，记录到 mytopic-invalid 和日志文件

===== 统计：valid = 150 条 =====
===== 统计：invalid = 25 条 =====
  → Kafka Streams 统计信息
```

---

## 八、快速测试流程

### 7.1 完整测试流程

```bash
# 1. 启动 Kafka 集群
cd /opt/kafka_streams_demo/docker && ./start-sasl.sh

# 等待 90 秒确保集群就绪 (OceanBase 启动较慢)

# 2. 创建 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic --partitions 3 --replication-factor 3
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-valid --partitions 3 --replication-factor 3
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-invalid --partitions 3 --replication-factor 3

# 3. 启动 Consumer (新终端)
cd /opt/kafka_streams_demo/Consumer
rm -rf /tmp/kafka-streams-quality-check/*
./start.sh

# 4. 启动 Producer (新终端)
cd /opt/kafka_streams_demo/Producer
./start.sh 60

# 5. 观察结果 (新终端)
watch -n 2 'docker exec kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9091 --topic mytopic-valid,mytopic-invalid 2>/dev/null'
```

### 7.2 预期结果

运行 60 秒后：
- 发送约 60 条消息
- mytopic-valid: 约 50-55 条
- mytopic-invalid: 约 5-10 条
- 日志文件：`Consumer/logs/invalid-data-YYYY-MM-DD.log`

---

## 九、故障排查

### 8.1 Kafka 集群无法启动

```bash
# 查看容器日志
docker-compose logs kafka-1
docker-compose logs zookeeper-1

# 清理并重启
docker-compose down -v
docker-compose up -d
```

### 8.2 Consumer 无法连接 Kafka

```bash
# 检查 Kafka 端口是否可访问
telnet localhost 19091

# 查看 Consumer 日志
tail -f /opt/kafka_streams_demo/Consumer/logs/app.log
```

### 8.3 Consumer Group 无法消费

```bash
# 删除 Consumer Group 重置 offset
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --delete --group quality-check-app

# 或者清理状态目录
rm -rf /tmp/kafka-streams-quality-check/*
```

---

## 十、开发指南

### 9.1 添加新验证规则

参考 `Consumer/README.md` 中的开发指南

### 9.2 修改 Producer 数据生成

参考 `Producer/README.md` 中的配置说明

---

## 十一、技术栈

| 组件 | 版本 | 说明 |
|------|------|------|
| Kafka | 2.8.2 | 消息队列 |
| Zookeeper | 3.6.3 | 分布式协调 |
| Kafka Streams | 2.8.2 | 流处理 |
| Confluent Platform | 6.2.0 | Kafka 发行版 |
| Kafka UI | latest | Web 管理界面 |
| OceanBase | 5.7.25-OceanBase-v4.3.5.5 | MySQL 兼容模式数据库 |
| OceanBase JDBC | 2.4.12 | OceanBase Java 客户端 |
| HikariCP | 4.0.3 | 数据库连接池 |

---

## 十二、OceanBase 数据表结构

### invalid_data (问题数据表)

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | BIGINT | 主键 (自增) |
| record_key | VARCHAR(500) | 记录主键 |
| database_name | VARCHAR(100) | 数据库名 |
| table_name | VARCHAR(100) | 表名 |
| opcode | VARCHAR(10) | 操作类型 (I/U/D) |
| failure_summary | TEXT | 失败原因汇总 |
| error_fields | TEXT | 错误字段列表 (JSON) |
| error_details | JSON | 详细错误信息 |
| raw_data | JSON | 原始数据 |
| log_timestamp | DATETIME | 日志记录时间 |

### quality_stats (统计表)

| 字段名 | 类型 | 说明 |
|--------|------|------|
| stat_date | DATE | 统计日期 |
| stat_hour | INT | 统计小时 |
| table_name | VARCHAR(100) | 表名 |
| total_count | INT | 总记录数 |
| invalid_count | INT | 问题数据数 |
| invalid_rate | DECIMAL(5,2) | 问题数据率 |

---

## 十三、参考文档

- [Consumer 详细文档](./Consumer/README.md)
- [Producer 详细文档](./Producer/README.md)
- [Kafka Streams 官方文档](https://kafka.apache.org/documentation/streams/)
