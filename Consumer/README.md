# Kafka Streams 数据质量检查应用 (Consumer)

基于 Kafka Streams 实现的 CDC 数据质量检查应用，采用结构化的处理流程，便于理解和扩展。

## 目录

1. [快速开始](#快速开始)
2. [Kafka Streams 启动流程](#kafka-streams-启动流程)
3. [数据处理流程](#数据处理流程)
4. [质量检查规则](#质量检查规则)
5. [监控和日志](#监控和日志)
6. [开发指南](#开发指南)
7. [配置说明](#配置说明)
8. [故障排查](#故障排查)

---

## 快速开始

### 前置条件

1. Kafka 集群已启动并运行
2. 所需 Topic 已创建

### 启动步骤

```bash
# 1. 进入 Consumer 目录
cd /opt/kafka_streams_demo/Consumer

# 2. 清理之前的状态 (重要！避免 offset 冲突)
rm -rf /tmp/kafka-streams-quality-check/*

# 3. 启动应用
./start.sh
```

### 启动脚本详解 (`start.sh`)

```bash
#!/bin/bash

# 步骤 1: 检查 Kafka 集群状态
echo "检查 Kafka 集群状态..."
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 --list

# 步骤 2: 创建输出 Topic (如果不存在)
echo "创建输出 Topic..."
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-valid --partitions 3 --replication-factor 3 --if-not-exists
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-invalid --partitions 3 --replication-factor 3 --if-not-exists

# 步骤 3: 编译项目
echo "编译项目..."
mvn clean package -DskipTests -q

# 步骤 4: 运行 Kafka Streams 应用
java -jar target/kafka-streams-consumer-1.0.0-jar-with-dependencies.jar
```

---

## Kafka Streams 启动流程

### 启动阶段说明

```
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 1: 应用初始化                                                │
│ - 加载配置文件 (ConsumerConfig)                                   │
│ - 初始化数据处理器 (DataProcessor)                                │
│ - 初始化异议数据日志 (InvalidDataLogger)                          │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 2: 构建拓扑 (Topology)                                       │
│ - 创建 StreamsBuilder                                            │
│ - 定义输入流：builder.stream("mytopic")                          │
│ - 定义处理流：mapValues(process)                                 │
│ - 定义输出流：to("mytopic-valid"), to("mytopic-invalid")         │
│ - 定义统计流：groupBy().count()                                  │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 3: 创建 KafkaStreams 实例                                     │
│ - 实例化 KafkaStreams(builder.build(), StreamsConfig)           │
│ - 注册关闭钩子 (Shutdown Hook)                                   │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 4: 启动 Streams                                             │
│ - streams.start()                                               │
│ - 创建 StreamThread                                             │
│ - 创建消费者客户端 (Consumer Client)                             │
│ - 创建生产者客户端 (Producer Client)                             │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 5: 消费者组加入和分区分配                                     │
│ - 加入消费者组 (quality-check-app)                               │
│ - 分区分配 (StreamThread-1 分配 mytopic-0,1,2)                    │
│ - 状态转换：CREATED → REBALANCING → STARTING → PARTITIONS_ASSIGNED │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│ 阶段 6: 开始消费和处理消息                                        │
│ - poll 消息                                                      │
│ - 处理：认证 → 解析 → 验证                                       │
│ - 路由到输出 Topic                                               │
│ - 提交 offset                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 状态转换日志

```
[main] INFO org.apache.kafka.streams.KafkaStreams - State transition from CREATED to REBALANCING
[StreamThread-1] INFO org.apache.kafka.streams.processor.internals.StreamThread - State transition from CREATED to STARTING
[StreamThread-1] INFO org.apache.kafka.clients.consumer.internals.AbstractCoordinator - Successfully joined group
[StreamThread-1] INFO org.apache.kafka.streams.processor.internals.StreamThread - State transition from STARTING to PARTITIONS_ASSIGNED
[StreamThread-1] INFO org.apache.kafka.streams.processor.internals.StreamThread - State transition from PARTITIONS_ASSIGNED to RUNNING
```

---

## 数据处理流程

### 详细处理流程

```
Source Topic (mytopic)
        │
        ▼
┌───────────────────────────────────────────────────────────────────┐
│  Step 1: 数据认证 (Authentication)                                │
│  - 检查必需的元数据字段 (databaseName, tableName, opcode, type)   │
│  - 验证数据库类型是否合法 (mysql, oracle, sqlserver)              │
│  - 验证操作类型是否合法 (I=插入，U=更新，D=删除)                  │
└───────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────┐
│  Step 2: 字段解析 (Parsing)                                       │
│  - JSON 字符串解析为 CdcEvent 对象                                 │
│  - 提取 after_data 和 before_data                                 │
│  - 类型转换和验证                                                 │
└───────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────┐
│  Step 3: 质量验证 (Validation)                                    │
│  - 性别验证：只能是"男"或"女"                                     │
│  - 年龄验证：0-120 之间                                            │
│  - 电话验证：1[3-9]\d{9}$                                         │
│  - 血型验证：A/B/O/AB                                             │
│  - 信用分验证：0-1000 之间                                         │
│  - 住房面积验证：非负数                                           │
└───────────────────────────────────────────────────────────────────┘
        │
        ├─────────────────┬─────────────────┐
        ▼                 ▼                 ▼
  mytopic-valid    mytopic-invalid     日志文件
  (通过验证)          (验证失败)      (JSON Lines 格式)
```

### 处理结果示例

**通过验证**:
```
[PASS] key=idcard:I:110101199001011234, opcode=I, table=baseinfo
→ 消息发送到 mytopic-valid
```

**验证失败**:
```
[FAIL] key=idcard:U:110101199001015678, reason=数据质量检查失败：[sex] 性别值'M'不在允许范围内; [telephone] 电话号码'138abc12345'不符合手机号格式
→ 消息发送到 mytopic-invalid
→ 记录到 logs/invalid-data-YYYY-MM-DD.log
```

---

## 质量检查规则

| 字段 | 检查类型 | 规则 | 允许值 | 错误示例 |
|------|----------|------|--------|----------|
| sex | 枚举检查 | 必须是中文 | 男，女 | M, F, male, female, 未知，1, 0, 空值 |
| age | 范围检查 | 0-120 | 0, 18, 60, 120 | -5, 150, 200, 非数字 |
| telephone | 正则检查 | ^1[3-9]\d{9}$ | 13812345678 | 138123, 138abc12345, 12345678901 |
| bloodtype | 枚举检查 | 精确匹配 | A, B, O, AB | A 型，B 型，Rh, C, Unknown |
| creditscore | 范围检查 | 0.0-1000.0 | 300.5, 700.0 | -100, 1500, 非数字 |
| housingareas | 非负检查 | >= 0.0 | 50, 100.5 | -50, -100 |

**注意**: 所有字段允许为 null，null 值不触发验证错误。

---

## 监控和日志

### 控制台日志

```bash
# 实时查看 Consumer 日志
tail -f /opt/kafka_streams_demo/Consumer/logs/app.log 2>/dev/null

# 或者查看标准输出 (如果前台运行)
```

### 异议数据日志

**日志文件位置**:
```
/opt/kafka_streams_demo/Consumer/logs/invalid-data-YYYY-MM-DD.log
```

**日志格式** (JSON Lines):
```json
{
  "recordKey": "idcard:U:110101199001015678",
  "logTimestamp": "2026-03-23 15:30:00.123",
  "record": {...},
  "qualityResult": {
    "passed": false,
    "errors": [
      {"field": "sex", "errorType": "枚举值错误", "message": "..."}
    ]
  },
  "failureSummary": "数据质量检查失败：[sex] 性别值'M'不在允许范围内",
  "errorFields": ["sex"]
}
```

**查看日志统计**:
```bash
# 统计今日错误类型分布
cat /opt/kafka_streams_demo/Consumer/logs/invalid-data-$(date +%Y-%m-%d).log | \
  jq -r '.failureSummary' | sort | uniq -c | sort -rn

# 查看最新的 10 条错误记录
tail -10 /opt/kafka_streams_demo/Consumer/logs/invalid-data-*.log | jq .
```

### Kafka Streams 统计

Consumer 会定期打印统计信息：
```
===== 统计：valid = 150 条 =====
===== 统计：invalid = 25 条 =====
```

### 使用 Kafka UI 监控

1. 访问 http://localhost:28080
2. 查看 Topic 消息量
3. 查看 Consumer Group 状态
4. 浏览 Topic 中的消息

---

## 开发指南

### 添加新验证规则

**步骤 1**: 在 `ConsumerConfig.java` 中添加配置

```java
// 是否检查邮箱
public static final boolean CHECK_EMAIL = true;

// 邮箱正则
public static final String EMAIL_REGEX = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$";
```

**步骤 2**: 在 `DataValidator.java` 中添加验证方法

```java
private void validateEmail(QualityCheckResult result) {
    if (!ConsumerConfig.CHECK_EMAIL) {
        return;
    }

    Object emailValue = getFieldValue(result.getRecord(), "email");
    if (emailValue == null) {
        return;  // 允许为空
    }

    String email = emailValue.toString().trim();
    if (!TELEPHONE_PATTERN.matcher(email).matches()) {
        result.addError("email", "格式错误",
            String.format("邮箱'%s'格式不正确", email),
            email,
            ConsumerConfig.EMAIL_REGEX);
    }
}
```

**步骤 3**: 在 `validate()` 方法中调用

```java
public QualityCheckResult validate(CdcEvent.DataRecord record, String recordKey) {
    QualityCheckResult result = new QualityCheckResult(record, recordKey);

    // 1. 登录认证检查
    if (!authenticate(record)) {
        result.addError("system", "认证失败", "认证未通过", null, null);
        return result;
    }

    // 2. 字段解析检查
    if (record.getAllfields() == null || record.getAllfields().getAfterData() == null) {
        result.addError("structure", "数据结构异常", "缺少 allfields 或 after_data", null, "必需字段");
        return result;
    }

    // 3. 字段质量检查
    validateSex(result);
    validateAge(result);
    validateTelephone(result);
    validateBloodType(result);
    validateCreditScore(result);
    validateHousingArea(result);
    validateEmail(result);  // 添加新检查

    return result;
}
```

### 添加新输出 Topic

**步骤 1**: 在 `ConsumerConfig.java` 中定义新 Topic

```java
public static final String DELETE_DATA_TOPIC = "mytopic-delete";
```

**步骤 2**: 在 `QualityCheckTopology.java` 中添加输出分支

```java
// 删除操作 -> mytopic-delete
processedStream
    .filter((key, result) -> "D".equals(result.getRecord().getOpcode()))
    .mapValues(this::resultToJson)
    .to(ConsumerConfig.DELETE_DATA_TOPIC,
        Produced.with(Serdes.String(), Serdes.String()));

log.info("已定义删除数据输出：Topic={}", ConsumerConfig.DELETE_DATA_TOPIC);
```

### 修改处理逻辑

编辑 `DataProcessor.java`:

```java
public ProcessingResult process(String key, String jsonValue) {
    ProcessingResult result = new ProcessingResult();
    result.setRecordKey(key);

    try {
        // 步骤 1: 认证 + 解析
        CdcEvent event = parseJsonToCdcEvent(jsonValue);

        if (event == null || event.getData() == null || event.getData().length == 0) {
            result.setSuccess(false);
            result.setMessage("数据解析失败：空数据或格式错误");
            return result;
        }

        CdcEvent.DataRecord record = event.getData()[0];
        result.setRecord(record);

        // 步骤 2: 质量检查
        QualityCheckResult qualityResult = validator.validate(record, key);
        result.setQualityResult(qualityResult);

        // 步骤 3: 添加新逻辑 (如：数据丰富化、转换等)
        // EnrichedData enriched = enrichData(record);
        // result.setEnrichedData(enriched);

        // 步骤 4: 记录结果
        if (qualityResult.isPassed()) {
            result.setSuccess(true);
            result.setMessage("数据质量检查通过");
        } else {
            result.setSuccess(false);
            result.setMessage(formatValidationErrors(qualityResult));
        }

    } catch (Exception e) {
        result.setSuccess(false);
        result.setMessage("处理异常：" + e.getMessage());
        log.error("处理异常 [key={}]: {}", key, e.getMessage(), e);
    }

    return result;
}
```

---

## 配置说明

### Kafka 连接配置

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| BOOTSTRAP_SERVERS | localhost:19091,localhost:19092,localhost:19093 | Kafka 集群地址 |
| SOURCE_TOPIC | mytopic | 源 Topic |
| VALID_DATA_TOPIC | mytopic-valid | 正常数据输出 Topic |
| INVALID_DATA_TOPIC | mytopic-invalid | 异常数据输出 Topic |
| APPLICATION_ID | quality-check-app | Kafka Streams 应用 ID |
| STATE_DIR | /tmp/kafka-streams-quality-check | 状态存储目录 |
| AUTO_OFFSET_RESET | earliest | Offset 重置策略 |

### SASL 认证配置

```java
// 启用 SASL 认证
public static final boolean ENABLE_SASL = true;
public static final String SASL_MECHANISM = "PLAIN";
public static final String SASL_JAAS_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username='user' password='password';";
```

### 质量检查规则配置

```java
// 开关配置
public static final boolean CHECK_SEX = true;
public static final boolean CHECK_AGE = true;
public static final boolean CHECK_TELEPHONE = true;
public static final boolean CHECK_BLOOD_TYPE = true;
public static final boolean CHECK_CREDIT_SCORE = true;
public static final boolean CHECK_HOUSING_AREA = true;

// 合法值定义
public static final String[] VALID_SEX_VALUES = {"男", "女"};
public static final String[] VALID_BLOOD_TYPES = {"A", "B", "O", "AB"};

// 范围配置
public static final int MIN_AGE = 0;
public static final int MAX_AGE = 120;
public static final double MIN_CREDIT_SCORE = 0.0;
public static final double MAX_CREDIT_SCORE = 1000.0;
public static final double MIN_HOUSING_AREA = 0.0;

// 正则配置
public static final String TELEPHONE_REGEX = "^1[3-9]\\d{9}$";
```

---

## 故障排查

### Consumer 无法启动

**问题**: `java.net.ConnectException: Connection refused`

**解决**:
```bash
# 检查 Kafka 集群状态
docker-compose ps

# 检查端口是否可访问
telnet localhost 19091

# 查看 Kafka 日志
docker-compose logs kafka-1
```

### Consumer Group 无法消费

**问题**: Consumer 启动但无法消费消息

**解决**:
```bash
# 删除 Consumer Group 重置
docker exec kafka-1 kafka-consumer-groups --bootstrap-server localhost:9091 \
  --delete --group quality-check-app

# 清理状态目录
rm -rf /tmp/kafka-streams-quality-check/*

# 重启 Consumer
```

### ClassCastException

**问题**: `ClassCastException while producing data to topic`

**原因**: groupBy 操作时 Serde 类型不匹配

**解决**: 使用 mapValues 转换为 String 后再 groupBy
```java
processedStream
    .mapValues(result -> result.isSuccess() ? "valid" : "invalid")
    .groupBy((key, val) -> val, Grouped.with(Serdes.String(), Serdes.String()))
    .count(Materialized.as("quality-check-stats"))
```

### 日志文件为空

**问题**: `logs/invalid-data-*.log` 文件存在但为空

**原因**: 没有数据验证失败

**解决**:
- 确认 Producer 发送了包含问题数据的消息
- 检查验证规则是否开启
- 增加 Producer 发送量或问题数据概率

---

## 构建说明

```bash
# 编译项目
mvn clean package -DskipTests

# 生成的 jar 包
target/kafka-streams-consumer-1.0.0-jar-with-dependencies.jar

# 手动运行
java -jar target/kafka-streams-consumer-1.0.0-jar-with-dependencies.jar
```

---

## 项目结构

```
Consumer/
├── src/main/java/com/kafka/consumer/
│   ├── KafkaStreamsQualityCheck.java  # 主程序入口
│   ├── config/
│   │   └── ConsumerConfig.java        # 配置类 (所有配置集中管理)
│   ├── model/
│   │   ├── CdcEvent.java              # CDC 数据模型
│   │   └── QualityCheckResult.java    # 质检结果模型
│   ├── processor/
│   │   ├── DataProcessor.java         # 数据处理器 (认证→解析→验证)
│   │   └── ProcessingResult.java      # 处理结果模型
│   ├── stream/
│   │   ├── QualityCheckTopology.java  # Kafka Streams 拓扑构建
│   │   └── InvalidDataLogger.java     # 异议数据日志记录
│   └── validator/
│       └── DataValidator.java         # 数据验证器 (所有验证规则)
├── logs/                               # 运行时日志目录
│   ├── app.log                        # 应用日志
│   └── invalid-data-YYYY-MM-DD.log    # 异议数据日志
├── pom.xml                             # Maven 配置
├── start.sh                            # 启动脚本
└── README.md                           # 本文档
```
