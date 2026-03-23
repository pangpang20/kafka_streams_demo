# Kafka Streams 数据质量检查应用 (Consumer)

基于 Kafka Streams 实现的 CDC 数据质量检查应用，采用结构化的处理流程，便于理解和扩展。

## 处理流程

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
     │ mytopic-valid  │ │ mytopic-invalid │ │  日志/监控   │
     │ (正常数据)      │ │ (异常数据)       │ │ (打印统计)   │
     └────────────────┘ └─────────────────┘ └──────────────┘
```

## 质量检查规则

| 字段 | 检查规则 | 允许值/范围 |
|------|----------|-------------|
| sex | 枚举检查 | 男，女 |
| age | 范围检查 | 0-120 |
| telephone | 正则检查 | ^1[3-9]\d{9}$ |
| bloodtype | 枚举检查 | A, B, O, AB |
| creditscore | 范围检查 | 0-1000 |
| housingareas | 非负检查 | >=0 |

## 快速开始

### 1. 启动 Kafka 集群

```bash
cd ../docker
./start.sh
```

### 2. 创建所需 Topic

```bash
# 源 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic --partitions 3 --replication-factor 3

# 输出 Topic
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-valid --partitions 3 --replication-factor 3

docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
  --create --topic mytopic-invalid --partitions 3 --replication-factor 3
```

### 3. 启动消费者

```bash
./start.sh
```

### 4. 启动生产者（测试用）

```bash
# 在另一个终端启动 Producer
cd ../Producer
./start.sh
```

## 项目结构

```
Consumer/
├── src/main/java/com/kafka/consumer/
│   ├── KafkaStreamsQualityCheck.java  # 主程序入口
│   ├── config/
│   │   └── ConsumerConfig.java        # 配置类
│   ├── model/
│   │   ├── CdcEvent.java              # CDC 数据模型
│   │   └── QualityCheckResult.java    # 质检结果模型
│   ├── processor/
│   │   ├── DataProcessor.java         # 数据处理器
│   │   └── ProcessingResult.java      # 处理结果模型
│   ├── stream/
│   │   └── QualityCheckTopology.java  # Streams 拓扑
│   └── validator/
│       └── DataValidator.java         # 数据验证器
└── pom.xml
```

## 开发指南

### 添加新的验证规则

1. **在 ConsumerConfig 中添加配置**:

```java
// 是否检查邮箱
public static final boolean CHECK_EMAIL = true;

// 邮箱正则
public static final String EMAIL_REGEX = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$";
```

2. **在 DataValidator 中添加验证方法**:

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
    if (!email.matches(ConsumerConfig.EMAIL_REGEX)) {
        result.addError("email", "格式错误",
            String.format("邮箱'%s'格式不正确", email),
            email,
            ConsumerConfig.EMAIL_REGEX);
    }
}
```

3. **在 validate 方法中调用**:

```java
public QualityCheckResult validate(CdcEvent.DataRecord record, String recordKey) {
    QualityCheckResult result = new QualityCheckResult(record, recordKey);

    // ... 现有检查 ...

    validateEmail(result);  // 添加新检查

    return result;
}
```

### 修改处理流程

编辑 `DataProcessor.java`:

```java
public ProcessingResult process(String key, String jsonValue) {
    // 步骤 1: 认证 + 解析
    CdcEvent event = parseJsonToCdcEvent(jsonValue);

    // 步骤 2: 质量检查
    QualityCheckResult qualityResult = validator.validate(record, key);

    // 步骤 3: 添加新步骤（如：数据转换、丰富化等）
    // enrichedData = enrichData(event);

    // ...
}
```

### 扩展输出目标

编辑 `QualityCheckTopology.java`:

```java
// 添加新的分支
processedStream
    .filter((key, result) -> result.getRecord().getOpcode().equals("D"))
    .mapValues(this::resultToJson)
    .to(ConsumerConfig.DELETE_DATA_TOPIC,  // 新 Topic
        Produced.with(Serdes.String(), Serdes.String()));
```

## 配置说明

编辑 `src/main/java/com/kafka/consumer/config/ConsumerConfig.java`:

```java
// Kafka 连接配置
public static final String BOOTSTRAP_SERVERS = "localhost:19091,localhost:19092,localhost:19093";
public static final String SOURCE_TOPIC = "mytopic";
public static final String VALID_DATA_TOPIC = "mytopic-valid";
public static final String INVALID_DATA_TOPIC = "mytopic-invalid";

// SASL 认证配置
public static final boolean ENABLE_SASL = false;  // 改为 true 启用认证

// 质量检查规则配置
public static final boolean CHECK_SEX = true;      // 是否检查性别
public static final boolean CHECK_AGE = true;      // 是否检查年龄
public static final boolean CHECK_TELEPHONE = true; // 是否检查电话
// ...
```

## 监控和日志

应用运行时会输出详细日志：

```
[PASS] key=idcard:123:I:1234567890, opcode=I, table=baseinfo
[FAIL] key=idcard:456:U:1234567891, reason=数据质量检查失败：[sex] 性别值'M'不在允许范围内
===== 统计：valid = 150 条 =====
===== 统计：invalid = 25 条 =====
```

## 构建

```bash
mvn clean package
```

生成的 jar 包：`target/kafka-streams-consumer-1.0.0-jar-with-dependencies.jar`
