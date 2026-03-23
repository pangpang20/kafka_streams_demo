# Kafka Streams Demo 项目

本项目包含两个部分：
1. **Producer**: CDC 数据生成器，模拟数据库变更记录
2. **Consumer**: Kafka Streams 数据质量检查应用

## 项目结构

```
/opt/kafka_streams_demo/
├── docker/                     # Docker 部署
│   ├── docker-compose.yml      # Kafka 集群配置
│   ├── start.sh                # 启动脚本
│   └── stop.sh                 # 停止脚本
├── Producer/                   # 数据生产者
│   ├── src/main/java/...       # 源代码
│   ├── pom.xml                 # Maven 配置
│   ├── start.sh                # 启动脚本
│   └── README.md               # 使用说明
└── Consumer/                   # 数据消费者
    ├── src/main/java/...       # 源代码
    ├── pom.xml                 # Maven 配置
    ├── start.sh                # 启动脚本
    └── README.md               # 使用说明
```

## 快速开始

### 1. 启动 Kafka 集群

```bash
cd /opt/kafka_streams_demo/docker
./start.sh
```

### 2. 创建 Topic

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

### 3. 启动 Consumer（数据质量检查）

```bash
cd /opt/kafka_streams_demo/Consumer
./start.sh
```

### 4. 启动 Producer（数据生成）

```bash
cd /opt/kafka_streams_demo/Producer
./start.sh          # 无限运行
./start.sh 60       # 运行 60 秒
```

## 数据流程

```
Producer --> mytopic --> Consumer (质量检查)
                              |
              +---------------+---------------+
              |               |               |
              v               v               v
         mytopic-valid  mytopic-invalid    日志/统计
         (正常数据)      (异常数据)
```

## 质量检查规则

| 字段 | 规则 | 允许值 |
|------|------|--------|
| sex | 枚举检查 | 男，女 |
| age | 范围检查 | 0-120 |
| telephone | 正则检查 | 1[3-9]\d{9} |
| bloodtype | 枚举检查 | A, B, O, AB |
| creditscore | 范围检查 | 0-1000 |
| housingareas | 非负检查 | >=0 |

## 开发指南

### Consumer 结构化处理流程

```
1. 认证 (Authentication) - 验证数据源合法性
2. 解析 (Parsing) - 将 JSON 解析为对象
3. 验证 (Validation) - 执行业务规则检查
```

**添加新验证规则**:
1. 在 `ConsumerConfig.java` 中添加配置项
2. 在 `DataValidator.java` 中添加验证方法
3. 在 `validate()` 方法中调用新验证

详细参考 `Consumer/README.md`

### Producer 数据生成

- 插入操作：50%
- 更新操作：45%
- 删除操作：5%
- 问题数据：10%

详细参考 `Producer/README.md`
