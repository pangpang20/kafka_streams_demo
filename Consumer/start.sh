#!/bin/bash

# Kafka Streams 数据质量检查应用 - 启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "Kafka Streams 数据质量检查应用"
echo "======================================"
echo

# 检查是否安装了 Maven
if ! command -v mvn &> /dev/null; then
    echo "错误：Maven 未安装，请先安装 Maven"
    exit 1
fi

# 检查 Kafka 集群是否运行
echo "检查 Kafka 集群状态..."
if ! docker ps 2>/dev/null | grep -q kafka-1; then
    echo "警告：Kafka 集群可能未运行，请先启动 Kafka 集群"
    echo "      运行：../docker/start.sh"
    echo
fi

# 创建输出 Topic（如果不存在）
echo "创建输出 Topic..."
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
    --create --topic mytopic-valid --partitions 3 --replication-factor 3 2>/dev/null || true
docker exec kafka-1 kafka-topics --bootstrap-server localhost:9091 \
    --create --topic mytopic-invalid --partitions 3 --replication-factor 3 2>/dev/null || true

echo "编译项目..."
mvn clean package -DskipTests -q

echo
echo "======================================"
echo "启动 Kafka Streams 应用"
echo "======================================"
echo

java -jar target/kafka-streams-consumer-1.0.0-jar-with-dependencies.jar "$@"
