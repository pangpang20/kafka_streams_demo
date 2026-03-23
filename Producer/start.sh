#!/bin/bash

# CDC 数据生成器 - 启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "CDC 数据生成器 - Kafka Producer"
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
    echo "警告：Kafka 集群可能未运行"
    echo
fi

echo "编译项目..."
mvn clean package -DskipTests -q

echo
echo "======================================"
echo "启动 CDC 数据生成器"
echo "======================================"
echo

# 运行时长参数（秒），0 表示无限运行
DURATION=${1:-0}

if [ "$DURATION" = "0" ]; then
    echo "运行模式：无限运行（按 Ctrl+C 停止）"
else
    echo "运行模式：运行 $DURATION 秒"
fi

echo

java -jar target/kafka-producer-1.0.0-jar-with-dependencies.jar "$DURATION"
