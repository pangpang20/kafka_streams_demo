#!/bin/bash

# Kafka 三节点高可用集群停止脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "  Kafka 三节点高可用集群停止脚本"
echo "======================================"
echo

if [ ! -f "docker-compose.yml" ]; then
    echo "错误: 未找到 docker-compose.yml 文件"
    exit 1
fi

echo "正在停止 Kafka 集群..."
docker-compose down --volumes=false

echo
echo "======================================"
echo "  Kafka 集群已停止"
echo "======================================"
echo
echo "数据卷已保留，如需删除数据请执行:"
echo "  docker volume rm docker_kafka-1-data docker_kafka-2-data docker_kafka-3-data"
echo "  docker volume rm docker_zookeeper-1-data docker_zookeeper-2-data docker_zookeeper-3-data"
echo
