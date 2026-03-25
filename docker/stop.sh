#!/bin/bash

# Kafka 三节点高可用集群停止脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "  Kafka 集群停止脚本"
echo "======================================"
echo

echo "正在停止 Kafka 集群..."

# 断开并停止孤立容器（如 mysql）与网络的连接
echo "检查并断开孤立容器的网络连接..."
COMPOSE_SERVICES=$(docker-compose ps --services 2>/dev/null)
for container in $(docker network inspect docker_kafka-cluster --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null); do
    # 检查容器是否在当前 compose 文件定义的服务中
    if ! echo "$COMPOSE_SERVICES" | grep -qx "$container"; then
        echo "  断开孤立容器：$container"
        docker network disconnect -f docker_kafka-cluster "$container" 2>/dev/null || true
    fi
done

# 清理 Zookeeper 中的 broker 注册信息（防止重启时 broker 节点冲突）
echo "清理 Zookeeper 中的 broker 注册信息..."
for broker_id in 1 2 3; do
    docker exec zookeeper-1 bash -c "
        echo 'delete /brokers/ids/${broker_id}' | zookeeper-shell localhost:2181 2>/dev/null || true
    " 2>/dev/null || true
    echo "  已清理 broker-${broker_id} 注册信息"
done

docker-compose down --remove-orphans

echo
echo "======================================"
echo "  Kafka 集群已停止"
echo "======================================"
echo

echo "数据卷已保留，如需删除数据请执行:"
echo "  docker-compose down -v"
echo
