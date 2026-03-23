#!/bin/bash

# Kafka 三节点高可用集群启动脚本
# 基于 Kafka 2.8.2 (Confluent Platform 6.2.0)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "  Kafka 三节点高可用集群启动脚本"
echo "  版本：Kafka 2.8.2 (CP 6.2.0)"
echo "======================================"
echo

# 检查 Docker 和 Docker Compose
if ! command -v docker &> /dev/null; then
    echo "错误：Docker 未安装"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "错误：Docker Compose 未安装"
    exit 1
fi

# 启动服务
echo "[1/4] 启动 Zookeeper 集群 (3 节点)..."
docker-compose up -d zookeeper-1 zookeeper-2 zookeeper-3

# 等待 Zookeeper 就绪
echo "[2/4] 等待 Zookeeper 集群就绪..."
sleep 5
echo "  ✓ Zookeeper 集群已启动"

echo "[3/4] 启动 Kafka 集群 (3 节点)..."
docker-compose up -d kafka-1 kafka-2 kafka-3 kafka-ui

# 等待 Kafka 就绪
echo "  等待 Kafka 集群就绪..."
sleep 15

echo "  ✓ Kafka 集群已就绪"

echo "[4/4] 启动 OceanBase 数据库..."
docker-compose up -d oceanbase

# 等待 OceanBase 就绪 (OceanBase 启动较慢，约需 60-90 秒)
echo "  等待 OceanBase 数据库就绪 (约 60-90 秒)..."
sleep 60

# 检查 OceanBase 是否就绪
MAX_RETRIES=30
RETRY_COUNT=0
while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec oceanbase mysql -h 127.0.0.1 -P 2881 -u root -e "SELECT 1" > /dev/null 2>&1; then
        echo "  ✓ OceanBase 数据库已就绪"
        break
    fi
    echo "  等待 OceanBase 启动中... ($((RETRY_COUNT + 1))/$MAX_RETRIES)"
    sleep 5
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "  ⚠ OceanBase 启动超时，请稍后检查日志：docker-compose logs oceanbase"
fi

echo
echo "======================================"
echo "      Kafka 集群启动成功!"
echo "======================================"
echo

# 获取本机 IP
HOST_IP=$(hostname -I | awk '{print $1}')

echo "┌─────────────────────────────────────────────────────────────┐"
echo "│                     集群连接信息                             │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│  【Bootstrap Servers】                                       │"
echo "│  localhost:19091,localhost:19092,localhost:19093            │"
echo "│                                                              │"
echo "│  【Web UI 管理界面】                                         │"
echo "│  http://localhost:28080                                     │"
echo "│  或：http://$HOST_IP:28080                                  │"
echo "│                                                              │"
echo "│  【OceanBase 数据库】                                        │"
echo "│  Host: oceanbase:2881 (Docker 网络内)                       │"
echo "│  Host: localhost:2881 (宿主机访问)                          │"
echo "│  User: root                                                 │"
echo "│  Database: kafka_quality_check                              │"
echo "│  注意：OceanBase 启动较慢，请耐心等待                         │"
echo "│                                                              │"
echo "│  【节点信息】                                                │"
echo "│  Kafka Node 1: localhost:19091 (Broker ID: 1)               │"
echo "│  Kafka Node 2: localhost:19092 (Broker ID: 2)               │"
echo "│  Kafka Node 3: localhost:19093 (Broker ID: 3)               │"
echo "└─────────────────────────────────────────────────────────────┘"
echo

# 显示集群状态
echo "当前运行容器:"
docker-compose ps
