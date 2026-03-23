#!/bin/bash

# Kafka 三节点高可用集群启动脚本
# 基于 Kafka 2.8.2 (Confluent Platform 6.2.0)
# SASL_PLAINTEXT 认证

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "  Kafka 三节点高可用集群启动脚本"
echo "  版本：Kafka 2.8.2 (CP 6.2.0)"
echo "  认证：SASL_PLAINTEXT"
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

# 拉取镜像
echo "[1/4] 拉取所需镜像..."
docker-compose pull -q

# 启动服务
echo "[2/4] 启动 Zookeeper 集群 (3节点)..."
docker-compose up -d zookeeper-1 zookeeper-2 zookeeper-3

# 等待 Zookeeper 就绪
echo "[3/4] 等待 Zookeeper 集群就绪..."
sleep 5

for i in {1..30}; do
    if docker exec zookeeper-1 bash -c '(exec 3<>/dev/tcp/localhost/2181) 2>/dev/null' && echo "  ✓ Zookeeper 集群已就绪" && break; then
        true
    fi
    if [ $i -eq 30 ]; then
        echo "  ✗ Zookeeper 启动超时"
        exit 1
    fi
    sleep 1
done

echo "[4/4] 启动 Kafka 集群 (3节点)..."
docker-compose up -d kafka-1 kafka-2 kafka-3 kafka-ui

# 等待 Kafka 就绪
echo "  等待 Kafka 集群就绪..."
sleep 15

for i in {1..90}; do
    if docker exec kafka-1 bash -c 'kafka-broker-api-versions --bootstrap-server kafka-1:9091' &>/dev/null; then
        echo "  ✓ Kafka 集群已就绪"
        break
    fi
    if [ $i -eq 90 ]; then
        echo "  ✗ Kafka 启动超时，请检查日志：docker-compose logs"
        exit 1
    fi
    sleep 1
done

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
echo "│                                                             │"
echo "│  【Bootstrap Servers】                                       │"
echo "│  localhost:19091,localhost:19092,localhost:19093            │"
echo "│                                                             │"
echo "│  【外部连接地址】                                            │"
echo "│  $HOST_IP:19091,$HOST_IP:19092,$HOST_IP:19093               │"
echo "│                                                             │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                     节点信息                                 │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                                                             │"
echo "│  Kafka Node 1:                                              │"
echo "│    - Broker ID: 1                                           │"
echo "│    - 端口：19091                                            │"
echo "│    - 容器名：kafka-1                                        │"
echo "│                                                             │"
echo "│  Kafka Node 2:                                              │"
echo "│    - Broker ID: 2                                           │"
echo "│    - 端口：19092                                            │"
echo "│    - 容器名：kafka-2                                        │"
echo "│                                                             │"
echo "│  Kafka Node 3:                                              │"
echo "│    - Broker ID: 3                                           │"
echo "│    - 端口：19093                                            │"
echo "│    - 容器名：kafka-3                                        │"
echo "│                                                             │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                     SASL 认证信息                            │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                                                             │"
echo "│  安全协议 (security.protocol): SASL_PLAINTEXT               │"
echo "│  SASL 机制 (sasl.mechanism): PLAIN                          │"
echo "│  用户名：admin                                              │"
echo "│  密码：Audaque@123                                          │"
echo "│                                                             │"
echo "│  客户端配置示例：                                            │"
echo "│  security.protocol=SASL_PLAINTEXT                           │"
echo "│  sasl.mechanism=PLAIN                                       │"
echo "│  sasl.jaas.config=org.apache.kafka.common.security.plain.   │"
echo "│    PlainLoginModule required username=\"admin\"             │"
echo "│    password=\"Audaque@123\";                                │"
echo "│                                                             │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                   Web UI 管理界面                            │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                                                             │"
echo "│  Kafka UI: http://localhost:28080                           │"
echo "│  或：http://$HOST_IP:28080                                  │"
echo "│                                                             │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                    常用命令                                  │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│                                                             │"
echo "│  查看 Topic 列表 (内部网络):                                │"
echo "│    docker exec kafka-1 kafka-topics \\                       │"
echo "│      --bootstrap-server kafka-1:9091 --list                 │"
echo "│                                                             │"
echo "│  创建 Topic (3分区,3副本):                                  │"
echo "│    docker exec kafka-1 kafka-topics \\                       │"
echo "│      --bootstrap-server kafka-1:9091 \\                      │"
echo "│      --create --topic my-topic \\                            │"
echo "│      --partitions 3 --replication-factor 3                  │"
echo "│                                                             │"
echo "│  外部客户端连接配置：                                        │"
echo "│    bootstrap.servers=localhost:19091                        │"
echo "│    security.protocol=SASL_PLAINTEXT                         │"
echo "│    sasl.mechanism=PLAIN                                     │"
echo "│    sasl.jaas.config=PlainLoginModule...                     │"
echo "│                                                             │"
echo "└─────────────────────────────────────────────────────────────┘"
echo

# 显示集群状态
echo "当前运行容器:"
docker-compose ps
