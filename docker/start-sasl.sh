#!/bin/bash

# Kafka 三节点高可用集群启动脚本 (SASL_PLAINTEXT 认证版本)
# 基于 Kafka 2.8.2 (Confluent Platform 6.2.0)
# 认证方式：SASL_PLAINTEXT / PLAIN
# 用户名：admin
# 密码：Audaque@123

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "  Kafka 三节点高可用集群 (SASL 认证)"
echo "  版本：Kafka 2.8.2 (CP 6.2.0)"
echo "======================================"
echo

# 启动服务
echo "[1/3] 启动 Zookeeper 集群 (3 节点)..."
docker-compose -f docker-compose-sasl.yml up -d zookeeper-1 zookeeper-2 zookeeper-3

echo "[2/3] 等待 Zookeeper 集群就绪..."
sleep 5
echo "  ✓ Zookeeper 集群已启动"

echo "[3/3] 启动 Kafka 集群 (3 节点)..."
docker-compose -f docker-compose-sasl.yml up -d kafka-1 kafka-2 kafka-3 kafka-ui

echo "  等待 Kafka 集群就绪..."
sleep 20

echo "  ✓ Kafka 集群已就绪"

echo
echo "======================================"
echo "      Kafka 集群启动成功!"
echo "======================================"
echo

HOST_IP=$(hostname -I | awk '{print $1}')

echo "┌─────────────────────────────────────────────────────────────┐"
echo "│                     集群连接信息                             │"
echo "├─────────────────────────────────────────────────────────────┤"
echo "│  【Bootstrap Servers】                                       │"
echo "│  localhost:19091,localhost:19092,localhost:19093            │"
echo "│                                                              │"
echo "│  【SASL 认证配置】                                           │"
echo "│  安全协议：SASL_PLAINTEXT                                    │"
echo "│  SASL 机制：PLAIN                                            │"
echo "│  用户名：admin                                               │"
echo "│  密码：Audaque@123                                           │"
echo "│                                                              │"
echo "│  【Web UI 管理界面】                                         │"
echo "│  http://localhost:28080                                     │"
echo "│  或：http://$HOST_IP:28080                                  │"
echo "└─────────────────────────────────────────────────────────────┘"
echo

echo "当前运行容器:"
docker-compose -f docker-compose-sasl.yml ps
