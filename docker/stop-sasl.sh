#!/bin/bash

# Kafka 三节点高可用集群停止脚本 (SASL 版本)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "  Kafka 集群停止脚本 (SASL 版本)"
echo "======================================"
echo

echo "正在停止 Kafka 集群..."
docker-compose -f docker-compose-sasl.yml down

echo
echo "======================================"
echo "  Kafka 集群已停止"
echo "======================================"
echo
echo "数据卷已保留，如需删除数据请执行:"
echo "  docker-compose -f docker-compose-sasl.yml down -v"
echo
