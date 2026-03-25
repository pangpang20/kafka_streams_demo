#!/bin/bash

# OceanBase 数据监控程序 - 启动脚本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "OceanBase 数据监控程序"
echo "======================================"
echo

# 检查 OceanBase 容器是否运行
if ! docker ps 2>/dev/null | grep -q oceanbase; then
    echo "错误：OceanBase 容器未运行"
    echo "请先启动：../docker/start-sasl.sh"
    exit 1
fi

# 检查 Python 是否安装
if ! command -v python3 &> /dev/null; then
    echo "错误：Python3 未安装"
    exit 1
fi

echo "OceanBase 连接信息:"
echo "  Host: localhost:2881"
echo "  User: root@test"
echo "  Database: kafka_quality_check"
echo
echo "启动监控程序..."
echo

python3 ob_monitor.py "$@"
