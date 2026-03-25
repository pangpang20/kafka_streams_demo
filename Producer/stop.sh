#!/bin/bash

# CDC 数据生成器 - 停止脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "CDC 数据生成器停止脚本"
echo "======================================"
echo

# 查找并停止 Producer 进程
echo "正在查找 Kafka Producer 进程..."

# 尝试多种方式查找进程
PID=$(pgrep -f "kafka-producer" 2>/dev/null || true)

if [ -z "$PID" ]; then
    # 尝试通过 jar 包名查找
    PID=$(pgrep -f "kafka-producer-1.0.0" 2>/dev/null || true)
fi

if [ -z "$PID" ]; then
    # 尝试通过主类名查找
    PID=$(pgrep -f "KafkaCDCProducer" 2>/dev/null || true)
fi

if [ -n "$PID" ]; then
    echo "找到 Producer 进程 (PID: $PID)"
    echo "正在停止 Producer 进程..."

    # 发送 SIGTERM 信号，优雅关闭
    kill -15 $PID 2>/dev/null || true

    # 等待进程停止
    echo "等待进程停止..."
    for i in {1..10}; do
        if ! kill -0 $PID 2>/dev/null; then
            echo "Producer 进程已停止"
            break
        fi
        sleep 1
    done

    # 如果还在运行，强制停止
    if kill -0 $PID 2>/dev/null; then
        echo "进程未响应，强制停止..."
        kill -9 $PID 2>/dev/null || true
        echo "Producer 进程已强制停止"
    fi
else
    echo "未找到运行中的 Producer 进程"
fi

echo
echo "======================================"
echo "Producer 已停止"
echo "======================================"
echo
