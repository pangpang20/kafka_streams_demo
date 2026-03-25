#!/bin/bash

# Kafka Streams 数据质量检查应用 - 停止脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "======================================"
echo "Kafka Streams 消费者停止脚本"
echo "======================================"
echo

# 查找并停止 Consumer 进程
echo "正在查找 Kafka Streams Consumer 进程..."

# 尝试多种方式查找进程
PID=$(pgrep -f "kafka-streams-consumer" 2>/dev/null || true)

if [ -z "$PID" ]; then
    # 尝试通过 jar 包名查找
    PID=$(pgrep -f "kafka-streams-consumer-1.0.0" 2>/dev/null || true)
fi

if [ -z "$PID" ]; then
    # 尝试通过主类名查找
    PID=$(pgrep -f "KafkaStreamsQualityCheck" 2>/dev/null || true)
fi

if [ -n "$PID" ]; then
    echo "找到 Consumer 进程 (PID: $PID)"
    echo "正在停止 Consumer 进程..."

    # 发送 SIGTERM 信号，优雅关闭
    kill -15 $PID 2>/dev/null || true

    # 等待进程停止
    echo "等待进程停止..."
    for i in {1..10}; do
        if ! kill -0 $PID 2>/dev/null; then
            echo "Consumer 进程已停止"
            break
        fi
        sleep 1
    done

    # 如果还在运行，强制停止
    if kill -0 $PID 2>/dev/null; then
        echo "进程未响应，强制停止..."
        kill -9 $PID 2>/dev/null || true
        echo "Consumer 进程已强制停止"
    fi
else
    echo "未找到运行中的 Consumer 进程"
fi

# 清理可能残留的流处理进程
echo "检查残留的流处理进程..."
STREAMS_PIDS=$(pgrep -f "quality-check-app" 2>/dev/null || true)
if [ -n "$STREAMS_PIDS" ]; then
    echo "发现残留进程，正在清理..."
    echo "$STREAMS_PIDS" | xargs kill -9 2>/dev/null || true
    echo "残留进程已清理"
fi

echo
echo "======================================"
echo "Consumer 已停止"
echo "======================================"
echo
