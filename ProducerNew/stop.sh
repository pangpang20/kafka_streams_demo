#!/bin/bash

# ProducerNew 停止脚本

echo "正在停止 ProducerNew..."

# 查找进程
PID=$(ps aux | grep "kafka-producer-new" | grep -v grep | awk '{print $2}')

if [ -n "$PID" ]; then
    echo "找到进程：$PID"
    kill $PID
    echo "已发送停止信号"
    sleep 2

    # 检查是否已停止
    if ps -p $PID > /dev/null; then
        echo "进程仍在运行，强制停止..."
        kill -9 $PID
    fi
    echo "ProducerNew 已停止"
else
    echo "未找到 ProducerNew 进程"
fi
