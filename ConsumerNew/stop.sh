#!/bin/bash

# ConsumerNew 停止脚本
# 使用说明：./stop.sh <task-id>
#
# 选项:
#   --help       显示帮助信息
#
# 示例:
#   ./stop.sh consumer-20260330-120000-12345

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TASK_DIR="${SCRIPT_DIR}/.kafka_tasks"

# 显示帮助
show_help() {
    echo "用法：$0 <task-id>"
    echo ""
    echo "选项:"
    echo "  --help   显示帮助"
    echo ""
    echo "示例:"
    echo "  $0 consumer-20260330-120000-12345"
    echo "  $0 my-task-001"
}

# 查找任务文件
find_task_file() {
    local task_id="$1"
    local task_file="${TASK_DIR}/${task_id}.task"

    if [ -f "$task_file" ]; then
        echo "$task_file"
        return 0
    fi

    # 尝试在所有任务文件中查找
    if [ -d "$TASK_DIR" ]; then
        for f in "$TASK_DIR"/*.task; do
            if [ -f "$f" ]; then
                local file_task_id=$(grep "^TASK_ID=" "$f" 2>/dev/null | cut -d'=' -f2 | tr -d '"')
                if [ "$file_task_id" = "$task_id" ]; then
                    echo "$f"
                    return 0
                fi
            fi
        done
    fi

    return 1
}

# 主程序
if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    show_help
    exit 0
fi

if [ -z "$1" ]; then
    echo -e "\033[0;31m错误：请指定任务 ID\033[0m"
    echo ""
    echo "使用 --help 查看帮助"
    exit 1
fi

TASK_ID="$1"
TASK_FILE=$(find_task_file "$TASK_ID")

if [ -z "$TASK_FILE" ] || [ ! -f "$TASK_FILE" ]; then
    echo -e "\033[0;31m未找到任务：$TASK_ID\033[0m"
    echo ""
    echo "使用 ./status.sh --list 查看所有任务"
    exit 1
fi

# 读取任务信息
source "$TASK_FILE"

echo "========================================"
echo "       停止 ConsumerNew 任务"
echo "========================================"
echo "任务 ID:   $TASK_ID"
echo "进程 PID:  $PID"
echo "========================================"

# 检查进程是否运行
if kill -0 "$PID" 2>/dev/null; then
    echo "正在停止进程 (PID: $PID)..."
    kill "$PID"

    # 等待进程结束
    for i in {1..10}; do
        if ! kill -0 "$PID" 2>/dev/null; then
            echo "任务已正常停止"
            break
        fi
        sleep 1
    done

    # 强制停止
    if kill -0 "$PID" 2>/dev/null; then
        echo "进程仍在运行，强制停止..."
        kill -9 "$PID"
        sleep 1
    fi

    if ! kill -0 "$PID" 2>/dev/null; then
        echo "任务已停止"
    else
        echo -e "\033[0;31m警告：无法停止进程\033[0m"
    fi
else
    echo "进程未运行 (PID: $PID)"
fi

# 更新任务文件状态
cat > "$TASK_FILE" << EOF
# Kafka Consumer 任务信息
TASK_ID="$TASK_ID"
APP_NAME="ConsumerNew"
CONFIG_DIR="$CONFIG_DIR"
PID="$PID"
START_TIME="$START_TIME"
STOP_TIME=$(date +%s%3N)
STATUS="stopped"
EOF

echo "任务文件已更新：$TASK_FILE"
echo ""
echo "使用 ./status.sh $TASK_ID 查看任务状态"
