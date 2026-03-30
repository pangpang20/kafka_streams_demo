#!/bin/bash

# ConsumerNew 启动脚本
# 使用说明：./start.sh [选项]
#
# 选项:
#   --config-dir <dir_path>  指定配置目录 (默认：config01)
#   --task-id <task_id>      指定任务 ID (默认：自动生成)
#   --help                   显示帮助信息
#
# 示例:
#   ./start.sh --config-dir config01 --task-id task-001
#   ./start.sh -c config02 -t my-task-001

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 默认配置
CONFIG_DIR="${SCRIPT_DIR}/config01"
TASK_ID=""
TASK_DIR="${SCRIPT_DIR}/.kafka_tasks"

# 解析参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--config-dir)
            CONFIG_DIR="$2"
            shift 2
            ;;
        -t|--task-id)
            TASK_ID="$2"
            shift 2
            ;;
        -h|--help)
            echo "用法：$0 [选项]"
            echo ""
            echo "选项:"
            echo "  -c, --config-dir <dir>   配置目录 (默认：config01)"
            echo "  -t, --task-id <id>       任务 ID (默认：自动生成)"
            echo "  -h, --help               显示帮助"
            echo ""
            echo "示例:"
            echo "  $0 --config-dir config01"
            echo "  $0 --config-dir config02 --task-id my-task-001"
            exit 0
            ;;
        *)
            echo "未知选项：$1"
            echo "使用 --help 查看帮助"
            exit 1
            ;;
    esac
done

# 自动生成任务 ID
if [ -z "$TASK_ID" ]; then
    TASK_ID="consumer-$(date +%Y%m%d-%H%M%S)-$$"
fi

# 验证配置目录
if [ ! -d "$CONFIG_DIR" ]; then
    echo -e "\033[0;31m错误：配置目录不存在：$CONFIG_DIR\033[0m"
    exit 1
fi

echo "========================================"
echo "       ConsumerNew 启动"
echo "========================================"
echo "任务 ID:     $TASK_ID"
echo "配置目录：   $CONFIG_DIR"
echo "========================================"

# 检查是否已编译
if [ ! -f "target/kafka-streams-consumer-new-1.0.0-jar-with-dependencies.jar" ]; then
    echo "正在编译项目..."
    mvn clean package -DskipTests
    if [ $? -ne 0 ]; then
        echo "编译失败"
        exit 1
    fi
fi

# 创建日志目录和任务目录
mkdir -p logs
mkdir -p "$TASK_DIR"

# 生成日志文件名
LOG_FILE="logs/app-${TASK_ID}.log"

# 启动应用
echo "启动 ConsumerNew..."
echo "日志文件：$LOG_FILE"

# 在后台运行 Java 进程
nohup java -Xms512m -Xmx2g \
     -jar target/kafka-streams-consumer-new-1.0.0-jar-with-dependencies.jar \
     --config-dir "$CONFIG_DIR" \
     --task-id "$TASK_ID" \
     > "$LOG_FILE" 2>&1 &

PID=$!

# 保存任务信息
TASK_FILE="${TASK_DIR}/${TASK_ID}.task"
cat > "$TASK_FILE" << EOF
# Kafka Consumer 任务信息
TASK_ID="$TASK_ID"
APP_NAME="ConsumerNew"
CONFIG_DIR="$CONFIG_DIR"
PID="$PID"
START_TIME=$(date +%s%3N)
EOF

echo "进程已启动 (PID: $PID)"
echo "任务文件：$TASK_FILE"
echo ""
echo "使用 ./status.sh $TASK_ID 查看状态"
echo "使用 ./stop.sh $TASK_ID 停止任务"
