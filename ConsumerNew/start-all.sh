#!/bin/bash

# ================================================
# Kafka Streams 批量启动脚本
# ================================================
#
# 用法:
#   ./start-all.sh --from 1 --to 250 --server localhost
#   ./start-all.sh --list hosts.txt --jar consumer-new.jar
#
# ================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="${SCRIPT_DIR}/target/kafka-streams-consumer-new-1.0.0-jar-with-dependencies.jar"
LOG_DIR="${SCRIPT_DIR}/logs"
TASK_DIR="${SCRIPT_DIR}/.kafka_tasks"
JVM_OPTS=${JVM_OPTS:="-Xmx256m -Xms128m"}

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 确保目录存在
mkdir -p "$LOG_DIR" "$TASK_DIR"

# 显示帮助
show_help() {
    cat <<EOF
用法：$0 [选项]

批量启动 Kafka Streams 进程（一进一表模式）

选项:
  --from <N>           起始表编号
  --to <N>             结束表编号
  --prefix <name>      Topic 和表名前缀（默认：table）
  --server <host>      服务器地址（默认：localhost）
  --jvm-opts <opts>    JVM 参数（默认：-Xmx256m -Xms128m）
  --dry-run            只显示命令，不实际执行
  --help               显示帮助信息

示例:
  # 启动表 1-250
  $0 --from 1 --to 250

  # 启动表 100-200，使用自定义前缀
  $0 --from 100 --to 200 --prefix mybiz

  # 预演模式（不实际启动）
  $0 --from 1 --to 10 --dry-run

环境变量:
  JVM_OPTS             JVM 参数
  CHECK_INTERVAL       看门狗检查间隔（秒）

配套命令:
  ./stop-all.sh        批量停止所有进程
  ./watchdog.sh start  启动看门狗
EOF
}

# 解析参数
FROM_NUM=""
TO_NUM=""
PREFIX="table"
SERVER="localhost"
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --from)
            FROM_NUM="$2"
            shift 2
            ;;
        --to)
            TO_NUM="$2"
            shift 2
            ;;
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --server)
            SERVER="$2"
            shift 2
            ;;
        --jvm-opts)
            JVM_OPTS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}未知选项：$1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# 验证参数
if [ -z "$FROM_NUM" ] || [ -z "$TO_NUM" ]; then
    echo -e "${RED}错误：必须指定 --from 和 --to 参数${NC}"
    show_help
    exit 1
fi

if [ "$FROM_NUM" -gt "$TO_NUM" ]; then
    echo -e "${RED}错误：--from 不能大于 --to${NC}"
    exit 1
fi

# 检查 JAR 文件
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${YELLOW}警告：未找到 JAR 文件：$JAR_FILE${NC}"
    echo "请先执行：mvn clean package"
    echo ""
fi

# 启动单个进程
start_single_task() {
    local table_num=$1
    local topic_name="${PREFIX}_${table_num}"
    local table_name="${PREFIX}_${table_num}"
    local task_id="${PREFIX}-${table_num}-$(date +%s)"
    local log_file="${LOG_DIR}/${PREFIX}_${table_num}.log"
    local pid_file="${TASK_DIR}/${PREFIX}_${table_num}.pid"

    # 检查是否已在运行
    if [ -f "$pid_file" ]; then
        local old_pid=$(cat "$pid_file")
        if kill -0 "$old_pid" 2>/dev/null; then
            echo -e "${YELLOW}跳过：${PREFIX}_${table_num} 已在运行 (PID: $old_pid)${NC}"
            return 0
        fi
    fi

    local cmd="java $JVM_OPTS -jar $JAR_FILE --topic $topic_name --task-id $task_id"

    if [ "$DRY_RUN" = true ]; then
        echo "[预演] $cmd"
    else
        # 后台启动
        nohup java $JVM_OPTS -jar "$JAR_FILE" --topic "$topic_name" --task-id "$task_id" > "$log_file" 2>&1 &
        local pid=$!

        # 保存 PID
        cat > "$TASK_DIR/${PREFIX}_${table_num}.task" <<EOF
TASK_ID=$task_id
APP_NAME=${PREFIX}_${table_num}
START_TIME=$(date +%s)
PID=$pid
TABLE_NAME=$table_name
TOPIC_NAME=$topic_name
EOF

        echo -e "${GREEN}已启动：${PREFIX}_${table_num} (PID: $pid)${NC}"
    fi
}

# 主程序
echo "========================================"
echo "Kafka Streams 批量启动"
echo "========================================"
echo "表范围：$FROM_NUM - $TO_NUM"
echo "前缀：$PREFIX"
echo "服务器：$SERVER"
echo "JVM 参数：$JVM_OPTS"
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}模式：预演（不实际启动）${NC}"
fi
echo "========================================"
echo ""

# 启动所有进程
started=0
skipped=0

for ((i=FROM_NUM; i<=TO_NUM; i++)); do
    # 检查是否已存在任务文件
    if [ -f "$TASK_DIR/${PREFIX}_${i}.task" ]; then
        old_pid=$(grep "^PID=" "$TASK_DIR/${PREFIX}_${i}.task" 2>/dev/null | cut -d'=' -f2)
        if kill -0 "$old_pid" 2>/dev/null; then
            ((skipped++))
            continue
        fi
    fi

    start_single_task $i
    ((started++))

    # 每启动 50 个，暂停一下避免系统过载
    if [ $((started % 50)) -eq 0 ]; then
        echo -e "${YELLOW}已启动 $started 个进程，暂停 2 秒...${NC}"
        sleep 2
    fi
done

echo ""
echo "========================================"
echo "启动完成"
echo "========================================"
echo "新启动：$started 个"
echo "跳过（已运行）：$skipped 个"
echo ""
echo "查看状态：./status.sh --count"
echo "启动看门狗：./watchdog.sh start"
echo "停止所有：./stop-all.sh"
