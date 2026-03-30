#!/bin/bash

# ================================================
# Kafka Streams 任务状态查看脚本
# ================================================
#
# 用法:
#   ./status.sh              # 查看所有运行中的任务状态
#   ./status.sh <task-id>    # 查看指定任务状态
#   ./status.sh --list       # 列出所有任务文件
#   ./status.sh --clean      # 清理已停止的任务文件
#   ./status.sh --help       # 显示帮助信息
#
# ================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TASK_DIR="${SCRIPT_DIR}/.kafka_tasks"
TASK_FILE="${TASK_DIR}/consumer-new.task"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 状态颜色
get_state_color() {
    case "$1" in
        RUNNING)
            echo -e "${GREEN}"
            ;;
        STARTING|REBALANCING)
            echo -e "${YELLOW}"
            ;;
        STOPPED|ERROR)
            echo -e "${RED}"
            ;;
        *)
            echo -e "${NC}"
            ;;
    esac
}

# 格式化时间
format_time() {
    local ms=$1
    local seconds=$((ms / 1000))
    local minutes=$((seconds / 60))
    local hours=$((minutes / 60))

    if [ $hours -gt 0 ]; then
        printf "%dh %dm %ds" $hours $((minutes % 60)) $((seconds % 60))
    elif [ $minutes -gt 0 ]; then
        printf "%dm %ds" $minutes $((seconds % 60))
    else
        printf "%ds" $seconds
    fi
}

# 格式化时间戳
format_timestamp() {
    local ms=$1
    local seconds=$((ms / 1000))
    date -d "@$seconds" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -r $((ms / 1000)) "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "$ms"
}

# 检查进程是否运行
is_process_running() {
    local pid=$1
    if [ -z "$pid" ]; then
        echo "false"
        return
    fi
    if kill -0 "$pid" 2>/dev/null; then
        echo "true"
    else
        echo "false"
    fi
}

# 显示单个任务状态
show_task_status() {
    local task_file=$1

    if [ ! -f "$task_file" ]; then
        echo -e "${RED}任务文件不存在：$task_file${NC}"
        return 1
    fi

    # 读取任务信息
    source "$task_file"

    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}       Kafka Streams 任务状态${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo -e "任务 ID:     ${BLUE}${TASK_ID}${NC}"
    echo -e "应用名称：   ${BLUE}${APP_NAME}${NC}"
    echo -e "启动时间：   ${BLUE}$(format_timestamp ${START_TIME})${NC}"

    # 检查进程状态
    local proc_status=$(is_process_running $PID)
    if [ "$proc_status" = "true" ]; then
        echo -e "进程状态：   ${GREEN}运行中 (PID: ${PID})${NC}"
    else
        echo -e "进程状态：   ${RED}已停止 (PID: ${PID})${NC}"
    fi

    # 尝试从日志文件获取更多信息
    local log_file="${SCRIPT_DIR}/logs/consumer.log"
    if [ -f "$log_file" ]; then
        local last_error=$(grep -i "error\|exception" "$log_file" | tail -1)
        if [ -n "$last_error" ]; then
            echo -e "最后错误：   ${YELLOW}${last_error:0:80}...${NC}"
        fi
    fi

    # 计算运行时长
    local now_ms=$(date +%s%3N 2>/dev/null || echo $(($(date +%s) * 1000)))
    local running_time=$((now_ms - START_TIME))
    echo -e "运行时长：   ${BLUE}$(format_time $running_time)${NC}"

    echo ""
}

# 列出所有任务
list_all_tasks() {
    if [ ! -d "$TASK_DIR" ]; then
        echo -e "${YELLOW}暂无任务文件${NC}"
        return 0
    fi

    local task_files=("$TASK_DIR"/*.task)
    if [ ${#task_files[@]} -eq 0 ] || [ ! -e "${task_files[0]}" ]; then
        echo -e "${YELLOW}暂无任务文件${NC}"
        return 0
    fi

    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}         所有任务列表${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    printf "%-40s %-20s %-15s %s\n" "TASK_ID" "APP_NAME" "STATUS" "START_TIME"
    echo "--------------------------------------------------------------------------------"

    for task_file in "$TASK_DIR"/*.task; do
        if [ -f "$task_file" ]; then
            source "$task_file"
            local proc_status=$(is_process_running $PID)
            local status
            local color
            if [ "$proc_status" = "true" ]; then
                status="RUNNING"
                color="${GREEN}"
            else
                status="STOPPED"
                color="${RED}"
            fi
            printf "%-40s %-20s ${color}%-15s${NC} %s\n" "$TASK_ID" "$APP_NAME" "$status" "$(format_timestamp ${START_TIME})"
        fi
    done

    echo ""
}

# 清理已停止的任务
clean_stopped_tasks() {
    if [ ! -d "$TASK_DIR" ]; then
        echo -e "${YELLOW}暂无任务文件${NC}"
        return 0
    fi

    local count=0
    for task_file in "$TASK_DIR"/*.task; do
        if [ -f "$task_file" ]; then
            source "$task_file"
            local proc_status=$(is_process_running $PID)
            if [ "$proc_status" = "false" ]; then
                rm -f "$task_file"
                echo -e "${GREEN}已清理：${NC} $(basename $task_file)"
                ((count++))
            fi
        fi
    done

    if [ $count -eq 0 ]; then
        echo -e "${YELLOW}没有已停止的任务需要清理${NC}"
    else
        echo -e "${GREEN}共清理 $count 个已停止的任务${NC}"
    fi
}

# 显示帮助信息
show_help() {
    echo ""
    echo "用法:"
    echo "  $0                    查看当前运行任务状态"
    echo "  $0 <task-id>          查看指定任务状态"
    echo "  $0 --list             列出所有任务"
    echo "  $0 --clean            清理已停止的任务"
    echo "  $0 --help             显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                    # 查看 consumer-new 任务状态"
    echo "  $0 my-task-001        # 查看指定 task-id 的状态"
    echo "  $0 --list             # 列出所有任务文件"
    echo ""
}

# 主程序
main() {
    case "${1:-}" in
        --list|-l)
            list_all_tasks
            ;;
        --clean|-c)
            clean_stopped_tasks
            ;;
        --help|-h)
            show_help
            ;;
        "")
            # 默认：查看当前任务状态
            if [ -f "$TASK_FILE" ]; then
                show_task_status "$TASK_FILE"
            else
                echo -e "${YELLOW}当前没有运行的任务${NC}"
                echo ""
                echo "提示：启动 ConsumerNew 后会在此目录创建任务文件"
                echo "      使用 --list 查看所有历史任务"
            fi
            ;;
        *)
            # 查找指定的 task-id
            local task_id="$1"
            local found=false

            if [ -d "$TASK_DIR" ]; then
                for task_file in "$TASK_DIR"/*.task; do
                    if [ -f "$task_file" ]; then
                        source "$task_file"
                        if [ "$TASK_ID" = "$task_id" ]; then
                            show_task_status "$task_file"
                            found=true
                            break
                        fi
                    fi
                done
            fi

            if [ "$found" = "false" ]; then
                echo -e "${RED}未找到任务：$task_id${NC}"
                echo ""
                echo "使用 --list 查看所有可用任务"
                exit 1
            fi
            ;;
    esac
}

main "$@"
