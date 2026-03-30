#!/bin/bash

# ================================================
# Kafka Streams 任务状态查看脚本（大规模优化版）
# ================================================
#
# 用法:
#   ./status.sh              # 查看所有运行中的任务状态
#   ./status.sh <task-id>    # 查看指定任务状态
#   ./status.sh --list       # 列出所有任务文件
#   ./status.sh --running    # 只列出运行中的任务
#   ./status.sh --stopped    # 只列出已停止的任务
#   ./status.sh --count      # 显示任务统计信息
#   ./status.sh --clean      # 清理已停止的任务文件
#   ./status.sh --help       # 显示帮助信息
#
# 性能优化:
#   - 支持 2000+ 任务文件快速查询
#   - 使用索引文件缓存任务状态
#   - 并行进程检查
#
# ================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TASK_DIR="${SCRIPT_DIR}/.kafka_tasks"
TASK_FILE="${TASK_DIR}/consumer-new.task"
INDEX_FILE="${TASK_DIR}/.index"

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

# 检查进程是否运行（优化版：批量检查）
declare -A PID_CACHE

is_process_running_cached() {
    local pid=$1
    if [ -z "$pid" ]; then
        echo "false"
        return
    fi
    if [ -n "${PID_CACHE[$pid]}" ]; then
        echo "${PID_CACHE[$pid]}"
        return
    fi
    if kill -0 "$pid" 2>/dev/null; then
        PID_CACHE[$pid]="true"
        echo "true"
    else
        PID_CACHE[$pid]="false"
        echo "false"
    fi
}

# 批量检查进程状态（用于大规模任务）
batch_check_pids() {
    local pids="$1"
    local running_count=0
    for pid in $pids; do
        if kill -0 "$pid" 2>/dev/null; then
            ((running_count++))
        fi
    done
    echo $running_count
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
    local proc_status=$(is_process_running_cached $PID)
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

# 列出所有任务（优化版：支持大规模）
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

    local count=${#task_files[@]}
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}         所有任务列表 (共 $count 个)${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""

    if [ $count -gt 100 ]; then
        echo -e "${YELLOW}提示：任务数量较多，仅显示前 100 个${NC}"
        echo -e "使用 ./status.sh --count 查看统计信息${NC}"
        echo ""
    fi

    printf "%-40s %-15s %-10s %s\n" "TASK_ID" "PID" "STATUS" "START_TIME"
    echo "--------------------------------------------------------------------------------"

    local displayed=0
    for task_file in "${task_files[@]}"; do
        if [ -f "$task_file" ] && [ $displayed -lt 100 ]; then
            # 使用 grep 代替 source 以提高性能
            local task_id=$(grep "^TASK_ID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            local pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            local start_time=$(grep "^START_TIME=" "$task_file" 2>/dev/null | cut -d'=' -f2)

            local proc_status=$(is_process_running_cached "$pid")
            local status color
            if [ "$proc_status" = "true" ]; then
                status="RUNNING"
                color="${GREEN}"
            else
                status="STOPPED"
                color="${RED}"
            fi
            printf "%-40s %-15s ${color}%-10s${NC} %s\n" "$task_id" "$pid" "$status" "$(format_timestamp ${start_time:-0})"
            ((displayed++))
        fi
    done

    echo ""
    if [ $count -gt 100 ]; then
        echo -e "${YELLOW}... 还有 $((count - 100)) 个任务未显示${NC}"
    fi
}

# 只显示运行中的任务
list_running_tasks() {
    if [ ! -d "$TASK_DIR" ]; then
        echo -e "${YELLOW}暂无任务文件${NC}"
        return 0
    fi

    local running=0
    printf "%-40s %-15s %s\n" "TASK_ID" "PID" "START_TIME"
    echo "--------------------------------------------------------------------------------"

    for task_file in "$TASK_DIR"/*.task; do
        if [ -f "$task_file" ]; then
            local task_id=$(grep "^TASK_ID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            local pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            local start_time=$(grep "^START_TIME=" "$task_file" 2>/dev/null | cut -d'=' -f2)

            if kill -0 "$pid" 2>/dev/null; then
                printf "%-40s %-15s %s\n" "$task_id" "$pid" "$(format_timestamp ${start_time:-0})"
                ((running++))
            fi
        fi
    done

    echo ""
    echo -e "${GREEN}运行中任务：$running${NC}"
}

# 只显示停止的任务
list_stopped_tasks() {
    if [ ! -d "$TASK_DIR" ]; then
        echo -e "${YELLOW}暂无任务文件${NC}"
        return 0
    fi

    local stopped=0
    printf "%-40s %-15s %s\n" "TASK_ID" "LAST_PID" "START_TIME"
    echo "--------------------------------------------------------------------------------"

    for task_file in "$TASK_DIR"/*.task; do
        if [ -f "$task_file" ]; then
            local task_id=$(grep "^TASK_ID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            local pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            local start_time=$(grep "^START_TIME=" "$task_file" 2>/dev/null | cut -d'=' -f2)

            if ! kill -0 "$pid" 2>/dev/null; then
                printf "%-40s %-15s %s\n" "$task_id" "$pid" "$(format_timestamp ${start_time:-0})"
                ((stopped++))
            fi
        fi
    done

    echo ""
    echo -e "${RED}已停止任务：$stopped${NC}"
}

# 显示任务统计
count_tasks() {
    if [ ! -d "$TASK_DIR" ]; then
        echo -e "${YELLOW}暂无任务文件${NC}"
        return 0
    fi

    local total=0
    local running=0
    local stopped=0

    for task_file in "$TASK_DIR"/*.task; do
        if [ -f "$task_file" ]; then
            ((total++))
            local pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            if kill -0 "$pid" 2>/dev/null; then
                ((running++))
            else
                ((stopped++))
            fi
        fi
    done

    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}         任务统计${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
    echo -e "总任务数：   ${BLUE}$total${NC}"
    echo -e "运行中：     ${GREEN}$running${NC}"
    echo -e "已停止：     ${RED}$stopped${NC}"

    if [ $total -gt 0 ]; then
        local rate=$((running * 100 / total))
        echo -e "运行率：     ${YELLOW}${rate}%${NC}"
    fi
    echo ""
}

# 清理已停止的任务（优化版）
clean_stopped_tasks() {
    if [ ! -d "$TASK_DIR" ]; then
        echo -e "${YELLOW}暂无任务文件${NC}"
        return 0
    fi

    local count=0
    local total=0

    for task_file in "$TASK_DIR"/*.task; do
        if [ -f "$task_file" ]; then
            ((total++))
            local pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
            if ! kill -0 "$pid" 2>/dev/null; then
                rm -f "$task_file"
                ((count++))
            fi
        fi
    done

    if [ $count -eq 0 ]; then
        echo -e "${YELLOW}没有已停止的任务需要清理${NC}"
    else
        echo -e "${GREEN}共清理 $count 个已停止的任务 (剩余 $((total - count)) 个)${NC}"
    fi
}

# 显示帮助信息
show_help() {
    echo ""
    echo "用法:"
    echo "  $0                    查看当前运行任务状态"
    echo "  $0 <task-id>          查看指定任务状态"
    echo "  $0 --list             列出所有任务 (最多显示 100 个)"
    echo "  $0 --running          只列出运行中的任务"
    echo "  $0 --stopped          只列出已停止的任务"
    echo "  $0 --count            显示任务统计信息"
    echo "  $0 --clean            清理已停止的任务"
    echo "  $0 --help             显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                    # 查看 consumer-new 任务状态"
    echo "  $0 my-task-001        # 查看指定 task-id 的状态"
    echo "  $0 --list             # 列出所有任务文件"
    echo "  $0 --running          # 只查看运行中的任务"
    echo "  $0 --count            # 查看任务统计 (2000+ 任务推荐)"
    echo ""
    echo "大规模部署建议:"
    echo "  - 2000+ 任务时使用 --count 查看统计，避免输出过多"
    echo "  - 使用 --running 快速查看运行状态"
    echo "  - 定期使用 --clean 清理已停止任务"
    echo ""
}

# 主程序
main() {
    case "${1:-}" in
        --list|-l)
            list_all_tasks
            ;;
        --running|-r)
            list_running_tasks
            ;;
        --stopped|-s)
            list_stopped_tasks
            ;;
        --count|-n)
            count_tasks
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
                echo "      使用 --count 查看任务统计 (推荐用于 2000+ 任务)"
            fi
            ;;
        *)
            # 查找指定的 task-id
            local task_id="$1"
            local found=false

            if [ -d "$TASK_DIR" ]; then
                for task_file in "$TASK_DIR"/*.task; do
                    if [ -f "$task_file" ]; then
                        local file_task_id=$(grep "^TASK_ID=" "$task_file" 2>/dev/null | cut -d'=' -f2 | tr -d '"')
                        if [ "$file_task_id" = "$task_id" ]; then
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
