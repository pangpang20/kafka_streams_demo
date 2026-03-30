#!/bin/bash

# ================================================
# Kafka Streams 进程看门狗脚本
# ================================================
#
# 功能：监控 2000+ 个独立进程，自动重启失败的进程
#
# 用法:
#   ./watchdog.sh start      # 启动看门狗（后台运行）
#   ./watchdog.sh stop       # 停止看门狗
#   ./watchdog.sh status     # 查看看门狗状态
#   ./watchdog.sh check      # 手动检查一次
#   ./watchdog.sh restart    # 重启看门狗
#
# ================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TASK_DIR="${SCRIPT_DIR}/.kafka_tasks"
WATCHDOG_PID_FILE="${SCRIPT_DIR}/.watchdog.pid"
WATCHDOG_LOG="${SCRIPT_DIR}/logs/watchdog.log"
CHECK_INTERVAL=${CHECK_INTERVAL:-30}  # 检查间隔（秒），默认 30 秒
MAX_RESTART_ATTEMPTS=${MAX_RESTART_ATTEMPTS:-3}  # 最大重启次数
RESTART_COOLDOWN=${RESTART_COOLDOWN:-300}  # 重启冷却时间（秒），默认 5 分钟

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 确保日志目录存在
mkdir -p "${SCRIPT_DIR}/logs"

# 记录日志
log() {
    local level="$1"
    local msg="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $msg" >> "$WATCHDOG_LOG"
    if [ "$level" != "DEBUG" ]; then
        echo -e "[$timestamp] [$level] $msg"
    fi
}

# 检查进程是否运行
is_process_running() {
    local pid=$1
    [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# 重启失败进程
restart_failed_task() {
    local task_file=$1
    local task_id=$(grep "^TASK_ID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
    local last_pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
    local app_name=$(grep "^APP_NAME=" "$task_file" 2>/dev/null | cut -d'=' -f2)
    local start_time=$(grep "^START_TIME=" "$task_file" 2>/dev/null | cut -d'=' -f2)
    local restart_count=$(grep "^RESTART_COUNT=" "$task_file" 2>/dev/null | cut -d'=' -f2)
    local last_restart=$(grep "^LAST_RESTART=" "$task_file" 2>/dev/null | cut -d'=' -f2)

    restart_count=${restart_count:-0}

    # 检查冷却时间
    if [ -n "$last_restart" ]; then
        local now=$(date +%s)
        local cooldown_remaining=$((last_restart + RESTART_COOLDOWN - now))
        if [ $cooldown_remaining -gt 0 ]; then
            log "DEBUG" "任务 $task_id 在冷却期内，跳过重启（剩余 ${cooldown_remaining}s）"
            return 0
        fi
    fi

    # 检查最大重启次数
    if [ $restart_count -ge $MAX_RESTART_ATTEMPTS ]; then
        log "WARN" "任务 $task_id 已达到最大重启次数 ($MAX_RESTART_ATTEMPTS)，不再重启"
        return 1
    fi

    log "INFO" "${YELLOW}正在重启失败任务：$task_id (PID: $last_pid, 重启次数：$restart_count)${NC}"

    # 获取启动参数（从启动脚本或配置文件）
    local launch_script="${SCRIPT_DIR}/start_${app_name}.sh"
    if [ -f "$launch_script" ]; then
        # 如果有独立的启动脚本，执行它
        log "INFO" "执行启动脚本：$launch_script"
        bash "$launch_script" &
        local new_pid=$!

        # 更新任务文件
        cat > "$task_file" <<EOF
TASK_ID=$task_id
APP_NAME=$app_name
START_TIME=$start_time
PID=$new_pid
RESTART_COUNT=$((restart_count + 1))
LAST_RESTART=$(date +%s)
LAST_ERROR_PID=$last_pid
EOF

        log "INFO" "${GREEN}任务 $task_id 已重启，新 PID: $new_pid${NC}"
        return 0
    else
        log "WARN" "未找到启动脚本：$launch_script，无法自动重启"
        return 1
    fi
}

# 检查所有任务
check_all_tasks() {
    if [ ! -d "$TASK_DIR" ]; then
        log "DEBUG" "任务目录不存在：$TASK_DIR"
        return 0
    fi

    local total=0
    local running=0
    local stopped=0
    local restarted=0

    for task_file in "$TASK_DIR"/*.task; do
        if [ -f "$task_file" ]; then
            ((total++))
            local pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)

            if is_process_running "$pid"; then
                ((running++))
            else
                ((stopped++))
                # 尝试重启
                if restart_failed_task "$task_file"; then
                    ((restarted++))
                fi
            fi
        fi
    done

    log "DEBUG" "检查完成：总计=$total, 运行中=$running, 已停止=$stopped, 已重启=$restarted"
}

# 主循环
watchdog_loop() {
    log "INFO" "${GREEN}看门狗启动成功${NC}"
    log "INFO" "检查间隔：${CHECK_INTERVAL}秒"
    log "INFO" "最大重启次数：${MAX_RESTART_ATTEMPTS}"
    log "INFO" "重启冷却时间：${RESTART_COOLDOWN}秒"
    log "INFO" "任务目录：$TASK_DIR"

    while true; do
        check_all_tasks
        sleep "$CHECK_INTERVAL"
    done
}

# 启动看门狗
start_watchdog() {
    if [ -f "$WATCHDOG_PID_FILE" ]; then
        local old_pid=$(cat "$WATCHDOG_PID_FILE")
        if is_process_running "$old_pid"; then
            log "WARN" "看门狗已在运行 (PID: $old_pid)"
            return 1
        fi
        rm -f "$WATCHDOG_PID_FILE"
    fi

    # 后台运行
    nohup bash "$0" --daemon > /dev/null 2>&1 &
    local watchdog_pid=$!

    echo "$watchdog_pid" > "$WATCHDOG_PID_FILE"

    log "INFO" "${GREEN}看门狗已启动 (PID: $watchdog_pid)${NC}"
    echo ""
    echo "查看日志：tail -f $WATCHDOG_LOG"
    echo "停止看门狗：$0 stop"
}

# 停止看门狗
stop_watchdog() {
    if [ ! -f "$WATCHDOG_PID_FILE" ]; then
        log "WARN" "看门狗未运行"
        return 1
    fi

    local pid=$(cat "$WATCHDOG_PID_FILE")
    if is_process_running "$pid"; then
        kill "$pid"
        log "INFO" "${YELLOW}看门狗已停止 (PID: $pid)${NC}"
    else
        log "WARN" "看门狗进程不存在"
    fi

    rm -f "$WATCHDOG_PID_FILE"
}

# 查看状态
watchdog_status() {
    if [ -f "$WATCHDOG_PID_FILE" ]; then
        local pid=$(cat "$WATCHDOG_PID_FILE")
        if is_process_running "$pid"; then
            echo -e "${GREEN}看门狗运行中${NC} (PID: $pid)"
            echo ""
            echo "最近日志:"
            tail -10 "$WATCHDOG_LOG"
        else
            echo -e "${RED}看门狗未运行${NC} (PID 文件存在但进程不存在)"
        fi
    else
        echo -e "${YELLOW}看门狗未启动${NC}"
    fi
}

# 手动检查
manual_check() {
    log "INFO" "执行手动检查..."
    check_all_tasks
    log "INFO" "检查完成"
}

# 主程序
case "${1:-}" in
    start)
        start_watchdog
        ;;
    stop)
        stop_watchdog
        ;;
    restart)
        stop_watchdog
        sleep 2
        start_watchdog
        ;;
    status)
        watchdog_status
        ;;
    check)
        manual_check
        ;;
    --daemon)
        watchdog_loop
        ;;
    *)
        echo "用法: $0 {start|stop|status|check|restart}"
        echo ""
        echo "命令说明:"
        echo "  start   - 启动看门狗（后台运行）"
        echo "  stop    - 停止看门狗"
        echo "  status  - 查看看门狗状态"
        echo "  check   - 手动检查一次所有任务"
        echo "  restart - 重启看门狗"
        echo ""
        echo "环境变量:"
        echo "  CHECK_INTERVAL     检查间隔（秒），默认 30"
        echo "  MAX_RESTART_ATTEMPTS  最大重启次数，默认 3"
        echo "  RESTART_COOLDOWN   重启冷却时间（秒），默认 300"
        echo ""
        exit 1
        ;;
esac
