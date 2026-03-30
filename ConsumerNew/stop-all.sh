#!/bin/bash

# ================================================
# Kafka Streams 批量停止脚本
# ================================================
#
# 用法:
#   ./stop-all.sh              # 停止所有进程
#   ./stop-all.sh --prefix table  # 停止指定前缀的进程
#   ./stop-all.sh --from 1 --to 100  # 停止指定范围的进程
#
# ================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TASK_DIR="${SCRIPT_DIR}/.kafka_tasks"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 显示帮助
show_help() {
    cat <<EOF
用法：$0 [选项]

批量停止 Kafka Streams 进程

选项:
  --prefix <name>      只停止指定前缀的进程
  --from <N>           起始表编号
  --to <N>             结束表编号
  --force              强制杀死进程（使用 kill -9）
  --dry-run            只显示命令，不实际执行
  --help               显示帮助信息

示例:
  # 停止所有进程
  $0

  # 停止指定前缀的进程
  $0 --prefix mybiz

  # 停止指定范围的进程
  $0 --from 1 --to 100

  # 强制停止所有
  $0 --force
EOF
}

# 解析参数
PREFIX=""
FROM_NUM=""
TO_NUM=""
FORCE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --prefix)
            PREFIX="$2"
            shift 2
            ;;
        --from)
            FROM_NUM="$2"
            shift 2
            ;;
        --to)
            TO_NUM="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
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

# 检查任务目录
if [ ! -d "$TASK_DIR" ]; then
    echo -e "${YELLOW}任务目录不存在：$TASK_DIR${NC}"
    exit 0
fi

# 停止单个进程
stop_single_task() {
    local task_file=$1
    local task_id=$(grep "^TASK_ID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
    local pid=$(grep "^PID=" "$task_file" 2>/dev/null | cut -d'=' -f2)
    local app_name=$(grep "^APP_NAME=" "$task_file" 2>/dev/null | cut -d'=' -f2)

    if [ -z "$pid" ]; then
        echo -e "${YELLOW}跳过：$task_id (无 PID 信息)${NC}"
        return 0
    fi

    # 检查进程是否运行
    if ! kill -0 "$pid" 2>/dev/null; then
        echo -e "${YELLOW}跳过：$app_name (进程已停止)${NC}"
        rm -f "$task_file"
        return 0
    fi

    if [ "$DRY_RUN" = true ]; then
        echo "[预演] 停止 $app_name (PID: $pid)"
    else
        if [ "$FORCE" = true ]; then
            kill -9 "$pid" 2>/dev/null
            echo -e "${RED}已杀死：$app_name (PID: $pid)${NC}"
        else
            kill "$pid" 2>/dev/null
            echo -e "${GREEN}已停止：$app_name (PID: $pid)${NC}"
        fi
        rm -f "$task_file"
    fi
}

# 主程序
echo "========================================"
echo "Kafka Streams 批量停止"
echo "========================================"
if [ "$FORCE" = true ]; then
    echo -e "${RED}模式：强制停止 (kill -9)${NC}"
fi
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}模式：预演（不实际执行）${NC}"
fi
echo "========================================"
echo ""

# 查找并停止进程
stopped=0

for task_file in "$TASK_DIR"/*.task; do
    if [ -f "$task_file" ]; then
        local app_name=$(grep "^APP_NAME=" "$task_file" 2>/dev/null | cut -d'=' -f2)
        local task_id=$(grep "^TASK_ID=" "$task_file" 2>/dev/null | cut -d'=' -f2)

        # 按前缀过滤
        if [ -n "$PREFIX" ]; then
            if [[ "$app_name" != "$PREFIX"* ]] && [[ "$task_id" != "$PREFIX"* ]]; then
                continue
            fi
        fi

        # 按范围过滤
        if [ -n "$FROM_NUM" ] && [ -n "$TO_NUM" ]; then
            # 提取数字部分
            local num=$(echo "$app_name" | grep -oE '[0-9]+$')
            if [ -z "$num" ] || [ "$num" -lt "$FROM_NUM" ] || [ "$num" -gt "$TO_NUM" ]; then
                continue
            fi
        fi

        stop_single_task "$task_file"
        ((stopped++))
    fi
done

echo ""
echo "========================================"
echo "停止完成"
echo "========================================"
echo "已停止：$stopped 个进程"
