#!/bin/bash

# ================================================
# Kafka Streams 集群状态查询脚本
# ================================================
#
# 用法:
#   ./status-cluster.sh              # 查询整个集群状态
#   ./status-cluster.sh --detail     # 显示每个节点的详情
#
# ================================================

# 集群节点配置（修改为你的服务器 IP）
CLUSTER_NODES=(
    "192.168.1.101:server1"
    "192.168.1.102:server2"
    "192.168.1.103:server3"
)

# SSH 用户
SSH_USER="root"

# 远程路径
REMOTE_PATH="/opt/kafka_streams_demo/ConsumerNew"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# 显示帮助
show_help() {
    cat <<EOF
用法：$0 [选项]

查询 Kafka Streams 集群状态（所有节点）

选项:
  --detail             显示每个节点的详情
  --nodes <list>       指定节点列表，逗号分隔
  --help               显示帮助信息

示例:
  $0                   # 查询所有节点
  $0 --detail          # 显示详细信息
  $0 --nodes server1,server2
EOF
}

# 查询单个节点状态
query_node() {
    local node_ip=$1
    local node_name=$2
    local detail=$3

    echo -e "${CYAN}查询节点：$node_name ($node_ip)${NC}"

    # SSH 执行远程命令
    local result=$(ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
        ${SSH_USER}@${node_ip} \
        "cd $REMOTE_PATH && ./status.sh --count 2>/dev/null" 2>&1)

    local exit_code=$?

    if [ $exit_code -eq 0 ]; then
        # 解析结果
        local total=$(echo "$result" | grep "总任务数" | awk '{print $NF}')
        local running=$(echo "$result" | grep "运行中" | awk '{print $NF}')
        local stopped=$(echo "$result" | grep "已停止" | awk '{print $NF}')

        if [ -n "$running" ]; then
            echo -e "  $node_name: ${GREEN}运行中${NC} (总=$total, 运行=$running, 停止=$stopped)"
            if [ "$detail" = "true" ]; then
                echo "$result" | grep -A 10 "任务统计" | tail -n +2
            fi
        else
            echo -e "  $node_name: ${RED}无响应${NC}"
        fi
    else
        echo -e "  $node_name: ${RED}连接失败${NC} (exit=$exit_code)"
    fi
}

# 主程序
main() {
    local detail=false
    local custom_nodes=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            --detail)
                detail=true
                shift
                ;;
            --nodes)
                custom_nodes="$2"
                shift 2
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

    echo "========================================"
    echo "Kafka Streams 集群状态查询"
    echo "========================================"
    echo ""

    local total_nodes=0
    local running_nodes=0
    local failed_nodes=0

    # 如果有自定义节点列表，使用自定义的
    if [ -n "$custom_nodes" ]; then
        CLUSTER_NODES=()
        IFS=',' read -ra NODES <<< "$custom_nodes"
        for node in "${NODES[@]}"; do
            CLUSTER_NODES+=("placeholder:$node")
        done
    fi

    for node_entry in "${CLUSTER_NODES[@]}"; do
        IFS=':' read -r ip name <<< "$node_entry"
        query_node "$ip" "$name" "$detail"
        ((total_nodes++))
    done

    echo ""
    echo "========================================"
    echo "集群汇总"
    echo "========================================"
    echo "节点总数：$total_nodes"
    echo ""
    echo "提示：使用 --detail 查看每个节点的详细信息"
    echo "      使用 --nodes server1,server2 指定节点"
}

main "$@"
