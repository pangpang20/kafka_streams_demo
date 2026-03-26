#!/bin/bash

# ConsumerNew 启动脚本
# 使用说明：./start.sh [选项]
#
# 选项:
#   --topic <topic_name>     指定 Kafka Topic
#   --config-dir <dir_path>  指定配置目录
#   --help                   显示帮助信息
#
# 示例:
#   ./start.sh --topic mytopic --config-dir /path/to/config
#   ./start.sh -t mytopic -c /path/to/config

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 检查是否已编译
if [ ! -f "target/kafka-streams-consumer-new-1.0.0-jar-with-dependencies.jar" ]; then
    echo "正在编译项目..."
    mvn clean package -DskipTests
    if [ $? -ne 0 ]; then
        echo "编译失败"
        exit 1
    fi
fi

# 创建日志目录
mkdir -p logs

# 启动应用
echo "启动 ConsumerNew..."

java -Xms512m -Xmx2g \
     -jar target/kafka-streams-consumer-new-1.0.0-jar-with-dependencies.jar \
     "$@" \
     2>&1 | tee logs/app-$(date +%Y%m%d-%H%M%S).log
