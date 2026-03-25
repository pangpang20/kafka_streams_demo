#!/bin/bash

# ConsumerNew 启动脚本
# 使用说明：./start.sh [配置文件路径]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 配置文件路径（可选参数）
CONFIG_PATH="${1:-src/main/resources}"

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
echo "配置文件路径：$CONFIG_PATH"

java -Xms512m -Xmx2g \
     -jar target/kafka-streams-consumer-new-1.0.0-jar-with-dependencies.jar \
     "$CONFIG_PATH" \
     2>&1 | tee logs/app-$(date +%Y%m%d-%H%M%S).log
