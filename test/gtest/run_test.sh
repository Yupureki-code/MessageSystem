#!/bin/bash
# 消息存储服务测试脚本

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== 消息存储服务测试 ===${NC}"
echo ""

# 检查gtest是否安装
if ! dpkg -l | grep -q libgtest-dev; then
    echo -e "${RED}错误: gtest未安装${NC}"
    echo "请运行: sudo apt-get install libgtest-dev"
    exit 1
fi

# 构建测试
echo -e "${YELLOW}1. 构建测试...${NC}"
cd "$(dirname "$0")"
mkdir -p build
cd build
cmake ..
make -j$(nproc)
echo -e "${GREEN}构建完成${NC}"
echo ""

# 运行数据库测试
echo -e "${YELLOW}2. 运行数据库测试...${NC}"
./test_database --gtest_color=yes
DB_RESULT=$?
echo ""

# 运行消息存储服务测试（如果提供了服务地址）
if [ $# -ge 1 ]; then
    SERVICE_ADDR=$1
    echo -e "${YELLOW}3. 运行消息存储服务测试...${NC}"
    echo "服务地址: $SERVICE_ADDR"
    echo ""
    
    ./test_message_store "$SERVICE_ADDR" --gtest_color=yes
    STORE_RESULT=$?
else
    echo -e "${YELLOW}3. 跳过消息存储服务测试 (未提供服务地址)${NC}"
    echo "要运行服务测试，请提供服务地址: $0 <服务地址:端口>"
    STORE_RESULT=0
fi

echo ""
if [ $DB_RESULT -eq 0 ] && [ $STORE_RESULT -eq 0 ]; then
    echo -e "${GREEN}=== 所有测试通过 ===${NC}"
else
    echo -e "${RED}=== 测试失败 ===${NC}"
    exit 1
fi
