#!/bin/bash

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color
YELLOW='\033[1;33m'

# DolphinDB路径
DOLPHINDB_PATH="/home/luke/ddb/server"
DOLPHINDB_CONFIG="$DOLPHINDB_PATH/dolphindb.cfg"

# 检查DolphinDB是否在运行
check_dolphindb() {
    if pgrep -x "dolphindb" > /dev/null; then
        echo -e "${GREEN}DolphinDB is running${NC}"
        return 0
    else
        echo -e "${RED}DolphinDB is not running${NC}"
        return 1
    fi
}

# 启动DolphinDB
start_dolphindb() {
    echo "Starting DolphinDB..."
    if [ -f "$DOLPHINDB_PATH/dolphindb" ]; then
        cd $DOLPHINDB_PATH
        if [ -f "$DOLPHINDB_CONFIG" ]; then
            ./dolphindb -console 0 -config dolphindb.cfg &
            sleep 2
            if check_dolphindb; then
                echo -e "${GREEN}DolphinDB started successfully${NC}"
            else
                echo -e "${RED}Failed to start DolphinDB${NC}"
            fi
        else
            echo -e "${RED}DolphinDB config file not found at $DOLPHINDB_CONFIG${NC}"
        fi
    else
        echo -e "${RED}DolphinDB executable not found at $DOLPHINDB_PATH${NC}"
        echo -e "${YELLOW}Please update DOLPHINDB_PATH in this script to point to your DolphinDB installation directory${NC}"
    fi
}

# 检查MongoDB是否在运行
check_mongodb() {
    if systemctl is-active --quiet mongod; then
        echo -e "${GREEN}MongoDB is running${NC}"
        return 0
    else
        echo -e "${RED}MongoDB is not running${NC}"
        return 1
    fi
}

# 启动MongoDB
start_mongodb() {
    echo "Starting MongoDB..."
    sudo systemctl start mongod
    sleep 2
    if check_mongodb; then
        echo -e "${GREEN}MongoDB started successfully${NC}"
    else
        echo -e "${RED}Failed to start MongoDB${NC}"
    fi
}

# 停止DolphinDB
stop_dolphindb() {
    echo "Stopping DolphinDB..."
    pkill -x dolphindb
    sleep 2
    if ! check_dolphindb; then
        echo -e "${GREEN}DolphinDB stopped successfully${NC}"
    else
        echo -e "${RED}Failed to stop DolphinDB${NC}"
    fi
}

# 停止MongoDB
stop_mongodb() {
    echo "Stopping MongoDB..."
    sudo systemctl stop mongod
    sleep 2
    if ! check_mongodb; then
        echo -e "${GREEN}MongoDB stopped successfully${NC}"
    else
        echo -e "${RED}Failed to stop MongoDB${NC}"
    fi
}

# 主函数
main() {
    case "$1" in
        start)
            echo "Starting database services..."
            
            # 检查并启动DolphinDB
            if ! check_dolphindb; then
                start_dolphindb
            fi
            
            # 检查并启动MongoDB
            if ! check_mongodb; then
                start_mongodb
            fi
            ;;
            
        stop)
            echo "Stopping database services..."
            
            # 停止DolphinDB
            if check_dolphindb; then
                stop_dolphindb
            fi
            
            # 停止MongoDB
            if check_mongodb; then
                stop_mongodb
            fi
            ;;
            
        status)
            echo "Checking database services status..."
            check_dolphindb
            check_mongodb
            ;;
            
        restart)
            echo "Restarting database services..."
            
            # 重启DolphinDB
            if check_dolphindb; then
                stop_dolphindb
            fi
            sleep 2
            start_dolphindb
            
            # 重启MongoDB
            if check_mongodb; then
                stop_mongodb
            fi
            sleep 2
            start_mongodb
            ;;
            
        *)
            echo "Usage: $0 {start|stop|status|restart}"
            exit 1
            ;;
    esac
    
    # 最终状态检查
    echo -e "\nFinal status check:"
    check_dolphindb
    check_mongodb
}

# 运行主函数
main "$@"
