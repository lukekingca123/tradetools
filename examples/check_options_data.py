#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dolphindb as ddb
import json
import logging
import sys
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_config():
    """加载配置文件"""
    try:
        with open('config.json', 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"加载配置文件失败: {str(e)}")
        sys.exit(1)

def check_database():
    """检查数据库中的数据"""
    try:
        # 加载配置
        config = load_config()
        db_config = config['dolphindb']
        
        # 连接DolphinDB
        conn = ddb.session()
        conn.connect(db_config['host'], db_config['port'])
        conn.login(db_config['username'], db_config['password'])
        
        try:
            # 尝试加载表
            table = conn.run("loadTable('dfs://optiondb', 'options')")
            logging.info("成功连接到数据库表")
        except Exception as e:
            logging.error(f"无法加载数据库表: {str(e)}")
            return
        
        # 查询数据总量
        total_count = conn.run("select count(*) as count from loadTable('dfs://optiondb', 'options')")
        logging.info(f"数据库中总记录数: {total_count['count'][0]}")
        
        if total_count['count'][0] == 0:
            logging.info("数据库为空，没有任何记录")
            return
            
        # 查询不同期权的数量
        symbol_count = conn.run("""
        select count(*) as count from 
        (select distinct symbol from loadTable('dfs://optiondb', 'options'))
        """)
        logging.info(f"不同期权数量: {symbol_count['count'][0]}")
        
        # 查询时间范围
        time_range = conn.run("""
        select 
            min(timestamp) as min_time,
            max(timestamp) as max_time
        from loadTable('dfs://optiondb', 'options')
        """)
        min_time = datetime.fromtimestamp(time_range['min_time'][0]/1000).strftime('%Y-%m-%d %H:%M:%S')
        max_time = datetime.fromtimestamp(time_range['max_time'][0]/1000).strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"数据时间范围: {min_time} 到 {max_time}")
        
        # 查询每个symbol的记录数量（前10个）
        symbol_stats = conn.run("""
        select 
            symbol,
            count(*) as count
        from loadTable('dfs://optiondb', 'options')
        group by symbol
        order by count desc
        limit 10
        """)
        logging.info("\n每个期权的记录数量（前10个）:")
        for i in range(len(symbol_stats['symbol'])):
            logging.info(f"{symbol_stats['symbol'][i]}: {symbol_stats['count'][i]}")
        
        # 查询最近的数据
        recent_data = conn.run("""
        select top 5 * from loadTable('dfs://optiondb', 'options')
        where timestamp = (select max(timestamp) from loadTable('dfs://optiondb', 'options'))
        """)
        logging.info("\n最新的5条记录:")
        for i in range(len(recent_data['symbol'])):
            time_str = datetime.fromtimestamp(recent_data['timestamp'][i]/1000).strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f"Symbol: {recent_data['symbol'][i]}, "
                        f"Strike: {recent_data['strike'][i]}, "
                        f"Type: {recent_data['type'][i]}, "
                        f"Time: {time_str}, "
                        f"Close: {recent_data['close'][i]}")
        
    except Exception as e:
        logging.error(f"查询数据库失败: {str(e)}")
        logging.error(f"错误类型: {type(e)}")
        import traceback
        logging.error(f"错误堆栈: {traceback.format_exc()}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_database()
