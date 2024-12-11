#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
from datetime import datetime, timedelta
import glob

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ml_predict.dolphin_provider import DolphinDBProvider

def test_option_data():
    try:
        # 1. 初始化DolphinDB连接
        print("初始化DolphinDB连接...")
        provider = DolphinDBProvider()
        
        # 2. 导入期权数据
        print("\n测试数据导入...")
        option_dir = "/home/luke/optionSource/source/60min"
        csv_files = glob.glob(os.path.join(option_dir, "*.csv"))
        
        print(f"\n找到 {len(csv_files)} 个CSV文件")
        for i, csv_file in enumerate(csv_files[:3], 1):  # 先测试前3个文件
            print(f"\n处理第 {i} 个文件: {os.path.basename(csv_file)}")
            try:
                provider.import_option_csv(csv_file)
            except Exception as e:
                print(f"导入失败: {str(e)}")
                continue
        
        # 3. 测试数据查询
        print("\n测试数据查询...")
        
        # 设置查询时间范围
        start_date = datetime(2020, 10, 1)
        end_date = datetime(2020, 12, 31)
        
        # 先检查数据库中的数据
        print("\n检查数据库中的数据:")
        provider.conn.run("""
        t = loadTable('dfs://options', 'options')
        print("总行数:", t.rows())
        print("\n时间范围:")
        select min(timestamp) as min_time, max(timestamp) as max_time from t;
        print("\n数据示例:")
        select top 5 * from t;
        """)
        
        # 场景1: 查询所有类型的期权
        print("\n场景1: 查询所有类型的期权")
        result = provider.get_option_data('AAPL', start_date, end_date)
        print(f"查询结果行数: {len(result) if result is not None else 0}")
        if result is not None and len(result) > 0:
            print("\n前5行数据:")
            print(result.head())
        
        # 场景2: 只查询看涨期权
        print("\n场景2: 只查询看涨期权")
        result = provider.get_option_data('AAPL', start_date, end_date, 'C')
        print(f"查询结果行数: {len(result) if result is not None else 0}")
        if result is not None and len(result) > 0:
            print("\n前5行数据:")
            print(result.head())
        
        # 场景3: 只查询看跌期权
        print("\n场景3: 只查询看跌期权")
        result = provider.get_option_data('AAPL', start_date, end_date, 'P')
        print(f"查询结果行数: {len(result) if result is not None else 0}")
        if result is not None and len(result) > 0:
            print("\n前5行数据:")
            print(result.head())
        
    except Exception as e:
        print(f"测试过程中出错: {str(e)}")

if __name__ == "__main__":
    test_option_data()
