"""
测试导入期权数据到DolphinDB
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ml_predict.dolphin_provider import DolphinDBProvider
import pandas as pd
from datetime import datetime, timedelta

def test_import_options():
    try:
        # 连接DolphinDB
        provider = DolphinDBProvider()
        print("成功连接到DolphinDB")
        
        # 删除现有的期权数据库
        provider.conn.run("""
        if(existsDatabase('dfs://options'))
            dropDatabase('dfs://options')
        """)
        print("已清除现有数据库")
        
        # 重新初始化数据库
        provider._use_database()
        print("已重新初始化数据库")
        
        # 获取数据文件列表
        data_dir = "/home/luke/options/source/60min"
        files = [
            # 几个看涨期权
            "AAPL160603C00090000.csv",  # 90美元行权价
            "AAPL160603C00095000.csv",  # 95美元行权价
            "AAPL160603C00100000.csv",  # 100美元行权价
            # 几个看跌期权
            "AAPL160603P00090000.csv",  # 90美元行权价
            "AAPL160603P00095000.csv",  # 95美元行权价
            "AAPL160603P00100000.csv",  # 100美元行权价
        ]
        
        print(f"\n找到 {len(files)} 个数据文件:")
        for file in files:
            print(file)
        
        # 导入每个文件
        for file in files:
            try:
                print(f"\n正在导入: {file}")
                file_path = os.path.join(data_dir, file)
                
                # 读取CSV文件
                df = pd.read_csv(file_path)
                print(f"读取到 {len(df)} 行数据")
                
                # 导入数据
                provider.import_option_csv(file_path)
                print(f"成功导入: {file}")
                
            except Exception as e:
                print(f"导入 {file} 时出错: {e}")
                import traceback
                print("错误详情:")
                traceback.print_exc()
        
        print("\n开始验证导入结果...")
        
        # 验证导入结果
        start_date = datetime(2016, 6, 1)
        end_date = datetime(2016, 6, 30)
        
        # 验证看涨期权
        print("\n验证看涨期权数据:")
        call_data = provider.get_option_data(
            underlying='AAPL',
            start_date=start_date,
            end_date=end_date,
            option_type='C',
            min_strike=90,
            max_strike=100
        )
        print(f"找到 {len(call_data)} 条看涨期权记录")
        if not call_data.empty:
            print("\n看涨期权数据示例:")
            print(call_data.head())
        
        # 验证看跌期权
        print("\n验证看跌期权数据:")
        put_data = provider.get_option_data(
            underlying='AAPL',
            start_date=start_date,
            end_date=end_date,
            option_type='P',
            min_strike=90,
            max_strike=100
        )
        print(f"找到 {len(put_data)} 条看跌期权记录")
        if not put_data.empty:
            print("\n看跌期权数据示例:")
            print(put_data.head())
        
        print("\n测试导入完成！")
        
    except Exception as e:
        print(f"测试过程中出现错误: {e}")
        import traceback
        print("错误详情:")
        traceback.print_exc()

if __name__ == "__main__":
    test_import_options()
