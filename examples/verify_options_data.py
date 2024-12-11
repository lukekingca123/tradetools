"""
验证导入到DolphinDB的期权数据
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ml_predict.dolphin_provider import DolphinDBProvider
import pandas as pd

def verify_options_data():
    try:
        print("开始验证数据...")
        provider = DolphinDBProvider()
        print("成功连接到DolphinDB")
        
        # 使用已知的期权代码
        symbols = [
            'AAPL160603C00090000',
            'AAPL160603C00095000',
            'AAPL160603C00100000',
            'AAPL160603P00090000',
            'AAPL160603P00095000',
            'AAPL160603P00100000'
        ]
        
        print(f"\n找到 {len(symbols)} 个期权代码:")
        for symbol in symbols:
            print(symbol)
            
        # 对每个期权代码进行统计
        total_count = 0
        for symbol in symbols:
            count_script = f"""
            select count(*) as count from loadTable("dfs://options", "options")
            where symbol = '{symbol}'
            """
            result = provider.conn.run(count_script)
            if result is not None:
                count = result['count'][0]
                total_count += count
                print(f"{symbol}: {count}条记录")
        
        print(f"\n总数据量: {total_count}")
        
        # 验证数据日期范围
        date_range_script = f"""
        select min(date) as min_date, max(date) as max_date 
        from loadTable("dfs://options", "options")
        where symbol = '{symbols[0]}'
        """
        print("\n执行日期范围查询...")
        result = provider.conn.run(date_range_script)
        if result is not None and len(result) > 0:
            print(f"数据日期范围: {result['min_date'][0]} 到 {result['max_date'][0]}")
        else:
            print("未能获取日期范围")
        
        # 验证空值
        null_count = 0
        for symbol in symbols:
            null_check_script = f"""
            select count(*) as null_count 
            from loadTable("dfs://options", "options")
            where symbol = '{symbol}'
            and (isNull(open) or isNull(high) or isNull(low) or isNull(close) or isNull(volume))
            """
            result = provider.conn.run(null_check_script)
            if result is not None:
                null_count += result['null_count'][0]
        
        print(f"\n空值数量: {null_count}")
        
        # 显示样本数据
        sample_script = f"""
        select top 5 * 
        from loadTable("dfs://options", "options")
        where symbol = '{symbols[0]}'
        """
        print("\n获取样本数据...")
        result = provider.conn.run(sample_script)
        if result is not None and len(result) > 0:
            print("样本数据:")
            print(result)
        else:
            print("未能获取样本数据")
        
    except Exception as e:
        print(f"验证过程中出现错误: {e}")
        import traceback
        print("错误详情:")
        traceback.print_exc()

if __name__ == "__main__":
    verify_options_data()
