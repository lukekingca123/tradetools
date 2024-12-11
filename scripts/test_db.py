"""
DolphinDB测试运行脚本
"""

from ..database.dolphindb_client import DolphinDBClient
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

def main():
    # 创建客户端
    client = DolphinDBClient()
    
    # 测试连接
    if not client.connect():
        print("Failed to connect to DolphinDB")
        return
        
    try:
        # 创建数据表
        print("Creating market data table...")
        client.create_market_data_table()
        
        # 生成测试数据
        print("Generating test data...")
        dates = pd.date_range(
            start='2023-01-01',
            end='2023-01-31',
            freq='1min'
        )
        
        test_data = pd.DataFrame({
            'timestamp': dates,
            'open': np.random.rand(len(dates)) * 100 + 100,
            'high': np.random.rand(len(dates)) * 100 + 110,
            'low': np.random.rand(len(dates)) * 100 + 90,
            'close': np.random.rand(len(dates)) * 100 + 100,
            'volume': np.random.randint(1000, 10000, len(dates))
        })
        
        # 保存数据
        print("Saving market data...")
        success = client.save_market_data('AAPL', test_data)
        if success:
            print("Successfully saved market data")
        else:
            print("Failed to save market data")
            
        # 查询数据
        print("Querying market data...")
        result = client.query_market_data(
            'AAPL',
            datetime(2023, 1, 1),
            datetime(2023, 1, 31)
        )
        
        if result is not None:
            print(f"Retrieved {len(result)} records")
            print("\nFirst few records:")
            print(result.head())
            
            print("\nData statistics:")
            print(result.describe())
        else:
            print("Failed to query market data")
            
    except Exception as e:
        print(f"Error during testing: {str(e)}")
        
    finally:
        # 关闭连接
        client.disconnect()
        
if __name__ == "__main__":
    main()
