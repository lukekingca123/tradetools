"""
使用DolphinDB处理美股期权数据的示例
"""
from ..data.option_data import OptionDataHandler
import pandas as pd
from datetime import datetime, timedelta

def main():
    # 创建数据处理器实例
    handler = OptionDataHandler(
        host="localhost",
        port=8848,
        username="admin",
        password="123456"
    )
    
    try:
        # 插入一些示例合约数据
        contracts_data = pd.DataFrame({
            'contract_id': ['AAPL230915C150', 'AAPL230915P150'],
            'symbol': ['AAPL230915C150', 'AAPL230915P150'],
            'underlying': ['AAPL', 'AAPL'],
            'strike': [150.0, 150.0],
            'expiry': [datetime(2023, 9, 15), datetime(2023, 9, 15)],
            'type': ['CALL', 'PUT'],
            'exchange': ['CBOE', 'CBOE']
        })
        handler.insert_contracts(contracts_data)
        print("合约数据插入成功")
        
        # 插入一些示例期权行情数据
        quotes_data = pd.DataFrame({
            'contract_id': ['AAPL230915C150', 'AAPL230915P150'],
            'timestamp': [datetime.now(), datetime.now()],
            'bid': [5.5, 2.5],
            'ask': [5.7, 2.7],
            'last': [5.6, 2.6],
            'volume': [1000, 500],
            'open_interest': [5000, 2500],
            'implied_vol': [0.25, 0.30]
        })
        handler.insert_quotes(quotes_data)
        print("期权行情数据插入成功")
        
        # 插入一些示例标的行情数据
        underlying_data = pd.DataFrame({
            'symbol': ['AAPL'],
            'timestamp': [datetime.now()],
            'open': [150.0],
            'high': [152.0],
            'low': [149.0],
            'close': [151.0],
            'volume': [1000000]
        })
        handler.insert_underlying_quotes(underlying_data)
        print("标的行情数据插入成功")
        
        # 获取期权链
        chain = handler.get_option_chain('AAPL', datetime.now())
        print("\n期权链数据:")
        print(chain)
        
        # 获取期权行情
        quotes = handler.get_option_quotes(
            ['AAPL230915C150', 'AAPL230915P150'],
            datetime.now() - timedelta(days=1),
            datetime.now()
        )
        print("\n期权行情数据:")
        print(quotes)
        
        # 获取标的行情
        underlying_quotes = handler.get_underlying_quotes(
            'AAPL',
            datetime.now() - timedelta(days=1),
            datetime.now()
        )
        print("\n标的行情数据:")
        print(underlying_quotes)
        
        # 获取波动率曲面
        vol_surface = handler.get_volatility_surface('AAPL', datetime.now())
        print("\n波动率曲面数据:")
        print(vol_surface)
        
        # 计算希腊字母
        greeks = handler.calculate_greeks(
            ['AAPL230915C150', 'AAPL230915P150'],
            datetime.now()
        )
        print("\n希腊字母:")
        print(greeks)
        
    finally:
        handler.close()

if __name__ == '__main__':
    main()
