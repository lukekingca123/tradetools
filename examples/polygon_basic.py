"""
使用Polygon Basic订阅获取股票数据的示例
注意：Basic订阅有15分钟延迟，且每分钟最多调用5次API
"""
import time
from datetime import datetime, timedelta
import requests

class PolygonBasicAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        
    def get_bars(self, symbol, timespan, multiplier, start_date, end_date):
        """
        获取K线数据
        :param symbol: 股票代码
        :param timespan: 时间单位 (minute/hour/day/week/month/quarter/year)
        :param multiplier: 时间单位的倍数
        :param start_date: 开始日期 (YYYY-MM-DD)
        :param end_date: 结束日期 (YYYY-MM-DD)
        """
        endpoint = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{start_date}/{end_date}"
        params = {'apiKey': self.api_key}
        
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    def get_last_trade(self, symbol):
        """
        获取最后一笔交易（15分钟延迟）
        :param symbol: 股票代码
        """
        endpoint = f"{self.base_url}/v2/last/trade/{symbol}"
        params = {'apiKey': self.api_key}
        
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None

def main():
    # 使用Basic订阅的API密钥
    api_key = "eyDkZwYssb2iKZ5Qoft_9Zn2AipeUdT7"
    client = PolygonBasicAPI(api_key)
    
    # 设置时间范围（最近7天）
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # 测试不同时间周期的数据获取
    symbol = 'AAPL'
    timeframes = [
        ('minute', 1),   # 1分钟K线
        ('minute', 5),   # 5分钟K线
        ('hour', 1),     # 1小时K线
        ('day', 1),      # 日K线
    ]
    
    for timespan, multiplier in timeframes:
        print(f"\n获取{multiplier}{timespan} K线数据...")
        # 添加延时以遵守API限制
        time.sleep(12)  # 确保不超过每分钟5次的限制
        
        data = client.get_bars(
            symbol=symbol,
            timespan=timespan,
            multiplier=multiplier,
            start_date=start_date,
            end_date=end_date
        )
        
        if data and data.get('results'):
            print(f"获取到 {len(data['results'])} 条记录")
            # 打印第一条记录作为示例
            print("数据示例:")
            print(data['results'][0])
    
    # 获取最后一笔交易
    print("\n获取最后一笔交易数据（15分钟延迟）...")
    last_trade = client.get_last_trade(symbol)
    if last_trade and last_trade.get('results'):
        print("最后一笔交易:")
        print(last_trade['results'])

if __name__ == "__main__":
    main()
