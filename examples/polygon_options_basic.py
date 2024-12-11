"""
使用Polygon Basic订阅获取期权数据的示例
注意：Basic订阅有15分钟延迟，且每分钟最多调用5次API
"""
import time
from datetime import datetime, timedelta
import requests

class PolygonOptionsAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        
    def get_option_contracts(self, underlying_symbol, expiration_date=None):
        """
        获取期权合约列表
        :param underlying_symbol: 标的股票代码
        :param expiration_date: 到期日 (YYYY-MM-DD)，可选
        """
        endpoint = f"{self.base_url}/v3/reference/options/contracts"
        params = {
            'underlying_ticker': underlying_symbol,
            'apiKey': self.api_key
        }
        if expiration_date:
            params['expiration_date'] = expiration_date
            
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    def get_option_aggregates(self, option_symbol, from_date, to_date, timespan='day', multiplier=1):
        """
        获取期权历史价格数据
        :param option_symbol: 期权合约代码
        :param from_date: 开始日期 (YYYY-MM-DD)
        :param to_date: 结束日期 (YYYY-MM-DD)
        :param timespan: 时间单位 (minute/hour/day/week/month/quarter/year)
        :param multiplier: 时间单位的倍数
        """
        endpoint = f"{self.base_url}/v2/aggs/ticker/{option_symbol}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        params = {'apiKey': self.api_key}
        
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(f"Response: {response.text}")
            return None

def main():
    # 使用Basic订阅的API密钥
    api_key = "eyDkZwYssb2iKZ5Qoft_9Zn2AipeUdT7"
    client = PolygonOptionsAPI(api_key)
    
    # 设置查询参数
    symbol = 'AAPL'
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')  # 获取最近30天的数据
    
    # 1. 获取期权合约列表
    print(f"\n获取{symbol}的期权合约列表...")
    contracts = client.get_option_contracts(symbol, expiration_date=end_date)
    
    if contracts and contracts.get('results'):
        print(f"找到 {len(contracts['results'])} 个期权合约")
        # 打印第一个合约的信息作为示例
        print("\n合约示例:")
        print(contracts['results'][0])
        
        # 2. 获取第一个期权合约的历史数据
        first_contract = contracts['results'][0]
        contract_symbol = first_contract['ticker']
        
        print(f"\n获取期权合约 {contract_symbol} 的历史数据...")
        # 添加延时以遵守API限制
        time.sleep(12)
        
        aggregates = client.get_option_aggregates(
            option_symbol=contract_symbol,
            from_date=start_date,
            to_date=end_date
        )
        
        if aggregates and aggregates.get('results'):
            print(f"获取到 {len(aggregates['results'])} 条历史数据")
            print("\n数据示例:")
            print(aggregates['results'][0])
    else:
        print("未找到期权合约")

if __name__ == "__main__":
    main()
