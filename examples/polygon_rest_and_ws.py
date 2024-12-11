"""
展示Polygon的REST API和WebSocket使用方式

注意：
1. Basic订阅的限制：
   - 延迟的市场数据（15分钟延迟）
   - API调用频率限制为每分钟5次
   - 只能访问基本的股票数据
   
2. 升级后可以使用的功能：
   - 实时市场数据（无延迟）
   - 更高的API调用频率
   - 访问更多的数据类型（如期权数据、Level 2数据等）
"""
import os
import sys
import time
from datetime import datetime, timedelta
import requests
import websocket
import json
import threading

class PolygonAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io"
        
    def get_daily_bars(self, symbol, start_date, end_date):
        """
        使用REST API获取日线数据
        """
        endpoint = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}"
        params = {'apiKey': self.api_key}
        
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None
            
    def get_option_chain(self, underlying_symbol, expiration_date):
        """
        使用REST API获取期权链数据
        """
        endpoint = f"{self.base_url}/v3/reference/options/contracts"
        params = {
            'underlying_ticker': underlying_symbol,
            'expiration_date': expiration_date,
            'apiKey': self.api_key
        }
        
        response = requests.get(endpoint, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None

class PolygonWebSocket:
    def __init__(self, api_key):
        self.api_key = api_key
        self.ws = None
        self.is_connected = False
        
    def on_message(self, ws, message):
        """处理接收到的WebSocket消息"""
        data = json.loads(message)
        print(f"Received: {data}")
        
    def on_error(self, ws, error):
        print(f"Error: {error}")
        
    def on_close(self, ws, close_status_code, close_msg):
        print("WebSocket connection closed")
        self.is_connected = False
        
    def on_open(self, ws):
        print("WebSocket connection opened")
        self.is_connected = True
        # 发送认证消息
        auth_data = {"action":"auth", "params": self.api_key}
        ws.send(json.dumps(auth_data))
        
        # 订阅实时数据
        subscribe_message = {
            "action": "subscribe",
            "params": "T.AAPL,Q.AAPL"  # T代表trades, Q代表quotes
        }
        ws.send(json.dumps(subscribe_message))
        
    def connect(self):
        """建立WebSocket连接"""
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            "wss://delayed.polygon.io/stocks",
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
            on_open=self.on_open
        )
        
        # 在新线程中运行WebSocket连接
        wst = threading.Thread(target=self.ws.run_forever)
        wst.daemon = True
        wst.start()
        
    def disconnect(self):
        """关闭WebSocket连接"""
        if self.ws:
            self.ws.close()

def main():
    # 使用Basic订阅的API密钥
    api_key = "eyDkZwYssb2iKZ5Qoft_9Zn2AipeUdT7"
    
    # 演示REST API
    print("\n=== 使用REST API获取数据（Basic订阅-15分钟延迟）===")
    rest_client = PolygonAPI(api_key)
    
    # 获取苹果公司的日线数据
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    print("\n获取AAPL的日线数据...")
    daily_bars = rest_client.get_daily_bars('AAPL', start_date, end_date)
    if daily_bars:
        print(f"获取到 {len(daily_bars.get('results', []))} 条日线数据")
    
    # 获取期权链数据
    print("\n获取AAPL的期权链...")
    options = rest_client.get_option_chain('AAPL', end_date)
    if options:
        print(f"获取到 {len(options.get('results', []))} 个期权合约")
    
    # 演示WebSocket API
    print("\n=== 使用WebSocket获取实时数据 ===")
    ws_client = PolygonWebSocket(api_key)
    
    # 连接WebSocket
    print("连接WebSocket...")
    ws_client.connect()
    
    # 保持运行一段时间以接收实时数据
    try:
        print("等待实时数据(30秒)...")
        time.sleep(30)
    except KeyboardInterrupt:
        print("手动终止程序")
    finally:
        ws_client.disconnect()
        print("已断开WebSocket连接")

if __name__ == "__main__":
    main()
