"""
Polygon.io API客户端，用于获取历史股价数据
API文档: https://polygon.io/docs/stocks/getting-started
"""
import os
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
from typing import List, Dict, Optional
import logging
import sys
import numpy as np

# 添加项目根目录到Python路径
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from db_utils import get_dolphindb_connection
from config import TABLES

class PolygonClient:
    """Polygon API客户端"""
    
    BASE_URL = "https://api.polygon.io"
    
    # 时间周期常量
    TIMESPAN_5MIN = ('5', 'minute')
    TIMESPAN_60MIN = ('60', 'minute')
    TIMESPAN_1DAY = ('1', 'day')
    
    def __init__(self, api_key: str = None):
        """
        初始化客户端
        
        Args:
            api_key: Polygon API密钥，如果为None则从环境变量POLYGON_API_KEY获取
        """
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("API key is required. Set POLYGON_API_KEY environment variable or pass it directly.")
            
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {self.api_key}'
        })
        
    def get_aggregates(self, ticker: str, from_date: str, to_date: str,
                      multiplier: int = 1, timespan: str = 'day',
                      adjusted: bool = True) -> pd.DataFrame:
        """
        获取聚合的股票价格数据
        
        Args:
            ticker: 股票代码
            from_date: 开始日期 (YYYY-MM-DD)
            to_date: 结束日期 (YYYY-MM-DD)
            multiplier: 时间间隔乘数
            timespan: 时间间隔单位 (minute/hour/day/week/month/quarter/year)
            adjusted: 是否进行除权除息调整
            
        Returns:
            DataFrame包含以下字段：
            - timestamp: 时间戳
            - open/high/low/close: OHLC价格
            - volume: 成交量
            - vwap: 成交量加权平均价
            - transactions: 成交笔数
        """
        endpoint = f"{self.BASE_URL}/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
        
        params = {
            'adjusted': str(adjusted).lower(),
            'sort': 'asc',
            'limit': 50000
        }
        
        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data['status'] != 'OK':
                raise ValueError(f"API returned error: {data.get('error')}")
                
            if not data.get('results'):
                logging.warning(f"No data returned for {ticker} from {from_date} to {to_date}")
                return pd.DataFrame()
                
            # 转换为DataFrame
            df = pd.DataFrame(data['results'])
            
            # 重命名列
            df = df.rename(columns={
                't': 'timestamp',
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                'vw': 'vwap',
                'n': 'transactions'
            })
            
            # 转换时间戳
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            # 设置索引
            df = df.set_index('timestamp')
            
            # 添加symbol列
            df['symbol'] = ticker
            
            return df
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {ticker}: {str(e)}")
            raise
            
    def get_splits(self, ticker: str, from_date: str, to_date: str) -> pd.DataFrame:
        """
        获取股票拆分信息
        
        Args:
            ticker: 股票代码
            from_date: 开始日期 (YYYY-MM-DD)
            to_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            DataFrame包含拆分信息
        """
        endpoint = f"{self.BASE_URL}/v3/reference/splits"
        
        params = {
            'ticker': ticker,
            'from': from_date,
            'to': to_date
        }
        
        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('results'):
                return pd.DataFrame()
                
            return pd.DataFrame(data['results'])
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching splits for {ticker}: {str(e)}")
            raise
            
    def get_dividends(self, ticker: str, from_date: str, to_date: str) -> pd.DataFrame:
        """
        获取股息信息
        
        Args:
            ticker: 股票代码
            from_date: 开始日期 (YYYY-MM-DD)
            to_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            DataFrame包含股息信息
        """
        endpoint = f"{self.BASE_URL}/v3/reference/dividends"
        
        params = {
            'ticker': ticker,
            'from': from_date,
            'to': to_date
        }
        
        try:
            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()
            
            if not data.get('results'):
                return pd.DataFrame()
                
            return pd.DataFrame(data['results'])
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching dividends for {ticker}: {str(e)}")
            raise
            
    def batch_download_stocks(self, tickers: List[str], from_date: str, to_date: str,
                            save_to_db: bool = True, batch_size: int = 5,
                            timespan: tuple = TIMESPAN_1DAY) -> Dict[str, pd.DataFrame]:
        """
        批量下载多个股票的历史数据
        
        Args:
            tickers: 股票代码列表
            from_date: 开始日期 (YYYY-MM-DD)
            to_date: 结束日期 (YYYY-MM-DD)
            save_to_db: 是否保存到DolphinDB
            batch_size: 每批处理的股票数量
            timespan: 时间周期，可选值：
                     TIMESPAN_5MIN: 5分钟线
                     TIMESPAN_60MIN: 60分钟线
                     TIMESPAN_1DAY: 日线
                     
        Returns:
            字典，key为股票代码，value为对应的DataFrame
        """
        results = {}
        
        # 将股票列表分成多个批次
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i+batch_size]
            
            for ticker in batch_tickers:
                try:
                    print(f"Downloading {ticker} data...")
                    df = self.get_aggregates(
                        ticker=ticker,
                        from_date=from_date,
                        to_date=to_date,
                        multiplier=int(timespan[0]),
                        timespan=timespan[1]
                    )
                    
                    if df.empty:
                        print(f"No data available for {ticker}")
                        continue
                        
                    # 添加股票代码列
                    df['symbol'] = ticker
                    
                    if save_to_db:
                        self._save_to_dolphindb(df, ticker)
                        
                    results[ticker] = df
                    
                except Exception as e:
                    print(f"Error downloading {ticker}: {str(e)}")
                    continue
                    
                # 避免触发API限制
                time.sleep(0.2)
                
        return results
        
    def _save_to_dolphindb(self, df: pd.DataFrame, ticker: str):
        """保存数据到DolphinDB，如果连接失败则保存到CSV文件"""
        try:
            from db_utils import DBConnection
            
            # 根据数据频率选择表名
            freq = pd.Timedelta(df.index[1] - df.index[0])
            if freq <= pd.Timedelta('5min'):
                table_name = TABLES['stocks']['minutes']
            elif freq <= pd.Timedelta('1h'):
                table_name = TABLES['stocks']['hourly']
            else:
                table_name = TABLES['stocks']['daily']
            
            # 连接数据库
            db = DBConnection()
            
            # 创建表（如果不存在）
            db.conn.run(f"""
            if(!existsTable("dfs://market", "{table_name}")) {{
                schema = table(
                    1:0, 
                    `symbol`timestamp`open`high`low`close`volume`vwap`transactions,
                    [SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG]
                )
                db = database("dfs://market")
                createPartitionedTable(
                    db,
                    schema,
                    `{table_name},
                    `symbol`timestamp
                )
            }}
            """)
            
            # 准备数据
            data = {
                "symbol": df['symbol'].values,
                "timestamp": df.index.values,
                "open": df['open'].values,
                "high": df['high'].values,
                "low": df['low'].values,
                "close": df['close'].values,
                "volume": df['volume'].values.astype(np.int64),
                "vwap": df['vwap'].values,
                "transactions": df['transactions'].values.astype(np.int64)
            }
            
            # 上传数据
            db.conn.upload({"data": data})
            
            # 插入数据
            db.conn.run(f"""
            t = table(
                data.symbol as symbol,
                data.timestamp as timestamp,
                data.open as open,
                data.high as high,
                data.low as low,
                data.close as close,
                data.volume as volume,
                data.vwap as vwap,
                data.transactions as transactions
            )
            loadTable("dfs://market", "{table_name}").append!(t)
            """)
            
            print(f"Successfully saved {len(df)} records to DolphinDB table {table_name}")
            
        except Exception as e:
            # 如果保存到数据库失败，保存到CSV文件
            logging.error(f"Error saving data to DolphinDB: {str(e)}")
            csv_file = f"data/stocks/{ticker}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
            os.makedirs(os.path.dirname(csv_file), exist_ok=True)
            df.to_csv(csv_file)
            print(f"Saved data to CSV file: {csv_file}")
