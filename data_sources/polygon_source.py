"""
Polygon.io API数据源
"""
import os
from typing import List, Dict, Optional
from datetime import datetime, date
from polygon import RESTClient
from dotenv import load_dotenv

load_dotenv()

class PolygonDataSource:
    def __init__(self):
        self.client = RESTClient(os.getenv('POLYGON_API_KEY'))
        
    def get_option_chain(self, underlying_symbol: str, 
                        expiration_date: Optional[date] = None) -> List[Dict]:
        """获取期权链数据"""
        try:
            # 如果没有指定到期日，获取所有可用的期权
            if expiration_date is None:
                return self.client.list_options(underlying_symbol)
            
            # 获取特定到期日的期权
            return self.client.list_options(
                underlying_symbol,
                expiration_date=expiration_date.strftime('%Y-%m-%d')
            )
        except Exception as e:
            print(f"Error fetching option chain from Polygon: {str(e)}")
            return []
            
    def get_historical_data(self, symbol: str, 
                          from_date: datetime,
                          to_date: datetime,
                          limit: int = 5000) -> List[Dict]:
        """获取历史数据"""
        try:
            aggs = self.client.get_aggs(
                symbol,
                1,
                'day',
                from_date.strftime('%Y-%m-%d'),
                to_date.strftime('%Y-%m-%d'),
                limit=limit
            )
            return [agg.__dict__ for agg in aggs]
        except Exception as e:
            print(f"Error fetching historical data from Polygon: {str(e)}")
            return []
            
    def get_last_quote(self, symbol: str) -> Optional[Dict]:
        """获取最新报价"""
        try:
            return self.client.get_last_quote(symbol).__dict__
        except Exception as e:
            print(f"Error fetching last quote from Polygon: {str(e)}")
            return None
            
    def get_news(self, symbol: Optional[str] = None, 
                 limit: int = 100) -> List[Dict]:
        """获取新闻数据"""
        try:
            if symbol:
                return self.client.list_ticker_news(symbol, limit=limit)
            return self.client.list_market_news(limit=limit)
        except Exception as e:
            print(f"Error fetching news from Polygon: {str(e)}")
            return []
