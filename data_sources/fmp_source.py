"""
Financial Model Prep API数据源
"""
import os
from typing import Dict, List, Optional
from datetime import datetime, date
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

class FMPDataSource:
    """Financial Model Prep数据源"""
    
    BASE_URL = "https://financialmodelingprep.com/api/v3"
    
    def __init__(self):
        self.api_key = os.getenv('FMP_API_KEY')
        if not self.api_key:
            raise ValueError("FMP_API_KEY not found in environment variables")
            
    def _get(self, endpoint: str, params: Dict = None) -> Dict:
        """发送GET请求"""
        if params is None:
            params = {}
        params['apikey'] = self.api_key
        
        response = requests.get(f"{self.BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    
    def get_treasury_rates(self) -> pd.DataFrame:
        """获取美国国债利率
        
        Returns:
            DataFrame包含不同期限的国债利率:
            - 1月
            - 2月
            - 3月
            - 6月
            - 1年
            - 2年
            - 3年
            - 5年
            - 7年
            - 10年
            - 20年
            - 30年
        """
        try:
            data = self._get('treasury')
            return pd.DataFrame(data)
        except Exception as e:
            print(f"获取国债利率失败: {str(e)}")
            return pd.DataFrame()
            
    def get_historical_volatility(self, 
                                symbol: str,
                                days: int = 252) -> Optional[float]:
        """计算历史波动率
        
        Args:
            symbol: 股票代码
            days: 交易日数量，默认一年
            
        Returns:
            年化历史波动率
        """
        try:
            # 获取历史价格
            params = {
                'symbol': symbol,
                'timeseries': days
            }
            data = self._get('historical-price-full', params)
            
            if not data.get('historical'):
                return None
                
            # 转换为DataFrame
            df = pd.DataFrame(data['historical'])
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # 计算对数收益率
            df['returns'] = np.log(df['close'] / df['close'].shift(1))
            
            # 计算年化波动率
            volatility = df['returns'].std() * np.sqrt(252)
            
            return volatility
        except Exception as e:
            print(f"计算历史波动率失败: {str(e)}")
            return None
            
    def get_economic_indicators(self) -> pd.DataFrame:
        """获取经济指标数据"""
        try:
            data = self._get('economic-calendar')
            return pd.DataFrame(data)
        except Exception as e:
            print(f"获取经济指标失败: {str(e)}")
            return pd.DataFrame()
            
    def get_company_profile(self, symbol: str) -> Dict:
        """获取公司信息，包括股息数据"""
        try:
            data = self._get(f'profile/{symbol}')
            return data[0] if data else {}
        except Exception as e:
            print(f"获取公司信息失败: {str(e)}")
            return {}
            
    def get_dividend_calendar(self, symbol: str) -> List[Dict]:
        """获取股息日历"""
        try:
            data = self._get(f'historical-price-full/stock_dividend/{symbol}')
            return data.get('historical', [])
        except Exception as e:
            print(f"获取股息日历失败: {str(e)}")
            return []
