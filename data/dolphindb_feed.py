"""
Backtrader数据源模块 - DolphinDB数据接入
"""
import backtrader as bt
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any
from .option_data import OptionDataHandler

class DolphinDBData(bt.feeds.PandasData):
    """
    DolphinDB数据源适配器，用于Backtrader回测
    继承自bt.feeds.PandasData，实现与DolphinDB的数据接口
    """
    # 定义列映射
    params = (
        ('datetime', 'timestamp'),  # 时间戳列
        ('open', 'open'),          # 开盘价
        ('high', 'high'),          # 最高价
        ('low', 'low'),            # 最低价
        ('close', 'close'),        # 收盘价
        ('volume', 'volume'),      # 成交量
        ('openinterest', 'open_interest'),  # 持仓量
        ('strike', 'strike'),      # 行权价
        ('impliedVolatility', 'implied_volatility'),  # 隐含波动率
        ('delta', 'delta'),        # Delta值
        ('gamma', 'gamma'),        # Gamma值
        ('vega', 'vega'),          # Vega值
        ('theta', 'theta'),        # Theta值
        ('rho', 'rho'),           # Rho值
    )

    def __init__(self, 
                 db_handler: Optional[OptionDataHandler] = None,
                 symbol: str = None,
                 start_date: datetime = None,
                 end_date: datetime = None,
                 **kwargs):
        """
        初始化数据源
        
        Args:
            db_handler: DolphinDB数据处理器实例
            symbol: 期权代码
            start_date: 开始日期
            end_date: 结束日期
        """
        if db_handler is None:
            db_handler = OptionDataHandler()
        
        self.db_handler = db_handler
        self.symbol = symbol
        self.start_date = start_date
        self.end_date = end_date
        
        # 从DolphinDB获取数据
        df = self._fetch_data()
        
        # 调用父类初始化
        super().__init__(dataname=df, **kwargs)
    
    def _fetch_data(self) -> pd.DataFrame:
        """
        从DolphinDB获取数据并转换为Backtrader所需格式
        
        Returns:
            pd.DataFrame: 处理后的数据框
        """
        # 使用OptionDataHandler查询数据
        df = self.db_handler.query_option_data(
            symbol=self.symbol,
            start_date=self.start_date,
            end_date=self.end_date
        )
        
        # 确保时间戳列格式正确
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df.sort_index(inplace=True)
        
        return df

class DolphinDBOptionFeed:
    """
    DolphinDB期权数据源管理器
    用于管理多个期权合约的数据源
    """
    def __init__(self, db_handler: Optional[OptionDataHandler] = None):
        """
        初始化数据源管理器
        
        Args:
            db_handler: DolphinDB数据处理器实例
        """
        self.db_handler = db_handler or OptionDataHandler()
        
    def get_option_data(self, 
                       symbol: str,
                       start_date: datetime,
                       end_date: datetime,
                       **kwargs) -> DolphinDBData:
        """
        获取指定期权合约的数据源
        
        Args:
            symbol: 期权代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            DolphinDBData: 期权数据源实例
        """
        return DolphinDBData(
            db_handler=self.db_handler,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            **kwargs
        )
    
    def get_multiple_options(self,
                           symbols: list,
                           start_date: datetime,
                           end_date: datetime,
                           **kwargs) -> Dict[str, DolphinDBData]:
        """
        获取多个期权合约的数据源
        
        Args:
            symbols: 期权代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            Dict[str, DolphinDBData]: 期权代码到数据源的映射
        """
        return {
            symbol: self.get_option_data(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                **kwargs
            )
            for symbol in symbols
        }
