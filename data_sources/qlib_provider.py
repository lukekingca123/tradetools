"""
QLib数据提供者模块，用于将期权数据转换为QLib可用的格式。
"""
import pandas as pd
import numpy as np
from qlib.data import D
from qlib.data.dataset.handler import DataHandlerLP
from qlib.data.dataset import DatasetH
from qlib.data.data import ExchangeDayData, LocalDataSource

from ..data.option_data import OptionDataHandler

class OptionDataProvider(LocalDataSource):
    """期权数据提供者，继承自QLib的LocalDataSource"""
    
    def __init__(self, option_handler: OptionDataHandler = None, 
                 provider_uri: str = None, **kwargs):
        """
        初始化期权数据提供者
        
        Args:
            option_handler: OptionDataHandler实例
            provider_uri: 数据存储路径
            **kwargs: 其他参数
        """
        super().__init__(provider_uri=provider_uri, **kwargs)
        self.option_handler = option_handler or OptionDataHandler()
        
    def _load_calendar(self) -> pd.DatetimeIndex:
        """加载交易日历
        
        Returns:
            pd.DatetimeIndex: 交易日期索引
        """
        # 从DolphinDB获取交易日历
        calendar_data = self.option_handler.get_trading_calendar()
        return pd.DatetimeIndex(calendar_data)
        
    def _load_instruments(self) -> pd.DataFrame:
        """加载期权合约信息
        
        Returns:
            pd.DataFrame: 期权合约信息，包含以下字段：
                - instrument: 合约代码
                - start_time: 上市日期
                - end_time: 到期日期
                - underlying: 标的代码
                - type: 期权类型
                - strike: 行权价
        """
        # 从DolphinDB获取期权合约信息
        instruments = self.option_handler.get_option_instruments()
        return instruments
        
    def _load_features(self, instruments, start_time: pd.Timestamp, 
                      end_time: pd.Timestamp, freq: str = 'day') -> pd.DataFrame:
        """加载期权特征数据
        
        Args:
            instruments: 合约列表
            start_time: 开始时间
            end_time: 结束时间
            freq: 数据频率
            
        Returns:
            pd.DataFrame: 期权特征数据，MultiIndex格式(instrument, datetime)
        """
        # 从DolphinDB获取期权数据
        raw_data = self.option_handler.get_option_data(
            symbols=instruments,
            start_date=start_time,
            end_date=end_time
        )
        
        # 计算期权特征
        feature_data = self.option_handler.calculate_option_features(raw_data, model='bcc97')
        
        # 转换为QLib要求的格式
        feature_data = self._format_features(feature_data)
        
        return feature_data
        
    def _format_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """将期权数据转换为QLib要求的格式
        
        Args:
            df: 原始特征数据
            
        Returns:
            pd.DataFrame: 格式化后的特征数据
        """
        # 设置MultiIndex
        df = df.set_index(['symbol', 'date'])
        
        # 重命名列以符合QLib的命名规范
        rename_dict = {
            'close': '$close',
            'open': '$open',
            'high': '$high',
            'low': '$low',
            'volume': '$volume',
            'bcc_price': '$theoretical',
            'bcc_delta': '$delta',
            'bcc_gamma': '$gamma',
            'bcc_theta': '$theta',
            'bcc_vega': '$vega',
            'bcc_rho': '$rho',
            'implied_vol': '$iv',
            'time_to_expiry': '$tte',
            'moneyness': '$moneyness',
            'chain_volume_ratio': '$volume_ratio',
            'spread': '$spread',
            'liquidity_score': '$liquidity'
        }
        
        df = df.rename(columns=rename_dict)
        
        # 添加额外的QLib所需字段
        df['$factor'] = 1.0  # 价格调整因子
        
        return df

class OptionDatasetProvider:
    """期权数据集提供者，用于创建QLib数据集"""
    
    def __init__(self, handler_kwargs: dict = None, provider_uri: str = None):
        """
        初始化数据集提供者
        
        Args:
            handler_kwargs: DataHandler的参数
            provider_uri: 数据存储路径
        """
        self.handler_kwargs = handler_kwargs or {}
        self.provider_uri = provider_uri
        
    def get_dataset(self, segments: dict) -> DatasetH:
        """获取数据集
        
        Args:
            segments: 数据集划分，例如：
                {
                    "train": ("2020-01-01", "2021-12-31"),
                    "valid": ("2022-01-01", "2022-06-30"),
                    "test": ("2022-07-01", "2022-12-31")
                }
                
        Returns:
            DatasetH: QLib数据集
        """
        # 创建数据处理器
        handler = DataHandlerLP(**self.handler_kwargs)
        
        # 创建数据集
        dataset = DatasetH(handler=handler, segments=segments)
        
        return dataset
