"""
波动率计算模块，包括历史波动率和隐含波动率曲面
"""
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from dataclasses import dataclass
from datetime import datetime, timedelta

@dataclass
class VolPoint:
    """波动率数据点"""
    strike: float
    expiry: datetime
    vol: float
    
class VolatilitySurface:
    """波动率曲面"""
    
    def __init__(self, spot_price: float):
        self.spot_price = spot_price
        self.vol_points: List[VolPoint] = []
        
    def add_vol_point(self, strike: float, expiry: datetime, vol: float):
        """添加波动率数据点"""
        self.vol_points.append(VolPoint(strike, expiry, vol))
        
    def get_vol(self, strike: float, expiry: datetime) -> float:
        """获取特定行权价和到期日的波动率
        
        使用2D插值计算任意点的波动率
        """
        if not self.vol_points:
            raise ValueError("No volatility points available")
            
        # 准备插值数据
        points = np.array([[p.strike/self.spot_price, (p.expiry - datetime.now()).days/365.0] 
                          for p in self.vol_points])
        values = np.array([p.vol for p in self.vol_points])
        
        # 计算目标点的单位化值
        moneyness = strike / self.spot_price
        time_to_expiry = (expiry - datetime.now()).days / 365.0
        
        # 2D插值
        return float(griddata(points, values, np.array([[moneyness, time_to_expiry]]), 
                            method='cubic')[0])
                            
class HistoricalVolatility:
    """历史波动率计算"""
    
    @staticmethod
    def calculate(prices: pd.Series, 
                 window: int = 252,
                 min_periods: int = 30) -> pd.Series:
        """计算历史波动率
        
        Args:
            prices: 价格时间序列
            window: 移动窗口大小，默认一年
            min_periods: 最小所需期数
            
        Returns:
            移动窗口年化波动率序列
        """
        # 计算对数收益率
        returns = np.log(prices / prices.shift(1))
        
        # 计算移动窗口标准差
        rolling_std = returns.rolling(window=window, 
                                    min_periods=min_periods).std()
        
        # 年化
        return rolling_std * np.sqrt(252)
        
    @staticmethod
    def calculate_term_structure(prices: pd.Series,
                               windows: List[int] = [5, 10, 21, 63, 126, 252]
                               ) -> Dict[int, float]:
        """计算不同期限的历史波动率
        
        Args:
            prices: 价格时间序列
            windows: 不同期限的天数列表
            
        Returns:
            不同期限对应的年化波动率字典
        """
        result = {}
        for window in windows:
            vol = HistoricalVolatility.calculate(prices, window)
            result[window] = vol.iloc[-1]  # 取最新值
        return result
        
    @staticmethod
    def parkinson_volatility(high: pd.Series, 
                           low: pd.Series,
                           window: int = 252) -> pd.Series:
        """使用Parkinson公式计算波动率
        
        基于日内高低价计算波动率，对跳空有更好的处理
        """
        # 计算对数高低价之比
        log_hl = np.log(high / low)
        
        # 计算Parkinson估计量
        estimator = log_hl ** 2 / (4 * np.log(2))
        
        # 计算移动窗口波动率
        rolling_var = estimator.rolling(window=window).mean()
        
        # 年化
        return np.sqrt(252 * rolling_var)
        
    @staticmethod
    def garman_klass_volatility(open_: pd.Series,
                               high: pd.Series,
                               low: pd.Series,
                               close: pd.Series,
                               window: int = 252) -> pd.Series:
        """使用Garman-Klass公式计算波动率
        
        结合开高低收价格计算波动率，提供更准确的估计
        """
        # 计算对数价格
        log_hl = np.log(high / low)
        log_co = np.log(close / open_)
        
        # 计算Garman-Klass估计量
        estimator = 0.5 * log_hl**2 - (2*np.log(2) - 1) * log_co**2
        
        # 计算移动窗口波动率
        rolling_var = estimator.rolling(window=window).mean()
        
        # 年化
        return np.sqrt(252 * rolling_var)
