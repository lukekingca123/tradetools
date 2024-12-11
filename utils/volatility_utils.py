"""
波动率相关工具类
"""
import numpy as np
import pandas as pd
from typing import List, Dict, Tuple
from datetime import datetime

class VolatilityUtils:
    @staticmethod
    def calculate_historical_vol(
        prices: np.ndarray,
        window: int = 20,
        annualize: bool = True
    ) -> np.ndarray:
        """计算历史波动率"""
        returns = np.log(prices[1:] / prices[:-1])
        vol = pd.Series(returns).rolling(window).std()
        if annualize:
            vol *= np.sqrt(252)
        return vol.values
    
    @staticmethod
    def calculate_volatility_cone(
        prices: np.ndarray,
        windows: List[int] = [5, 20, 60, 120],
        quantiles: List[float] = [0.25, 0.5, 0.75]
    ) -> pd.DataFrame:
        """计算波动率锥"""
        vols = {}
        for window in windows:
            vol = VolatilityUtils.calculate_historical_vol(prices, window)
            vols[window] = [np.quantile(vol, q) for q in quantiles]
        return pd.DataFrame(vols, index=[f'{q*100}%' for q in quantiles])
    
    @staticmethod
    def calculate_realized_vol(
        prices: np.ndarray,
        window: int = 20,
        sampling_freq: str = '5min'
    ) -> float:
        """计算已实现波动率"""
        # 将价格数据重采样到指定频率
        prices = pd.Series(prices).resample(sampling_freq).last()
        # 计算对数收益率
        returns = np.log(prices / prices.shift(1))
        # 计算已实现波动率
        realized_vol = np.sqrt(np.sum(returns**2) * (252*78/window))  # 假设一天有78个5分钟
        return realized_vol
    
    @staticmethod
    def calculate_forward_vol(
        current_price: float,
        strike: float,
        option_price: float,
        risk_free_rate: float,
        time_to_expiry: float,
        option_type: str = 'call'
    ) -> float:
        """计算远期波动率"""
        from py_vollib.black_scholes import implied_volatility
        try:
            iv = implied_volatility(
                option_price,
                current_price,
                strike,
                time_to_expiry,
                risk_free_rate,
                option_type
            )
            return iv
        except Exception as e:
            print(f"Error calculating implied volatility: {e}")
            return None

class EventAnalysisUtils:
    @staticmethod
    def calculate_event_vol_pattern(
        prices: Dict[str, np.ndarray],
        event_dates: List[datetime],
        window: int = 10
    ) -> pd.DataFrame:
        """分析事件前后的波动率模式"""
        patterns = []
        for symbol, price_data in prices.items():
            for event_date in event_dates:
                # 获取事件窗口数据
                event_window = price_data[event_date-window:event_date+window]
                if len(event_window) == 2*window:
                    vol = VolatilityUtils.calculate_historical_vol(event_window)
                    patterns.append({
                        'symbol': symbol,
                        'event_date': event_date,
                        'vol_pattern': vol
                    })
        return pd.DataFrame(patterns)
    
    @staticmethod
    def find_similar_events(
        target_event: Dict,
        historical_events: List[Dict],
        features: List[str],
        n: int = 5
    ) -> List[Dict]:
        """查找相似历史事件"""
        similarities = []
        target_vector = np.array([target_event[f] for f in features])
        for event in historical_events:
            event_vector = np.array([event[f] for f in features])
            similarity = np.linalg.norm(target_vector - event_vector)
            similarities.append((event, similarity))
        return sorted(similarities, key=lambda x: x[1])[:n]
    
    @staticmethod
    def calculate_event_impact(
        prices: np.ndarray,
        event_dates: List[datetime],
        window: int = 10,
        metric: str = 'volatility'
    ) -> pd.DataFrame:
        """计算事件对市场的影响
        
        Args:
            prices: 价格数据
            event_dates: 事件日期列表
            window: 事件窗口大小
            metric: 影响指标，可选 'volatility', 'volume', 'price'
            
        Returns:
            DataFrame包含事件前后的指标变化
        """
        impacts = []
        for event_date in event_dates:
            pre_event = prices[event_date-window:event_date]
            post_event = prices[event_date:event_date+window]
            
            if metric == 'volatility':
                pre_metric = VolatilityUtils.calculate_historical_vol(pre_event)
                post_metric = VolatilityUtils.calculate_historical_vol(post_event)
            elif metric == 'volume':
                pre_metric = np.mean(pre_event['volume'])
                post_metric = np.mean(post_event['volume'])
            elif metric == 'price':
                pre_metric = (pre_event[-1] - pre_event[0]) / pre_event[0]
                post_metric = (post_event[-1] - post_event[0]) / post_event[0]
            
            impacts.append({
                'event_date': event_date,
                'pre_event': pre_metric,
                'post_event': post_metric,
                'impact': post_metric - pre_metric
            })
            
        return pd.DataFrame(impacts)
