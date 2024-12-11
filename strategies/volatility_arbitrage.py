"""
波动率套利策略
"""
from typing import List, Dict, Optional
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from .option_strategy_base import OptionStrategyBase
from ..utils.option_utils import OptionUtils
from ..utils.volatility_utils import VolatilityUtils

class VolatilityArbitrage(OptionStrategyBase):
    """波动率套利策略
    
    主要策略逻辑：
    1. 识别波动率定价偏差
    2. 构建delta中性组合
    3. 动态调仓和风险管理
    """
    
    def __init__(
        self,
        capital: float = 1000000,
        vol_window: int = 20,
        vol_z_threshold: float = 2.0,
        min_vol_spread: float = 0.05,
        max_position_size: float = 0.1,
        rebalance_threshold: float = 0.1
    ):
        """
        Args:
            capital: 初始资金
            vol_window: 波动率计算窗口
            vol_z_threshold: 波动率z-score阈值
            min_vol_spread: 最小波动率价差
            max_position_size: 最大持仓比例
            rebalance_threshold: 再平衡阈值
        """
        super().__init__('Volatility Arbitrage', capital)
        self.vol_window = vol_window
        self.vol_z_threshold = vol_z_threshold
        self.min_vol_spread = min_vol_spread
        self.max_position_size = max_position_size
        self.rebalance_threshold = rebalance_threshold
        
        # 历史数据缓存
        self.price_history = {}
        self.volatility_history = {}
        self.iv_history = {}
        
    def update_history(self, market_data: Dict):
        """更新历史数据"""
        timestamp = market_data['timestamp']
        
        # 更新价格历史
        for symbol, data in market_data['prices'].items():
            if symbol not in self.price_history:
                self.price_history[symbol] = []
            self.price_history[symbol].append((timestamp, data['price']))
            
        # 更新波动率历史
        for symbol, prices in self.price_history.items():
            if len(prices) >= self.vol_window:
                price_array = np.array([p[1] for p in prices[-self.vol_window:]])
                hist_vol = VolatilityUtils.calculate_historical_vol(price_array)
                if symbol not in self.volatility_history:
                    self.volatility_history[symbol] = []
                self.volatility_history[symbol].append((timestamp, hist_vol[-1]))
                
        # 更新隐含波动率历史
        for symbol, data in market_data['options'].items():
            if symbol not in self.iv_history:
                self.iv_history[symbol] = []
            self.iv_history[symbol].append((timestamp, data['implied_volatility']))
            
    def calculate_vol_zscore(self, symbol: str) -> Optional[float]:
        """计算波动率z-score"""
        if symbol not in self.volatility_history or len(self.volatility_history[symbol]) < self.vol_window:
            return None
            
        recent_vols = [v[1] for v in self.volatility_history[symbol][-self.vol_window:]]
        mean_vol = np.mean(recent_vols)
        std_vol = np.std(recent_vols)
        
        if std_vol == 0:
            return None
            
        current_vol = recent_vols[-1]
        return (current_vol - mean_vol) / std_vol
        
    def find_vol_opportunities(self, market_data: Dict) -> List[Dict]:
        """寻找波动率交易机会"""
        opportunities = []
        
        for symbol, option_data in market_data['options'].items():
            # 计算历史波动率z-score
            vol_zscore = self.calculate_vol_zscore(symbol)
            if vol_zscore is None:
                continue
                
            # 获取期权数据
            current_price = market_data['prices'][symbol]['price']
            implied_vol = option_data['implied_volatility']
            hist_vol = self.volatility_history[symbol][-1][1]
            
            # 计算波动率价差
            vol_spread = implied_vol - hist_vol
            
            if abs(vol_zscore) > self.vol_z_threshold and abs(vol_spread) > self.min_vol_spread:
                opportunities.append({
                    'symbol': symbol,
                    'current_price': current_price,
                    'implied_vol': implied_vol,
                    'historical_vol': hist_vol,
                    'vol_spread': vol_spread,
                    'vol_zscore': vol_zscore,
                    'options': option_data['options']
                })
                
        return opportunities
        
    def generate_signals(self, market_data: Dict) -> List[Dict]:
        """生成交易信号"""
        # 1. 更新历史数据
        self.update_history(market_data)
        
        # 2. 寻找交易机会
        opportunities = self.find_vol_opportunities(market_data)
        
        # 3. 生成交易信号
        signals = []
        for opportunity in opportunities:
            # 根据波动率价差方向确定交易方向
            if opportunity['vol_spread'] > 0:  # 隐含波动率高于历史波动率
                signal_type = 'short_vol'  # 做空波动率
            else:
                signal_type = 'long_vol'  # 做多波动率
                
            # 选择合适的期权组合
            selected_options = []
            for option in opportunity['options']:
                if signal_type == 'short_vol':
                    # 做空波动率时，选择平值期权
                    if OptionUtils.is_atm(
                        option['type'],
                        opportunity['current_price'],
                        option['strike'],
                        delta=0.1 * opportunity['current_price']
                    ):
                        selected_options.append(option)
                else:
                    # 做多波动率时，选择虚值期权
                    if OptionUtils.is_otm(
                        option['type'],
                        opportunity['current_price'],
                        option['strike']
                    ):
                        selected_options.append(option)
                        
            if selected_options:
                signals.append({
                    'type': 'volatility',
                    'direction': signal_type,
                    'symbol': opportunity['symbol'],
                    'vol_spread': opportunity['vol_spread'],
                    'options': selected_options
                })
                
        return signals
        
    def on_market_data(self, market_data: Dict):
        """处理市场数据"""
        # 1. 检查是否需要再平衡
        portfolio_greeks = self.get_portfolio_greeks()
        if abs(portfolio_greeks['delta']) > self.rebalance_threshold:
            self.rebalance(market_data)
            
        # 2. 检查期权到期
        current_time = market_data['timestamp']
        for position in self.positions:
            if position.get_type() in ['call', 'put']:
                expiry = position.get_expiry()
                if expiry and expiry <= current_time:
                    # 平仓即将到期的期权
                    self.remove_position(self.positions.index(position))
                    
    def on_trade_data(self, trade_data: Dict):
        """处理成交数据"""
        self.record_trade(trade_data)
