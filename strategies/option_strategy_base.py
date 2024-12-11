"""
期权策略基类
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional
import numpy as np
import pandas as pd
from datetime import datetime

from ..utils.option_utils import OptionUtils
from ..utils.volatility_utils import VolatilityUtils

class Position:
    """持仓类"""
    def __init__(self, quantity: float, instrument: dict):
        """
        Args:
            quantity: 持仓数量，正数为多头，负数为空头
            instrument: 期权或股票信息字典，必须包含type字段
        """
        self._quantity = quantity
        self._instrument = instrument
        
    @property
    def quantity(self) -> float:
        return self._quantity
    
    @quantity.setter
    def quantity(self, value: float):
        self._quantity = value
        
    @property
    def instrument(self) -> dict:
        return self._instrument
    
    @instrument.setter
    def instrument(self, value: dict):
        self._instrument = value
        
    def get_type(self) -> str:
        return self._instrument['type']
    
    def get_strike(self) -> Optional[float]:
        return self._instrument.get('strike')
    
    def get_expiry(self) -> Optional[datetime]:
        return self._instrument.get('expiry')
    
    def get_greeks(self) -> Optional[Dict[str, float]]:
        """获取持仓的Greeks"""
        if 'greeks' not in self._instrument:
            return None
        return {k: v * self._quantity for k, v in self._instrument['greeks'].items()}

class OptionStrategyBase(ABC):
    """期权策略基类"""
    
    def __init__(self, name: str, capital: float = 1000000):
        """
        Args:
            name: 策略名称
            capital: 初始资金
        """
        self.name = name
        self.capital = capital
        self.positions: List[Position] = []
        self.trade_history: List[Dict] = []
        
    def add_position(self, positions: List[Tuple[float, dict]]):
        """添加持仓
        
        Args:
            positions: [(quantity, instrument), ...]
        """
        self.positions.extend([Position(*p) for p in positions])
        
    def remove_position(self, index: int):
        """移除持仓"""
        if 0 <= index < len(self.positions):
            self.positions.pop(index)
            
    def clear_positions(self):
        """清空所有持仓"""
        self.positions = []
        
    def get_portfolio_greeks(self) -> Dict[str, float]:
        """计算组合Greeks"""
        portfolio_greeks = {'delta': 0, 'gamma': 0, 'vega': 0, 'theta': 0, 'rho': 0}
        for position in self.positions:
            pos_greeks = position.get_greeks()
            if pos_greeks:
                for greek, value in pos_greeks.items():
                    portfolio_greeks[greek] += value
        return portfolio_greeks
    
    def calculate_portfolio_value(self, current_prices: Dict[str, float]) -> float:
        """计算组合市值"""
        total_value = 0
        for position in self.positions:
            instrument_id = position.instrument['instrument_id']
            if instrument_id in current_prices:
                total_value += position.quantity * current_prices[instrument_id]
        return total_value
    
    def calculate_margin_requirement(self) -> float:
        """计算保证金要求"""
        margin = 0
        for position in self.positions:
            # 根据不同类型的持仓计算保证金
            if position.get_type() == 'stock':
                margin += abs(position.quantity * position.instrument['price'])
            elif position.get_type() in ['call', 'put']:
                if position.quantity < 0:  # 空头期权需要保证金
                    margin += abs(position.quantity * position.instrument['margin_requirement'])
        return margin
    
    def record_trade(self, trade: Dict):
        """记录交易"""
        trade['timestamp'] = datetime.now()
        self.trade_history.append(trade)
        
    @abstractmethod
    def generate_signals(self, market_data: Dict) -> List[Dict]:
        """生成交易信号"""
        pass
    
    @abstractmethod
    def on_market_data(self, market_data: Dict):
        """处理市场数据"""
        pass
    
    @abstractmethod
    def on_trade_data(self, trade_data: Dict):
        """处理成交数据"""
        pass
    
    def risk_check(self) -> bool:
        """风险检查
        
        Returns:
            是否通过风险检查
        """
        # 1. 检查保证金
        margin = self.calculate_margin_requirement()
        if margin > self.capital:
            return False
            
        # 2. 检查Greeks限额
        portfolio_greeks = self.get_portfolio_greeks()
        if abs(portfolio_greeks['delta']) > 0.3:  # delta限额
            return False
        if abs(portfolio_greeks['gamma']) > 0.1:  # gamma限额
            return False
        if abs(portfolio_greeks['vega']) > 0.1:   # vega限额
            return False
            
        return True
    
    def rebalance(self, market_data: Dict):
        """组合再平衡
        
        Args:
            market_data: 市场数据
        """
        # 1. 生成交易信号
        signals = self.generate_signals(market_data)
        
        # 2. 构建目标组合
        target_positions = []
        for signal in signals:
            # 根据信号构建期权组合
            if signal['type'] == 'volatility':
                # 波动率交易信号
                options = signal['options']
                # 构建delta中性组合
                position_sizes, total_delta = OptionUtils.build_delta_neutral_portfolio(options)
                target_positions.extend(zip(position_sizes, options))
                
        # 3. 检查风险限额
        test_positions = [Position(*p) for p in target_positions]
        self.positions = test_positions
        if not self.risk_check():
            print("Risk check failed, keeping current positions")
            return
            
        # 4. 执行调仓
        self.positions = test_positions
        
    def calculate_pnl(self, current_prices: Dict[str, float]) -> Dict[str, float]:
        """计算收益
        
        Returns:
            包含total_pnl, realized_pnl, unrealized_pnl的字典
        """
        realized_pnl = sum(trade.get('realized_pnl', 0) for trade in self.trade_history)
        current_value = self.calculate_portfolio_value(current_prices)
        initial_value = sum(pos.quantity * pos.instrument['initial_price'] 
                          for pos in self.positions)
        unrealized_pnl = current_value - initial_value
        return {
            'total_pnl': realized_pnl + unrealized_pnl,
            'realized_pnl': realized_pnl,
            'unrealized_pnl': unrealized_pnl
        }
