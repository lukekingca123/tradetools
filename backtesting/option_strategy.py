"""
期权策略回测框架
"""
from typing import Dict, List, Optional, Union, Tuple
import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass

from ..pricing.stochastic_vol import (
    HestonModel, HestonParameters,
    SABRModel, SABRParameters,
    GARCHModel
)

@dataclass
class OptionPosition:
    """期权持仓"""
    strike: float
    expiry: datetime
    is_call: bool
    quantity: int  # 正数为多头，负数为空头
    entry_price: float
    entry_date: datetime
    
class OptionData(bt.feeds.PandasData):
    """期权数据源"""
    
    lines = ('strike', 'expiry', 'is_call', 'impl_vol',)
    params = (
        ('strike', -1),
        ('expiry', -1),
        ('is_call', -1),
        ('impl_vol', -1),
    )

class BaseOptionStrategy(bt.Strategy):
    """期权策略基类"""
    
    params = (
        ('risk_free_rate', 0.03),  # 无风险利率
        ('max_positions', 10),      # 最大持仓数量
        ('max_risk_pct', 0.02),     # 单个持仓最大风险
    )
    
    def __init__(self):
        self.options = {}  # 期权持仓字典
        self.heston_model = None  # Heston模型
        self.garch_model = None   # GARCH模型
        self.vol_forecast = None  # 波动率预测
        
        # 记录Greeks
        self.total_delta = 0
        self.total_gamma = 0
        self.total_theta = 0
        self.total_vega = 0
        
    def next(self):
        """每个bar调用一次"""
        # 更新模型
        self.update_models()
        
        # 更新Greeks
        self.update_greeks()
        
        # 检查期权到期
        self.check_expirations()
        
        # 执行策略逻辑
        self.strategy_logic()
        
    def update_models(self):
        """更新定价模型"""
        # 获取历史数据
        hist_data = pd.Series([d for d in self.data.close.get(size=252)])
        returns = np.log(hist_data / hist_data.shift(1)).dropna()
        
        # 更新GARCH模型
        if len(returns) >= 30:  # 至少需要30个数据点
            if self.garch_model is None:
                self.garch_model = GARCHModel(0.1, 0.1, 0.8)
            try:
                self.garch_model.fit(returns.values)
                self.vol_forecast = np.sqrt(self.garch_model.forecast_variance(
                    current_var=returns.var(),
                    last_return=returns.iloc[-1]
                ) * 252)  # 年化
            except:
                self.vol_forecast = returns.std() * np.sqrt(252)
                
        # 更新Heston模型
        if self.heston_model is None:
            # 使用简单的初始参数
            self.heston_model = HestonModel(HestonParameters(
                kappa=2.0,
                theta=self.vol_forecast**2 if self.vol_forecast else 0.04,
                sigma=0.3,
                rho=-0.7,
                v0=self.vol_forecast**2 if self.vol_forecast else 0.04
            ))
            
    def update_greeks(self):
        """更新组合Greeks"""
        self.total_delta = 0
        self.total_gamma = 0
        self.total_theta = 0
        self.total_vega = 0
        
        for opt_key, position in self.options.items():
            # 计算期权剩余期限
            tau = (position.expiry - self.datetime.date()).days / 365.0
            if tau <= 0:
                continue
                
            # 使用Heston模型计算Greeks
            if self.heston_model:
                try:
                    # 计算价格和Greeks
                    price = self.heston_model.price_european(
                        S0=self.data.close[0],
                        K=position.strike,
                        T=tau,
                        r=self.p.risk_free_rate,
                        is_call=position.is_call
                    )
                    
                    # 计算数值Greeks
                    h = self.data.close[0] * 0.001  # 价格变动步长
                    t_step = 1/252  # 时间步长
                    
                    # Delta
                    price_up = self.heston_model.price_european(
                        S0=self.data.close[0] + h,
                        K=position.strike,
                        T=tau,
                        r=self.p.risk_free_rate,
                        is_call=position.is_call
                    )
                    delta = (price_up - price) / h
                    
                    # Gamma
                    price_down = self.heston_model.price_european(
                        S0=self.data.close[0] - h,
                        K=position.strike,
                        T=tau,
                        r=self.p.risk_free_rate,
                        is_call=position.is_call
                    )
                    gamma = (price_up - 2*price + price_down) / (h**2)
                    
                    # Theta
                    price_next = self.heston_model.price_european(
                        S0=self.data.close[0],
                        K=position.strike,
                        T=tau - t_step,
                        r=self.p.risk_free_rate,
                        is_call=position.is_call
                    )
                    theta = (price_next - price) / t_step
                    
                    # Vega (波动率变动1%)
                    orig_v0 = self.heston_model.params.v0
                    self.heston_model.params.v0 *= 1.01
                    price_vol_up = self.heston_model.price_european(
                        S0=self.data.close[0],
                        K=position.strike,
                        T=tau,
                        r=self.p.risk_free_rate,
                        is_call=position.is_call
                    )
                    vega = (price_vol_up - price) / (0.01)
                    self.heston_model.params.v0 = orig_v0
                    
                    # 更新总Greeks
                    qty = position.quantity
                    self.total_delta += delta * qty
                    self.total_gamma += gamma * qty
                    self.total_theta += theta * qty
                    self.total_vega += vega * qty
                    
                except:
                    pass
                    
    def check_expirations(self):
        """检查并处理到期期权"""
        for opt_key in list(self.options.keys()):
            position = self.options[opt_key]
            if position.expiry <= self.datetime.date():
                # 计算到期收益
                if position.is_call:
                    payoff = max(0, self.data.close[0] - position.strike)
                else:
                    payoff = max(0, position.strike - self.data.close[0])
                    
                # 平仓并记录收益
                pnl = (payoff - position.entry_price) * position.quantity
                self.log(f'Option expired: {opt_key}, PnL: {pnl:.2f}')
                del self.options[opt_key]
                
    def can_trade(self, cost: float) -> bool:
        """检查是否可以交易
        
        Args:
            cost: 交易成本
        """
        # 检查持仓数量
        if len(self.options) >= self.p.max_positions:
            return False
            
        # 检查风险
        if cost > self.broker.get_value() * self.p.max_risk_pct:
            return False
            
        return True
        
    def log(self, txt: str):
        """输出日志"""
        dt = self.datetime.date()
        print(f'{dt.isoformat()}: {txt}')
        
    def strategy_logic(self):
        """策略逻辑，由子类实现"""
        pass

class VolatilityStrategy(BaseOptionStrategy):
    """波动率策略"""
    
    params = (
        ('vol_entry_z', 2.0),      # 波动率入场Z分数
        ('vol_exit_z', 0.0),       # 波动率出场Z分数
        ('lookback', 20),          # 回看周期
        ('min_hold_days', 5),      # 最小持有天数
    )
    
    def __init__(self):
        super().__init__()
        self.vol_z = bt.indicators.ZScore(
            self.data.impl_vol,
            period=self.p.lookback
        )
        self.hold_days = 0
        
    def strategy_logic(self):
        """波动率策略逻辑"""
        self.hold_days += 1
        
        # 检查是否可以交易
        if not self.can_trade(self.data.close[0] * 0.01):  # 假设成本为1%
            return
            
        # 寻找适合的期权
        if self.vol_z > self.p.vol_entry_z and self.hold_days >= self.p.min_hold_days:
            # 波动率高，做空跨式策略
            self.short_straddle()
        elif self.vol_z < -self.p.vol_entry_z and self.hold_days >= self.p.min_hold_days:
            # 波动率低，做多跨式策略
            self.long_straddle()
        elif abs(self.vol_z) < self.p.vol_exit_z:
            # 平仓所有持仓
            self.close_all()
            
    def short_straddle(self):
        """做空跨式策略"""
        # 找到平值期权
        atm_strike = round(self.data.close[0] / 5) * 5  # 四舍五入到最近的5
        
        # 构建期权组合
        expiry = self.datetime.date() + timedelta(days=30)  # 30天后到期
        
        # 做空看涨和看跌期权
        opt_key = f'C_{atm_strike}_{expiry}'
        if opt_key not in self.options:
            self.options[opt_key] = OptionPosition(
                strike=atm_strike,
                expiry=expiry,
                is_call=True,
                quantity=-1,
                entry_price=self.data.close[0],
                entry_date=self.datetime.date()
            )
            
        opt_key = f'P_{atm_strike}_{expiry}'
        if opt_key not in self.options:
            self.options[opt_key] = OptionPosition(
                strike=atm_strike,
                expiry=expiry,
                is_call=False,
                quantity=-1,
                entry_price=self.data.close[0],
                entry_date=self.datetime.date()
            )
            
    def long_straddle(self):
        """做多跨式策略"""
        # 找到平值期权
        atm_strike = round(self.data.close[0] / 5) * 5
        
        # 构建期权组合
        expiry = self.datetime.date() + timedelta(days=30)
        
        # 做多看涨和看跌期权
        opt_key = f'C_{atm_strike}_{expiry}'
        if opt_key not in self.options:
            self.options[opt_key] = OptionPosition(
                strike=atm_strike,
                expiry=expiry,
                is_call=True,
                quantity=1,
                entry_price=self.data.close[0],
                entry_date=self.datetime.date()
            )
            
        opt_key = f'P_{atm_strike}_{expiry}'
        if opt_key not in self.options:
            self.options[opt_key] = OptionPosition(
                strike=atm_strike,
                expiry=expiry,
                is_call=False,
                quantity=1,
                entry_price=self.data.close[0],
                entry_date=self.datetime.date()
            )
            
    def close_all(self):
        """平仓所有持仓"""
        self.options.clear()
        self.hold_days = 0
