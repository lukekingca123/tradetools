"""
BCC97期权定价模型

参考文献：
Bakshi, G., Cao, C., & Chen, Z. (1997). 
Empirical Performance of Alternative Option Pricing Models. 
The Journal of Finance, 52(5), 2003-2049.
"""
import numpy as np
from scipy.stats import norm
from typing import Dict, Tuple, Optional
from dataclasses import dataclass

@dataclass
class BCC97Params:
    """BCC97模型参数"""
    spot: float           # 标的价格
    strike: float        # 行权价
    time_to_maturity: float  # 到期时间（年）
    risk_free_rate: float    # 无风险利率
    dividend_rate: float     # 股息率
    volatility: float        # 波动率
    jump_intensity: float    # 跳跃强度
    jump_mean: float         # 跳跃均值
    jump_volatility: float   # 跳跃波动率
    
class BCC97PricingModel:
    """BCC97期权定价模型实现"""
    
    def __init__(self, params: BCC97Params):
        self.params = params
        
    def price_and_greeks(self, is_call: bool = True) -> Dict[str, float]:
        """计算期权价格和希腊字母
        
        Args:
            is_call: True为看涨期权，False为看跌期权
            
        Returns:
            包含价格和希腊字母的字典
        """
        # 计算d1和d2
        d1, d2 = self._calculate_d1_d2()
        
        # 计算跳跃项
        jump_term = self._calculate_jump_term(is_call)
        
        # 计算BS项
        bs_term = self._calculate_bs_term(d1, d2, is_call)
        
        # 总价格
        price = bs_term + jump_term
        
        # 计算希腊字母
        greeks = self._calculate_greeks(d1, d2, is_call)
        
        return {
            "price": price,
            **greeks
        }
    
    def _calculate_d1_d2(self) -> Tuple[float, float]:
        """计算d1和d2"""
        sigma = self.params.volatility
        t = self.params.time_to_maturity
        s = self.params.spot
        k = self.params.strike
        r = self.params.risk_free_rate
        q = self.params.dividend_rate
        
        sigma_sqrt_t = sigma * np.sqrt(t)
        
        d1 = (np.log(s/k) + (r - q + sigma**2/2) * t) / sigma_sqrt_t
        d2 = d1 - sigma_sqrt_t
        
        return d1, d2
    
    def _calculate_bs_term(self, d1: float, d2: float, is_call: bool) -> float:
        """计算Black-Scholes项"""
        s = self.params.spot
        k = self.params.strike
        r = self.params.risk_free_rate
        q = self.params.dividend_rate
        t = self.params.time_to_maturity
        
        if is_call:
            return (s * np.exp(-q * t) * norm.cdf(d1) - 
                   k * np.exp(-r * t) * norm.cdf(d2))
        else:
            return (k * np.exp(-r * t) * norm.cdf(-d2) - 
                   s * np.exp(-q * t) * norm.cdf(-d1))
    
    def _calculate_jump_term(self, is_call: bool) -> float:
        """计算跳跃项"""
        lambda_ = self.params.jump_intensity
        mu_j = self.params.jump_mean
        sigma_j = self.params.jump_volatility
        t = self.params.time_to_maturity
        
        # 跳跃调整后的期权价格
        jump_price = 0.0
        max_jumps = 10  # 最大跳跃次数
        
        for n in range(1, max_jumps + 1):
            # 调整后的参数
            adjusted_t = t
            adjusted_vol = np.sqrt(
                self.params.volatility**2 + 
                n * sigma_j**2 / t
            )
            adjusted_rate = (
                self.params.risk_free_rate + 
                n * np.log(1 + mu_j) / t
            )
            
            # 使用调整后的参数计算期权价格
            temp_params = BCC97Params(
                spot=self.params.spot,
                strike=self.params.strike,
                time_to_maturity=adjusted_t,
                risk_free_rate=adjusted_rate,
                dividend_rate=self.params.dividend_rate,
                volatility=adjusted_vol,
                jump_intensity=0,  # 不再考虑跳跃
                jump_mean=0,
                jump_volatility=0
            )
            
            temp_model = BCC97PricingModel(temp_params)
            d1, d2 = temp_model._calculate_d1_d2()
            temp_price = temp_model._calculate_bs_term(d1, d2, is_call)
            
            # 泊松分布权重
            weight = np.exp(-lambda_ * t) * (lambda_ * t)**n / np.math.factorial(n)
            
            jump_price += temp_price * weight
            
        return jump_price
    
    def _calculate_greeks(self, d1: float, d2: float, is_call: bool) -> Dict[str, float]:
        """计算希腊字母"""
        s = self.params.spot
        k = self.params.strike
        r = self.params.risk_free_rate
        q = self.params.dividend_rate
        t = self.params.time_to_maturity
        sigma = self.params.volatility
        
        # Delta
        exp_qt = np.exp(-q * t)
        if is_call:
            delta = exp_qt * norm.cdf(d1)
        else:
            delta = exp_qt * (norm.cdf(d1) - 1)
            
        # Gamma
        gamma = exp_qt * norm.pdf(d1) / (s * sigma * np.sqrt(t))
        
        # Theta
        exp_rt = np.exp(-r * t)
        sqrt_t = np.sqrt(t)
        if is_call:
            theta = (-exp_qt * s * norm.pdf(d1) * sigma / (2 * sqrt_t) +
                    q * s * exp_qt * norm.cdf(d1) -
                    r * k * exp_rt * norm.cdf(d2))
        else:
            theta = (-exp_qt * s * norm.pdf(d1) * sigma / (2 * sqrt_t) -
                    q * s * exp_qt * norm.cdf(-d1) +
                    r * k * exp_rt * norm.cdf(-d2))
            
        # Vega
        vega = s * exp_qt * norm.pdf(d1) * sqrt_t
        
        # Rho
        if is_call:
            rho = k * t * exp_rt * norm.cdf(d2)
        else:
            rho = -k * t * exp_rt * norm.cdf(-d2)
            
        return {
            "delta": delta,
            "gamma": gamma,
            "theta": theta,
            "vega": vega,
            "rho": rho
        }
