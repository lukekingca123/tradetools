"""
美式期权定价模块
使用最小二乘蒙特卡洛(LSM)方法
参考：Derivatives Analytics with Python (Yves Hilpisch)
"""
import numpy as np
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class AmericanOptionParams:
    """美式期权参数"""
    S0: float  # 标的价格
    K: float   # 行权价
    T: float   # 到期时间
    r: float   # 无风险利率
    sigma: float  # 波动率
    div: float = 0.0  # 分红率
    is_call: bool = False  # 是否为看涨期权

class AmericanOptionPricer:
    """美式期权定价器"""
    
    def __init__(
        self,
        num_steps: int = 50,
        num_paths: int = 50000,
        num_basis: int = 10
    ):
        """
        初始化定价器
        
        参数:
            num_steps: int, 时间步数
            num_paths: int, 模拟路径数
            num_basis: int, 基函数数量
        """
        self.num_steps = num_steps
        self.num_paths = num_paths
        self.num_basis = num_basis
        
    def simulate_paths(self, params: AmericanOptionParams) -> Dict[str, np.ndarray]:
        """
        模拟路径
        
        参数:
            params: AmericanOptionParams, 期权参数
            
        返回:
            Dict[str, np.ndarray], 包含价格、利率和波动率路径
        """
        dt = params.T / self.num_steps
        
        # 生成随机数
        rn = np.random.standard_normal((self.num_steps + 1, self.num_paths))
        
        # 模拟价格路径
        S = np.zeros((self.num_steps + 1, self.num_paths))
        S[0] = params.S0
        
        # 使用对数正态过程模拟价格
        for t in range(1, self.num_steps + 1):
            S[t] = S[t-1] * np.exp((params.r - params.div - 0.5 * params.sigma ** 2) * dt + 
                                 params.sigma * np.sqrt(dt) * rn[t])
            
        return {
            'price': S,
            'rate': np.full_like(S, params.r),
            'vol': np.full_like(S, params.sigma)
        }
        
    def price(self, params: AmericanOptionParams) -> Dict[str, float]:
        """
        定价美式期权
        
        参数:
            params: AmericanOptionParams, 期权参数
            
        返回:
            Dict[str, float], 包含期权价格和希腊字母
        """
        paths = self.simulate_paths(params)
        S = paths['price']
        r = paths['rate']
        v = paths['vol']
        
        dt = params.T / self.num_steps
        
        # 计算内在价值
        if params.is_call:
            h = np.maximum(S - params.K, 0)
        else:
            h = np.maximum(params.K - S, 0)
            
        # 价值矩阵
        V = h.copy()
        
        # 从到期日向前回溯
        for t in range(self.num_steps - 1, 0, -1):
            df = np.exp(-r[t] * dt)  # 折现因子
            
            # 选择价内期权
            if params.is_call:
                itm = S[t] > params.K
            else:
                itm = S[t] < params.K
                
            itm_indices = np.nonzero(itm)[0]
            
            if len(itm_indices) > 0:
                # 提取价内路径
                rel_S = S[t, itm]
                rel_V = V[t + 1, itm] * df[itm]
                
                # 构建回归矩阵
                matrix = np.zeros((self.num_basis + 1, len(itm_indices)))
                for i in range(self.num_basis + 1):
                    matrix[i] = rel_S ** i
                    
                # 最小二乘回归
                reg = np.linalg.lstsq(matrix.T, rel_V, rcond=None)[0]
                continuation_value = np.dot(reg, matrix)
                
                # 更新价值矩阵
                exercise = h[t, itm]
                V[t, itm] = np.where(exercise > continuation_value,
                                   exercise,
                                   V[t + 1, itm] * df[itm])
                
        # 计算t=0时刻的期权价值
        option_value = max(np.mean(V[1] * np.exp(-r[0, 0] * dt)), h[0, 0])
        
        # 计算希腊字母
        delta = self.compute_delta(params)
        gamma = self.compute_gamma(params)
        theta = self.compute_theta(params)
        vega = self.compute_vega(params)
        
        return {
            'price': option_value,
            'delta': delta,
            'gamma': gamma,
            'theta': theta,
            'vega': vega
        }
        
    def compute_delta(self, params: AmericanOptionParams, eps: float = 0.01) -> float:
        """计算Delta"""
        params_up = AmericanOptionParams(**params.__dict__)
        params_up.S0 *= (1 + eps)
        params_down = AmericanOptionParams(**params.__dict__)
        params_down.S0 *= (1 - eps)
        
        price_up = self.price(params_up)['price']
        price_down = self.price(params_down)['price']
        
        return (price_up - price_down) / (2 * eps * params.S0)
        
    def compute_gamma(self, params: AmericanOptionParams, eps: float = 0.01) -> float:
        """计算Gamma"""
        params_up = AmericanOptionParams(**params.__dict__)
        params_up.S0 *= (1 + eps)
        params_down = AmericanOptionParams(**params.__dict__)
        params_down.S0 *= (1 - eps)
        
        delta_up = self.compute_delta(params_up)
        delta_down = self.compute_delta(params_down)
        
        return (delta_up - delta_down) / (2 * eps * params.S0)
        
    def compute_theta(self, params: AmericanOptionParams, eps: float = 1/365) -> float:
        """计算Theta"""
        params_down = AmericanOptionParams(**params.__dict__)
        params_down.T -= eps
        
        price = self.price(params)['price']
        price_down = self.price(params_down)['price']
        
        return -(price - price_down) / eps
        
    def compute_vega(self, params: AmericanOptionParams, eps: float = 0.001) -> float:
        """计算Vega"""
        params_up = AmericanOptionParams(**params.__dict__)
        params_up.sigma += eps
        params_down = AmericanOptionParams(**params.__dict__)
        params_down.sigma -= eps
        
        price_up = self.price(params_up)['price']
        price_down = self.price(params_down)['price']
        
        return (price_up - price_down) / (2 * eps)
