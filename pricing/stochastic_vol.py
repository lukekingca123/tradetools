"""
随机波动率模型模块，包括Heston, SABR等
"""
from typing import Dict, List, Optional, Tuple, Callable
import numpy as np
from scipy.integrate import quad
from scipy.optimize import minimize
from dataclasses import dataclass
import warnings

@dataclass
class HestonParameters:
    """Heston模型参数"""
    kappa: float  # 均值回归速度
    theta: float  # 长期波动率水平
    sigma: float  # vol of vol
    rho: float    # 相关系数
    v0: float     # 初始波动率
    
    def validate(self):
        """验证参数是否满足Feller条件"""
        if 2 * self.kappa * self.theta <= self.sigma ** 2:
            warnings.warn("Parameters don't satisfy Feller condition")
            
class HestonModel:
    """Heston随机波动率模型"""
    
    def __init__(self, params: HestonParameters):
        self.params = params
        self.params.validate()
        
    def characteristic_function(self, u: complex, tau: float, 
                              S0: float, r: float) -> complex:
        """特征函数
        
        Args:
            u: 傅里叶变换变量
            tau: 到期时间
            S0: 当前价格
            r: 无风险利率
        """
        kappa = self.params.kappa
        theta = self.params.theta
        sigma = self.params.sigma
        rho = self.params.rho
        v0 = self.params.v0
        
        # 计算d
        d = np.sqrt((kappa - 1j*rho*sigma*u)**2 + 
                    sigma**2*(1j*u + u**2))
        
        # 计算g
        g = (kappa - 1j*rho*sigma*u - d)/(kappa - 1j*rho*sigma*u + d)
        
        # 计算特征函数的各项
        C = (r*u*1j*tau + 
             kappa*theta/(sigma**2)*((kappa - 1j*rho*sigma*u - d)*tau - 
                                    2*np.log((1-g*np.exp(-d*tau))/(1-g))))
        
        D = ((kappa - 1j*rho*sigma*u - d)/sigma**2*
             ((1-np.exp(-d*tau))/(1-g*np.exp(-d*tau))))
        
        # 返回特征函数值
        return np.exp(C + D*v0 + 1j*u*np.log(S0))
    
    def price_european(self, S0: float, K: float, T: float, r: float, 
                      is_call: bool = True, N: int = 100) -> float:
        """使用特征函数方法定价欧式期权
        
        Args:
            S0: 当前价格
            K: 行权价
            T: 到期时间
            r: 无风险利率
            is_call: 是否为看涨期权
            N: 积分点数量
        """
        # 积分上限
        a = 0
        b = np.inf
        
        # 被积函数
        def integrand(u: float) -> float:
            if is_call:
                phi = self.characteristic_function(u - 0.5j, T, S0, r)
            else:
                phi = self.characteristic_function(u + 0.5j, T, S0, r)
            return float(np.real(np.exp(-1j * u * np.log(K)) * phi / (1j * u)))
        
        # 数值积分
        price, _ = quad(integrand, a, b, limit=N)
        
        if is_call:
            price = S0 - np.exp(-r * T) * K * price / np.pi
        else:
            price = np.exp(-r * T) * K * price / np.pi - S0
            
        return max(0, price)
        
@dataclass
class SABRParameters:
    """SABR模型参数"""
    alpha: float  # 初始波动率
    beta: float   # CEV参数
    rho: float    # 相关系数
    nu: float     # vol of vol
    
class SABRModel:
    """SABR模型实现"""
    
    def __init__(self, params: SABRParameters):
        self.params = params
        
    def implied_vol(self, F: float, K: float, T: float) -> float:
        """计算SABR隐含波动率
        
        Args:
            F: 远期价格
            K: 行权价
            T: 到期时间
        """
        alpha = self.params.alpha
        beta = self.params.beta
        rho = self.params.rho
        nu = self.params.nu
        
        # 计算z
        F_mid = (F + K) / 2
        z = (nu/alpha) * (F_mid)**(1-beta) * np.log(F/K)
        
        # 计算x(z)
        x_z = np.log((np.sqrt(1 - 2*rho*z + z**2) + z - rho)/(1 - rho))
        
        # 计算各项
        A = alpha / ((F*K)**((1-beta)/2) * (1 + (1-beta)**2/24 * 
            np.log(F/K)**2 + (1-beta)**4/1920 * np.log(F/K)**4))
            
        B = 1 + ((1-beta)**2/24 * alpha**2/((F*K)**(1-beta)) + 
                 1/4 * rho*beta*nu*alpha/((F*K)**((1-beta)/2)) + 
                 (2-3*rho**2)/24 * nu**2) * T
                 
        # 返回隐含波动率
        return A * (z/x_z) * B
        
class LocalVolModel:
    """局部波动率模型"""
    
    def __init__(self, spot_price: float):
        self.spot = spot_price
        self.vol_surface = {}  # (K, T) -> vol
        
    def add_market_vol(self, K: float, T: float, implied_vol: float):
        """添加市场隐含波动率点"""
        self.vol_surface[(K, T)] = implied_vol
        
    def dupire_local_vol(self, K: float, T: float) -> float:
        """使用Dupire公式计算局部波动率
        
        Args:
            K: 行权价
            T: 到期时间
        """
        # 获取最近的波动率点
        nearest_points = sorted(self.vol_surface.keys(), 
                              key=lambda x: abs(x[0]-K) + abs(x[1]-T))[:4]
        
        # 简单插值
        total_weight = 0
        weighted_vol = 0
        
        for point in nearest_points:
            weight = 1 / (abs(point[0]-K) + abs(point[1]-T) + 1e-6)
            total_weight += weight
            weighted_vol += weight * self.vol_surface[point]
            
        return weighted_vol / total_weight
        
class GARCHModel:
    """GARCH(1,1)模型"""
    
    def __init__(self, omega: float, alpha: float, beta: float):
        """
        Args:
            omega: 长期波动率水平
            alpha: ARCH项系数
            beta: GARCH项系数
        """
        self.omega = omega
        self.alpha = alpha
        self.beta = beta
        
        # 验证参数
        if alpha + beta >= 1:
            raise ValueError("Alpha + Beta should be less than 1 for stationarity")
            
    def forecast_variance(self, current_var: float, 
                         last_return: float, horizon: int = 1) -> float:
        """预测未来方差
        
        Args:
            current_var: 当前方差
            last_return: 最近的收益率
            horizon: 预测期数
        """
        if horizon == 1:
            return (self.omega + 
                   self.alpha * last_return**2 + 
                   self.beta * current_var)
        else:
            # 长期方差
            long_run_var = self.omega / (1 - self.alpha - self.beta)
            
            # 多期预测
            forecast = current_var
            for _ in range(horizon-1):
                forecast = (self.omega + 
                          (self.alpha + self.beta) * forecast)
                
            return forecast
            
    def fit(self, returns: np.ndarray, 
            init_guess: Optional[Tuple[float, float, float]] = None) -> None:
        """使用最大似然估计拟合模型参数
        
        Args:
            returns: 收益率序列
            init_guess: 初始参数猜测 (omega, alpha, beta)
        """
        if init_guess is None:
            init_guess = (np.var(returns) * 0.1, 0.1, 0.8)
            
        # 负对数似然函数
        def neg_loglik(params):
            omega, alpha, beta = params
            
            if omega <= 0 or alpha <= 0 or beta <= 0 or alpha + beta >= 1:
                return np.inf
                
            var = np.zeros_like(returns)
            var[0] = np.var(returns)
            
            for t in range(1, len(returns)):
                var[t] = (omega + 
                         alpha * returns[t-1]**2 + 
                         beta * var[t-1])
                
            loglik = -0.5 * np.sum(np.log(var) + returns**2/var)
            return -loglik
            
        # 优化
        result = minimize(neg_loglik, init_guess, 
                        method='L-BFGS-B',
                        bounds=((1e-6, None), (1e-6, 1), (1e-6, 1)))
                        
        if result.success:
            self.omega, self.alpha, self.beta = result.x
        else:
            raise RuntimeError("Failed to fit GARCH model")
