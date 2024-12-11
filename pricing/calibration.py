"""
模型校准和回测工具
"""
from typing import Dict, List, Optional, Tuple, Callable
import numpy as np
from scipy.optimize import minimize
from dataclasses import dataclass
from datetime import datetime
import pandas as pd

from .stochastic_vol import (
    HestonModel, HestonParameters,
    SABRModel, SABRParameters,
    GARCHModel
)

@dataclass
class MarketOption:
    """市场期权数据"""
    strike: float
    expiry: datetime
    price: float
    implied_vol: float
    is_call: bool = True

class HestonCalibrator:
    """Heston模型校准器"""
    
    def __init__(self, spot: float, r: float, options: List[MarketOption]):
        self.spot = spot
        self.r = r
        self.options = options
        
    def objective(self, params: np.ndarray) -> float:
        """目标函数：最小化模型价格和市场价格的差异"""
        kappa, theta, sigma, rho, v0 = params
        
        # 创建Heston模型
        model = HestonModel(HestonParameters(
            kappa=kappa,
            theta=theta,
            sigma=sigma,
            rho=rho,
            v0=v0
        ))
        
        # 计算所有期权的价格差异
        total_error = 0
        for opt in self.options:
            T = (opt.expiry - datetime.now()).days / 365.0
            model_price = model.price_european(
                S0=self.spot,
                K=opt.strike,
                T=T,
                r=self.r,
                is_call=opt.is_call
            )
            # 使用相对误差
            error = ((model_price - opt.price) / opt.price) ** 2
            total_error += error
            
        return total_error
        
    def calibrate(self, 
                 init_guess: Optional[Tuple[float, float, float, float, float]] = None
                 ) -> HestonParameters:
        """校准Heston模型参数"""
        if init_guess is None:
            # 使用常见的初始值
            init_guess = (2.0, 0.04, 0.3, -0.7, 0.04)
            
        # 参数约束
        bounds = (
            (0.1, 10.0),    # kappa
            (0.01, 0.25),   # theta
            (0.01, 0.8),    # sigma
            (-0.99, 0.99),  # rho
            (0.01, 0.25)    # v0
        )
        
        result = minimize(
            self.objective,
            init_guess,
            method='L-BFGS-B',
            bounds=bounds
        )
        
        if not result.success:
            raise RuntimeError("Heston calibration failed")
            
        return HestonParameters(*result.x)

class SABRCalibrator:
    """SABR模型校准器"""
    
    def __init__(self, forward: float, options: List[MarketOption]):
        self.forward = forward
        self.options = options
        
    def objective(self, params: np.ndarray) -> float:
        """目标函数：最小化模型隐含波动率和市场隐含波动率的差异"""
        alpha, beta, rho, nu = params
        
        # 创建SABR模型
        model = SABRModel(SABRParameters(
            alpha=alpha,
            beta=beta,
            rho=rho,
            nu=nu
        ))
        
        # 计算所有期权的波动率差异
        total_error = 0
        for opt in self.options:
            T = (opt.expiry - datetime.now()).days / 365.0
            model_vol = model.implied_vol(
                F=self.forward,
                K=opt.strike,
                T=T
            )
            # 使用相对误差
            error = ((model_vol - opt.implied_vol) / opt.implied_vol) ** 2
            total_error += error
            
        return total_error
        
    def calibrate(self, 
                 init_guess: Optional[Tuple[float, float, float, float]] = None
                 ) -> SABRParameters:
        """校准SABR模型参数"""
        if init_guess is None:
            # 使用常见的初始值
            init_guess = (0.2, 0.5, -0.3, 0.4)
            
        # 参数约束
        bounds = (
            (0.01, 1.0),    # alpha
            (0.01, 1.0),    # beta
            (-0.99, 0.99),  # rho
            (0.01, 1.0)     # nu
        )
        
        result = minimize(
            self.objective,
            init_guess,
            method='L-BFGS-B',
            bounds=bounds
        )
        
        if not result.success:
            raise RuntimeError("SABR calibration failed")
            
        return SABRParameters(*result.x)

class ModelBacktester:
    """模型回测器"""
    
    def __init__(self, 
                 price_history: pd.Series,
                 option_history: pd.DataFrame):
        """
        Args:
            price_history: 标的价格历史
            option_history: 期权价格历史，包含列：
                - date: 日期
                - strike: 行权价
                - expiry: 到期日
                - price: 期权价格
                - implied_vol: 隐含波动率
                - is_call: 是否为看涨期权
        """
        self.price_history = price_history
        self.option_history = option_history
        
    def backtest_heston(self, 
                       window: int = 30,
                       r: float = 0.03) -> pd.DataFrame:
        """回测Heston模型
        
        Args:
            window: 校准窗口大小
            r: 无风险利率
        """
        results = []
        dates = sorted(self.option_history['date'].unique())
        
        for i in range(window, len(dates)):
            # 获取校准窗口的数据
            calib_date = dates[i]
            start_date = dates[i-window]
            
            # 准备校准数据
            calib_options = []
            for _, row in self.option_history[
                self.option_history['date'] == calib_date].iterrows():
                calib_options.append(MarketOption(
                    strike=row['strike'],
                    expiry=row['expiry'],
                    price=row['price'],
                    implied_vol=row['implied_vol'],
                    is_call=row['is_call']
                ))
                
            # 校准模型
            spot = self.price_history[calib_date]
            calibrator = HestonCalibrator(spot, r, calib_options)
            try:
                params = calibrator.calibrate()
                
                # 记录结果
                results.append({
                    'date': calib_date,
                    'kappa': params.kappa,
                    'theta': params.theta,
                    'sigma': params.sigma,
                    'rho': params.rho,
                    'v0': params.v0
                })
            except:
                print(f"Failed to calibrate for date {calib_date}")
                
        return pd.DataFrame(results)
        
    def backtest_garch(self, 
                      window: int = 252) -> pd.DataFrame:
        """回测GARCH模型
        
        Args:
            window: 校准窗口大小
        """
        results = []
        
        # 计算收益率
        returns = np.log(self.price_history / self.price_history.shift(1))
        
        for i in range(window, len(returns)):
            # 获取校准窗口的数据
            train_returns = returns.iloc[i-window:i]
            
            # 拟合模型
            model = GARCHModel(0.1, 0.1, 0.8)  # 初始值
            try:
                model.fit(train_returns.values)
                
                # 记录结果
                results.append({
                    'date': returns.index[i],
                    'omega': model.omega,
                    'alpha': model.alpha,
                    'beta': model.beta,
                    'forecast_vol': np.sqrt(model.forecast_variance(
                        current_var=train_returns.var(),
                        last_return=train_returns.iloc[-1]
                    ) * 252)  # 年化
                })
            except:
                print(f"Failed to fit GARCH for date {returns.index[i]}")
                
        return pd.DataFrame(results)
