"""
美股期权定价模块

特点：
1. 考虑美股期权的特殊性质（如周期期权）
2. 使用实际交易日历
3. 处理除息影响
4. 支持多种定价模型
"""
import QuantLib as ql
from typing import Dict, Optional, Union, List
from dataclasses import dataclass
from datetime import datetime, date
import pandas as pd
import numpy as np

@dataclass
class USOptionParams:
    """美股期权参数"""
    symbol: str          # 股票代码
    spot: float         # 现价
    strike: float       # 行权价
    maturity_date: date # 到期日
    option_type: str    # CALL或PUT
    exercise_type: str  # AMERICAN或EUROPEAN
    evaluation_date: Optional[date] = None  # 估值日期
    risk_free_rate: Optional[float] = None  # 无风险利率，如果为None则自动获取美国国债利率
    dividend_schedule: Optional[List[Dict[str, Union[date, float]]]] = None  # 除息计划
    volatility: Optional[float] = None  # 波动率，如果为None则使用历史波动率
    
class USOptionPricer:
    """美股期权定价器"""
    
    def __init__(self, params: USOptionParams):
        self.params = params
        self._setup_environment()
        
    def _setup_environment(self):
        """设置定价环境"""
        # 设置估值日期
        if self.params.evaluation_date is None:
            self.params.evaluation_date = date.today()
            
        self.eval_date = ql.Date(
            self.params.evaluation_date.day,
            self.params.evaluation_date.month,
            self.params.evaluation_date.year
        )
        ql.Settings.instance().evaluationDate = self.eval_date
        
        # 使用美国交易日历（包含NYSE和NASDAQ）
        self.calendar = ql.UnitedStates(ql.UnitedStates.NYSE)
        
        # 设置到期日
        self.maturity = ql.Date(
            self.params.maturity_date.day,
            self.params.maturity_date.month,
            self.params.maturity_date.year
        )
        
        # 使用实际/365日计算基准（美股标准）
        self.day_counter = ql.Actual365Fixed()
        
        # 设置无风险利率
        self.risk_free_rate = (
            self.params.risk_free_rate if self.params.risk_free_rate is not None
            else self._get_us_treasury_rate()
        )
        
        # 设置波动率
        self.volatility = (
            self.params.volatility if self.params.volatility is not None
            else self._calculate_historical_volatility()
        )
        
        # 设置定价所需的曲线和过程
        self._setup_curves()
        
    def _get_us_treasury_rate(self) -> float:
        """获取对应期限的美国国债利率
        TODO: 实现实际的国债利率获取逻辑
        """
        return 0.05  # 临时返回固定值
        
    def _calculate_historical_volatility(self) -> float:
        """计算历史波动率
        TODO: 实现实际的历史波动率计算逻辑
        """
        return 0.3  # 临时返回固定值
        
    def _setup_curves(self):
        """设置利率曲线、股息曲线和波动率曲线"""
        # 创建无风险利率曲线
        self.risk_free_ts = ql.YieldTermStructureHandle(
            ql.FlatForward(
                0, self.calendar,
                self.risk_free_rate,
                self.day_counter
            )
        )
        
        # 创建股息曲线
        if self.params.dividend_schedule:
            # 如果有具体的除息计划，创建离散股息曲线
            dividends = []
            for div in self.params.dividend_schedule:
                div_date = ql.Date(
                    div['date'].day,
                    div['date'].month,
                    div['date'].year
                )
                dividends.append(
                    ql.FixedDividend(div['amount'], div_date)
                )
            
            self.dividend_ts = ql.YieldTermStructureHandle(
                ql.DividendSchedule(dividends)
            )
        else:
            # 否则假设连续股息率为0
            self.dividend_ts = ql.YieldTermStructureHandle(
                ql.FlatForward(0, self.calendar, 0.0, self.day_counter)
            )
        
        # 创建波动率曲线
        self.volatility_ts = ql.BlackVolTermStructureHandle(
            ql.BlackConstantVol(
                0, self.calendar,
                self.volatility,
                self.day_counter
            )
        )
        
        # 创建标的资产价格
        self.spot_handle = ql.QuoteHandle(
            ql.SimpleQuote(self.params.spot)
        )
        
        # 创建BSM过程
        self.bsm_process = ql.BlackScholesMertonProcess(
            self.spot_handle,
            self.dividend_ts,
            self.risk_free_ts,
            self.volatility_ts
        )
        
    def price(self) -> Dict[str, float]:
        """计算期权价格和Greeks"""
        is_call = self.params.option_type.upper() == 'CALL'
        is_american = self.params.exercise_type.upper() == 'AMERICAN'
        
        # 创建期权收益结构
        payoff = ql.PlainVanillaPayoff(
            ql.Option.Call if is_call else ql.Option.Put,
            self.params.strike
        )
        
        # 创建行权类型
        if is_american:
            exercise = ql.AmericanExercise(self.eval_date, self.maturity)
        else:
            exercise = ql.EuropeanExercise(self.maturity)
        
        # 创建期权对象
        option = ql.VanillaOption(payoff, exercise)
        
        # 设置定价引擎
        if is_american:
            # 美式期权使用有限差分法
            time_steps = 200
            grid_points = 200
            engine = ql.FDAmericanEngine(
                self.bsm_process,
                time_steps,
                grid_points
            )
        else:
            # 欧式期权使用解析解
            engine = ql.AnalyticEuropeanEngine(self.bsm_process)
            
        option.setPricingEngine(engine)
        
        # 计算价格和Greeks
        try:
            results = {
                "price": option.NPV(),
                "delta": option.delta(),
                "gamma": option.gamma(),
                "theta": option.theta() / 365.0,  # 转换为每日theta
                "vega": option.vega() / 100.0,   # 转换为对应1%波动率变化
                "rho": option.rho() / 100.0,     # 转换为对应1%利率变化
            }
            
            # 添加隐含波动率
            try:
                results["implied_vol"] = self.calculate_implied_vol(
                    results["price"],
                    is_call
                )
            except:
                results["implied_vol"] = None
                
        except RuntimeError as e:
            print(f"定价错误: {str(e)}")
            results = {
                "price": None,
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
                "rho": None,
                "implied_vol": None
            }
            
        return results
    
    def calculate_implied_vol(self, 
                            market_price: float, 
                            is_call: bool) -> float:
        """计算隐含波动率"""
        try:
            vol = ql.blackFormulaImpliedStdDev(
                ql.Option.Call if is_call else ql.Option.Put,
                self.params.strike,
                self.params.spot,
                market_price,
                1.0,  # 贴现因子
                1e-7,  # 精度
                100,   # 最大迭代次数
                1e-7,  # 最小波动率
                4.0    # 最大波动率
            ) / np.sqrt(self.day_counter.yearFraction(
                self.eval_date, 
                self.maturity
            ))
            return vol
        except:
            return None
            
    def price_binary(self) -> Dict[str, float]:
        """计算二元期权价格（用于计算概率）"""
        is_call = self.params.option_type.upper() == 'CALL'
        
        # 创建二元期权收益结构
        payoff = ql.CashOrNothingPayoff(
            ql.Option.Call if is_call else ql.Option.Put,
            self.params.strike,
            1.0  # 支付金额为1
        )
        
        exercise = ql.EuropeanExercise(self.maturity)
        option = ql.VanillaOption(payoff, exercise)
        
        engine = ql.AnalyticEuropeanEngine(self.bsm_process)
        option.setPricingEngine(engine)
        
        try:
            # 二元期权价格即为概率
            probability = option.NPV() * np.exp(
                self.risk_free_rate * 
                self.day_counter.yearFraction(self.eval_date, self.maturity)
            )
            return {"probability": probability}
        except:
            return {"probability": None}
