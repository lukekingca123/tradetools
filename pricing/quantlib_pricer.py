"""
使用QuantLib进行期权定价和Greeks计算
"""
import QuantLib as ql
from typing import Dict, Optional
from dataclasses import dataclass
from datetime import datetime, date

@dataclass
class OptionParams:
    """期权参数"""
    spot: float           # 标的价格
    strike: float        # 行权价
    maturity_date: date  # 到期日
    risk_free_rate: float    # 无风险利率
    dividend_rate: float     # 股息率
    volatility: float        # 波动率
    evaluation_date: Optional[date] = None  # 估值日期，默认为今天
    
class QuantLibPricer:
    """QuantLib期权定价器"""
    
    def __init__(self, params: OptionParams):
        self.params = params
        self._setup_environment()
        
    def _setup_environment(self):
        """设置QuantLib环境"""
        # 设置估值日期
        if self.params.evaluation_date is None:
            self.params.evaluation_date = date.today()
            
        ql_eval_date = ql.Date(
            self.params.evaluation_date.day,
            self.params.evaluation_date.month,
            self.params.evaluation_date.year
        )
        ql.Settings.instance().evaluationDate = ql_eval_date
        
        # 创建日历
        self.calendar = ql.UnitedStates()
        
        # 创建到期日
        self.maturity = ql.Date(
            self.params.maturity_date.day,
            self.params.maturity_date.month,
            self.params.maturity_date.year
        )
        
        # 创建日计算基准
        self.day_counter = ql.Actual365Fixed()
        
        # 计算期权期限
        self.time_to_maturity = self.day_counter.yearFraction(
            ql_eval_date, self.maturity
        )
        
        # 创建利率曲线
        self.risk_free_ts = ql.YieldTermStructureHandle(
            ql.FlatForward(
                0, self.calendar, 
                self.params.risk_free_rate, 
                self.day_counter
            )
        )
        
        # 创建股息率曲线
        self.dividend_ts = ql.YieldTermStructureHandle(
            ql.FlatForward(
                0, self.calendar, 
                self.params.dividend_rate, 
                self.day_counter
            )
        )
        
        # 创建波动率曲线
        self.volatility = ql.BlackVolTermStructureHandle(
            ql.BlackConstantVol(
                0, self.calendar, 
                self.params.volatility, 
                self.day_counter
            )
        )
        
        # 创建标的资产价格
        self.spot_handle = ql.QuoteHandle(
            ql.SimpleQuote(self.params.spot)
        )
        
        # 创建Black-Scholes-Merton过程
        self.bsm_process = ql.BlackScholesMertonProcess(
            self.spot_handle,
            self.dividend_ts,
            self.risk_free_ts,
            self.volatility
        )
        
    def price_european(self, is_call: bool = True) -> Dict[str, float]:
        """计算欧式期权价格和Greeks
        
        Args:
            is_call: True为看涨期权，False为看跌期权
            
        Returns:
            包含价格和Greeks的字典
        """
        # 创建期权
        payoff = ql.PlainVanillaPayoff(
            ql.Option.Call if is_call else ql.Option.Put,
            self.params.strike
        )
        
        exercise = ql.EuropeanExercise(self.maturity)
        
        option = ql.VanillaOption(payoff, exercise)
        
        # 设置定价引擎
        engine = ql.AnalyticEuropeanEngine(self.bsm_process)
        option.setPricingEngine(engine)
        
        # 计算价格和Greeks
        try:
            results = {
                "price": option.NPV(),
                "delta": option.delta(),
                "gamma": option.gamma(),
                "theta": option.theta(),
                "vega": option.vega(),
                "rho": option.rho()
            }
        except RuntimeError as e:
            print(f"QuantLib计算错误: {str(e)}")
            results = {
                "price": None,
                "delta": None,
                "gamma": None,
                "theta": None,
                "vega": None,
                "rho": None
            }
            
        return results
    
    def price_american(self, is_call: bool = True, 
                      time_steps: int = 100) -> Dict[str, float]:
        """计算美式期权价格和Greeks
        
        Args:
            is_call: True为看涨期权，False为看跌期权
            time_steps: 二叉树时间步数
            
        Returns:
            包含价格和Greeks的字典
        """
        # 创建期权
        payoff = ql.PlainVanillaPayoff(
            ql.Option.Call if is_call else ql.Option.Put,
            self.params.strike
        )
        
        exercise = ql.AmericanExercise(
            ql.Settings.instance().evaluationDate,
            self.maturity
        )
        
        option = ql.VanillaOption(payoff, exercise)
        
        # 设置定价引擎（使用二叉树）
        engine = ql.BinomialVanillaEngine(
            self.bsm_process, 
            "crr",  # Cox-Ross-Rubinstein二叉树
            time_steps
        )
        option.setPricingEngine(engine)
        
        # 计算价格和Greeks
        try:
            results = {
                "price": option.NPV(),
                "delta": option.delta(),
                "gamma": option.gamma(),
                "theta": option.theta()
            }
        except RuntimeError as e:
            print(f"QuantLib计算错误: {str(e)}")
            results = {
                "price": None,
                "delta": None,
                "gamma": None,
                "theta": None
            }
            
        return results
