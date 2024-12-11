"""
期权相关工具类
"""
import numpy as np
from typing import List, Tuple
import pandas as pd

class OptionUtils:
    @staticmethod
    def get_nearest_strikes(
        current_price: float,
        strikes: List[float],
        n: int = 1
    ) -> List[float]:
        """获取最接近当前价格的n个执行价"""
        sorted_strikes = sorted(strikes, key=lambda x: abs(x - current_price))
        return sorted_strikes[:n]
    
    @staticmethod
    def get_strike_by_delta(
        current_price: float,
        strikes: List[float],
        deltas: List[float],
        target_delta: float
    ) -> float:
        """根据目标delta获取执行价"""
        idx = min(range(len(deltas)), key=lambda i: abs(deltas[i] - target_delta))
        return strikes[idx]
    
    @staticmethod
    def calculate_iv_skew(
        strikes: List[float],
        ivs: List[float],
        current_price: float
    ) -> Tuple[float, float]:
        """计算波动率偏斜"""
        moneyness = [(k/current_price - 1) for k in strikes]
        slope, intercept = np.polyfit(moneyness, ivs, 1)
        return slope, intercept
    
    @staticmethod
    def is_otm(option_type: str, stock_price: float, strike_price: float) -> bool:
        """判断期权是否虚值"""
        if option_type.lower() == 'call':
            return strike_price > stock_price
        elif option_type.lower() == 'put':
            return strike_price < stock_price
        else:
            raise ValueError(f'Option type "{option_type}" not recognized.')
    
    @staticmethod
    def is_itm(option_type: str, stock_price: float, strike_price: float) -> bool:
        """判断期权是否实值"""
        if option_type.lower() == 'call':
            return strike_price < stock_price
        elif option_type.lower() == 'put':
            return strike_price > stock_price
        else:
            raise ValueError(f'Option type "{option_type}" not recognized.')
    
    @staticmethod
    def is_atm(
        option_type: str,
        stock_price: float,
        strike_price: float,
        delta: float = None
    ) -> bool:
        """判断期权是否平值"""
        if option_type.lower() not in ['call', 'put']:
            raise ValueError(f'Option type "{option_type}" not recognized.')
            
        if delta is None:
            return strike_price == stock_price
        else:
            return (strike_price >= stock_price - delta and 
                   strike_price <= stock_price + delta)
    
    @staticmethod
    def calculate_greeks(
        current_price: float,
        strike: float,
        risk_free_rate: float,
        volatility: float,
        time_to_expiry: float,
        option_type: str = 'call'
    ) -> dict:
        """计算期权Greeks
        
        Returns:
            dict包含delta, gamma, vega, theta, rho
        """
        from py_vollib.black_scholes.greeks import analytical
        try:
            greeks = {}
            for greek in ['delta', 'gamma', 'vega', 'theta', 'rho']:
                greeks[greek] = getattr(analytical, greek)(
                    option_type,
                    current_price,
                    strike,
                    time_to_expiry,
                    risk_free_rate,
                    volatility
                )
            return greeks
        except Exception as e:
            print(f"Error calculating greeks: {e}")
            return None
            
    @staticmethod
    def build_delta_neutral_portfolio(
        options: List[dict],
        position_sizes: List[float] = None
    ) -> Tuple[List[float], float]:
        """构建Delta中性组合
        
        Args:
            options: 期权列表，每个期权是一个包含delta的字典
            position_sizes: 初始持仓大小列表，如果为None则自动计算
            
        Returns:
            (position_sizes, total_delta)
        """
        if position_sizes is None:
            # 使用最小二乘法求解目标持仓
            deltas = np.array([opt['delta'] for opt in options])
            A = np.vstack([deltas, np.ones(len(deltas))]).T
            b = np.array([0, 1])  # 目标delta为0，权重和为1
            position_sizes = np.linalg.lstsq(A, b, rcond=None)[0]
            
        total_delta = sum(size * opt['delta'] for size, opt in zip(position_sizes, options))
        return position_sizes, total_delta

"""
Option trading utilities for calculating option metrics and managing positions.
"""

from typing import List, Dict, Optional
import numpy as np
from datetime import datetime, timedelta
import py_vollib.black_scholes.implied_volatility as iv
import py_vollib.black_scholes.greeks.analytical as greeks

def calculate_option_metrics(
    symbol: str,
    strike: float,
    direction: str,
    stock_price: float,
    risk_free_rate: float = 0.02,
    days_to_expiry: int = 5,
    historical_volatility: Optional[float] = None
) -> Dict:
    """
    Calculate key metrics for an option contract.
    
    Args:
        symbol: Stock symbol
        strike: Option strike price
        direction: 'call' or 'put'
        stock_price: Current stock price
        risk_free_rate: Risk-free interest rate (default: 2%)
        days_to_expiry: Days until option expiration (default: 5 days)
        historical_volatility: Historical volatility (if None, will be calculated)
        
    Returns:
        Dict containing:
            - price: Option price
            - delta: Option delta
            - gamma: Option gamma
            - theta: Option theta
            - vega: Option vega
            - implied_volatility: Option implied volatility
            - leverage: Option leverage ratio
    """
    # Convert days to years
    time_to_expiry = days_to_expiry / 365.0
    
    # Get or calculate historical volatility
    if historical_volatility is None:
        historical_volatility = calculate_historical_volatility(symbol)
    
    # Calculate option price and greeks
    flag = direction[0].lower()  # 'c' for call, 'p' for put
    
    try:
        # Calculate implied volatility
        implied_vol = iv.implied_volatility(
            stock_price,
            strike,
            time_to_expiry,
            risk_free_rate,
            historical_volatility,
            flag
        )
        
        # Calculate option price and greeks
        price = calculate_option_price(
            stock_price,
            strike,
            time_to_expiry,
            risk_free_rate,
            implied_vol,
            flag
        )
        
        delta = greeks.delta(
            flag,
            stock_price,
            strike,
            time_to_expiry,
            risk_free_rate,
            implied_vol
        )
        
        gamma = greeks.gamma(
            flag,
            stock_price,
            strike,
            time_to_expiry,
            risk_free_rate,
            implied_vol
        )
        
        theta = greeks.theta(
            flag,
            stock_price,
            strike,
            time_to_expiry,
            risk_free_rate,
            implied_vol
        )
        
        vega = greeks.vega(
            flag,
            stock_price,
            strike,
            time_to_expiry,
            risk_free_rate,
            implied_vol
        )
        
        # Calculate leverage ratio
        leverage = abs(delta * stock_price / price)
        
        return {
            'price': price,
            'delta': delta,
            'gamma': gamma,
            'theta': theta,
            'vega': vega,
            'implied_volatility': implied_vol,
            'leverage': leverage
        }
        
    except Exception as e:
        print(f"Error calculating option metrics for {symbol}: {str(e)}")
        return None

def calculate_historical_volatility(
    symbol: str,
    lookback_period: int = 252
) -> float:
    """
    Calculate historical volatility for a stock.
    
    Args:
        symbol: Stock symbol
        lookback_period: Number of trading days to look back (default: 252)
        
    Returns:
        Historical volatility as a decimal
    """
    # TODO: Implement historical volatility calculation
    # This should use actual historical price data
    return 0.3  # Placeholder

def get_nearest_strikes(
    symbol: str,
    stock_price: float,
    direction: str = 'both',
    num_strikes: int = 5
) -> List[float]:
    """
    Get the nearest option strike prices for a stock.
    
    Args:
        symbol: Stock symbol
        stock_price: Current stock price
        direction: 'call', 'put', or 'both' (default: 'both')
        num_strikes: Number of strikes to return (default: 5)
        
    Returns:
        List of strike prices
    """
    # TODO: Implement strike price fetching from option chain
    # This should use actual option chain data
    
    # Placeholder implementation
    strike_increment = stock_price * 0.01  # 1% increments
    strikes = []
    
    if direction in ['call', 'both']:
        for i in range(num_strikes):
            strikes.append(stock_price + (i + 1) * strike_increment)
            
    if direction in ['put', 'both']:
        for i in range(num_strikes):
            strikes.append(stock_price - (i + 1) * strike_increment)
            
    return sorted(strikes)

def calculate_option_price(
    stock_price: float,
    strike: float,
    time_to_expiry: float,
    risk_free_rate: float,
    volatility: float,
    option_type: str
) -> float:
    """
    Calculate theoretical option price using Black-Scholes model.
    
    Args:
        stock_price: Current stock price
        strike: Option strike price
        time_to_expiry: Time to expiry in years
        risk_free_rate: Risk-free interest rate
        volatility: Option implied volatility
        option_type: 'c' for call, 'p' for put
        
    Returns:
        Theoretical option price
    """
    from py_vollib.black_scholes import black_scholes as bs
    
    try:
        price = bs(option_type, stock_price, strike, time_to_expiry, risk_free_rate, volatility)
        return price
    except Exception as e:
        print(f"Error calculating option price: {str(e)}")
        return None
