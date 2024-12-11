import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from .base_strategy import BaseStrategy
import py_vollib.black_scholes as bs
import py_vollib.black_scholes.implied_volatility as iv
import py_vollib.black_scholes.greeks.analytical as greeks

class KellyOptionStrategy(BaseStrategy):
    """Kelly criterion based option portfolio strategy
    
    This strategy uses the Kelly criterion to optimize position sizes in an option portfolio,
    considering win rates, profit ratios, and implied volatilities.
    """
    
    def initialize(self,
                  lookback_period: int = 252,     # 1 year of data
                  min_win_rate: float = 0.4,      # Minimum required win rate
                  min_profit_ratio: float = 1.5,  # Minimum profit/loss ratio
                  max_portfolio_iv: float = 0.5,  # Maximum portfolio implied volatility
                  risk_free_rate: float = 0.02):  # Risk-free rate
        """Initialize strategy parameters
        
        Args:
            lookback_period: Period for calculating historical metrics
            min_win_rate: Minimum acceptable win rate for positions
            min_profit_ratio: Minimum profit/loss ratio
            max_portfolio_iv: Maximum portfolio implied volatility
            risk_free_rate: Risk-free rate for option pricing
        """
        self.params = {
            'lookback_period': lookback_period,
            'min_win_rate': min_win_rate,
            'min_profit_ratio': min_profit_ratio,
            'max_portfolio_iv': max_portfolio_iv,
            'risk_free_rate': risk_free_rate
        }
        self.option_positions = {}  # Current option positions
        
    def calculate_option_metrics(self, 
                               option_data: pd.DataFrame,
                               underlying_data: pd.DataFrame) -> Dict[str, Dict[str, float]]:
        """Calculate option metrics including win rates and profit ratios
        
        Args:
            option_data: Historical option data (price, strike, expiry, type)
            underlying_data: Historical underlying price data
            
        Returns:
            Dictionary of option metrics
        """
        metrics = {}
        
        for option_id in option_data.index:
            opt = option_data.loc[option_id]
            
            # Calculate historical win rate
            historical_pnl = self._calculate_historical_pnl(opt, underlying_data)
            win_rate = (historical_pnl > 0).mean()
            
            # Calculate profit ratio (average win / average loss)
            avg_win = historical_pnl[historical_pnl > 0].mean()
            avg_loss = abs(historical_pnl[historical_pnl < 0].mean())
            profit_ratio = avg_win / avg_loss if avg_loss != 0 else float('inf')
            
            # Calculate implied volatility
            try:
                impl_vol = iv.implied_volatility(
                    opt['price'],
                    underlying_data.iloc[-1]['close'],
                    opt['strike'],
                    self.params['risk_free_rate'],
                    (opt['expiry'] - datetime.now()).days / 365,
                    opt['type'].lower()
                )
            except:
                impl_vol = float('nan')
            
            metrics[option_id] = {
                'win_rate': win_rate,
                'profit_ratio': profit_ratio,
                'implied_vol': impl_vol,
                'days_to_expiry': (opt['expiry'] - datetime.now()).days
            }
            
        return metrics
        
    def _calculate_historical_pnl(self, 
                                option: pd.Series,
                                underlying_data: pd.DataFrame) -> pd.Series:
        """Calculate historical P&L for an option
        
        Args:
            option: Option data series
            underlying_data: Historical underlying price data
            
        Returns:
            Series of historical P&L
        """
        # Simulate historical option prices using Black-Scholes
        historical_prices = []
        
        for _, row in underlying_data.iterrows():
            try:
                price = bs.black_scholes(
                    option['type'].lower(),
                    row['close'],
                    option['strike'],
                    self.params['risk_free_rate'],
                    option['implied_vol'],
                    (option['expiry'] - row.name).days / 365
                )
                historical_prices.append(price)
            except:
                historical_prices.append(np.nan)
                
        return pd.Series(historical_prices).pct_change()
        
    def calculate_kelly_fraction(self, 
                               win_rate: float, 
                               profit_ratio: float,
                               implied_vol: float) -> float:
        """Calculate Kelly fraction with volatility adjustment
        
        Args:
            win_rate: Probability of winning
            profit_ratio: Ratio of average win to average loss
            implied_vol: Option implied volatility
            
        Returns:
            Adjusted Kelly fraction
        """
        # Basic Kelly fraction
        kelly = win_rate - (1 - win_rate) / profit_ratio
        
        # Adjust for implied volatility
        vol_adjustment = 1 - (implied_vol / self.params['max_portfolio_iv'])
        
        return max(0, kelly * vol_adjustment)
        
    def generate_signals(self, 
                        option_data: pd.DataFrame,
                        underlying_data: pd.DataFrame) -> Dict[str, float]:
        """Generate trading signals for options
        
        Args:
            option_data: Current option chain data
            underlying_data: Historical underlying price data
            
        Returns:
            Dictionary of option position signals
        """
        # Calculate metrics for all options
        metrics = self.calculate_option_metrics(option_data, underlying_data)
        
        signals = {}
        portfolio_iv = 0
        
        for option_id, metric in metrics.items():
            # Check if option meets minimum criteria
            if (metric['win_rate'] >= self.params['min_win_rate'] and 
                metric['profit_ratio'] >= self.params['min_profit_ratio']):
                
                # Calculate Kelly fraction
                kelly = self.calculate_kelly_fraction(
                    metric['win_rate'],
                    metric['profit_ratio'],
                    metric['implied_vol']
                )
                
                # Add to signals if Kelly fraction is positive
                if kelly > 0:
                    signals[option_id] = kelly
                    portfolio_iv += kelly * metric['implied_vol']
                    
                    # Check portfolio implied volatility constraint
                    if portfolio_iv > self.params['max_portfolio_iv']:
                        break
                        
        return signals
        
    def update_positions(self, 
                        positions: Dict[str, float],
                        option_data: pd.DataFrame,
                        timestamp: Optional[datetime] = None):
        """Update strategy positions with option details
        
        Args:
            positions: New position sizes dictionary
            option_data: Current option data
            timestamp: Update timestamp
        """
        self.position = positions.copy()
        self.option_positions = {
            option_id: {
                'size': size,
                'type': option_data.loc[option_id, 'type'],
                'strike': option_data.loc[option_id, 'strike'],
                'expiry': option_data.loc[option_id, 'expiry']
            }
            for option_id, size in positions.items()
        }
        self._last_update_time = timestamp or datetime.now()
