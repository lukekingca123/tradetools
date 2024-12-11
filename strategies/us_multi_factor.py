"""
US Stock Multi-Factor Strategy
This strategy implements a multi-factor model for US stocks, focusing on:
- Value factors (P/E, P/B)
- Momentum factors
- Quality factors (ROE, Gross Margin)
- Size factor (Market Cap)
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any
from .base_strategy import BaseStrategy
from data_sources.yahoo_finance import YahooFinanceDataSource

class USMultiFactorStrategy(BaseStrategy):
    def __init__(self, 
                 symbols: List[str],
                 rebalance_days: int = 15,
                 lookback_days: int = 63,
                 top_n: int = 20,
                 factors: List[str] = None):
        """
        Initialize US Multi-Factor Strategy
        
        Args:
            symbols: List of stock symbols to trade
            rebalance_days: How often to rebalance the portfolio
            lookback_days: Historical data lookback period
            top_n: Number of stocks to hold
            factors: List of factors to use, default includes:
                    ["market_cap", "pe_ratio", "pb_ratio", "roe", "momentum"]
        """
        super().__init__()
        self.symbols = symbols
        self.rebalance_days = rebalance_days
        self.lookback_days = lookback_days
        self.top_n = top_n
        self.factors = factors or ["market_cap", "pe_ratio", "pb_ratio", "roe", "momentum"]
        
        # Factor weights (1 means smaller is better, -1 means larger is better)
        self.factor_weights = {
            "market_cap": 1,  # Prefer smaller caps
            "pe_ratio": 1,    # Prefer lower P/E
            "pb_ratio": 1,    # Prefer lower P/B
            "roe": -1,        # Prefer higher ROE
            "momentum": -1    # Prefer higher momentum
        }
        
        self.data_source = YahooFinanceDataSource()
        self.days = 0
        
    def calculate_factors(self, data: pd.DataFrame) -> Dict[str, pd.Series]:
        """Calculate factor values for all stocks"""
        factors = {}
        
        # Market Cap
        factors["market_cap"] = data["close"] * data["volume"]
        
        # P/E Ratio (using adjusted close)
        factors["pe_ratio"] = data["close"] / data["earnings_per_share"]
        
        # P/B Ratio
        factors["pb_ratio"] = data["close"] / data["book_value_per_share"]
        
        # ROE
        factors["roe"] = data["net_income"] / data["total_equity"]
        
        # Momentum (20-day returns)
        factors["momentum"] = data["close"].pct_change(20)
        
        return factors
        
    def rank_stocks(self, factors: Dict[str, pd.Series]) -> pd.Series:
        """Rank stocks based on factor values"""
        # Initialize scores
        final_scores = pd.Series(0, index=factors[self.factors[0]].index)
        
        # Calculate score for each factor
        for factor in self.factors:
            if factor in factors:
                factor_series = factors[factor]
                # Rank stocks (0 to 1)
                ranked = factor_series.rank(pct=True)
                # Adjust direction based on weight
                if self.factor_weights[factor] > 0:
                    ranked = 1 - ranked
                final_scores += ranked
                
        return final_scores
        
    def generate_signals(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate trading signals based on factor scores"""
        self.days += 1
        
        # Only rebalance every rebalance_days
        if self.days % self.rebalance_days != 0:
            return {}
            
        # Calculate factors
        factors = self.calculate_factors(data)
        
        # Rank stocks
        scores = self.rank_stocks(factors)
        
        # Select top N stocks
        top_stocks = scores.nlargest(self.top_n)
        
        # Generate equal-weight positions for top stocks
        position_size = 1.0 / self.top_n
        signals = {symbol: position_size for symbol in top_stocks.index}
        
        return signals

    def on_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Strategy execution on new data"""
        signals = self.generate_signals(data)
        return {"signals": signals}
