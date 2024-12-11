"""
US Stock RSI Divergence Strategy
This strategy identifies and trades RSI divergences in US stocks
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
from .base_strategy import BaseStrategy
from data_sources.yahoo_finance import YahooFinanceDataSource

class USRSIDivergenceStrategy(BaseStrategy):
    def __init__(self, 
                 symbols: List[str],
                 rsi_period: int = 14,
                 divergence_window: int = 20,
                 oversold_threshold: float = 30,
                 overbought_threshold: float = 70):
        """
        Initialize US RSI Divergence Strategy
        
        Args:
            symbols: List of stock symbols to trade
            rsi_period: Period for RSI calculation
            divergence_window: Window to look for divergences
            oversold_threshold: RSI threshold for oversold condition
            overbought_threshold: RSI threshold for overbought condition
        """
        super().__init__()
        self.symbols = symbols
        self.rsi_period = rsi_period
        self.divergence_window = divergence_window
        self.oversold_threshold = oversold_threshold
        self.overbought_threshold = overbought_threshold
        
        self.data_source = YahooFinanceDataSource()
        
    def calculate_rsi(self, prices: pd.Series) -> pd.Series:
        """Calculate RSI indicator"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.rsi_period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.rsi_period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
        
    def find_peaks(self, series: pd.Series, window: int = 5) -> Tuple[List[int], List[int]]:
        """Find peaks and troughs in a series"""
        highs = []
        lows = []
        
        for i in range(window, len(series) - window):
            if all(series[i] > series[i-j] for j in range(1, window+1)) and \
               all(series[i] > series[i+j] for j in range(1, window+1)):
                highs.append(i)
            if all(series[i] < series[i-j] for j in range(1, window+1)) and \
               all(series[i] < series[i+j] for j in range(1, window+1)):
                lows.append(i)
                
        return highs, lows
        
    def check_bullish_divergence(self, 
                               prices: pd.Series, 
                               rsi: pd.Series, 
                               window: int) -> bool:
        """Check for bullish divergence (price lower lows, RSI higher lows)"""
        # Get recent lows
        _, price_lows = self.find_peaks(prices[-window:])
        _, rsi_lows = self.find_peaks(rsi[-window:])
        
        if len(price_lows) >= 2 and len(rsi_lows) >= 2:
            # Check if price made lower low but RSI made higher low
            price_lower_low = prices.iloc[price_lows[-1]] < prices.iloc[price_lows[-2]]
            rsi_higher_low = rsi.iloc[rsi_lows[-1]] > rsi.iloc[rsi_lows[-2]]
            
            return price_lower_low and rsi_higher_low and rsi.iloc[rsi_lows[-1]] < self.oversold_threshold
            
        return False
        
    def check_bearish_divergence(self, 
                                prices: pd.Series, 
                                rsi: pd.Series, 
                                window: int) -> bool:
        """Check for bearish divergence (price higher highs, RSI lower highs)"""
        # Get recent highs
        price_highs, _ = self.find_peaks(prices[-window:])
        rsi_highs, _ = self.find_peaks(rsi[-window:])
        
        if len(price_highs) >= 2 and len(rsi_highs) >= 2:
            # Check if price made higher high but RSI made lower high
            price_higher_high = prices.iloc[price_highs[-1]] > prices.iloc[price_highs[-2]]
            rsi_lower_high = rsi.iloc[rsi_highs[-1]] < rsi.iloc[rsi_highs[-2]]
            
            return price_higher_high and rsi_lower_high and rsi.iloc[rsi_highs[-1]] > self.overbought_threshold
            
        return False
        
    def generate_signals(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate trading signals based on RSI divergences"""
        signals = {}
        
        for symbol in self.symbols:
            if symbol in data.index:
                prices = data.loc[symbol, "close"]
                rsi = self.calculate_rsi(prices)
                
                # Check for divergences
                if self.check_bullish_divergence(prices, rsi, self.divergence_window):
                    signals[symbol] = 1.0  # Buy signal
                elif self.check_bearish_divergence(prices, rsi, self.divergence_window):
                    signals[symbol] = -1.0  # Sell signal
                    
        return signals

    def on_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Strategy execution on new data"""
        signals = self.generate_signals(data)
        return {"signals": signals}
