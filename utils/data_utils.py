"""
Data utilities for fetching and processing stock data.
"""

from typing import List, Dict, Optional
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yfinance as yf
from qlib.data import D
from qlib.config import REG_CN, REG_US
import os
import financialmodelingprep

class DataManager:
    def __init__(self, region: str = 'us'):
        """
        Initialize data manager.
        
        Args:
            region: Market region ('us' or 'cn')
        """
        self.region = region
        self._initialize_qlib()
        
    def _initialize_qlib(self):
        """Initialize QLib with proper region settings"""
        if self.region == 'us':
            # Initialize QLib with US market
            D.register_data_vendor(REG_US)
        else:
            # Initialize QLib with CN market
            D.register_data_vendor(REG_CN)
            
    def get_stock_data(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        fields: List[str] = ['close', 'open', 'high', 'low', 'volume']
    ) -> pd.DataFrame:
        """
        Get historical stock data for multiple symbols.
        
        Args:
            symbols: List of stock symbols
            start_date: Start date
            end_date: End date
            fields: List of fields to fetch
            
        Returns:
            DataFrame with stock data
        """
        try:
            # Use QLib to fetch data
            data = D.features(
                symbols,
                fields,
                start_time=start_date,
                end_time=end_date,
                freq='day'
            )
            return data
        except Exception as e:
            print(f"Error fetching data from QLib: {str(e)}")
            # Fallback to yfinance
            return self._get_data_from_yfinance(symbols, start_date, end_date, fields)
            
    def _get_data_from_yfinance(
        self,
        symbols: List[str],
        start_date: datetime,
        end_date: datetime,
        fields: List[str]
    ) -> pd.DataFrame:
        """Fallback method to get data from yfinance"""
        all_data = []
        
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(start=start_date, end=end_date)
                data['symbol'] = symbol
                all_data.append(data)
            except Exception as e:
                print(f"Error fetching data for {symbol}: {str(e)}")
                continue
                
        if not all_data:
            return pd.DataFrame()
            
        return pd.concat(all_data)
        
    def calculate_technical_features(
        self,
        data: pd.DataFrame,
        lookback_periods: List[int] = [5, 10, 20, 60]
    ) -> pd.DataFrame:
        """
        Calculate technical indicators for stock data.
        
        Args:
            data: DataFrame with stock data
            lookback_periods: List of periods for calculating indicators
            
        Returns:
            DataFrame with additional technical indicators
        """
        features = data.copy()
        
        # Calculate returns
        features['daily_return'] = features['close'].pct_change()
        
        # Calculate moving averages
        for period in lookback_periods:
            features[f'ma_{period}'] = features['close'].rolling(period).mean()
            features[f'std_{period}'] = features['close'].rolling(period).std()
            features[f'rsi_{period}'] = self._calculate_rsi(features['close'], period)
            
        # Calculate volatility
        features['volatility'] = features['daily_return'].rolling(20).std() * np.sqrt(252)
        
        return features
        
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate Relative Strength Index"""
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        return 100 - (100 / (1 + rs))
        
    def get_nasdaq100_symbols(self) -> List[str]:
        """Get list of Nasdaq 100 symbols"""
        try:
            # Use yfinance to get Nasdaq 100 components
            nasdaq100 = yf.Ticker('^NDX')
            return nasdaq100.components
        except Exception as e:
            print(f"Error fetching Nasdaq 100 symbols: {str(e)}")
            # Return a placeholder list of major tech stocks
            return ['AAPL', 'MSFT', 'AMZN', 'GOOGL', 'META', 'NVDA', 'TSLA']
            
    def get_option_chain(
        self,
        symbol: str,
        expiry_date: Optional[datetime] = None
    ) -> Dict:
        """
        Get option chain data for a symbol.
        
        Args:
            symbol: Stock symbol
            expiry_date: Option expiry date (if None, get nearest expiry)
            
        Returns:
            Dictionary containing calls and puts data
        """
        try:
            ticker = yf.Ticker(symbol)
            chain = ticker.option_chain(expiry_date)
            return {
                'calls': chain.calls,
                'puts': chain.puts
            }
        except Exception as e:
            print(f"Error fetching option chain for {symbol}: {str(e)}")
            return None

    def get_earnings_calendar(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> pd.DataFrame:
        """
        Get earnings announcement calendar.
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with earnings dates
        """
        try:
            # Use Financial Modeling Prep API to get earnings calendar
            api_key = os.getenv('FMP_API_KEY')
            if not api_key:
                print("Warning: FMP_API_KEY not found in environment variables")
                return pd.DataFrame()
                
            fmp = financialmodelingprep.FinancialModelingPrep(api_key)
            
            # Get earnings calendar
            calendar = fmp.get_earnings_calendar(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            if not calendar:
                return pd.DataFrame()
                
            # Convert to DataFrame
            df = pd.DataFrame(calendar)
            df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            print(f"Error fetching earnings calendar: {str(e)}")
            return pd.DataFrame()
            
    def get_economic_calendar(
        self,
        start_date: datetime,
        end_date: datetime,
        events: List[str] = None
    ) -> pd.DataFrame:
        """
        Get economic calendar events.
        
        Args:
            start_date: Start date
            end_date: End date
            events: List of event types to filter (e.g., ['FOMC', 'CPI'])
            
        Returns:
            DataFrame with economic events
        """
        try:
            # Use Financial Modeling Prep API
            api_key = os.getenv('FMP_API_KEY')
            if not api_key:
                print("Warning: FMP_API_KEY not found in environment variables")
                return pd.DataFrame()
                
            fmp = financialmodelingprep.FinancialModelingPrep(api_key)
            
            # Get economic calendar
            calendar = fmp.get_economic_calendar(
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            if not calendar:
                return pd.DataFrame()
                
            # Convert to DataFrame
            df = pd.DataFrame(calendar)
            df['date'] = pd.to_datetime(df['date'])
            
            # Filter by event types if specified
            if events:
                df = df[df['event'].isin(events)]
                
            return df
            
        except Exception as e:
            print(f"Error fetching economic calendar: {str(e)}")
            return pd.DataFrame()
