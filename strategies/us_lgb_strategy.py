"""
US Stock LightGBM Strategy
This strategy uses LightGBM to predict stock returns and generate trading signals
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from typing import List, Dict, Any, Tuple
from sklearn.model_selection import train_test_split
from .base_strategy import BaseStrategy
from data_sources.yahoo_finance import YahooFinanceDataSource

class USLightGBMStrategy(BaseStrategy):
    def __init__(self, 
                 symbols: List[str],
                 lookback_days: int = 252,  # 1 year of data
                 prediction_days: int = 5,   # Predict 5-day returns
                 top_n: int = 10,
                 retrain_days: int = 30):    # Retrain model monthly
        """
        Initialize US LightGBM Strategy
        
        Args:
            symbols: List of stock symbols to trade
            lookback_days: Days of historical data to use
            prediction_days: Days ahead to predict returns
            top_n: Number of stocks to hold
            retrain_days: How often to retrain the model
        """
        super().__init__()
        self.symbols = symbols
        self.lookback_days = lookback_days
        self.prediction_days = prediction_days
        self.top_n = top_n
        self.retrain_days = retrain_days
        
        self.data_source = YahooFinanceDataSource()
        self.model = None
        self.days = 0
        
    def prepare_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Prepare features for machine learning"""
        df = pd.DataFrame()
        
        # Technical indicators
        df["rsi"] = data["close"].rolling(14).apply(lambda x: 100 - (100 / (1 + (x.diff(1)[x.diff(1) > 0].mean() / -x.diff(1)[x.diff(1) < 0].mean()))))
        df["ma5"] = data["close"].rolling(5).mean()
        df["ma20"] = data["close"].rolling(20).mean()
        df["ma60"] = data["close"].rolling(60).mean()
        
        # Price momentum
        df["returns_5d"] = data["close"].pct_change(5)
        df["returns_20d"] = data["close"].pct_change(20)
        df["returns_60d"] = data["close"].pct_change(60)
        
        # Volatility
        df["volatility_20d"] = data["returns_5d"].rolling(20).std()
        
        # Volume indicators
        df["volume_ma5"] = data["volume"].rolling(5).mean()
        df["volume_ma20"] = data["volume"].rolling(20).mean()
        df["volume_ratio"] = df["volume_ma5"] / df["volume_ma20"]
        
        return df
        
    def prepare_labels(self, data: pd.DataFrame) -> pd.Series:
        """Prepare labels (future returns) for machine learning"""
        future_returns = data["close"].pct_change(self.prediction_days).shift(-self.prediction_days)
        return future_returns
        
    def train_model(self, features: pd.DataFrame, labels: pd.Series) -> None:
        """Train LightGBM model"""
        # Split data
        X_train, X_val, y_train, y_val = train_test_split(
            features.iloc[:-self.prediction_days],  # Remove last few days where we don't have labels
            labels.iloc[:-self.prediction_days],
            test_size=0.2,
            random_state=42
        )
        
        # Create datasets
        train_data = lgb.Dataset(X_train, label=y_train)
        val_data = lgb.Dataset(X_val, label=y_val)
        
        # Parameters
        params = {
            'objective': 'regression',
            'metric': 'mse',
            'learning_rate': 0.05,
            'max_depth': 5,
            'num_leaves': 32,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 1,
            'lambda_l1': 0.1,
            'lambda_l2': 0.1,
            'min_data_in_leaf': 20,
        }
        
        # Train model
        self.model = lgb.train(
            params,
            train_data,
            num_boost_round=100,
            valid_sets=[val_data],
            early_stopping_rounds=10,
            verbose_eval=False
        )
        
    def generate_signals(self, data: pd.DataFrame) -> Dict[str, float]:
        """Generate trading signals using model predictions"""
        self.days += 1
        
        # Prepare features
        features = self.prepare_features(data)
        
        # Train/retrain model periodically
        if self.days % self.retrain_days == 0 or self.model is None:
            labels = self.prepare_labels(data)
            self.train_model(features, labels)
            
        # Make predictions
        if self.model is not None:
            predictions = self.model.predict(features.iloc[-1:])
            
            # Select top N stocks based on predicted returns
            pred_series = pd.Series(predictions, index=data.index)
            top_stocks = pred_series.nlargest(self.top_n)
            
            # Generate equal-weight positions for top stocks
            position_size = 1.0 / self.top_n
            signals = {symbol: position_size for symbol in top_stocks.index}
            
            return signals
            
        return {}

    def on_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Strategy execution on new data"""
        signals = self.generate_signals(data)
        return {"signals": signals}
