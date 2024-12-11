"""
Stock Backtesting Module
Implements stock-specific backtesting functionality
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
from datetime import datetime
import yfinance as yf
from .base_backtest import BaseBacktest

class StockBacktest(BaseBacktest):
    def fetch_data(self) -> pd.DataFrame:
        """Fetch stock historical data"""
        data = {}
        for symbol in self.strategy.symbols:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=self.start_date, end=self.end_date)
            data[symbol] = hist
            
        return pd.concat(data, axis=1)
        
    def execute_trades(self, date: datetime, signals: Dict[str, Any]) -> None:
        """Execute stock trades"""
        if "signals" not in signals:
            return
            
        for symbol, signal in signals["signals"].items():
            if signal != 0:
                # Get current price
                price = signals["data"].loc[symbol, "Close"]
                
                # Calculate position size
                position_value = self.current_capital * abs(signal)
                shares = position_value / price
                
                # Apply commission
                commission_cost = position_value * self.commission
                self.current_capital -= commission_cost
                
                # Record trade
                self.trades.append({
                    "date": date,
                    "symbol": symbol,
                    "action": "BUY" if signal > 0 else "SELL",
                    "price": price,
                    "shares": shares,
                    "value": position_value,
                    "commission": commission_cost,
                    "return": 0  # Will be updated later
                })
                
                # Update positions
                if symbol in self.positions:
                    self.positions[symbol] += shares if signal > 0 else -shares
                else:
                    self.positions[symbol] = shares if signal > 0 else -shares
                    
    def calculate_portfolio_value(self, date: datetime, data: pd.DataFrame) -> float:
        """Calculate current portfolio value"""
        portfolio_value = self.current_capital
        
        # Add value of all positions
        for symbol, shares in self.positions.items():
            if shares != 0:
                price = data.loc[symbol, "Close"]
                position_value = shares * price
                portfolio_value += position_value
                
                # Update trade returns
                if len(self.trades) > 0 and self.trades[-1]["symbol"] == symbol:
                    last_trade = self.trades[-1]
                    if last_trade["date"] != date:  # Only update if it's not the same day
                        returns = ((price - last_trade["price"]) / last_trade["price"]) * 100
                        self.trades[-1]["return"] = returns if last_trade["action"] == "BUY" else -returns
                
        return portfolio_value
        
    def plot_strategy_specific(self):
        """Plot strategy-specific analysis"""
        if hasattr(self.strategy, "factors"):
            self._plot_factor_analysis()
        elif hasattr(self.strategy, "model"):
            self._plot_ml_analysis()
        elif hasattr(self.strategy, "calculate_rsi"):
            self._plot_technical_analysis()
            
    def _plot_factor_analysis(self):
        """Plot factor analysis for multi-factor strategy"""
        trades_df = pd.DataFrame(self.trades)
        if "factor" not in trades_df.columns:
            return
            
        plt.figure(figsize=(12, 6))
        sns.boxplot(x="factor", y="return", data=trades_df)
        plt.title("Returns Distribution by Factor")
        plt.xlabel("Factor")
        plt.ylabel("Return (%)")
        plt.xticks(rotation=45)
        plt.show()
        
    def _plot_ml_analysis(self):
        """Plot machine learning analysis"""
        if not hasattr(self.strategy, "model") or not hasattr(self.strategy.model, "feature_importances_"):
            return
            
        importance = self.strategy.model.feature_importances_
        features = self.strategy.feature_names
        
        plt.figure(figsize=(12, 6))
        plt.bar(features, importance)
        plt.title("Feature Importance")
        plt.xlabel("Features")
        plt.ylabel("Importance")
        plt.xticks(rotation=45)
        plt.show()
        
    def _plot_technical_analysis(self):
        """Plot technical analysis"""
        trades_df = pd.DataFrame(self.trades)
        if len(trades_df) == 0:
            return
            
        # Plot price and signals for each symbol
        for symbol in self.strategy.symbols:
            symbol_trades = trades_df[trades_df["symbol"] == symbol]
            if len(symbol_trades) == 0:
                continue
                
            # Get price data
            data = yf.download(symbol, self.start_date, self.end_date)
            
            plt.figure(figsize=(15, 10))
            
            # Plot price
            plt.subplot(2, 1, 1)
            plt.plot(data.index, data["Close"], label="Price")
            
            # Add buy/sell signals
            buys = symbol_trades[symbol_trades["action"] == "BUY"]
            sells = symbol_trades[symbol_trades["action"] == "SELL"]
            
            plt.scatter(buys["date"], buys["price"], color="green", marker="^", label="Buy")
            plt.scatter(sells["date"], sells["price"], color="red", marker="v", label="Sell")
            
            plt.title(f"{symbol} Price and Signals")
            plt.xlabel("Date")
            plt.ylabel("Price")
            plt.legend()
            plt.grid(True)
            
            # Plot technical indicators if available
            if hasattr(self.strategy, "calculate_rsi"):
                plt.subplot(2, 1, 2)
                rsi = self.strategy.calculate_rsi(data["Close"])
                plt.plot(data.index, rsi, label="RSI")
                plt.axhline(y=70, color="r", linestyle="--")
                plt.axhline(y=30, color="g", linestyle="--")
                plt.title("Technical Indicators")
                plt.xlabel("Date")
                plt.ylabel("Value")
                plt.legend()
                plt.grid(True)
                
            plt.tight_layout()
            plt.show()
