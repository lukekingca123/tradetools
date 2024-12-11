"""
Strategy Backtesting Module
This module provides backtesting and visualization capabilities for trading strategies
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import yfinance as yf
from ..strategies.base_strategy import BaseStrategy

class StrategyBacktest:
    def __init__(self,
                 strategy: BaseStrategy,
                 start_date: str,
                 end_date: str,
                 initial_capital: float = 1000000.0,
                 commission: float = 0.001):
        """
        Initialize Strategy Backtest
        
        Args:
            strategy: Trading strategy instance
            start_date: Start date for backtest (YYYY-MM-DD)
            end_date: End date for backtest (YYYY-MM-DD)
            initial_capital: Initial capital for backtest
            commission: Commission rate per trade
        """
        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.commission = commission
        
        self.portfolio_value = []
        self.positions = {}
        self.trades = []
        self.current_capital = initial_capital
        
    def fetch_data(self) -> pd.DataFrame:
        """Fetch historical data for all symbols"""
        data = {}
        for symbol in self.strategy.symbols:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=self.start_date, end=self.end_date)
            data[symbol] = hist
            
        return pd.concat(data, axis=1)
        
    def run_backtest(self) -> Dict[str, Any]:
        """Run backtest simulation"""
        data = self.fetch_data()
        dates = data.index.unique()
        
        for date in dates:
            # Get current data slice
            current_data = data.loc[date]
            
            # Get strategy signals
            signals = self.strategy.on_data(current_data)["signals"]
            
            # Execute trades
            for symbol, signal in signals.items():
                if signal != 0:
                    price = current_data[symbol]["Close"]
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
                        "commission": commission_cost
                    })
                    
                    # Update positions
                    if symbol in self.positions:
                        self.positions[symbol] += shares if signal > 0 else -shares
                    else:
                        self.positions[symbol] = shares if signal > 0 else -shares
                        
            # Calculate portfolio value
            portfolio_value = self.current_capital
            for symbol, shares in self.positions.items():
                portfolio_value += shares * current_data[symbol]["Close"]
                
            self.portfolio_value.append({
                "date": date,
                "value": portfolio_value
            })
            
        return self.get_results()
        
    def get_results(self) -> Dict[str, Any]:
        """Calculate backtest results and metrics"""
        portfolio_df = pd.DataFrame(self.portfolio_value)
        portfolio_df.set_index("date", inplace=True)
        
        # Calculate returns
        returns = portfolio_df["value"].pct_change()
        
        # Calculate metrics
        total_return = (portfolio_df["value"].iloc[-1] / self.initial_capital - 1) * 100
        annual_return = (1 + total_return/100) ** (252/len(returns)) - 1
        sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std()
        max_drawdown = (portfolio_df["value"] / portfolio_df["value"].cummax() - 1).min() * 100
        
        return {
            "portfolio_value": portfolio_df,
            "trades": pd.DataFrame(self.trades),
            "total_return": total_return,
            "annual_return": annual_return * 100,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "number_of_trades": len(self.trades)
        }
        
    def plot_results(self, benchmark_symbol: Optional[str] = None):
        """Plot backtest results with optional benchmark comparison"""
        results = self.get_results()
        portfolio_df = results["portfolio_value"]
        
        # Create figure
        plt.figure(figsize=(15, 10))
        
        # Plot portfolio value
        plt.subplot(2, 1, 1)
        plt.plot(portfolio_df.index, portfolio_df["value"], label="Portfolio Value")
        
        # Add benchmark if specified
        if benchmark_symbol:
            benchmark = yf.download(benchmark_symbol, self.start_date, self.end_date)
            benchmark_returns = benchmark["Close"] / benchmark["Close"].iloc[0] * self.initial_capital
            plt.plot(benchmark_returns.index, benchmark_returns, label=f"Benchmark ({benchmark_symbol})")
            
        plt.title("Portfolio Value Over Time")
        plt.xlabel("Date")
        plt.ylabel("Value ($)")
        plt.legend()
        plt.grid(True)
        
        # Plot drawdown
        plt.subplot(2, 1, 2)
        drawdown = (portfolio_df["value"] / portfolio_df["value"].cummax() - 1) * 100
        plt.fill_between(drawdown.index, drawdown, 0, color="red", alpha=0.3)
        plt.title("Portfolio Drawdown")
        plt.xlabel("Date")
        plt.ylabel("Drawdown (%)")
        plt.grid(True)
        
        plt.tight_layout()
        plt.show()
        
    def plot_factor_analysis(self):
        """Plot factor analysis for multi-factor strategy"""
        if not hasattr(self.strategy, "factors"):
            print("Factor analysis only available for multi-factor strategy")
            return
            
        results = self.get_results()
        trades_df = results["trades"]
        
        if len(trades_df) == 0:
            print("No trades to analyze")
            return
            
        # Analyze factor contributions
        plt.figure(figsize=(12, 6))
        
        # Get factor returns
        factor_returns = {}
        for factor in self.strategy.factors:
            factor_trades = trades_df[trades_df["factor"] == factor]
            factor_returns[factor] = factor_trades["return"].mean()
            
        # Plot factor returns
        plt.bar(factor_returns.keys(), factor_returns.values())
        plt.title("Average Return by Factor")
        plt.xlabel("Factor")
        plt.ylabel("Average Return (%)")
        plt.xticks(rotation=45)
        plt.grid(True)
        plt.show()
        
    def plot_ml_feature_importance(self):
        """Plot feature importance for machine learning strategy"""
        if not hasattr(self.strategy, "model"):
            print("Feature importance only available for machine learning strategy")
            return
            
        if hasattr(self.strategy.model, "feature_importances_"):
            importance = self.strategy.model.feature_importances_
            features = self.strategy.feature_names
            
            plt.figure(figsize=(12, 6))
            plt.bar(features, importance)
            plt.title("Feature Importance")
            plt.xlabel("Features")
            plt.ylabel("Importance")
            plt.xticks(rotation=45)
            plt.grid(True)
            plt.show()
            
    def plot_rsi_signals(self, symbol: str):
        """Plot RSI signals for RSI strategy"""
        if not isinstance(self.strategy, "USRSIDivergenceStrategy"):
            print("RSI signals only available for RSI strategy")
            return
            
        data = yf.download(symbol, self.start_date, self.end_date)
        rsi = self.strategy.calculate_rsi(data["Close"])
        
        plt.figure(figsize=(15, 10))
        
        # Plot price
        plt.subplot(2, 1, 1)
        plt.plot(data.index, data["Close"], label="Price")
        
        # Add buy/sell signals
        trades_df = self.get_results()["trades"]
        symbol_trades = trades_df[trades_df["symbol"] == symbol]
        
        buys = symbol_trades[symbol_trades["action"] == "BUY"]
        sells = symbol_trades[symbol_trades["action"] == "SELL"]
        
        plt.scatter(buys["date"], buys["price"], color="green", marker="^", label="Buy")
        plt.scatter(sells["date"], sells["price"], color="red", marker="v", label="Sell")
        
        plt.title(f"{symbol} Price and Signals")
        plt.xlabel("Date")
        plt.ylabel("Price")
        plt.legend()
        plt.grid(True)
        
        # Plot RSI
        plt.subplot(2, 1, 2)
        plt.plot(data.index, rsi, label="RSI")
        plt.axhline(y=self.strategy.overbought_threshold, color="r", linestyle="--")
        plt.axhline(y=self.strategy.oversold_threshold, color="g", linestyle="--")
        
        plt.title("RSI Indicator")
        plt.xlabel("Date")
        plt.ylabel("RSI")
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()
        plt.show()
