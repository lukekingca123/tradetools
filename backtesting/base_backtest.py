"""
Base Backtesting Module
Provides base functionality for both stock and option backtesting
"""

from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

class BaseBacktest(ABC):
    def __init__(self,
                 strategy: Any,
                 start_date: str,
                 end_date: str,
                 initial_capital: float = 1000000.0,
                 commission: float = 0.001):
        """
        Initialize Base Backtest
        
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
        
    @abstractmethod
    def fetch_data(self) -> pd.DataFrame:
        """Fetch historical data for backtesting"""
        pass
        
    @abstractmethod
    def execute_trades(self, date: datetime, signals: Dict[str, Any]) -> None:
        """Execute trades based on strategy signals"""
        pass
        
    @abstractmethod
    def calculate_portfolio_value(self, date: datetime, data: pd.DataFrame) -> float:
        """Calculate current portfolio value"""
        pass
        
    def run_backtest(self) -> Dict[str, Any]:
        """Run backtest simulation"""
        data = self.fetch_data()
        dates = data.index.unique()
        
        for date in dates:
            # Get strategy signals
            signals = self.strategy.on_data(data.loc[date])
            
            # Execute trades
            self.execute_trades(date, signals)
            
            # Calculate and record portfolio value
            portfolio_value = self.calculate_portfolio_value(date, data.loc[date])
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
        """Plot backtest results"""
        results = self.get_results()
        portfolio_df = results["portfolio_value"]
        
        # Create subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))
        
        # Plot portfolio value
        ax1.plot(portfolio_df.index, portfolio_df["value"], label="Portfolio Value")
        ax1.set_title("Portfolio Value Over Time")
        ax1.set_xlabel("Date")
        ax1.set_ylabel("Value ($)")
        ax1.grid(True)
        ax1.legend()
        
        # Plot drawdown
        drawdown = (portfolio_df["value"] / portfolio_df["value"].cummax() - 1) * 100
        ax2.fill_between(drawdown.index, drawdown, 0, color="red", alpha=0.3)
        ax2.set_title("Portfolio Drawdown")
        ax2.set_xlabel("Date")
        ax2.set_ylabel("Drawdown (%)")
        ax2.grid(True)
        
        plt.tight_layout()
        plt.show()
        
        # Print metrics
        print(f"\nBacktest Results:")
        print(f"Total Return: {results['total_return']:.2f}%")
        print(f"Annual Return: {results['annual_return']:.2f}%")
        print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
        print(f"Max Drawdown: {results['max_drawdown']:.2f}%")
        print(f"Number of Trades: {results['number_of_trades']}")
        
    def plot_trade_analysis(self):
        """Plot trade analysis"""
        trades_df = pd.DataFrame(self.trades)
        if len(trades_df) == 0:
            print("No trades to analyze")
            return
            
        # Create subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Plot trade returns distribution
        trades_df["return"].hist(bins=50, ax=ax1)
        ax1.set_title("Trade Returns Distribution")
        ax1.set_xlabel("Return (%)")
        ax1.set_ylabel("Frequency")
        
        # Plot cumulative returns
        trades_df["cumulative_return"] = (1 + trades_df["return"]/100).cumprod()
        ax2.plot(trades_df.index, trades_df["cumulative_return"])
        ax2.set_title("Cumulative Returns")
        ax2.set_xlabel("Trade Number")
        ax2.set_ylabel("Cumulative Return")
        
        plt.tight_layout()
        plt.show()
        
    @abstractmethod
    def plot_strategy_specific(self):
        """Plot strategy-specific analysis"""
        pass
