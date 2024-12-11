"""
Option Backtesting Module
Implements option-specific backtesting functionality
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Tuple
from datetime import datetime
import yfinance as yf
from py_vollib.black_scholes import black_scholes as bs
from py_vollib.black_scholes.greeks.analytical import delta, gamma, theta, vega
from .base_backtest import BaseBacktest

class OptionBacktest(BaseBacktest):
    def __init__(self,
                 strategy: Any,
                 start_date: str,
                 end_date: str,
                 initial_capital: float = 1000000.0,
                 commission: float = 0.001,
                 risk_free_rate: float = 0.02):
        """
        Initialize Option Backtest
        
        Args:
            strategy: Trading strategy instance
            start_date: Start date for backtest (YYYY-MM-DD)
            end_date: End date for backtest (YYYY-MM-DD)
            initial_capital: Initial capital for backtest
            commission: Commission rate per trade
            risk_free_rate: Risk-free rate for option pricing
        """
        super().__init__(strategy, start_date, end_date, initial_capital, commission)
        self.risk_free_rate = risk_free_rate
        self.greeks = {}  # Store greeks for each position
        
    def fetch_data(self) -> pd.DataFrame:
        """Fetch option and underlying data"""
        data = {}
        
        # Fetch underlying data
        for symbol in self.strategy.symbols:
            # Underlying data
            ticker = yf.Ticker(symbol)
            hist = ticker.history(start=self.start_date, end=self.end_date)
            data[symbol] = hist
            
            # Option chain data (if available)
            try:
                options = ticker.option_chain()
                data[f"{symbol}_options"] = options
            except:
                print(f"No option data available for {symbol}")
                
        return pd.concat(data, axis=1)
        
    def calculate_option_price(self,
                             underlying_price: float,
                             strike_price: float,
                             time_to_expiry: float,
                             volatility: float,
                             option_type: str) -> float:
        """Calculate theoretical option price using Black-Scholes"""
        try:
            price = bs(option_type.lower(), underlying_price, strike_price,
                      time_to_expiry, self.risk_free_rate, volatility)
            return price
        except:
            return 0.0
            
    def calculate_greeks(self,
                        underlying_price: float,
                        strike_price: float,
                        time_to_expiry: float,
                        volatility: float,
                        option_type: str) -> Dict[str, float]:
        """Calculate option Greeks"""
        try:
            return {
                "delta": delta(option_type.lower(), underlying_price, strike_price,
                             time_to_expiry, self.risk_free_rate, volatility),
                "gamma": gamma(option_type.lower(), underlying_price, strike_price,
                             time_to_expiry, self.risk_free_rate, volatility),
                "theta": theta(option_type.lower(), underlying_price, strike_price,
                             time_to_expiry, self.risk_free_rate, volatility),
                "vega": vega(option_type.lower(), underlying_price, strike_price,
                           time_to_expiry, self.risk_free_rate, volatility)
            }
        except:
            return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
            
    def execute_trades(self, date: datetime, signals: Dict[str, Any]) -> None:
        """Execute option trades"""
        if "signals" not in signals or "data" not in signals:
            return
            
        for symbol, signal in signals["signals"].items():
            if signal != 0:
                # Get option details from signal
                option_data = signals.get("option_data", {}).get(symbol, {})
                if not option_data:
                    continue
                    
                strike = option_data.get("strike", 0)
                expiry = option_data.get("expiry", date)
                option_type = option_data.get("type", "call")
                volatility = option_data.get("volatility", 0.2)
                
                # Calculate time to expiry in years
                tte = (expiry - date).days / 365.0
                
                # Get underlying price
                underlying_price = signals["data"].loc[symbol, "Close"]
                
                # Calculate option price and greeks
                option_price = self.calculate_option_price(
                    underlying_price, strike, tte, volatility, option_type
                )
                greeks = self.calculate_greeks(
                    underlying_price, strike, tte, volatility, option_type
                )
                
                # Calculate position size
                position_value = self.current_capital * abs(signal)
                contracts = position_value / (option_price * 100)  # Each contract is 100 shares
                
                # Apply commission
                commission_cost = position_value * self.commission
                self.current_capital -= commission_cost
                
                # Record trade
                trade_id = f"{symbol}_{strike}_{expiry.strftime('%Y%m%d')}_{option_type}"
                self.trades.append({
                    "date": date,
                    "symbol": symbol,
                    "trade_id": trade_id,
                    "action": "BUY" if signal > 0 else "SELL",
                    "option_type": option_type,
                    "strike": strike,
                    "expiry": expiry,
                    "price": option_price,
                    "contracts": contracts,
                    "value": position_value,
                    "commission": commission_cost,
                    "underlying_price": underlying_price,
                    "return": 0,  # Will be updated later
                    **greeks  # Add greeks to trade record
                })
                
                # Update positions and greeks
                if trade_id in self.positions:
                    self.positions[trade_id] += contracts if signal > 0 else -contracts
                else:
                    self.positions[trade_id] = contracts if signal > 0 else -contracts
                    
                self.greeks[trade_id] = greeks
                
    def calculate_portfolio_value(self, date: datetime, data: pd.DataFrame) -> float:
        """Calculate current portfolio value including options"""
        portfolio_value = self.current_capital
        
        # Add value of all positions
        for trade_id, contracts in self.positions.items():
            if contracts != 0:
                # Parse trade_id
                symbol, strike, expiry, option_type = trade_id.split("_")
                strike = float(strike)
                expiry = datetime.strptime(expiry, "%Y%m%d")
                
                # Skip expired options
                if date > expiry:
                    continue
                    
                # Get current underlying price
                underlying_price = data.loc[symbol, "Close"]
                
                # Calculate time to expiry
                tte = (expiry - date).days / 365.0
                
                # Get implied volatility (using historical volatility as approximation)
                volatility = data.loc[symbol, "Close"].pct_change().std() * np.sqrt(252)
                
                # Calculate current option price
                option_price = self.calculate_option_price(
                    underlying_price, strike, tte, volatility, option_type
                )
                
                # Calculate position value
                position_value = contracts * option_price * 100
                portfolio_value += position_value
                
                # Update greeks
                self.greeks[trade_id] = self.calculate_greeks(
                    underlying_price, strike, tte, volatility, option_type
                )
                
                # Update trade returns
                if len(self.trades) > 0 and self.trades[-1]["trade_id"] == trade_id:
                    last_trade = self.trades[-1]
                    if last_trade["date"] != date:
                        returns = ((option_price - last_trade["price"]) / last_trade["price"]) * 100
                        self.trades[-1]["return"] = returns if last_trade["action"] == "BUY" else -returns
                
        return portfolio_value
        
    def plot_strategy_specific(self):
        """Plot option-specific analysis"""
        self._plot_greeks_analysis()
        self._plot_risk_analysis()
        
    def _plot_greeks_analysis(self):
        """Plot Greeks analysis"""
        trades_df = pd.DataFrame(self.trades)
        if len(trades_df) == 0:
            return
            
        # Create subplots for each Greek
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))
        
        # Plot Delta distribution
        sns.histplot(trades_df["delta"], ax=ax1)
        ax1.set_title("Delta Distribution")
        ax1.set_xlabel("Delta")
        
        # Plot Gamma distribution
        sns.histplot(trades_df["gamma"], ax=ax2)
        ax2.set_title("Gamma Distribution")
        ax2.set_xlabel("Gamma")
        
        # Plot Theta distribution
        sns.histplot(trades_df["theta"], ax=ax3)
        ax3.set_title("Theta Distribution")
        ax3.set_xlabel("Theta")
        
        # Plot Vega distribution
        sns.histplot(trades_df["vega"], ax=ax4)
        ax4.set_title("Vega Distribution")
        ax4.set_xlabel("Vega")
        
        plt.tight_layout()
        plt.show()
        
    def _plot_risk_analysis(self):
        """Plot risk analysis"""
        trades_df = pd.DataFrame(self.trades)
        if len(trades_df) == 0:
            return
            
        plt.figure(figsize=(15, 10))
        
        # Plot P&L by option type
        plt.subplot(2, 1, 1)
        sns.boxplot(x="option_type", y="return", data=trades_df)
        plt.title("P&L Distribution by Option Type")
        plt.xlabel("Option Type")
        plt.ylabel("Return (%)")
        
        # Plot P&L by moneyness
        plt.subplot(2, 1, 2)
        trades_df["moneyness"] = trades_df["underlying_price"] / trades_df["strike"]
        trades_df["moneyness_category"] = pd.cut(
            trades_df["moneyness"],
            bins=[0, 0.95, 1.05, float("inf")],
            labels=["OTM", "ATM", "ITM"]
        )
        sns.boxplot(x="moneyness_category", y="return", data=trades_df)
        plt.title("P&L Distribution by Moneyness")
        plt.xlabel("Moneyness")
        plt.ylabel("Return (%)")
        
        plt.tight_layout()
        plt.show()
