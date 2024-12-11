"""
Example script demonstrating the QLib Option Strategy.
"""

from datetime import datetime, timedelta
from tradetools.strategies.qlib_option_strategy import QlibOptionStrategy
from tradetools.utils.data_utils import DataManager

def main():
    # Initialize data manager
    data_manager = DataManager(region='us')
    
    # Get Nasdaq 100 symbols
    symbols = data_manager.get_nasdaq100_symbols()
    
    # Initialize strategy
    strategy = QlibOptionStrategy(
        stock_pool=symbols,
        lookback_period=252,      # 1 year of trading days
        prediction_period=5,      # 5-day prediction window
        top_k=2,                 # Top 2 stocks
        min_leverage=2.0,        # Minimum option leverage
        max_option_price=2.5,    # Maximum option price in dollars
        stop_loss=0.5,          # 50% stop loss
        take_profit=[1.5, 2.0], # 150% and 200% take profit levels
        initial_capital=10000,
        weekly_investment=500
    )
    
    # Get option trading signals
    signals = strategy.get_option_signals()
    
    if not signals:
        print("No valid trading signals found")
        return
        
    # Calculate position sizes
    position_sizes = strategy.calculate_position_sizes(signals)
    
    # Apply risk management
    position_sizes = strategy.manage_risk(position_sizes)
    
    # Print trading signals and positions
    print("\nTrading Signals:")
    print("-" * 50)
    
    for signal in signals:
        position_size = position_sizes[signal['symbol']]
        print(f"Symbol: {signal['symbol']}")
        print(f"Direction: {signal['direction']}")
        print(f"Strike: ${signal['strike']:.2f}")
        print(f"Expiry: {signal['expiry'].strftime('%Y-%m-%d')}")
        print(f"Leverage: {signal['leverage']:.2f}x")
        print(f"Score: {signal['score']:.4f}")
        print(f"Position Size: ${position_size:.2f}")
        print("-" * 50)
        
    # Execute trades
    strategy.execute_trades(signals, position_sizes)
    
    # Monitor positions
    strategy.monitor_positions()

if __name__ == "__main__":
    main()
