"""
回测引擎
"""
from typing import List, Dict, Optional, Union
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ..strategies.option_strategy_base import OptionStrategyBase

class BacktestEngine:
    """回测引擎"""
    
    def __init__(
        self,
        strategy: OptionStrategyBase,
        start_date: datetime,
        end_date: datetime,
        initial_capital: float = 1000000,
        transaction_cost: float = 0.0001
    ):
        """
        Args:
            strategy: 策略实例
            start_date: 回测开始日期
            end_date: 回测结束日期
            initial_capital: 初始资金
            transaction_cost: 交易成本率
        """
        self.strategy = strategy
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        
        # 回测结果
        self.equity_curve = []
        self.positions_history = []
        self.trades_history = []
        self.metrics = {}
        
    def load_market_data(self, data_loader) -> pd.DataFrame:
        """加载市场数据"""
        return data_loader.load_data(self.start_date, self.end_date)
        
    def calculate_transaction_cost(self, trade_value: float) -> float:
        """计算交易成本"""
        return abs(trade_value) * self.transaction_cost
        
    def run_backtest(self, market_data: pd.DataFrame):
        """运行回测"""
        # 初始化
        current_capital = self.initial_capital
        current_positions = {}
        
        # 按时间顺序遍历数据
        for timestamp, data in market_data.groupby('timestamp'):
            # 1. 更新市场数据
            market_snapshot = self._prepare_market_snapshot(data)
            self.strategy.on_market_data(market_snapshot)
            
            # 2. 生成交易信号
            signals = self.strategy.generate_signals(market_snapshot)
            
            # 3. 执行交易
            for signal in signals:
                trades = self._execute_signal(signal, current_positions, current_capital)
                for trade in trades:
                    # 更新资金
                    trade_value = trade['quantity'] * trade['price']
                    transaction_cost = self.calculate_transaction_cost(trade_value)
                    current_capital -= (trade_value + transaction_cost)
                    
                    # 更新持仓
                    symbol = trade['symbol']
                    if symbol not in current_positions:
                        current_positions[symbol] = 0
                    current_positions[symbol] += trade['quantity']
                    
                    # 记录交易
                    trade['timestamp'] = timestamp
                    trade['transaction_cost'] = transaction_cost
                    self.trades_history.append(trade)
            
            # 4. 更新持仓市值
            portfolio_value = current_capital
            for symbol, quantity in current_positions.items():
                if quantity != 0:
                    current_price = market_snapshot['prices'][symbol]['price']
                    portfolio_value += quantity * current_price
            
            # 5. 记录权益曲线
            self.equity_curve.append({
                'timestamp': timestamp,
                'portfolio_value': portfolio_value,
                'cash': current_capital
            })
            
            # 6. 记录持仓
            self.positions_history.append({
                'timestamp': timestamp,
                'positions': current_positions.copy()
            })
            
        # 计算回测指标
        self._calculate_metrics()
        
    def _prepare_market_snapshot(self, data: pd.DataFrame) -> Dict:
        """准备市场数据快照"""
        snapshot = {
            'timestamp': data['timestamp'].iloc[0],
            'prices': {},
            'options': {}
        }
        
        # 整理标的价格数据
        for _, row in data[data['type'] == 'stock'].iterrows():
            snapshot['prices'][row['symbol']] = {
                'price': row['price'],
                'volume': row['volume']
            }
            
        # 整理期权数据
        for _, row in data[data['type'].isin(['call', 'put'])].iterrows():
            if row['symbol'] not in snapshot['options']:
                snapshot['options'][row['symbol']] = {
                    'implied_volatility': row['implied_volatility'],
                    'options': []
                }
            snapshot['options'][row['symbol']]['options'].append({
                'type': row['type'],
                'strike': row['strike'],
                'expiry': row['expiry'],
                'price': row['price'],
                'delta': row['delta'],
                'gamma': row['gamma'],
                'vega': row['vega'],
                'theta': row['theta'],
                'rho': row['rho']
            })
            
        return snapshot
        
    def _execute_signal(
        self,
        signal: Dict,
        current_positions: Dict,
        current_capital: float
    ) -> List[Dict]:
        """执行交易信号"""
        trades = []
        
        if signal['type'] == 'volatility':
            options = signal['options']
            # 计算目标持仓
            position_sizes, _ = self.strategy.build_delta_neutral_portfolio(options)
            
            # 生成交易指令
            for size, option in zip(position_sizes, options):
                symbol = f"{option['type']}_{option['strike']}_{option['expiry']}"
                current_size = current_positions.get(symbol, 0)
                trade_size = size - current_size
                
                if trade_size != 0:
                    trades.append({
                        'symbol': symbol,
                        'quantity': trade_size,
                        'price': option['price'],
                        'type': option['type'],
                        'strike': option['strike'],
                        'expiry': option['expiry']
                    })
                    
        return trades
        
    def _calculate_metrics(self):
        """计算回测指标"""
        # 转换权益曲线为DataFrame
        equity_df = pd.DataFrame(self.equity_curve)
        returns = equity_df['portfolio_value'].pct_change()
        
        # 1. 收益指标
        total_return = (equity_df['portfolio_value'].iloc[-1] - self.initial_capital) / self.initial_capital
        annual_return = total_return * (252 / len(returns))
        sharpe_ratio = np.sqrt(252) * returns.mean() / returns.std()
        
        # 2. 风险指标
        max_drawdown = 0
        peak = equity_df['portfolio_value'].iloc[0]
        for value in equity_df['portfolio_value']:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            max_drawdown = max(max_drawdown, drawdown)
            
        # 3. 交易指标
        trades_df = pd.DataFrame(self.trades_history)
        if not trades_df.empty:
            win_trades = trades_df[trades_df['realized_pnl'] > 0]
            win_rate = len(win_trades) / len(trades_df)
            profit_factor = abs(win_trades['realized_pnl'].sum() / 
                              trades_df[trades_df['realized_pnl'] < 0]['realized_pnl'].sum())
        else:
            win_rate = 0
            profit_factor = 0
            
        self.metrics = {
            'total_return': total_return,
            'annual_return': annual_return,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            'total_trades': len(trades_df),
            'transaction_costs': trades_df['transaction_cost'].sum()
        }
        
    def plot_results(self):
        """绘制回测结果"""
        import matplotlib.pyplot as plt
        
        # 1. 绘制权益曲线
        equity_df = pd.DataFrame(self.equity_curve)
        plt.figure(figsize=(12, 6))
        plt.plot(equity_df['timestamp'], equity_df['portfolio_value'])
        plt.title('Equity Curve')
        plt.xlabel('Time')
        plt.ylabel('Portfolio Value')
        plt.grid(True)
        plt.show()
        
        # 2. 绘制回撤
        equity = equity_df['portfolio_value']
        peak = equity.expanding(min_periods=1).max()
        drawdown = (peak - equity) / peak
        
        plt.figure(figsize=(12, 6))
        plt.plot(equity_df['timestamp'], drawdown)
        plt.title('Drawdown')
        plt.xlabel('Time')
        plt.ylabel('Drawdown')
        plt.grid(True)
        plt.show()
        
        # 3. 打印指标
        print("\nBacktest Results:")
        print(f"Total Return: {self.metrics['total_return']:.2%}")
        print(f"Annual Return: {self.metrics['annual_return']:.2%}")
        print(f"Sharpe Ratio: {self.metrics['sharpe_ratio']:.2f}")
        print(f"Max Drawdown: {self.metrics['max_drawdown']:.2%}")
        print(f"Win Rate: {self.metrics['win_rate']:.2%}")
        print(f"Profit Factor: {self.metrics['profit_factor']:.2f}")
        print(f"Total Trades: {self.metrics['total_trades']}")
        print(f"Transaction Costs: ${self.metrics['transaction_costs']:.2f}")
