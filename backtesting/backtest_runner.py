"""
回测运行器
"""
from typing import Dict, List, Optional, Type, Union
import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from pathlib import Path

from .option_strategy import (
    BaseOptionStrategy,
    VolatilityStrategy,
    OptionData
)

class BacktestRunner:
    """回测运行器"""
    
    def __init__(self,
                 strategy_class: Type[BaseOptionStrategy],
                 data_dir: Path,
                 start_date: datetime,
                 end_date: datetime,
                 initial_cash: float = 1000000,
                 commission: float = 0.001):
        """
        Args:
            strategy_class: 策略类
            data_dir: 数据目录
            start_date: 开始日期
            end_date: 结束日期
            initial_cash: 初始资金
            commission: 手续费率
        """
        self.strategy_class = strategy_class
        self.data_dir = data_dir
        self.start_date = start_date
        self.end_date = end_date
        self.initial_cash = initial_cash
        self.commission = commission
        
        # 创建cerebro
        self.cerebro = bt.Cerebro()
        self.cerebro.broker.setcash(initial_cash)
        self.cerebro.broker.setcommission(commission=commission)
        
        # 添加分析器
        self.add_analyzers()
        
    def add_analyzers(self):
        """添加分析器"""
        # 回报分析
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio,
                                _name='sharpe',
                                riskfreerate=0.03,
                                annualize=True)
                                
        self.cerebro.addanalyzer(bt.analyzers.Returns,
                                _name='returns')
                                
        self.cerebro.addanalyzer(bt.analyzers.DrawDown,
                                _name='drawdown')
                                
        # 交易分析
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer,
                                _name='trades')
                                
    def load_data(self) -> None:
        """加载数据"""
        # 加载标的数据
        underlying_df = pd.read_csv(
            self.data_dir / 'underlying.csv',
            parse_dates=['date'],
            index_col='date'
        )
        
        data = bt.feeds.PandasData(
            dataname=underlying_df,
            fromdate=self.start_date,
            todate=self.end_date
        )
        self.cerebro.adddata(data)
        
        # 加载期权数据
        options_df = pd.read_csv(
            self.data_dir / 'options.csv',
            parse_dates=['date', 'expiry']
        )
        
        # 按到期日分组添加期权数据
        for expiry, group in options_df.groupby('expiry'):
            opt_data = OptionData(
                dataname=group.set_index('date'),
                fromdate=self.start_date,
                todate=self.end_date
            )
            self.cerebro.adddata(opt_data)
            
    def run(self) -> Dict:
        """运行回测
        
        Returns:
            回测结果统计
        """
        # 添加策略
        self.cerebro.addstrategy(self.strategy_class)
        
        # 运行回测
        results = self.cerebro.run()
        strat = results[0]
        
        # 收集结果
        stats = {}
        
        # 回报统计
        stats['initial_value'] = self.initial_cash
        stats['final_value'] = self.cerebro.broker.getvalue()
        stats['total_return'] = (
            stats['final_value'] / stats['initial_value'] - 1
        )
        stats['sharpe_ratio'] = strat.analyzers.sharpe.get_analysis()['sharperatio']
        
        # 回撤统计
        dd_analysis = strat.analyzers.drawdown.get_analysis()
        stats['max_drawdown'] = dd_analysis['max']['drawdown']
        stats['max_drawdown_len'] = dd_analysis['max']['len']
        
        # 交易统计
        trade_analysis = strat.analyzers.trades.get_analysis()
        stats['total_trades'] = trade_analysis['total']['total']
        stats['win_rate'] = (
            trade_analysis['won']['total'] / stats['total_trades']
            if stats['total_trades'] > 0 else 0
        )
        stats['avg_trade_length'] = (
            trade_analysis['len']['average']
            if stats['total_trades'] > 0 else 0
        )
        
        return stats
        
    def plot(self, filename: Optional[str] = None):
        """绘制回测结果
        
        Args:
            filename: 保存文件名，如果为None则显示图像
        """
        plt.figure(figsize=(15, 10))
        
        # 绘制权益曲线
        self.cerebro.plot(style='candlestick')
        
        if filename:
            plt.savefig(filename)
        else:
            plt.show()
            
def run_volatility_strategy(
    data_dir: Union[str, Path],
    start_date: datetime,
    end_date: datetime,
    vol_entry_z: float = 2.0,
    vol_exit_z: float = 0.0,
    lookback: int = 20,
    min_hold_days: int = 5,
    initial_cash: float = 1000000,
    commission: float = 0.001
) -> Dict:
    """运行波动率策略回测
    
    Args:
        data_dir: 数据目录
        start_date: 开始日期
        end_date: 结束日期
        vol_entry_z: 波动率入场Z分数
        vol_exit_z: 波动率出场Z分数
        lookback: 回看周期
        min_hold_days: 最小持有天数
        initial_cash: 初始资金
        commission: 手续费率
        
    Returns:
        回测统计结果
    """
    # 创建回测运行器
    runner = BacktestRunner(
        strategy_class=VolatilityStrategy,
        data_dir=Path(data_dir),
        start_date=start_date,
        end_date=end_date,
        initial_cash=initial_cash,
        commission=commission
    )
    
    # 设置策略参数
    runner.cerebro.addstrategy(
        VolatilityStrategy,
        vol_entry_z=vol_entry_z,
        vol_exit_z=vol_exit_z,
        lookback=lookback,
        min_hold_days=min_hold_days
    )
    
    # 加载数据
    runner.load_data()
    
    # 运行回测
    stats = runner.run()
    
    # 绘制结果
    runner.plot()
    
    return stats
