"""
回测结果可视化模块
将backtrader的回测结果转换为更美观的图表
"""
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional, Union, Tuple

class BacktestVisualizer:
    """回测结果可视化器"""
    
    def __init__(self, results: pd.DataFrame):
        """
        初始化可视化器
        
        Args:
            results: 回测结果数据框
        """
        self.results = results
        
    def plot_equity_curve(self) -> go.Figure:
        """绘制权益曲线"""
        fig = go.Figure()
        
        # 添加权益曲线
        fig.add_trace(go.Scatter(
            x=self.results.index,
            y=self.results['portfolio_value'],
            name='Portfolio Value',
            line=dict(color='#2ecc71', width=2)
        ))
        
        # 添加回撤阴影
        drawdown = self.results['drawdown']
        fig.add_trace(go.Scatter(
            x=self.results.index,
            y=self.results['portfolio_value'] * (1 - drawdown),
            fill='tonexty',
            fillcolor='rgba(231, 76, 60, 0.2)',
            line=dict(color='rgba(231, 76, 60, 0.5)'),
            name='Drawdown'
        ))
        
        # 更新布局
        fig.update_layout(
            title='Portfolio Performance',
            xaxis_title='Date',
            yaxis_title='Value',
            template='plotly_white',
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01
            ),
            margin=dict(l=0, r=0, t=30, b=0)
        )
        
        return fig
    
    def plot_returns_analysis(self) -> go.Figure:
        """绘制收益分析图"""
        # 创建子图
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=(
                'Daily Returns Distribution',
                'Rolling Volatility',
                'Rolling Sharpe Ratio',
                'Rolling Beta'
            )
        )
        
        # 1. 日收益分布
        daily_returns = self.results['returns']
        fig.add_trace(
            go.Histogram(
                x=daily_returns,
                nbinsx=50,
                name='Daily Returns',
                marker_color='#3498db'
            ),
            row=1, col=1
        )
        
        # 2. 滚动波动率
        rolling_vol = self.results['rolling_volatility']
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=rolling_vol,
                name='30D Volatility',
                line=dict(color='#e74c3c')
            ),
            row=1, col=2
        )
        
        # 3. 滚动夏普比率
        rolling_sharpe = self.results['rolling_sharpe']
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=rolling_sharpe,
                name='30D Sharpe',
                line=dict(color='#2ecc71')
            ),
            row=2, col=1
        )
        
        # 4. 滚动贝塔
        rolling_beta = self.results['rolling_beta']
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=rolling_beta,
                name='30D Beta',
                line=dict(color='#9b59b6')
            ),
            row=2, col=2
        )
        
        # 更新布局
        fig.update_layout(
            height=800,
            showlegend=True,
            template='plotly_white',
            title_text="Strategy Analysis"
        )
        
        return fig
    
    def plot_position_analysis(self) -> go.Figure:
        """绘制持仓分析图"""
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Position Size', 'Position Value'),
            shared_xaxes=True,
            vertical_spacing=0.1
        )
        
        # 持仓数量
        fig.add_trace(
            go.Bar(
                x=self.results.index,
                y=self.results['position_size'],
                name='Position Size',
                marker_color='#3498db'
            ),
            row=1, col=1
        )
        
        # 持仓市值
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=self.results['position_value'],
                name='Position Value',
                line=dict(color='#2ecc71')
            ),
            row=2, col=1
        )
        
        # 更新布局
        fig.update_layout(
            height=600,
            showlegend=True,
            template='plotly_white',
            title_text="Position Analysis"
        )
        
        return fig
    
    def plot_greeks_exposure(self) -> go.Figure:
        """绘制Greeks敞口分析图"""
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Delta Exposure', 'Gamma Exposure', 
                          'Theta Exposure', 'Vega Exposure')
        )
        
        # Delta敞口
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=self.results['delta_exposure'],
                name='Delta',
                line=dict(color='#3498db')
            ),
            row=1, col=1
        )
        
        # Gamma敞口
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=self.results['gamma_exposure'],
                name='Gamma',
                line=dict(color='#e74c3c')
            ),
            row=1, col=2
        )
        
        # Theta敞口
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=self.results['theta_exposure'],
                name='Theta',
                line=dict(color='#2ecc71')
            ),
            row=2, col=1
        )
        
        # Vega敞口
        fig.add_trace(
            go.Scatter(
                x=self.results.index,
                y=self.results['vega_exposure'],
                name='Vega',
                line=dict(color='#9b59b6')
            ),
            row=2, col=2
        )
        
        # 更新布局
        fig.update_layout(
            height=800,
            showlegend=True,
            template='plotly_white',
            title_text="Greeks Exposure Analysis"
        )
        
        return fig
    
    def plot_all(self) -> Dict[str, go.Figure]:
        """生成所有分析图表"""
        return {
            'equity': self.plot_equity_curve(),
            'returns': self.plot_returns_analysis(),
            'position': self.plot_position_analysis(),
            'greeks': self.plot_greeks_exposure()
        }
    
    def save_html_report(self, filename: str = 'backtest_report.html'):
        """保存HTML分析报告"""
        figures = self.plot_all()
        
        # 创建HTML报告
        html_content = """
        <html>
        <head>
            <title>Backtest Analysis Report</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        </head>
        <body>
        <h1>Backtest Analysis Report</h1>
        """
        
        # 添加每个图表
        for name, fig in figures.items():
            html_content += f"<h2>{name.title()} Analysis</h2>"
            html_content += fig.to_html(full_html=False)
        
        html_content += "</body></html>"
        
        # 保存文件
        with open(filename, 'w') as f:
            f.write(html_content)
