"""
使用backtrader进行回测并用Dash进行可视化展示
"""
import backtrader as bt
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from datetime import datetime
import yfinance as yf

class VolatilityStrategy(bt.Strategy):
    """基于波动率的交易策略"""
    params = (
        ('lookback', 20),  # 用于计算波动率的回看期
        ('zscore_threshold', 2.0),  # z-score阈值
    )
    
    def __init__(self):
        self.volatility = bt.indicators.StdDev(
            self.data.close, 
            period=self.params.lookback,
            annualize=True
        )
        self.zscore = bt.indicators.ZScore(self.volatility, period=self.params.lookback)
        
    def next(self):
        if not self.position:  # 没有持仓
            if self.zscore > self.params.zscore_threshold:  # 波动率异常高
                self.sell()  # 做空
            elif self.zscore < -self.params.zscore_threshold:  # 波动率异常低
                self.buy()  # 做多
        else:  # 有持仓
            if abs(self.zscore) < 0.5:  # 波动率回归
                self.close()  # 平仓

class BacktestDashboard:
    def __init__(self):
        self.app = dash.Dash(__name__)
        self.setup_layout()
        self.results = None
        
    def setup_layout(self):
        """设置Dash应用布局"""
        self.app.layout = html.Div([
            html.H1('交易策略回测面板'),
            
            html.Div([
                html.Label('股票代码'),
                dcc.Input(
                    id='symbol-input',
                    value='AAPL',
                    type='text'
                ),
                
                html.Label('开始日期'),
                dcc.DatePickerSingle(
                    id='start-date',
                    date=datetime(2020, 1, 1)
                ),
                
                html.Label('结束日期'),
                dcc.DatePickerSingle(
                    id='end-date',
                    date=datetime.now()
                ),
                
                html.Button('运行回测', id='run-backtest', n_clicks=0)
            ], style={'margin': '20px'}),
            
            html.Div([
                dcc.Graph(id='equity-curve'),
                dcc.Graph(id='drawdown-chart'),
                dcc.Graph(id='volatility-chart')
            ])
        ])
        
        self.setup_callbacks()
        
    def setup_callbacks(self):
        """设置回调函数"""
        @self.app.callback(
            [Output('equity-curve', 'figure'),
             Output('drawdown-chart', 'figure'),
             Output('volatility-chart', 'figure')],
            [Input('run-backtest', 'n_clicks')],
            [dash.dependencies.State('symbol-input', 'value'),
             dash.dependencies.State('start-date', 'date'),
             dash.dependencies.State('end-date', 'date')]
        )
        def update_charts(n_clicks, symbol, start_date, end_date):
            if n_clicks == 0:
                return {}, {}, {}
                
            # 运行回测
            results = self.run_backtest(symbol, start_date, end_date)
            
            # 创建图表
            equity_fig = go.Figure()
            equity_fig.add_trace(go.Scatter(
                x=results.index,
                y=results['portfolio_value'],
                mode='lines',
                name='Portfolio Value'
            ))
            equity_fig.update_layout(title='权益曲线')
            
            drawdown_fig = go.Figure()
            drawdown_fig.add_trace(go.Scatter(
                x=results.index,
                y=results['drawdown'],
                mode='lines',
                name='Drawdown',
                fill='tozeroy'
            ))
            drawdown_fig.update_layout(title='回撤')
            
            volatility_fig = go.Figure()
            volatility_fig.add_trace(go.Scatter(
                x=results.index,
                y=results['volatility'],
                mode='lines',
                name='Volatility'
            ))
            volatility_fig.update_layout(title='波动率')
            
            return equity_fig, drawdown_fig, volatility_fig
            
    def run_backtest(self, symbol, start_date, end_date):
        """运行回测"""
        # 获取数据
        data = yf.download(symbol, start=start_date, end=end_date)
        
        # 创建cerebro实例
        cerebro = bt.Cerebro()
        
        # 添加数据
        data_feed = bt.feeds.PandasData(dataname=data)
        cerebro.adddata(data_feed)
        
        # 设置初始资金
        cerebro.broker.setcash(100000.0)
        
        # 设置手续费
        cerebro.broker.setcommission(commission=0.001)
        
        # 添加策略
        cerebro.addstrategy(VolatilityStrategy)
        
        # 添加分析器
        cerebro.addanalyzer(bt.analyzers.DrawDown)
        cerebro.addanalyzer(bt.analyzers.Returns)
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer)
        
        # 运行回测
        results = cerebro.run()
        
        # 提取结果
        portfolio_value = pd.Series(cerebro.broker.getvalue())
        drawdown = pd.Series(results[0].analyzers.drawdown.get_analysis()['drawdown'])
        volatility = pd.Series(results[0].analyzers.returns.get_analysis()['volatility'])
        
        return pd.DataFrame({
            'portfolio_value': portfolio_value,
            'drawdown': drawdown,
            'volatility': volatility
        })
        
    def run_server(self, debug=True):
        """运行Dash服务器"""
        self.app.run_server(debug=debug)

if __name__ == '__main__':
    dashboard = BacktestDashboard()
    dashboard.run_server()
