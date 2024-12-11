"""
期权交易策略分析仪表板
使用Dash构建交互式可视化界面
"""
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from ..data.option_data import OptionDataHandler
from ..data.event_analyzer import EventAnalyzer

class OptionDashboard:
    """期权分析仪表板"""
    
    def __init__(self):
        """初始化Dash应用"""
        self.app = dash.Dash(__name__, 
                           suppress_callback_exceptions=True,
                           external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])
        
        self.db_handler = OptionDataHandler()
        self.event_analyzer = EventAnalyzer()
        
        self.setup_layout()
        self.setup_callbacks()
    
    def setup_layout(self):
        """设置仪表板布局"""
        self.app.layout = html.Div([
            # 顶部导航栏
            html.Div([
                html.H1("期权交易策略分析平台", 
                       style={'margin-bottom': '0px', 'color': '#2c3e50'}),
                html.Hr()
            ], style={'margin-bottom': '20px'}),
            
            # 主控制面板
            html.Div([
                html.Div([
                    # 股票选择
                    html.Label("选择股票"),
                    dcc.Dropdown(
                        id='stock-selector',
                        options=[
                            {'label': 'AAPL', 'value': 'AAPL'},
                            {'label': 'TSLA', 'value': 'TSLA'},
                            {'label': 'GOOGL', 'value': 'GOOGL'}
                        ],
                        value='AAPL'
                    ),
                    
                    # 日期范围选择
                    html.Label("选择日期范围"),
                    dcc.DatePickerRange(
                        id='date-range',
                        start_date=datetime.now() - timedelta(days=30),
                        end_date=datetime.now(),
                        display_format='YYYY-MM-DD'
                    ),
                    
                    # 分析模式选择
                    html.Label("分析模式"),
                    dcc.Dropdown(
                        id='analysis-mode',
                        options=[
                            {'label': '期权链分析', 'value': 'chain'},
                            {'label': '波动率分析', 'value': 'volatility'},
                            {'label': 'Greeks分析', 'value': 'greeks'},
                            {'label': '事件分析', 'value': 'events'}
                        ],
                        value='chain'
                    )
                ], style={'width': '20%', 'float': 'left', 'padding': '20px'}),
                
                # 主图表区域
                html.Div([
                    dcc.Graph(id='main-chart', style={'height': '600px'})
                ], style={'width': '80%', 'float': 'right'})
            ], style={'display': 'flex'}),
            
            # 下方详细信息面板
            html.Div([
                dcc.Tabs([
                    dcc.Tab(label='期权链数据', children=[
                        html.Div(id='option-chain-table')
                    ]),
                    dcc.Tab(label='Greeks分析', children=[
                        html.Div(id='greeks-analysis')
                    ]),
                    dcc.Tab(label='事件影响', children=[
                        html.Div(id='event-analysis')
                    ]),
                    dcc.Tab(label='策略建议', children=[
                        html.Div(id='strategy-suggestions')
                    ])
                ])
            ], style={'margin-top': '20px', 'clear': 'both'})
        ])
    
    def setup_callbacks(self):
        """设置交互回调"""
        @self.app.callback(
            Output('main-chart', 'figure'),
            [Input('stock-selector', 'value'),
             Input('date-range', 'start_date'),
             Input('date-range', 'end_date'),
             Input('analysis-mode', 'value')]
        )
        def update_main_chart(symbol, start_date, end_date, mode):
            if mode == 'chain':
                return self._create_option_chain_chart(symbol, start_date, end_date)
            elif mode == 'volatility':
                return self._create_volatility_chart(symbol, start_date, end_date)
            elif mode == 'greeks':
                return self._create_greeks_surface(symbol, start_date, end_date)
            else:
                return self._create_event_impact_chart(symbol, start_date, end_date)
    
    def _create_option_chain_chart(self, symbol, start_date, end_date):
        """创建期权链可视化"""
        # 获取期权数据
        options_data = self.db_handler.query_option_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        # 创建3D散点图
        fig = go.Figure(data=[
            go.Scatter3d(
                x=options_data['strike'],
                y=options_data['days_to_expiry'],
                z=options_data['implied_volatility'],
                mode='markers',
                marker=dict(
                    size=5,
                    color=options_data['implied_volatility'],
                    colorscale='Viridis',
                    opacity=0.8
                ),
                text=[f"Strike: {s}<br>DTR: {d}<br>IV: {iv:.2%}" 
                      for s, d, iv in zip(options_data['strike'],
                                        options_data['days_to_expiry'],
                                        options_data['implied_volatility'])],
                hoverinfo='text'
            )
        ])
        
        fig.update_layout(
            title=f"{symbol} 期权链分析",
            scene=dict(
                xaxis_title="行权价",
                yaxis_title="到期时间",
                zaxis_title="隐含波动率"
            ),
            margin=dict(l=0, r=0, b=0, t=30)
        )
        
        return fig
    
    def _create_volatility_chart(self, symbol, start_date, end_date):
        """创建波动率分析图表"""
        # 获取数据
        data = self.db_handler.query_option_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        # 创建波动率锥图
        fig = go.Figure()
        
        # 添加实际波动率
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data['realized_volatility'],
            name='实际波动率',
            line=dict(color='blue')
        ))
        
        # 添加隐含波动率
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data['implied_volatility'],
            name='隐含波动率',
            line=dict(color='red')
        ))
        
        # 添加波动率锥
        percentiles = [25, 50, 75]
        for p in percentiles:
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[f'vol_percentile_{p}'],
                name=f'{p}分位数',
                line=dict(dash='dash')
            ))
        
        fig.update_layout(
            title=f"{symbol} 波动率分析",
            xaxis_title="日期",
            yaxis_title="波动率",
            legend_title="指标",
            hovermode='x unified'
        )
        
        return fig
    
    def _create_greeks_surface(self, symbol, start_date, end_date):
        """创建Greeks曲面图"""
        # 获取数据
        data = self.db_handler.query_option_data(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        # 创建Delta曲面
        fig = go.Figure(data=[
            go.Surface(
                x=data['strike'].unique(),
                y=data['days_to_expiry'].unique(),
                z=data.pivot(
                    index='days_to_expiry',
                    columns='strike',
                    values='delta'
                ).values,
                colorscale='RdYlBu',
                name='Delta曲面'
            )
        ])
        
        fig.update_layout(
            title=f"{symbol} Delta曲面",
            scene=dict(
                xaxis_title="行权价",
                yaxis_title="到期时间",
                zaxis_title="Delta"
            ),
            margin=dict(l=0, r=0, b=0, t=30)
        )
        
        return fig
    
    def _create_event_impact_chart(self, symbol, start_date, end_date):
        """创建事件影响分析图"""
        # 获取事件数据
        events = self.event_analyzer.get_events(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date
        )
        
        # 创建事件影响图
        fig = go.Figure()
        
        # 添加IV变化
        fig.add_trace(go.Scatter(
            x=events['date'],
            y=events['iv_change'],
            name='IV变化',
            mode='markers+lines',
            marker=dict(
                size=10,
                color=events['impact_score'],
                colorscale='Viridis',
                showscale=True
            )
        ))
        
        # 添加事件标记
        for _, event in events.iterrows():
            fig.add_annotation(
                x=event['date'],
                y=event['iv_change'],
                text=event['event_type'],
                showarrow=True,
                arrowhead=1
            )
        
        fig.update_layout(
            title=f"{symbol} 事件影响分析",
            xaxis_title="日期",
            yaxis_title="IV变化",
            showlegend=True
        )
        
        return fig
    
    def run_server(self, debug=True, port=8050):
        """运行Dash服务器"""
        self.app.run_server(debug=debug, port=port)

if __name__ == "__main__":
    dashboard = OptionDashboard()
    dashboard.run_server()
