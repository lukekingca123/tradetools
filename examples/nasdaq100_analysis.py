"""
NASDAQ 100 股票数据分析示例
"""
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import plotly.io as pio
import numpy as np

def analyze_nasdaq100():
    # 生成一些测试数据
    symbols = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL"]  # 为了简单起见，只用5只股票
    dates = pd.date_range(start="2024-10-27", end="2024-11-26", freq="D")
    
    data = []
    for symbol in symbols:
        base_price = np.random.uniform(100, 1000)  # 随机基准价格
        daily_returns = np.random.normal(0.001, 0.02, len(dates))  # 每日收益率
        prices = base_price * np.exp(np.cumsum(daily_returns))  # 生成价格序列
        
        for i, date in enumerate(dates):
            data.append({
                "symbol": symbol,
                "date": date,
                "open": prices[i] * (1 + np.random.normal(0, 0.005)),
                "high": prices[i] * (1 + abs(np.random.normal(0, 0.01))),
                "low": prices[i] * (1 - abs(np.random.normal(0, 0.01))),
                "close": prices[i],
                "volume": int(np.random.uniform(1000000, 10000000)),
                "amount": prices[i] * np.random.uniform(1000000, 10000000),
                "factor": 1.0
            })
    
    # 将测试数据转换为DataFrame
    df = pd.DataFrame(data)
    
    # 按股票代码和日期排序
    df = df.sort_values(['symbol', 'date'])
    
    # 计算每只股票的收益率
    df['returns'] = df.groupby('symbol')['close'].pct_change()
    
    # 计算每只股票的累计收益率
    df['cumulative_returns'] = df.groupby('symbol')['returns'].transform(
        lambda x: (1 + x).cumprod() - 1
    )
    
    # 获取每只股票的最终累计收益率
    latest_returns = df.groupby('symbol')['cumulative_returns'].last()
    
    # 获取表现最好的10只股票
    top_10_symbols = latest_returns.nlargest(10)
    
    # 创建收益率图表
    fig = go.Figure()
    for symbol in top_10_symbols.index:
        stock_data = df[df['symbol'] == symbol]
        fig.add_trace(go.Scatter(
            x=stock_data['date'],
            y=stock_data['cumulative_returns'],
            name=symbol,
            mode='lines'
        ))
    
    fig.update_layout(
        title=f'NASDAQ 100 Top Performers (2024-10-27 - 2024-11-26)',
        xaxis_title='Date',
        yaxis_title='Cumulative Returns',
        template='plotly_white',
        showlegend=True
    )
    
    # 保存图表
    pio.write_html(fig, 'nasdaq100_top_performers.html')
    print(f"交互式图表已保存为 nasdaq100_top_performers.html")
    
    # 打印表现最好的10只股票的收益率
    print("\nNASDAQ 100 表现最好的10只股票:")
    print(top_10_symbols.round(4) * 100)

if __name__ == "__main__":
    analyze_nasdaq100()
