"""
从Polygon下载股票和期权数据的示例脚本
"""
import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载环境变量
load_dotenv()

from data_sources.polygon_client import PolygonClient

def download_stock_data(client, ticker, from_date, to_date):
    """下载股票数据"""
    print(f"\n下载 {ticker} 的股票数据...")
    
    # 下载日线数据
    print("下载日线数据...")
    daily_data = client.batch_download_stocks(
        tickers=[ticker],
        from_date=from_date,
        to_date=to_date,
        timespan=client.TIMESPAN_1DAY,
        save_to_db=True
    )
    
    # 下载分钟线数据
    print("下载分钟线数据...")
    minute_data = client.batch_download_stocks(
        tickers=[ticker],
        from_date=from_date,
        to_date=to_date,
        timespan=client.TIMESPAN_1MIN,
        save_to_db=True
    )
    
    return daily_data, minute_data

def download_options_data(client, underlying_ticker, from_date, to_date):
    """下载期权数据"""
    print(f"\n下载 {underlying_ticker} 的期权数据...")
    
    # 获取期权合约列表
    print("获取期权合约列表...")
    contracts = client.list_options(
        underlying_ticker=underlying_ticker,
        expiration_date_gte=from_date,
        expiration_date_lte=to_date
    )
    
    if not contracts:
        print(f"未找到 {underlying_ticker} 的期权合约")
        return None
    
    print(f"找到 {len(contracts)} 个期权合约")
    
    # 下载期权数据
    print("下载期权行情数据...")
    options_data = []
    for contract in contracts[:5]:  # 为了示例，这里只下载前5个合约
        contract_data = client.download_option_trades(
            contract_ticker=contract['ticker'],
            from_date=from_date,
            to_date=to_date
        )
        if contract_data is not None:
            options_data.append(contract_data)
    
    return options_data

def main():
    # 初始化Polygon客户端
    client = PolygonClient()  # 将自动从.env文件中读取POLYGON_API_KEY
    
    # 设置下载参数
    ticker = 'AAPL'  # 以苹果公司为例
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')  # 下载最近7天的数据
    
    try:
        # 下载股票数据
        stock_daily, stock_minute = download_stock_data(
            client=client,
            ticker=ticker,
            from_date=start_date,
            to_date=end_date
        )
        
        # 下载期权数据
        options_data = download_options_data(
            client=client,
            underlying_ticker=ticker,
            from_date=start_date,
            to_date=end_date
        )
        
        # 打印统计信息
        print("\n数据下载完成！")
        print(f"股票日线数据条数: {len(stock_daily) if stock_daily else 0}")
        print(f"股票分钟线数据条数: {len(stock_minute) if stock_minute else 0}")
        print(f"期权数据集数量: {len(options_data) if options_data else 0}")
        
    except Exception as e:
        print(f"下载数据时发生错误: {str(e)}")

if __name__ == "__main__":
    main()
