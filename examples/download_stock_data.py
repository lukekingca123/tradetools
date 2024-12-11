"""
从Polygon下载股票数据的示例脚本
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_sources.polygon_client import PolygonClient

def download_multiple_timeframes(client, tickers, from_date, to_date):
    """下载多个时间周期的数据"""
    
    # 下载5分钟线
    print("\n下载5分钟线数据...")
    data_5min = client.batch_download_stocks(
        tickers=tickers,
        from_date=from_date,
        to_date=to_date,
        timespan=client.TIMESPAN_5MIN,
        save_to_db=True
    )
    
    # 下载60分钟线
    print("\n下载60分钟线数据...")
    data_60min = client.batch_download_stocks(
        tickers=tickers,
        from_date=from_date,
        to_date=to_date,
        timespan=client.TIMESPAN_60MIN,
        save_to_db=True
    )
    
    # 下载日线
    print("\n下载日线数据...")
    data_daily = client.batch_download_stocks(
        tickers=tickers,
        from_date=from_date,
        to_date=to_date,
        timespan=client.TIMESPAN_1DAY,
        save_to_db=True
    )
    
    return data_5min, data_60min, data_daily

def main():
    # 设置API密钥
    os.environ['POLYGON_API_KEY'] = "3SExIweoFS0x2aFnFae2TaYFGzEi_RZs"
    client = PolygonClient()
    
    # 要下载的股票列表
    tickers = ['AAPL']
    
    print("开始下载数据...")
    try:
        # 下载多个时间周期的数据
        data_5min, data_60min, data_daily = download_multiple_timeframes(
            client=client,
            tickers=tickers,
            from_date='2023-12-01',  # 使用较短的时间段进行测试
            to_date='2023-12-31'
        )
        
        # 打印统计信息
        for timeframe, data in [
            ("5分钟", data_5min),
            ("60分钟", data_60min),
            ("日线", data_daily)
        ]:
            print(f"\n{timeframe}数据统计:")
            for ticker, df in data.items():
                print(f"\n{ticker}:")
                print(f"数据范围: {df.index.min()} 到 {df.index.max()}")
                print(f"记录数: {len(df)}")
                print("\n样本数据:")
                print(df.head())
            
    except Exception as e:
        print(f"Error: {str(e)}")
        
if __name__ == "__main__":
    main()
