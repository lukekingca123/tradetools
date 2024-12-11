"""
将股票数据导入到DolphinDB的脚本 - 版本3
使用DolphinDB官方推荐的数据导入方式
"""
import os
import sys
import pandas as pd
import numpy as np
import dolphindb as ddb
from datetime import datetime
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def import_stock_data(csv_file: str, host: str = "localhost", port: int = 8848):
    """
    将股票数据导入到DolphinDB
    
    Args:
        csv_file: CSV文件路径
        host: DolphinDB服务器地址
        port: DolphinDB端口
    """
    # 连接DolphinDB
    conn = ddb.session()
    conn.connect(host, port, "admin", "123456")
    
    # 读取CSV数据
    print(f"Reading data from {csv_file}...")
    df = pd.read_csv(csv_file)
    
    # 处理日期时间
    df['date'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y.%m.%d')
    
    # 计算amount (volume * vwap)
    df['amount'] = df['volume'] * df['vwap']
    
    try:
        # 将数据写入DolphinDB
        print("Writing data to DolphinDB...")
        
        # 创建数据库目录
        print("Creating database schema...")
        conn.run("""
        dbPath = "dfs://market"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
            
        // 创建复合分区数据库
        db1 = database("", HASH, [SYMBOL, 10])
        db2 = database("", VALUE, 2010.01.01..2030.12.31)
        db = database(dbPath, COMPO, [db1, db2])
        
        // 创建表结构
        schema = table(
            1:0, `symbol`date`open`high`low`close`volume`amount`factor,
            [SYMBOL, DATE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE]
        )
        
        // 创建分布式表
        pt = createPartitionedTable(db, schema, `stock_daily, `symbol`date)
        """)
        
        # 分批处理数据
        batch_size = 1000
        total_rows = len(df)
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            print(f"Processing batch {i//batch_size + 1} of {(total_rows-1)//batch_size + 1}")
            
            # 准备批次数据
            data = {
                "symbol": batch_df['symbol'].values,
                "date": batch_df['date'].values,
                "open": batch_df['open'].values,
                "high": batch_df['high'].values,
                "low": batch_df['low'].values,
                "close": batch_df['close'].values,
                "volume": batch_df['volume'].values.astype(np.int64),
                "amount": batch_df['amount'].values,
                "factor": np.ones(len(batch_df), dtype=np.float64)
            }
            
            # 上传数据到DolphinDB服务器
            conn.upload({"data": data})
            
            # 插入数据到表中
            conn.run("""
            t = table(
                data.symbol as symbol,
                temporalParse(data.date, "yyyy.MM.dd") as date,
                data.open as open,
                data.high as high,
                data.low as low,
                data.close as close,
                data.volume as volume,
                data.amount as amount,
                data.factor as factor
            )
            loadTable("dfs://market", "stock_daily").append!(t)
            """)
            
            print(f"Imported {min(i+batch_size, total_rows)} of {total_rows} rows")
        
        print("Data successfully imported to DolphinDB!")
        
        # 验证导入的数据
        print("\nVerifying imported data...")
        count = conn.run("""
        select count(*) from loadTable('dfs://market', 'stock_daily')
        where symbol = 'AAPL'
        """)
        print(f"Total AAPL records in database: {count}")
        
        # 显示一些示例数据
        print("\nSample AAPL data:")
        sample = conn.run("""
        select top 5 * from loadTable('dfs://market', 'stock_daily')
        where symbol = 'AAPL'
        order by date desc
        """)
        print(sample)
        
    except Exception as e:
        print(f"Error importing data: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    # 导入AAPL数据
    csv_file = "data/stocks/AAPL.csv"
    import_stock_data(csv_file)

if __name__ == "__main__":
    main()
