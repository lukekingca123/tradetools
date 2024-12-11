"""
将股票数据导入到DolphinDB的脚本
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
        
        # 准备数据
        data = {
            "symbol": df['symbol'].tolist(),
            "date": df['date'].tolist(),
            "open": df['open'].tolist(),
            "high": df['high'].tolist(),
            "low": df['low'].tolist(),
            "close": df['close'].tolist(),
            "volume": df['volume'].astype(np.int64).tolist(),
            "amount": df['amount'].tolist(),
            "factor": [1.0] * len(df)
        }
        
        # 将数据转换为DolphinDB表格并上传
        script = """
        t = table(
            {:symbol},
            {:date},
            {:open},
            {:high},
            {:low},
            {:close},
            {:volume},
            {:amount},
            {:factor}
        )
        pt = loadTable("dfs://market", "stock_daily")
        pt.append!(t)
        """.format(
            symbol=str(data["symbol"]),
            date=str(data["date"]),
            open=str(data["open"]),
            high=str(data["high"]),
            low=str(data["low"]),
            close=str(data["close"]),
            volume=str(data["volume"]),
            amount=str(data["amount"]),
            factor=str(data["factor"])
        )
        
        conn.run(script)
        print("Data successfully imported to DolphinDB!")
        
    except Exception as e:
        print(f"Error importing data: {str(e)}")
    finally:
        conn.close()

def main():
    # 导入AAPL数据
    csv_file = "data/stocks/AAPL.csv"
    import_stock_data(csv_file)

if __name__ == "__main__":
    main()
