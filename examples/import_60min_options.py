import os
import pandas as pd
from datetime import datetime
import dolphindb as ddb
import logging
from tqdm import tqdm

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DolphinDB连接设置
HOST = "localhost"
PORT = 8848
s = ddb.session()
s.connect(HOST, PORT)
# 添加登录信息
s.login("admin", "123456")

def setup_database():
    """设置数据库和表结构"""
    # 检查并删除已存在的数据库
    s.run("""
    if(existsDatabase("dfs://options60min")){
        dropDatabase("dfs://options60min")
    }
    """)
    
    # 创建数据库
    s.run("""
    db = database("dfs://options60min", VALUE, 2020.01M..2024.12M)
    """)
    
    # 创建表结构
    s.run("""
    schema = table(
        1:0, `symbol`timestamp`type`strike`expiry`last`volume`openInterest`bid`ask`size`iv`delta`gamma`theta`vega`rho,
        [SYMBOL,TIMESTAMP,SYMBOL,DOUBLE,DATE,DOUBLE,INT,INT,DOUBLE,DOUBLE,INT,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE]
    )
    """)
    
    # 创建分区表
    s.run("""
    db.createPartitionedTable(
        schema,
        `options,
        `timestamp
    )
    """)
    logger.info("Database and table structure created successfully")

def import_csv_file(file_path):
    """导入单个CSV文件到DolphinDB"""
    try:
        # 读取CSV文件
        df = pd.read_csv(file_path)
        
        # 转换时间戳列
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # 将数据写入DolphinDB
        s.run("t = loadTable('dfs://options60min', 'options')")
        s.upload({'data': df})
        s.run("t.append!(data)")
        
        logger.info(f"Successfully imported {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error importing {file_path}: {str(e)}")
        return False

def main():
    # 设置数据源目录
    source_dir = "/home/luke/optionSource/source/60min"
    
    # 创建数据库结构
    setup_database()
    
    # 获取所有CSV文件
    csv_files = [f for f in os.listdir(source_dir) if f.endswith('.csv')]
    
    # 使用tqdm显示进度
    success_count = 0
    for file in tqdm(csv_files, desc="Importing files"):
        file_path = os.path.join(source_dir, file)
        if import_csv_file(file_path):
            success_count += 1
    
    logger.info(f"Import completed. Successfully imported {success_count} out of {len(csv_files)} files")

if __name__ == "__main__":
    main()
