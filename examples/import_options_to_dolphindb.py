"""
将期权数据导入到DolphinDB的脚本
使用复合分区：按日期和期权代码分区
优化性能和可靠性
"""
import os
import glob
import shutil
import logging
import pandas as pd
from tqdm import tqdm
import dolphindb
from datetime import datetime
import numpy as np

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('option_import.log'),
        logging.StreamHandler()
    ]
)

# 定义数据类型映射
DTYPE_MAP = {
    'symbol': 'SYMBOL',
    'strike': 'DOUBLE',
    'expiry': 'DATE',
    'type': 'SYMBOL',
    'open': 'DOUBLE',
    'high': 'DOUBLE',
    'low': 'DOUBLE',
    'close': 'DOUBLE',
    'volume': 'LONG',
    'vwap': 'DOUBLE',
    'timestamp': 'LONG',
    'transactions': 'LONG',
    'otc': 'BOOL',
    'date': 'DATE',
    'oi': 'LONG'
}

def parse_option_filename(filename):
    # Example: AAPL160603C00055000.csv
    # Extract: symbol=AAPL, expiry=2016-06-03, type=C, strike=55.0
    symbol = filename[:4]
    year = "20" + filename[4:6]
    month = filename[6:8]
    day = filename[8:10]
    option_type = filename[10]
    strike = float(filename[11:-4]) / 1000  # Convert strike to actual value
    expiry = f"{year}-{month}-{day}"
    return {
        'symbol': symbol,
        'expiry': expiry,
        'type': 'CALL' if option_type == 'C' else 'PUT',
        'strike': strike
    }

def create_database(conn):
    """创建数据库和表结构，使用RANGE分区"""
    try:
        create_db_script = """
        if(existsDatabase("dfs://optiondb")){
            dropDatabase("dfs://optiondb")
        }
        
        // 创建分区数据库：使用RANGE分区
        db = database("dfs://optiondb", RANGE, [2016.01.01, 2017.01.01, 2018.01.01, 2019.01.01, 2020.01.01, 2021.01.01, 2022.01.01, 2023.01.01])
        
        // 创建表结构
        schema = table(1:0, `symbol`strike`expiry`type`open`high`low`close`volume`vwap`timestamp`transactions`otc`date`oi, 
                      [SYMBOL, DOUBLE, DATE, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, LONG, BOOL, DATE, LONG])
        
        // 创建分区表
        db.createPartitionedTable(schema, `options, `date)
        """
        conn.run(create_db_script)
        logging.info("数据库结构创建成功，使用RANGE分区")
        return True
    except Exception as e:
        logging.error(f"创建数据库失败: {str(e)}")
        return False

def import_batch(conn, batch_data):
    """批量导入数据到DolphinDB"""
    try:
        # 合并批次数据
        merged_data = {
            'symbol': [],
            'strike': [],
            'expiry': [],
            'type': [],
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': [],
            'vwap': [],
            'timestamp': [],
            'transactions': [],
            'otc': [],
            'date': [],
            'oi': []
        }
        
        for data in batch_data:
            for key in merged_data:
                merged_data[key].extend(data[key])
        
        # 上传数据
        conn.upload({
            "data": merged_data
        })
        
        # 执行导入
        result = conn.run("""
        t = table(
            data.symbol as symbol,
            data.strike as strike,
            temporalParse(data.expiry, "yyyy-MM-dd") as expiry,
            data.type as type,
            data.open as open,
            data.high as high,
            data.low as low,
            data.close as close,
            data.volume as volume,
            data.vwap as vwap,
            data.timestamp as timestamp,
            data.transactions as transactions,
            data.otc as otc,
            temporalParse(data.date, "yyyy-MM-dd") as date,
            data.oi as oi
        )
        loadTable("dfs://optiondb", "options").append!(t)
        size(t)
        """)
        
        return result
        
    except Exception as e:
        logging.error(f"批量导入失败: {str(e)}")
        return 0

def process_csv_files(csv_files, batch_size=10000):
    """处理CSV文件并导入到DolphinDB，使用复合分区和批量导入优化性能"""
    try:
        # 创建数据库连接
        conn = dolphindb.session()
        conn.connect("localhost", 8848, "admin", "123456")
        
        # 创建数据库
        if not create_database(conn):
            return
            
        logging.info(f"找到 {len(csv_files)} 个数据文件")
        
        # 批量处理变量
        current_batch = []
        batch_count = 0
        total_imported = 0
        
        # 设置pandas选项
        pd.set_option('future.no_silent_downcasting', True)
        
        # 使用tqdm显示进度
        for csv_file in tqdm(csv_files):
            try:
                # 读取CSV文件
                df = pd.read_csv(csv_file)
                
                if df.empty:
                    logging.warning(f"跳过空文件: {csv_file}")
                    continue
                    
                required_columns = ['symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'timestamp', 'transactions', 'otc', 'date']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logging.error(f"文件 {csv_file} 缺少必需列: {missing_columns}")
                    continue
                
                # 从文件名解析期权信息
                filename = os.path.basename(csv_file)
                option_info = parse_option_filename(filename)
                
                # 数据清洗和类型转换
                df['strike'] = option_info['strike']
                df['expiry'] = pd.to_datetime(option_info['expiry'])
                df['type'] = option_info['type']
                df['oi'] = df.get('oi', 0)  # 设置默认值
                
                # 数据类型验证和转换
                df['otc'] = df['otc'].fillna(False)
                df['otc'] = df['otc'].infer_objects(copy=False).astype(bool)
                df['date'] = pd.to_datetime(df['date'])
                df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(np.int64)
                df['transactions'] = pd.to_numeric(df['transactions'], errors='coerce').fillna(0).astype(np.int64)
                df['oi'] = pd.to_numeric(df['oi'], errors='coerce').fillna(0).astype(np.int64)
                
                # 转换为DolphinDB可接受的格式
                data = {
                    'symbol': df['symbol'].tolist(),
                    'strike': df['strike'].tolist(),
                    'expiry': [d.strftime("%Y-%m-%d") for d in df['expiry']],
                    'type': df['type'].tolist(),
                    'open': df['open'].tolist(),
                    'high': df['high'].tolist(),
                    'low': df['low'].tolist(),
                    'close': df['close'].tolist(),
                    'volume': df['volume'].tolist(),
                    'vwap': df['vwap'].tolist(),
                    'timestamp': df['timestamp'].tolist(),
                    'transactions': df['transactions'].tolist(),
                    'otc': df['otc'].tolist(),
                    'date': [d.strftime("%Y-%m-%d") for d in df['date']],
                    'oi': df['oi'].tolist()
                }
                
                current_batch.append(data)
                batch_count += len(df)
                
                # 当达到批处理大小时执行导入
                if batch_count >= batch_size:
                    imported_count = import_batch(conn, current_batch)
                    total_imported += imported_count
                    current_batch = []
                    batch_count = 0
                    
                    # 验证导入数据
                    actual_count = conn.run("select count(*) from loadTable('dfs://optiondb', 'options')")
                    if actual_count != total_imported:
                        logging.warning(f"数据验证不匹配: 预期 {total_imported}, 实际 {actual_count}")
                
            except Exception as e:
                logging.error(f"导入文件 {csv_file} 时发生错误: {str(e)}")
                continue
        
        # 导入剩余的批次
        if current_batch:
            imported_count = import_batch(conn, current_batch)
            total_imported += imported_count

        logging.info(f"成功导入 {total_imported} 条记录")
        
        # 最终验证
        final_count = conn.run("select count(*) from loadTable('dfs://optiondb', 'options')")
        logging.info(f"数据库中总记录数: {final_count}")
        
    except Exception as e:
        logging.error(f"处理过程中发生错误: {str(e)}")
        raise

def main():
    """主函数"""
    try:
        # 指定数据目录
        data_dir = os.path.expanduser("~/optionSource/source")
        if not os.path.exists(data_dir):
            logging.error(f"数据目录不存在: {data_dir}")
            return
            
        # 获取所有子目录中的CSV文件
        csv_files = []
        for root, dirs, files in os.walk(data_dir):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
                    
        if not csv_files:
            logging.error("未找到CSV文件")
            return
            
        logging.info(f"找到 {len(csv_files)} 个CSV文件")
            
        # 处理所有文件
        process_csv_files(csv_files)
        
    except Exception as e:
        logging.error(f"主程序执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    main()
