"""
将所有期权数据导入到DolphinDB的脚本
基于import_options_to_dolphindb.py修改
用于导入目录中的所有期权数据
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
import re
import multiprocessing
from functools import partial

# 配置日志
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"options_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.DEBUG,  # 修改为DEBUG级别以显示更多信息
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# 定义数据类型映射
DTYPE_MAP = {
    'symbol': 'SYMBOL',
    'strike': 'DOUBLE',
    'expiry': 'MONTH',  # 修改为MONTH类型
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
    """从期权文件名中解析信息"""
    try:
        # 从文件名中提取股票代码、日期和期权类型
        basename = os.path.basename(filename)
        match = re.match(r'([A-Z]+)(\d{6})([CP])(\d+)\.csv$', basename)
        if not match:
            logging.error(f"文件名格式无效: {filename}")
            return None
        
        symbol, date_str, option_type, strike_str = match.groups()
        logging.debug(f"解析文件名: {basename}, 提取到的symbol: {symbol}, date_str: {date_str}")
        
        # 解析日期
        try:
            year = int(date_str[:2]) + 2000  # 假设所有年份都是2000年以后
            month = int(date_str[2:4])
            day = int(date_str[4:])
            expiry_date = f"{year}-{month:02d}-{day:02d}"
            logging.debug(f"解析到的日期: {expiry_date}")
        except ValueError as e:
            logging.error(f"日期解析失败: {e}")
            return None
            
        # 解析strike价格
        try:
            strike = float(strike_str) / 1000.0
            logging.debug(f"解析到的strike价格: {strike}")
        except ValueError as e:
            logging.error(f"Strike价格解析失败: {e}")
            return None
            
        return {
            'symbol': symbol,
            'expiry': expiry_date,
            'type': 'CALL' if option_type == 'C' else 'PUT',
            'strike': strike
        }
        
    except Exception as e:
        logging.error(f"文件名解析失败: {e}")
        return None

def create_database(conn):
    """创建数据库和表结构，使用RANGE分区"""
    try:
        logging.info("开始创建数据库结构...")
        create_db_script = """
        if(existsDatabase("dfs://optiondb")){
            dropDatabase("dfs://optiondb")
        }
        
        // 创建分区数据库：使用RANGE分区
        db = database("dfs://optiondb", RANGE, 2016.01M + (0..96))
        
        // 创建表结构
        schema = table(
            1:0, 
            `timestamp`symbol`type`strike`expiry`open`high`low`close`volume`vwap`transactions`otc`date`oi,
            [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, MONTH, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, BOOL, DATE, LONG]
        )
        
        // 创建分区表，使用date作为分区键
        db.createPartitionedTable(schema, `options, `date)
        """
        conn.run(create_db_script)
        logging.info("数据库结构创建成功")
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
        
        logging.info(f"成功导入 {result} 条记录")
        return result
        
    except Exception as e:
        logging.error(f"批量导入失败: {str(e)}")
        return 0

def process_csv_file(filepath):
    """处理单个CSV文件"""
    try:
        # 解析文件名中的期权信息
        option_info = parse_option_filename(filepath)
        if not option_info:
            return None
            
        # 读取CSV文件
        df = pd.read_csv(filepath)
        
        # 确保必需的列存在
        required_columns = ['open', 'high', 'low', 'close', 'volume', 'vwap', 'timestamp', 'transactions']
        if not all(col in df.columns for col in required_columns):
            logging.error(f"文件缺少必需的列: {filepath}")
            return None
            
        # 设置pandas选项，避免类型转换警告
        pd.set_option('future.no_silent_downcasting', True)
            
        # 处理缺失值并转换数据类型
        df['volume'] = df['volume'].fillna(0).astype('int64')
        df['transactions'] = df['transactions'].fillna(0).astype('int64')
        df['timestamp'] = df['timestamp'].fillna(0).astype('int64')
        df['vwap'] = df['vwap'].fillna(0.0).astype('float64')
        df['open'] = df['open'].fillna(0.0).astype('float64')
        df['high'] = df['high'].fillna(0.0).astype('float64')
        df['low'] = df['low'].fillna(0.0).astype('float64')
        df['close'] = df['close'].fillna(0.0).astype('float64')
        
        # 处理otc列
        if 'otc' not in df.columns:
            df['otc'] = False
        else:
            df['otc'] = df['otc'].fillna(False)
            df['otc'] = df['otc'].astype('bool')
        
        # 添加期权信息到每一行
        df['symbol'] = option_info['symbol']
        df['strike'] = option_info['strike']
        df['expiry'] = option_info['expiry']
        df['type'] = option_info['type']
        
        # 处理oi列（如果存在）
        if 'oi' not in df.columns:
            df['oi'] = 0
        else:
            df['oi'] = df['oi'].fillna(0).astype('int64')
            
        # 添加日期列
        df['date'] = pd.to_datetime(option_info['expiry']).strftime('%Y-%m-%d')
        
        # 检查数据类型
        logging.debug("处理后的数据类型:\n" + df.dtypes.to_string())
        
        # 转换为字典列表
        records = df.to_dict('records')
        
        return records
        
    except Exception as e:
        logging.error(f"处理文件失败 {filepath}: {str(e)}")
        return None

def load_imported_files():
    """加载已成功导入的文件列表"""
    imported_files = set()
    try:
        if os.path.exists('imported_files.txt'):
            with open('imported_files.txt', 'r') as f:
                imported_files = set(line.strip() for line in f)
            logging.info(f"加载了 {len(imported_files)} 个已导入文件记录")
    except Exception as e:
        logging.error(f"加载导入记录失败: {str(e)}")
    return imported_files

def save_imported_file(filepath):
    """记录成功导入的文件"""
    try:
        with open('imported_files.txt', 'a') as f:
            f.write(f"{filepath}\n")
    except Exception as e:
        logging.error(f"保存导入记录失败: {str(e)}")

def process_batch_parallel(file_batch, conn_params):
    """并行处理一个文件批次
    
    Args:
        file_batch: 要处理的文件列表
        conn_params: DolphinDB连接参数
    """
    try:
        # 为每个进程创建新的连接
        conn = dolphindb.session()
        host, port = conn_params['host'], conn_params['port']
        conn.connect(host, port)
        conn.login(conn_params['userid'], conn_params['password'])
        logging.info(f"进程 {multiprocessing.current_process().name} 开始处理 {len(file_batch)} 个文件")
        
        batch_data = []
        for filepath in tqdm(file_batch, desc=f"Process {multiprocessing.current_process().name}"):
            try:
                file_data = process_csv_file(filepath)
                if file_data:
                    batch_data.extend(file_data)
                    if len(batch_data) >= 5000:
                        import_batch(conn, batch_data)
                        batch_data = []
            except Exception as e:
                logging.error(f"处理文件 {filepath} 时出错: {e}")
                continue
        
        # 处理剩余的数据
        if batch_data:
            import_batch(conn, batch_data)
        
        conn.close()
        logging.info(f"进程 {multiprocessing.current_process().name} 完成处理")
        
    except Exception as e:
        logging.error(f"批次处理失败: {e}")
        raise

def process_csv_files(csv_files, batch_size=5000, conn=None):
    """并行处理CSV文件并导入到DolphinDB
    
    Args:
        csv_files: 要处理的CSV文件列表
        batch_size: 每个批次的文件数量
        conn: DolphinDB连接对象
    """
    try:
        if not csv_files:
            logging.warning("没有找到CSV文件")
            return

        # 获取连接参数
        conn_params = {
            'host': 'localhost',
            'port': 8848,
            'userid': 'admin',
            'password': '123456'
        }

        # 创建数据库结构
        create_database(conn)
        
        # 将文件列表分成多个批次
        num_files = len(csv_files)
        num_processes = min(multiprocessing.cpu_count(), (num_files + batch_size - 1) // batch_size)
        batch_size = (num_files + num_processes - 1) // num_processes
        
        file_batches = [csv_files[i:i + batch_size] for i in range(0, len(csv_files), batch_size)]
        logging.info(f"总共 {len(csv_files)} 个文件，分成 {len(file_batches)} 个批次，每批次约 {batch_size} 个文件")
        
        # 创建进程池并行处理
        with multiprocessing.Pool(processes=num_processes) as pool:
            process_batch_with_params = partial(process_batch_parallel, conn_params=conn_params)
            pool.map(process_batch_with_params, file_batches)
        
        logging.info("所有批次处理完成")
        
    except Exception as e:
        logging.error(f"文件处理失败: {e}")
        raise

def analyze_dates(csv_files):
    """分析所有文件名中的日期格式
    
    Args:
        csv_files: 文件路径列表
    """
    dates = set()  # 用于存储所有不同的日期字符串
    years = set()  # 存储所有的年份
    
    for filepath in csv_files:
        basename = os.path.basename(filepath)
        name = os.path.splitext(basename)[0]
        
        # 提取股票代码部分
        symbol_end = 0
        for i in range(3, 5):
            if name[i].isdigit():
                symbol_end = i
                break
        if symbol_end == 0:
            continue
            
        # 提取期权类型位置
        type_pos = -1
        for i, c in enumerate(name):
            if c in ['C', 'P']:
                type_pos = i
                break
        if type_pos == -1:
            continue
            
        # 提取日期部分
        date_str = name[symbol_end:type_pos]
        if len(date_str) != 6 or not date_str.isdigit():
            continue
            
        dates.add(date_str)
        years.add(date_str[:2])
    
    # 输出分析结果
    logging.info(f"\n=== 日期分析结果 ===")
    logging.info(f"找到 {len(dates)} 个不同的日期")
    logging.info(f"年份分布: {sorted(list(years))}")
    logging.info("\n具体日期列表:")
    for date in sorted(list(dates)):
        year = date[:2]
        month = date[2:4]
        day = date[4:]
        logging.info(f"原始日期: {date}, 解析为: 20{year}年{month}月{day}日")
    logging.info("=== 分析结束 ===\n")
    
    return dates, years

def main():
    """主函数"""
    try:
        # DolphinDB连接配置
        DOLPHINDB_CONFIG = {
            'host': '127.0.0.1',
            'port': 8848,
            'userid': 'admin',
            'password': '123456'
        }
        
        # 连接DolphinDB
        logging.info("正在连接DolphinDB...")
        conn = dolphindb.session()
        conn.connect(DOLPHINDB_CONFIG['host'], 
                    DOLPHINDB_CONFIG['port'],
                    DOLPHINDB_CONFIG['userid'],
                    DOLPHINDB_CONFIG['password'])
        logging.info("DolphinDB连接成功")
        
        # 指定数据目录 - 直接使用60min目录
        data_dir = "/home/luke/optionSource/source/60min"
        if not os.path.exists(data_dir):
            logging.error(f"数据目录不存在: {data_dir}")
            return
            
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
        logging.info(f"找到 {len(csv_files)} 个CSV文件")
        
        # 先分析所有日期
        dates, years = analyze_dates(csv_files)
        
        logging.info("\n开始导入数据...")
        
        # 按文件名排序
        csv_files = sorted(csv_files)
        logging.info(f"找到 {len(csv_files)} 个CSV文件")
        
        # 分批处理所有文件
        batch_size = 5000  # 每批处理5000个文件
        process_csv_files(csv_files, batch_size=batch_size, conn=conn)  # 传入conn参数
            
    except KeyboardInterrupt:
        logging.info("\n用户中断操作")
    except Exception as e:
        logging.error(f"发生错误: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("DolphinDB连接已关闭")

if __name__ == "__main__":
    main()
