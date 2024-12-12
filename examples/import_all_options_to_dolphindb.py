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
import signal
import sys
import json
import time

# 全局变量用于控制程序运行
running = True

def signal_handler(signum, frame):
    """处理Ctrl+C信号"""
    global running
    print("\n收到停止信号，正在优雅退出...")
    running = False

# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)

# 配置日志
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_file = os.path.join(log_dir, f"options_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# 设置pandas选项，避免警告
pd.set_option('future.no_silent_downcasting', True)

# 定义数据类型映射
DTYPE_MAP = {
    'timestamp': 'LONG',
    'symbol': 'SYMBOL',
    'strike': 'DOUBLE',
    'expiry': 'DATE',  # 修改为DATE类型
    'type': 'SYMBOL',
    'open': 'DOUBLE',
    'high': 'DOUBLE',
    'low': 'DOUBLE',
    'close': 'DOUBLE',
    'volume': 'LONG',
    'vwap': 'DOUBLE',
    'transactions': 'LONG',
    'otc': 'BOOL',
    'date': 'DATE',
    'oi': 'LONG'
}

def parse_option_filename(filename):
    """从期权文件名中解析信息"""
    try:
        # 提取文件名（不含路径和扩展名）
        basename = os.path.splitext(os.path.basename(filename))[0]
        
        # 使用正则表达式提取信息
        pattern = r'([A-Z]+)(\d{6})([CP])(\d+)'
        match = re.match(pattern, basename)
        
        if not match:
            logging.error(f"无法解析文件名: {filename}")
            return None
            
        symbol, date_str, option_type, strike_str = match.groups()
        
        # 转换日期格式
        year = int('20' + date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:])
        
        try:
            date = datetime(year, month, day)
        except ValueError:
            logging.error(f"无效的日期: {year}-{month}-{day}, 文件: {filename}")
            return None
            
        # 转换执行价格
        strike = float(strike_str) / 1000
        
        return {
            'symbol': symbol,
            'expiry': date.strftime('%Y-%m-%d'),
            'type': 'CALL' if option_type == 'C' else 'PUT',
            'strike': strike
        }
        
    except Exception as e:
        logging.error(f"解析文件名失败 {filename}: {str(e)}")
        return None

def create_database(conn):
    """创建数据库和表结构，使用复合分区（RANGE + HASH）"""
    try:
        logging.info("开始创建数据库结构...")
        create_db_script = """
        if(existsDatabase("dfs://optiondb")){
            dropDatabase("dfs://optiondb")
        }
        
        // 创建复合分区数据库
        // 按到期日（expiry）进行VALUE分区
        dbDate = database("", VALUE, date(2010.01.01)..date(2040.12.31))
        
        // 按标的（symbol）进行HASH分区，分为20个桶
        dbSymbol = database("", HASH, [SYMBOL, 20])
        
        // 创建复合分区数据库
        db = database("dfs://optiondb", COMPOUND, [dbDate, dbSymbol])
        
        // 创建表结构，注意列的顺序要和导入数据时一致
        schema = table(
            1:0, 
            `date`timestamp`symbol`type`strike`expiry`open`high`low`close`volume`vwap`transactions`otc`oi,
            [DATE, LONG, SYMBOL, SYMBOL, DOUBLE, DATE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, LONG, BOOL, LONG]
        )
        
        // 创建分区表，使用expiry和symbol作为分区键
        db.createPartitionedTable(schema, `options, `expiry`symbol, , true)
        """
        conn.run(create_db_script)
        logging.info("数据库结构创建成功")
        
        # 检查表结构
        check_script = """
        print("表结构:")
        print(schema(loadTable("dfs://optiondb", "options")))
        print("\n分区信息:")
        print(loadTable("dfs://optiondb", "options").schema().partitionSchema)
        """
        conn.run(check_script)
        return True
    except Exception as e:
        logging.error(f"创建数据库失败: {str(e)}")
        logging.error("错误详情:", exc_info=True)
        return False

def test_dolphindb_connection(conn):
    """测试DolphinDB连接状态"""
    try:
        # 测试基本运算
        result = conn.run("1 + 1")
        logging.info(f"DolphinDB连接测试 - 基本运算: 1 + 1 = {result}")
        
        # 测试数据库访问
        db_exists = conn.run("existsDatabase('dfs://optiondb')")
        logging.info(f"DolphinDB连接测试 - 数据库存在: {db_exists}")
        
        if db_exists:
            # 测试表访问
            table_exists = conn.run("existsTable('dfs://optiondb', 'options')")
            logging.info(f"DolphinDB连接测试 - 表存在: {table_exists}")
            
            # 获取表大小
            if table_exists:
                table_size = conn.run("select count(*) from loadTable('dfs://optiondb', 'options')")
                logging.info(f"DolphinDB连接测试 - 表中记录数: {table_size}")
        
        return True
    except Exception as e:
        logging.error(f"DolphinDB连接测试失败: {str(e)}")
        return False

def process_csv_file(filepath):
    """处理单个CSV文件"""
    try:
        # 从文件名中提取期权信息
        option_info = parse_option_filename(filepath)
        if not option_info:
            return None
            
        # 明确指定数据类型
        dtype_dict = {
            'open': np.float64,
            'high': np.float64,
            'low': np.float64,
            'close': np.float64,
            'volume': np.int64,
            'vwap': np.float64,
            'timestamp': np.int64,
            'transactions': np.int64,
            'otc': 'object',  # 先用 object，后面再转 bool
            'oi': np.int64
        }
        
        # 读取CSV文件
        df = pd.read_csv(filepath, dtype=dtype_dict)
        
        # 记录数据类型信息
        logging.info(f"处理文件: {filepath}")
        logging.info("原始DataFrame数据类型:")
        for col in df.columns:
            logging.info(f"{col}: {df[col].dtype}")
            if len(df) > 0:
                logging.info(f"{col} 样本值: {df[col].iloc[0]}")
        
        # 构建数据字典
        expiry_date = pd.to_datetime(option_info['expiry'])
        trade_date = expiry_date  # 这里使用到期日作为交易日，实际应该从数据中获取
        
        data = {
            'date': [trade_date.strftime('%Y-%m-%d')] * len(df),  # DATE 格式
            'timestamp': df['timestamp'].astype(np.int64).tolist(),
            'symbol': [option_info['symbol']] * len(df),
            'type': [option_info['type']] * len(df),
            'strike': [float(option_info['strike'])] * len(df),
            'expiry': [expiry_date.strftime('%Y-%m-%d')] * len(df),  # DATE 格式
            'open': df['open'].tolist(),
            'high': df['high'].tolist(),
            'low': df['low'].tolist(),
            'close': df['close'].tolist(),
            'volume': df['volume'].tolist(),
            'vwap': df['vwap'].tolist(),
            'transactions': df['transactions'].tolist(),
            'otc': df['otc'].fillna(False).astype(bool).tolist() if 'otc' in df.columns else [False] * len(df),
            'oi': df['oi'].fillna(0).astype(np.int64).tolist() if 'oi' in df.columns else [0] * len(df)
        }
        
        # 创建DataFrame并确保列顺序正确
        result_df = pd.DataFrame(data)
        correct_column_order = ['date', 'timestamp', 'symbol', 'type', 'strike', 'expiry', 
                              'open', 'high', 'low', 'close', 'volume', 'vwap', 
                              'transactions', 'otc', 'oi']
        result_df = result_df.reindex(columns=correct_column_order)
        
        logging.info("处理后的数据类型:")
        for col in result_df.columns:
            logging.info(f"{col}: {result_df[col].dtype}")
            if len(result_df) > 0:
                logging.info(f"{col} 样本值: {result_df[col].iloc[0]}")
        
        return result_df
        
    except Exception as e:
        logging.error(f"处理文件 {filepath} 失败: {str(e)}")
        logging.error("错误详情:", exc_info=True)
        return None

def import_batch(conn, batch_data):
    """批量导入数据到DolphinDB"""
    try:
        # 确保列顺序正确
        correct_column_order = ['date', 'timestamp', 'symbol', 'type', 'strike', 'expiry', 
                              'open', 'high', 'low', 'close', 'volume', 'vwap', 
                              'transactions', 'otc', 'oi']
        
        # 合并数据并重置索引
        df = pd.concat(batch_data, ignore_index=True)
        df = df.reindex(columns=correct_column_order)
        
        logging.info(f"准备导入 {len(df)} 条记录")
        logging.info("导入数据的列顺序:")
        for col in df.columns:
            logging.info(f"{col}: {df[col].dtype}")
        
        # 使用loadTable().append!直接导入DataFrame
        table = conn.loadTable("dfs://optiondb", "options")
        table.append!(df)
        
        logging.info(f"成功导入 {len(df)} 条记录")
        return len(df)
        
    except Exception as e:
        logging.error(f"批量导入失败: {str(e)}")
        logging.error("错误详情:", exc_info=True)
        return 0

def load_imported_files():
    """加载已成功导入的文件列表"""
    imported_files = set()
    try:
        if os.path.exists('imported_files.txt'):
            # 备份旧文件
            backup_file = f'imported_files_{int(time.time())}.txt'
            os.rename('imported_files.txt', backup_file)
            logging.info(f"备份旧的导入记录到 {backup_file}")
            
            # 从备份文件中读取记录
            with open(backup_file, 'r') as f:
                imported_files = set(line.strip() for line in f)
            logging.info(f"加载了 {len(imported_files)} 个已导入文件记录")
            
        # 创建新的空文件
        with open('imported_files.txt', 'w') as f:
            pass
        logging.info("创建新的导入记录文件")
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

def process_csv_files(csv_files, batch_size=5000, conn=None):
    """并行处理CSV文件并导入到DolphinDB
    
    Args:
        csv_files: 要处理的CSV文件列表
        batch_size: 每个批次的文件数量
        conn: DolphinDB连接对象
    """
    global running
    
    try:
        total_files = len(csv_files)
        logging.info(f"\n开始处理 {total_files} 个文件...")
        
        # 加载已导入文件记录
        imported_files = load_imported_files()
        logging.info(f"加载了 {len(imported_files)} 个已导入文件记录")
        
        # 过滤已导入的文件
        files_to_process = [f for f in csv_files if f not in imported_files]
        logging.info(f"过滤已导入文件后，剩余 {len(files_to_process)} 个文件需要导入")
        
        # 分析日期
        analyze_dates(files_to_process)
        
        # 加载配置
        config = load_config()
        conn_params = config['dolphindb']
        
        # 分批处理文件
        for i in range(0, len(files_to_process), batch_size):
            if not running:
                logging.info("收到停止信号，正在结束处理...")
                break
                
            batch = files_to_process[i:i+batch_size]
            logging.info(f"\n处理批次 {i//batch_size + 1}/{(len(files_to_process)-1)//batch_size + 1}")
            
            try:
                process_batch_parallel(batch, conn_params)
            except Exception as e:
                logging.error(f"批次处理失败: {str(e)}")
                continue
        
        logging.info("\n所有文件处理完成")
        
    except Exception as e:
        logging.error(f"处理文件失败: {str(e)}")
        raise

def process_batch_parallel(file_batch, conn_params):
    """并行处理一个文件批次
    
    Args:
        file_batch: 要处理的文件列表
        conn_params: DolphinDB连接参数
    """
    global running
    
    try:
        # 加载配置
        config = load_config()
        num_processes = config.get('max_processes', 8)  # 默认值为8
        chunk_size = max(len(file_batch) // (num_processes * 4), 1)  # 确保每个进程有足够的工作量
        
        logging.info(f"使用 {num_processes} 个进程处理 {len(file_batch)} 个文件，每个进程每次处理 {chunk_size} 个文件")
        
        with multiprocessing.Pool(num_processes) as pool:
            # 使用imap处理文件，这样可以实时获取结果
            results = []
            processed_files = []  # 记录处理成功的文件
            for idx, result in enumerate(pool.imap_unordered(process_csv_file, file_batch, chunksize=chunk_size)):
                if not running:
                    pool.terminate()
                    break
                if result is not None:
                    results.append(result)
                    processed_files.append(file_batch[idx])
                    logging.info(f"成功处理文件: {file_batch[idx]}")
            
            if not running:
                logging.info("批次处理被中断")
                return
            
            if not results:
                logging.info("没有新数据需要导入")
                return
                
            # 合并批次数据
            merged_data = pd.concat(results, ignore_index=True)
            
            # 连接DolphinDB并导入数据
            conn = dolphindb.session()
            conn.connect(conn_params['host'], conn_params['port'])
            conn.login(conn_params['username'], conn_params['password'])
            
            # 导入数据
            try:
                rows_imported = import_batch(conn, [merged_data])
                if rows_imported > 0:  # 只有在实际导入了数据时才记录文件
                    # 记录成功导入的文件
                    for file in processed_files:  # 只记录成功处理的文件
                        save_imported_file(file)
                        logging.info(f"记录已导入文件: {file}")
            except Exception as e:
                logging.error(f"导入数据失败: {str(e)}")
            finally:
                conn.close()
                
    except Exception as e:
        logging.error(f"并行处理批次失败: {str(e)}")
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

def load_config():
    """加载配置文件"""
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"加载配置文件失败: {str(e)}")
        # 使用默认配置
        return {
            "dolphindb": {
                "host": "127.0.0.1",
                "port": 8848,
                "username": "admin",
                "password": "123456"
            },
            "data_dir": "/home/luke/optionSource/source/60min",
            "batch_size": 5000,
            "max_processes": 8
        }

def main():
    """主函数"""
    global running
    
    try:
        # 加载配置
        config = load_config()
        data_dir = config['data_dir']
        
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
        
        if not csv_files:
            logging.error(f"在 {data_dir} 中没有找到CSV文件")
            return
            
        # 处理所有文件
        process_csv_files(csv_files, batch_size=config['batch_size'])
        
    except KeyboardInterrupt:
        logging.info("\n程序被用户中断")
    except Exception as e:
        logging.error(f"程序执行失败: {str(e)}")
    finally:
        if not running:
            logging.info("程序已停止")

if __name__ == "__main__":
    main()
