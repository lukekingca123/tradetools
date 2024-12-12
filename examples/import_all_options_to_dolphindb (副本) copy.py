import os
import glob
import logging
import pandas as pd
import dolphindb
from datetime import datetime
import re
import multiprocessing
import signal
import json
import time
import numpy as np
import subprocess
import sys

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
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"options_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),  # 添加编码，防止中文乱码
        logging.StreamHandler()
    ]
)

# 定义数据类型映射
DTYPE_MAP = {
    'date': 'DATE',
    'timestamp': 'LONG',
    'symbol': 'SYMBOL',
    'type': 'SYMBOL',
    'strike': 'DOUBLE',
    'expiry': 'DATE',
    'open': 'DOUBLE',
    'high': 'DOUBLE',
    'low': 'DOUBLE',
    'close': 'DOUBLE',
    'volume': 'LONG',
    'vwap': 'DOUBLE',
    'transactions': 'LONG',
    'otc': 'BOOL',
    'oi': 'LONG'
}

# 定义列顺序
COLUMN_ORDER = list(DTYPE_MAP.keys())

def parse_option_filename(filename):
    """从期权文件名中解析信息
    支持的文件名格式:
    1. 旧格式: AAPL160603C00084000.csv (AAPL + YYMMDD + C/P + STRIKE)
    2. 带数字前缀格式: AAPL7170120C00105000.csv (AAPL + [0-9] + YYMMDD + C/P + STRIKE)
    """
    try:
        # 从文件名中提取股票代码、日期和期权类型
        basename = os.path.basename(filename)
        
        # 使用更通用的模式匹配所有格式
        # 1. 匹配带数字前缀的格式
        # 2. 匹配不带前缀的格式
        patterns = [
            r'([A-Z]+)\d(\d{6})([CP])(\d+)\.csv$',  # 带数字前缀的格式
            r'([A-Z]+)(\d{6})([CP])(\d+)\.csv$'     # 不带前缀的格式
        ]
        
        match = None
        for pattern in patterns:
            match = re.match(pattern, basename)
            if match:
                break
                
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
            logging.error(f"日期解析失败 {basename}: {e}")
            return None
            
        # 解析strike价格
        try:
            strike = float(strike_str) / 1000.0
            logging.debug(f"解析到的strike价格: {strike}")
        except ValueError as e:
            logging.error(f"Strike价格解析失败 {basename}: {e}")
            return None
            
        return {
            'symbol': symbol,
            'expiry': expiry_date,
            'type': 'CALL' if option_type == 'C' else 'PUT',
            'strike': strike
        }
        
    except Exception as e:
        logging.error(f"文件名解析失败 {basename}: {e}")
        return None

def create_database(conn, config):
    """创建数据库和表结构，使用RANGE分区"""
    try:
        db_config = config['database']
        db_name = db_config['name']
        partition_config = db_config['partition']
        table_name = db_config['table_name']
        
        # 生成年份范围
        start_year = partition_config['start_year']
        end_year = partition_config['end_year']
        years_range = f"{start_year}..{end_year}"
        
        # 创建数据库
        script = f"""
        if(existsDatabase('{db_name}')){{
            dropDatabase('{db_name}')
        }}
        // 创建数据库，使用RANGE分区
        dates = take(2016.01.01..2024.12.31, 9)  // 生成9个分区点
        db = database('{db_name}', RANGE, dates)
        
        // 创建表结构
        schema = table(
            1:0, 
            `date`symbol`type`strike`expiry`open`high`low`close`volume,
            [DATE,SYMBOL,SYMBOL,DOUBLE,DATE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,LONG]
        )
        
        // 创建分布式表，按date分区
        db.createPartitionedTable(
            schema,
            `{table_name},
            `date
        )
        """
        conn.run(script)
        logging.info(f"数据库结构创建成功，分区范围：{start_year}-{end_year}年")
        return True
    except Exception as e:
        logging.error(f"创建数据库结构失败: {str(e)}")
        logging.error(f"SQL: {script}")
        return False

def process_csv_files(csv_files, conn_params=None, batch_size=100000, config=None):
    """并行处理CSV文件并导入到DolphinDB"""
    try:
        conn = dolphindb.session()
        conn.connect(conn_params['host'], conn_params['port'], 
                    conn_params['username'], conn_params['password'])
        
        # 创建数据库结构
        if not create_database(conn, config):
            return
            
        db_name = config['database']['name']
        table_name = config['database']['table_name']
        
        total_files = len(csv_files)
        logging.info(f"\n开始处理 {total_files} 个文件...")
        
        # 加载已导入文件列表
        imported_files = load_imported_files()
        remaining_files = [f for f in csv_files if f not in imported_files]
        logging.info(f"过滤已导入文件后，剩余 {len(remaining_files)} 个文件需要导入")
        
        for i, filepath in enumerate(remaining_files):
            if not running:
                logging.info("收到停止信号，正在结束处理...")
                break
                
            try:
                file_info = parse_option_filename(os.path.basename(filepath))
                if not file_info:
                    continue
                    
                # 使用DolphinDB的loadText直接导入数据
                script = f"""
                // 读取数据并转换
                data = select 
                    temporalParse(string(date), 'yyyy-MM-dd HH:mm:ss') as date,
                    symbol('{file_info['symbol']}') as symbol,
                    symbol('{file_info['type']}') as type,
                    {file_info['strike']} as strike,
                    temporalParse('{file_info['expiry']}', 'yyyy-MM-dd') as expiry,
                    double(open) as open,
                    double(high) as high,
                    double(low) as low,
                    double(close) as close,
                    long(volume) as volume
                from loadText('{filepath}')
                
                // 批量写入数据库
                loadTable('{db_name}', '{table_name}').append!(data)
                """
                
                conn.run(script)
                save_imported_file(filepath)
                
                if (i + 1) % 100 == 0:
                    logging.info(f"已处理 {i + 1}/{total_files} 个文件")
                    
            except Exception as e:
                logging.error(f"处理文件 {filepath} 时发生错误: {str(e)}")
                continue
                
        logging.info("文件处理完成")
        
        # 检查导入结果
        try:
            count = conn.run(f"select count(*) from loadTable('{db_name}', '{table_name}')")
            logging.info(f"数据库中总记录数: {count}")
        except Exception as e:
            logging.error(f"查询记录数时发生错误: {str(e)}")
            
    except Exception as e:
        logging.error(f"处理过程中发生错误: {str(e)}")
        raise

def load_imported_files():
    """加载已成功导入的文件列表"""
    try:
        imported_files_path = os.path.join(os.path.dirname(__file__), 'imported_files.txt')
        if os.path.exists(imported_files_path):
            with open(imported_files_path, 'r', encoding='utf-8') as f:
                return set(line.strip() for line in f)
        return set()
    except Exception as e:
        logging.error(f"加载已导入文件列表失败: {e}")
        return set()

def save_imported_file(filepath):
    """记录成功导入的文件"""
    try:
        imported_files_path = os.path.join(os.path.dirname(__file__), 'imported_files.txt')
        with open(imported_files_path, 'a', encoding='utf-8') as f:
            f.write(f"{filepath}\n")
    except Exception as e:
        logging.error(f"记录已导入文件失败: {e}")

def load_config():
    """加载配置文件"""
    try:
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f: 
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"配置文件 config.json 未找到，请检查配置文件路径")
        sys.exit(1) 
    except json.JSONDecodeError as e:
        logging.error(f"配置文件 config.json 格式错误: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"加载配置文件失败: {e}")
        sys.exit(1)

def start_dolphindb_server(config):
    """启动DolphinDB服务器"""
    if not config.get('dolphindb_server'):
        logging.warning("配置文件中缺少dolphindb_server配置，跳过自动启动")
        return
        
    start_script = config['dolphindb_server']['start_script']
    if not os.path.exists(start_script):
        logging.error(f"启动脚本不存在: {start_script}")
        return
        
    try:
        subprocess.run([start_script, 'start'], check=True)
        logging.info("DolphinDB服务器启动成功")
        # 等待服务器完全启动
        time.sleep(5)
    except subprocess.CalledProcessError as e:
        logging.error(f"启动DolphinDB服务器失败: {e}")
        raise Exception("DolphinDB服务器启动失败")

def main():
    """主函数"""
    try:
        config = load_config()
        if not config:
            return
            
        # 启动DolphinDB服务器
        start_dolphindb_server(config)
            
        data_dir = config['data_dir']
        if not os.path.exists(data_dir):
            logging.error(f"数据目录 {data_dir} 不存在")
            return
            
        csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
        if not csv_files:
            logging.error(f"在 {data_dir} 中没有找到CSV文件")
            return

        process_csv_files(
            csv_files, 
            conn_params=config['dolphindb'], 
            batch_size=config.get('batch_size', 100000),
            config=config
        )

    except KeyboardInterrupt:
        logging.info("\n程序被用户中断")
    except Exception as e:
        logging.error(f"程序执行失败: {e}")
        logging.error("错误详情:", exc_info=True)

if __name__ == "__main__":
    main()