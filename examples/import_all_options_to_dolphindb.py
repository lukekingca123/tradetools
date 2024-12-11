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
    """解析期权文件名
    支持格式：
    1. AAPL160603C00055000.csv
    2. AAPL171201P00180000.csv
    
    返回:
    {
        'symbol': str,  # 股票代码
        'expiry': str,  # 到期日 YYYY-MM-DD
        'type': str,    # CALL 或 PUT
        'strike': float # 行权价，如 55.00
    }
    """
    try:
        basename = os.path.basename(filename)
        name = os.path.splitext(basename)[0]
        logging.info(f"开始解析文件: {filename}")
        
        # 提取股票代码（支持3-4位代码）
        symbol_end = 0
        for i in range(3, 5):
            if name[i].isdigit():
                symbol_end = i
                break
        if symbol_end == 0:
            logging.error(f"无法确定股票代码长度: {filename}")
            return None
            
        symbol = name[:symbol_end]
        logging.debug(f"提取到股票代码: {symbol}")
        
        # 提取期权类型
        type_pos = -1
        for i, c in enumerate(name):
            if c in ['C', 'P']:
                type_pos = i
                break
        
        if type_pos == -1:
            logging.error(f"无法在文件名中找到期权类型标记: {filename}")
            return None
            
        option_type = 'CALL' if name[type_pos] == 'C' else 'PUT'
        logging.debug(f"提取到期权类型: {option_type}")
        
        # 提取日期 (在股票代码之后，C/P之前)
        date_str = name[symbol_end:type_pos]
        if len(date_str) != 6 or not date_str.isdigit():
            logging.error(f"日期格式无效: {date_str}")
            return None
            
        year = int("20" + date_str[:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])
        
        # 验证日期有效性
        if not (2000 <= year <= 2099 and 1 <= month <= 12 and 1 <= day <= 31):
            logging.error(f"日期值无效: year={year}, month={month}, day={day}")
            return None
            
        expiry = f"{year:04d}-{month:02d}-{day:02d}"
        logging.debug(f"提取到到期日: {expiry}")
        
        # 提取行权价
        strike_str = name[type_pos+1:]
        if not strike_str.isdigit():
            logging.error(f"行权价格字符串无效: {strike_str}")
            return None
            
        # 将行权价除以1000得到实际价格（例如：00055000 -> 55.00）
        strike = float(strike_str) / 1000.0
        logging.debug(f"提取到行权价: {strike}")
        
        result = {
            'symbol': symbol,
            'expiry': expiry,
            'type': option_type,
            'strike': strike
        }
        logging.info(f"文件解析成功: {filename} -> {result}")
        return result
        
    except Exception as e:
        logging.error(f"解析文件名失败 {filename}: {str(e)}")
        logging.debug(f"文件名解析详情: basename={basename}, name={name}")
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
        merged_data = pd.DataFrame(batch_data)
        record_count = len(merged_data)
        logging.info(f"准备导入批次数据，记录数: {record_count}")
        logging.debug(f"数据类型:\n{merged_data.dtypes}")
        
        # 转换日期列
        try:
            merged_data['expiry'] = pd.to_datetime(merged_data['expiry'])
            merged_data['date'] = pd.to_datetime(merged_data['date'])
            logging.debug("日期列转换成功")
        except Exception as e:
            logging.error(f"日期转换失败: {str(e)}")
            logging.error(f"数据示例: \n{merged_data.head()}")
            raise
        
        # 上传数据
        try:
            conn.upload({
                "data": merged_data
            })
            logging.debug("数据上传到DolphinDB成功")
        except Exception as e:
            logging.error(f"数据上传失败: {str(e)}")
            logging.error(f"数据类型: \n{merged_data.dtypes}")
            raise
        
        # 执行导入
        try:
            result = conn.run("""
            t = select 
                symbol,
                strike,
                temporalParse(strftime(expiry, '%Y-%m-%d'), "yyyy-MM-dd") as expiry,
                type,
                open,
                high,
                low,
                close,
                volume,
                vwap,
                timestamp,
                transactions,
                otc,
                temporalParse(strftime(date, '%Y-%m-%d'), "yyyy-MM-dd") as date,
                oi
            from data

            loadTable("dfs://optiondb", "options").append!(t)
            size(t)
            """)
            logging.info(f"成功导入 {result} 条记录")
            if result != record_count:
                logging.warning(f"导入记录数({result})与原始数据记录数({record_count})不匹配")
            return result
        except Exception as e:
            logging.error(f"DolphinDB执行失败: {str(e)}")
            raise
        
    except Exception as e:
        logging.error(f"批量导入失败: {str(e)}")
        return 0

def process_csv_file(filepath):
    """处理单个CSV文件"""
    try:
        # 解析文件名获取期权信息
        option_info = parse_option_filename(filepath)
        if not option_info:
            return None
            
        # 读取CSV文件
        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            logging.error(f"读取CSV文件失败 {filepath}: {str(e)}")
            return None
            
        if df.empty:
            logging.warning(f"空文件: {filepath}")
            return None
            
        # 检查必需列
        required_columns = ['symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap', 'timestamp', 'transactions', 'otc', 'date']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.warning(f"文件 {filepath} 缺少列: {', '.join(missing_columns)}")
            return None
            
        # 添加期权信息
        df['symbol'] = option_info['symbol']
        df['strike'] = option_info['strike']
        df['expiry'] = pd.to_datetime(option_info['expiry'])
        df['type'] = option_info['type']
        
        # 数据类型验证和转换
        try:
            df['otc'] = df['otc'].fillna(False)
            df['otc'] = df['otc'].astype(bool)
            
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(np.int64)
            df['transactions'] = pd.to_numeric(df['transactions'], errors='coerce').fillna(0).astype(np.int64)
            df['oi'] = df.get('oi', 0)  # 设置默认值
            df['oi'] = pd.to_numeric(df['oi'], errors='coerce').fillna(0).astype(np.int64)
            
            # 转换日期列
            df['date'] = pd.to_datetime(df['date'])
            
            # 检查数据类型
            logging.debug(f"数据类型转换后: \n{df.dtypes}")
            
        except Exception as e:
            logging.error(f"数据类型转换失败 {filepath}: {str(e)}")
            return None
        
        # 准备导入数据
        import_data = {
            'symbol': df['symbol'].tolist(),
            'strike': df['strike'].tolist(),
            'expiry': df['expiry'].dt.strftime('%Y-%m-%d').tolist(),
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
            'date': df['date'].dt.strftime('%Y-%m-%d').tolist(),
            'oi': df['oi'].tolist()
        }
        
        return import_data
        
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

def process_csv_files(csv_files, batch_size=10000):
    """处理CSV文件并导入到DolphinDB，使用复合分区和批量导入优化性能"""
    try:
        # 加载已导入文件记录
        imported_files = load_imported_files()
        
        # 过滤掉已导入的文件
        remaining_files = [f for f in csv_files if f not in imported_files]
        if len(remaining_files) < len(csv_files):
            logging.info(f"跳过 {len(csv_files) - len(remaining_files)} 个已导入文件")
        
        if not remaining_files:
            logging.info("所有文件都已导入完成")
            return
            
        # 创建数据库连接
        conn = dolphindb.session()
        conn.connect("localhost", 8848, "admin", "123456")
        
        # 创建数据库
        if not create_database(conn):
            return
            
        logging.info(f"找到 {len(remaining_files)} 个待处理文件")
        
        # 批量处理变量
        current_batch = []
        batch_count = 0
        total_imported = 0
        
        # 设置pandas选项
        pd.set_option('future.no_silent_downcasting', True)
        
        # 使用tqdm显示进度
        for csv_file in tqdm(remaining_files):
            try:
                # 处理单个CSV文件
                import_data = process_csv_file(csv_file)
                if not import_data:
                    continue
                    
                current_batch.append(import_data)
                batch_count += 1
                
                # 当达到批次大小时导入数据
                if batch_count >= batch_size:
                    imported = import_batch(conn, current_batch)
                    if imported > 0:
                        total_imported += imported
                        # 记录成功导入的文件
                        for f in csv_files[total_imported-imported:total_imported]:
                            save_imported_file(f)
                        logging.info(f"已导入 {total_imported} 条记录")
                    current_batch = []
                    batch_count = 0
                    
            except Exception as e:
                logging.error(f"处理文件失败 {csv_file}: {str(e)}")
                continue
                
        # 处理最后一批数据
        if current_batch:
            imported = import_batch(conn, current_batch)
            if imported > 0:
                total_imported += imported
                # 记录成功导入的文件
                for f in csv_files[total_imported-imported:total_imported]:
                    save_imported_file(f)
                logging.info(f"已导入 {total_imported} 条记录")
            
        logging.info(f"导入完成，总共导入 {total_imported} 条记录")
        
    except Exception as e:
        logging.error(f"处理CSV文件失败: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    """主函数"""
    try:
        # 指定数据目录 - 直接使用60min目录
        data_dir = "/home/luke/optionSource/source/60min"
        if not os.path.exists(data_dir):
            logging.error(f"数据目录不存在: {data_dir}")
            return
            
        # 获取所有CSV文件
        csv_files = glob.glob(os.path.join(data_dir, "*.csv"))
        if not csv_files:
            logging.error("未找到CSV文件")
            return
            
        # 按文件名排序
        csv_files = sorted(csv_files)
        logging.info(f"找到 {len(csv_files)} 个CSV文件")
        
        # 分批处理所有文件
        batch_size = 1000  # 每批处理1000个文件
        for i in range(0, len(csv_files), batch_size):
            batch_files = csv_files[i:i+batch_size]
            logging.info(f"处理第 {i//batch_size + 1} 批文件，共 {len(batch_files)} 个文件")
            process_csv_files(batch_files)
            
    except Exception as e:
        logging.error(f"主程序执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    main()
