"""
DolphinDB 数据提供者
"""
import dolphindb as ddb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Tuple
import glob
import os
import re
import traceback

class DolphinDBProvider:
    """DolphinDB数据提供者"""
    
    def __init__(self, host: str = "127.0.0.1", port: int = 8848):
        """初始化 DolphinDB 连接"""
        try:
            print("\n=== 初始化DolphinDB连接 ===")
            print(f"连接到 {host}:{port}")
            
            self.conn = ddb.session()
            self.conn.connect(host, port)
            
            # 测试连接
            result = self.conn.run("1+1")
            print(f"连接测试结果: {result}")
            
            # 登录
            print("\n执行登录...")
            result = self.conn.run("login('admin', '123456')")
            print(f"登录结果: {result}")
            
            # 设置数据库路径
            self.db_path = "dfs://options"
            print(f"\n数据库路径: {self.db_path}")
            
            # 使用已有的数据库
            self._use_database()
            
        except Exception as e:
            print(f"\n连接失败: {str(e)}")
            print("详细错误信息:")
            traceback.print_exc()
            raise
            
    def _use_database(self):
        """初始化和使用数据库"""
        try:
            print("\n=== 开始初始化数据库 ===")
            
            # 检查连接状态
            print("\n1. 检查连接状态...")
            status = self.conn.run("1+1")
            print(f"连接状态: {status}")
            
            # 检查数据库是否存在
            print("\n2. 检查数据库是否存在...")
            script = f"""
            if(exists('{self.db_path}')) {{
                return 1;
            }}
            return 0;
            """
            exists = self.conn.run(script)
            print(f"数据库存在: {exists}")
            
            if not exists:
                print("\n3. 创建数据库...")
                # 创建数据库
                script = f"""
                if(!exists('{self.db_path}')) {{
                    db = database('{self.db_path}', VALUE, `AAPL`MSFT`AMZN`NVDA`GOOGL);
                    return 1;
                }}
                return 0;
                """
                print("执行脚本:")
                print(script)
                result = self.conn.run(script)
                print(f"创建结果: {result}")
            
            # 检查表是否存在
            print("\n4. 检查表是否存在...")
            script = f"""
            if(!exists('{self.db_path}')) {{
                return -1;
            }}
            if(!existsTable('{self.db_path}', 'options')) {{
                return 0;
            }}
            return 1;
            """
            exists = self.conn.run(script)
            print(f"表存在: {exists}")
            
            if not exists:
                print("\n5. 创建表 options...")
                # 创建表结构
                script = f"""
                // 使用数据库
                db = database('{self.db_path}');
                
                // 创建表结构
                schema = table(
                    1:0, 
                    `symbol`date`timestamp`open`high`low`close`volume`type`strike`expiry,
                    [SYMBOL, DATE, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, SYMBOL, DOUBLE, INT]
                );
                
                // 创建分区表
                if(!existsTable('{self.db_path}', 'options')) {{
                    db.createPartitionedTable(
                        schema,
                        'options',
                        'symbol'
                    );
                }}
                """
                print("执行脚本:")
                print(script)
                result = self.conn.run(script)
                print(f"创建结果: {result}")
            
            print("\n=== 数据库初始化完成 ===")
            
        except Exception as e:
            print(f"\n数据库初始化失败: {str(e)}")
            print("详细错误信息:")
            traceback.print_exc()
            raise
    
    def _parse_option_symbol(self, symbol: str) -> tuple:
        """解析期权代码
        
        Args:
            symbol: 期权代码，如 AAPL160603C00090000
            
        Returns:
            (标的代码, 到期日, 期权类型, 行权价)
        """
        # 使用正则表达式解析期权代码
        pattern = r"([A-Z]+)(\d{6})([CP])(\d+)"
        match = re.match(pattern, symbol)
        if not match:
            raise ValueError(f"无效的期权代码: {symbol}")
        
        underlying, date_str, option_type, strike_str = match.groups()
        
        # 验证标的代码格式
        if not underlying.isalpha():
            raise ValueError(f"无效的标的代码: {underlying}")
        
        # 解析日期
        try:
            expiry_date = datetime.strptime(date_str, "%y%m%d").date()
        except ValueError:
            raise ValueError(f"无效的到期日: {date_str}")
        
        # 验证期权类型
        if option_type not in ['C', 'P']:
            raise ValueError(f"无效的期权类型: {option_type}")
        
        # 解析行权价
        try:
            strike_price = float(strike_str) / 1000.0
            if strike_price <= 0:
                raise ValueError(f"无效的行权价: {strike_price}")
        except ValueError:
            raise ValueError(f"无效的行权价格字符串: {strike_str}")
        
        return underlying, expiry_date, option_type, strike_price
    
    def parse_option_symbol(self, symbol: str) -> tuple:
        """解析期权代码
        
        Args:
            symbol: 期权代码，如 AAPL160603C00090000
            
        Returns:
            (标的代码, 到期日, 期权类型, 行权价)
        """
        # 使用正则表达式解析期权代码
        pattern = r"([A-Z]+)(\d{6})([CP])(\d+)"
        match = re.match(pattern, symbol)
        if not match:
            raise ValueError(f"无效的期权代码: {symbol}")
        
        underlying, date_str, option_type, strike_str = match.groups()
        
        # 解析日期
        expiry_date = datetime.strptime(date_str, "%y%m%d").date()
        
        # 解析行权价
        strike_price = float(strike_str) / 1000.0
        
        return underlying, expiry_date, option_type, strike_price
    
    def import_option_csv(self, csv_file: str):
        """导入期权CSV数据
        
        Args:
            csv_file: CSV文件路径
        """
        try:
            print(f"\n=== 开始导入: {csv_file} ===")
            
            # 1. 读取CSV文件
            print("\n1. 读取CSV文件...")
            df = pd.read_csv(csv_file)
            print(f"CSV文件行数: {len(df)}")
            print("列名:", df.columns.tolist())
            
            # 2. 从文件名解析期权信息
            print("\n2. 解析期权信息...")
            symbol = os.path.basename(csv_file).split('.')[0]
            underlying, expiry_date, option_type, strike_price = self.parse_option_symbol(symbol)
            
            print(f"Symbol: {symbol}")
            print(f"Underlying: {underlying}")
            print(f"Expiry: {expiry_date}")
            print(f"Type: {option_type}")
            print(f"Strike: {strike_price}")
            
            # 3. 处理日期和时间戳
            print("\n3. 处理日期...")
            df['date'] = pd.to_datetime(df['date']).dt.date  # 只保留日期部分
            df['timestamp'] = pd.to_datetime(df['date']).astype('int64') // 10**9  # 转换为Unix时间戳
            print("日期样例:", df['date'].head())
            print("时间戳样例:", df['timestamp'].head())
            
            # 4. 准备数据
            print("\n4. 准备数据...")
            
            # 转换数据类型
            df['symbol'] = underlying  # 使用标的代码作为分区键
            df['type'] = option_type
            df['strike'] = pd.to_numeric(strike_price, errors='coerce')
            df['expiry'] = int(expiry_date.strftime('%Y%m%d'))
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)
            df['open'] = pd.to_numeric(df['open'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            
            # 检查数据类型
            print("\n数据类型:")
            print(df.dtypes)
            print("\n前5行:")
            print(df.head())
            
            # 检查是否有空值
            print("\n检查空值:")
            print(df.isnull().sum())
            
            # 5. 导入数据
            print("\n5. 导入数据...")
            print("上传数据到DolphinDB...")
            self.conn.upload({'options_data': df})
            
            print("执行插入脚本...")
            insert_script = f"""
            try
                // 使用数据库
                db = database('{self.db_path}')
                
                // 插入数据
                t = loadTable('{self.db_path}', 'options')
                t.append!(options_data)
                return 1;
            catch(ex)
                print(ex)
                return 0;
            """
            print(insert_script)
            success = self.conn.run(insert_script)
            print(f"插入结果: {success}")
            
            if not success:
                raise ValueError("数据导入失败")
            
            # 6. 验证导入结果
            print("\n6. 验证导入...")
            verify_script = f"""
            // 使用数据库
            db = database('{self.db_path}')
            t = loadTable('{self.db_path}', 'options')
            
            // 查询结果
            select 
                count(*) as total_rows, 
                min(date) as min_date, 
                max(date) as max_date 
            from t 
            where symbol = '{underlying}'
            """
            print("执行验证脚本:")
            print(verify_script)
            result = self.conn.run(verify_script)
            
            print("导入结果:")
            print(f"总行数: {result[0][0]}")
            print(f"日期范围: {result[0][1]} - {result[0][2]}")
            
            print("\n=== 导入完成 ===")
            
        except Exception as e:
            print(f"\n导入失败: {str(e)}")
            print("详细错误信息:")
            traceback.print_exc()
            raise
    
    def import_option_directory(self, directory: str):
        """导入目录下的所有期权CSV数据
        
        Args:
            directory: 包含期权CSV文件的目录路径
        """
        try:
            print(f"\n=== 开始导入目录: {directory} ===")
            
            # 确保数据库和表已初始化
            self._use_database()
            
            # 构建导入脚本
            script = f"""
            try {{
                // 使用数据库
                db = database('{self.db_path}')
                t = loadTable('{self.db_path}', 'options')
                
                // 获取目录下所有CSV文件
                files = files('{directory}')
                csvFiles = select filename from files where filename like '%.csv'
                
                // 遍历每个文件
                for (file in csvFiles) {{
                    try {{
                        // 从文件名解析期权信息
                        symbol = substr(file, 1, strlen(file)-4)  // 移除.csv后缀
                        
                        // 使用正则表达式解析期权代码
                        regex = regex(symbol, "([A-Z]+)\\\\d{{6}}([CP])\\\\d+")
                        if(regex.matched) {{
                            underlying = regex.groups[0]
                            option_type = regex.groups[1]
                            
                            // 解析日期
                            date_str = substr(symbol, strlen(underlying), 6)
                            expiry = temporalParse(date_str, "yyMMdd")
                            
                            // 解析行权价
                            strike_str = substr(symbol, strlen(underlying)+7, strlen(symbol)-strlen(underlying)-7)
                            strike = double(strike_str) / 1000.0
                            
                            // 读取CSV文件
                            data = loadText('{directory}/' + file)
                            
                            // 添加期权信息列
                            data[`symbol] = underlying
                            data[`type] = option_type
                            data[`strike] = strike
                            data[`expiry] = date(expiry)
                            
                            // 转换数据类型
                            update data set 
                                date = date(date),
                                timestamp = temporalParse(date, "yyyy.MM.dd"),
                                volume = int(volume),
                                open = double(open),
                                high = double(high),
                                low = double(low),
                                close = double(close)
                            
                            // 插入数据
                            t.append!(data)
                            print("成功导入文件: ", file)
                        }}
                    }} catch(ex) {{
                        print("导入文件失败: ", file, " 错误: ", ex)
                        continue
                    }}
                }}
                return 1;
            }} catch(ex) {{
                print(ex)
                return 0;
            }}
            """
            
            print("\n执行导入脚本...")
            print(script)
            success = self.conn.run(script)
            
            if not success:
                raise ValueError("数据导入失败")
            
            print("\n=== 目录导入完成 ===")
            
            # 验证导入结果
            verify_script = f"""
            // 使用数据库
            db = database('{self.db_path}')
            t = loadTable('{self.db_path}', 'options')
            
            // 查询结果
            select 
                symbol,
                count(*) as total_rows,
                min(date) as min_date,
                max(date) as max_date
            from t
            group by symbol
            order by symbol
            """
            print("\n验证导入结果:")
            print(verify_script)
            result = self.conn.run(verify_script)
            print("\n导入统计:")
            print(result)
            
        except Exception as e:
            print(f"\n目录导入失败: {str(e)}")
            print("详细错误信息:")
            traceback.print_exc()
            raise
    
    def insert_stock_daily(self, data: pd.DataFrame):
        """插入股票日线数据
        
        Args:
            data: 包含股票日线数据的DataFrame，需要包含以下列：
                - symbol: 股票代码
                - date: 日期
                - open: 开盘价
                - high: 最高价
                - low: 最低价
                - close: 收盘价
                - volume: 成交量
                - amount: 成交额
                - factor: 复权因子
        """
        # 将数据转换为 DolphinDB 表格式
        records = []
        for _, row in data.iterrows():
            record = (
                row['symbol'],
                row['date'],
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume']),
                float(row['amount']),
                float(row['factor'])
            )
            records.append(record)
        
        # 一次性插入所有数据
        script = """
        def insertData(records) {
            t = table(records, `symbol`date`open`high`low`close`volume`amount`factor)
            loadTable("dfs://market", "stock_daily").append!(t)
            return 1;
        }
        """
        self.conn.run(script)
        self.conn.run("insertData", records)
    
    def get_nasdaq100_data(self, start_date: str, end_date: str) -> pd.DataFrame:
        """获取 NASDAQ 100 成分股的日线数据
        
        Args:
            start_date: 开始日期，格式为 'YYYY.MM.DD'
            end_date: 结束日期，格式为 'YYYY.MM.DD'
            
        Returns:
            包含以下列的 DataFrame:
            - symbol: 股票代码
            - date: 日期
            - open: 开盘价
            - high: 最高价
            - low: 最低价
            - close: 收盘价
            - volume: 成交量
            - amount: 成交额
            - factor: 复权因子
        """
        script = f"""
        select * from loadTable('dfs://market', 'stock_daily') 
        where date >= '{start_date}' and date <= '{end_date}'
        """
        return pd.DataFrame(self.conn.run(script))

    def get_option_data(self, symbol: str, start_date: datetime, end_date: datetime, option_type: str = None):
        """获取期权数据
        
        Args:
            symbol: 期权代码
            start_date: 开始日期
            end_date: 结束日期
            option_type: 期权类型，'call' 或 'put'
        """
        try:
            # 打印表结构和数据
            print("表结构:")
            self.conn.run("schema(loadTable('dfs://options', 'options'))")
            print("\n前10行数据:")
            self.conn.run("select top 10 * from loadTable('dfs://options', 'options')")
            print("\n总行数:")
            self.conn.run("select count(*) from loadTable('dfs://options', 'options')")
            
            # 构建查询条件
            start_timestamp = int(start_date.timestamp() * 1000)
            end_timestamp = int(end_date.timestamp() * 1000)
            
            print(f"\n查询条件:")
            print(f"symbol: {symbol}")
            print(f"start_timestamp: {start_timestamp}")
            print(f"end_timestamp: {end_timestamp}")
            print(f"option_type: {option_type}")
            
            # 构建基本查询
            query = f"""
            select * from loadTable('dfs://options', 'options') 
            where symbol like '{symbol}%'
            and timestamp(timestamp) between timestamp({start_timestamp}) and timestamp({end_timestamp})
            """
            
            # 如果指定了期权类型，添加类型过滤
            if option_type:
                query += f" and type = '{option_type.upper()}'"
            
            print("\n执行的查询语句:")
            print(query)
            
            # 执行查询
            print("\n查询结果:")
            result = self.conn.run(query)
            print(f"返回行数: {len(result) if result is not None else 0}")
            if result is not None and len(result) > 0:
                print("\n前5行数据:")
                print(result.head())
            
            return result
            
        except Exception as e:
            print(f"查询期权数据时出错: {str(e)}")
            raise
