"""
使用DolphinDB处理美股期权数据的模块
"""
import dolphindb as ddb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Tuple
import glob
import os
import logging
import time
from datetime import datetime
import ta

# 设置日志
log_filename = f"option_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class OptionDataHandler:
    def __init__(self, host: str = "localhost", port: int = 8848, username: str = "admin", password: str = "123456"):
        """初始化DolphinDB连接
        
        Args:
            host: DolphinDB服务器地址
            port: DolphinDB端口
            username: 用户名
            password: 密码
        """
        print("Connecting to DolphinDB...")
        self.conn = ddb.session()
        self.conn.connect(host, port, username, password)
        print("Connected successfully")
        self.initialize_database()
        
    def initialize_database(self):
        """初始化数据库schema"""
        try:
            logging.info("Initializing database schema...")
            # 创建数据库，按日期分区
            self.conn.run("""
                if(existsDatabase("dfs://options")){
                    dropDatabase("dfs://options")
                }
                db = database("dfs://options", VALUE, 2023.01.01..2025.12.31)
            """)
            logging.info("Database created successfully")

            # 创建分区表
            self.conn.run("""
                schema = table(
                    1:0, `symbol`date`timestamp`open`high`low`close`volume`openinterest,
                    [SYMBOL,DATE,TIMESTAMP,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE,DOUBLE]
                )
                db = database("dfs://options")
                table_name = "option_60min"
                if(existsTable(db, table_name)){
                    dropTable(db, table_name)
                }
                db.createPartitionedTable(
                    schema, table_name, `date
                )
            """)
            logging.info("Table created successfully")
            
        except Exception as e:
            logging.error(f"Error initializing database: {str(e)}")
            raise

    def clean_option_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """清洗和处理期权数据，考虑期权市场的特殊性
        
        Args:
            df: 原始期权数据DataFrame
            
        Returns:
            处理后的DataFrame
        """
        logging.info("Cleaning option data...")
        
        # 1. 基本数据检查和清洗
        # 删除完全重复的行
        df = df.drop_duplicates()
        
        # 2. 处理缺失值
        price_cols = ['open', 'high', 'low', 'close']
        
        # 记录填充前的缺失值数量
        missing_before = df[price_cols].isnull().sum()
        if missing_before.any():
            logging.info(f"Missing values before filling: {missing_before.to_dict()}")
        
        # 对于期权数据，我们需要更谨慎地处理缺失值
        for col in price_cols:
            # 创建填充标记列
            fill_col = f"{col}_filled"
            df[fill_col] = df[col].isnull()
            
            # 对于连续的缺失值，我们不进行填充，而是标记为无效数据
            # 因为期权价格波动大，用前值填充可能产生误导
            null_groups = df[col].isnull().astype(int).groupby(df['symbol']).cumsum()
            long_null_periods = null_groups > 3  # 连续3个以上的缺失值视为无效数据段
            
            # 只对短期缺失进行填充
            short_null = df[col].isnull() & ~long_null_periods
            if short_null.any():
                # 使用同组期权（相同到期日和行权价）的中位数填充
                if 'expiry' in df.columns and 'strike' in df.columns:
                    df.loc[short_null, col] = df[short_null].groupby(['expiry', 'strike'])[col].transform(
                        lambda x: x.fillna(x.median()))
                else:
                    # 如果没有期权具体信息，使用简单的前值填充
                    df.loc[short_null, col] = df.loc[short_null, col].fillna(method='ffill')
            
            # 标记长期缺失的数据
            df.loc[long_null_periods, f"{col}_valid"] = False
            
        # 3. 处理异常值
        # 期权价格波动较大，我们使用更宽松的异常值检测标准
        def detect_option_outliers(group):
            """基于期权特性的异常值检测"""
            if len(group) < 5:  # 样本太少不做检测
                return pd.Series(False, index=group.index)
                
            # 使用价格变化率而不是绝对价格来检测异常
            returns = group.pct_change()
            # 期权允许更大的价格波动，使用10个标准差
            mean_ret = returns.mean()
            std_ret = returns.std()
            if std_ret == 0:
                return pd.Series(False, index=group.index)
                
            z_scores = abs((returns - mean_ret) / std_ret)
            return z_scores > 10
        
        # 按期权合约分组检测异常值
        for col in price_cols:
            if 'expiry' in df.columns and 'strike' in df.columns:
                outliers = df.groupby(['expiry', 'strike'])[col].apply(detect_option_outliers)
            else:
                outliers = df.groupby('symbol')[col].apply(detect_option_outliers)
                
            if outliers.any():
                logging.warning(f"Found {outliers.sum()} potential outliers in {col}")
                # 记录异常值但不自动修正，因为期权价格剧烈波动可能是正常的
                df.loc[outliers, f"{col}_outlier"] = True
        
        # 4. 添加流动性指标
        df['liquidity_score'] = 0
        # 基于volume的流动性评分
        volume_quantiles = df.groupby(['symbol'])['volume'].transform(
            lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop'))
        df['liquidity_score'] += volume_quantiles.fillna(0)
        
        # 基于价差的流动性评分（如果有bid/ask数据）
        if 'bid' in df.columns and 'ask' in df.columns:
            spread = (df['ask'] - df['bid']) / ((df['ask'] + df['bid'])/2)
            spread_quantiles = df.groupby(['symbol'])['spread'].transform(
                lambda x: pd.qcut(x, q=5, labels=False, duplicates='drop'))
            df['liquidity_score'] += (4 - spread_quantiles.fillna(4))  # 价差越小流动性越好
        
        # 标记低流动性期间
        df['low_liquidity'] = df['liquidity_score'] <= 2
        
        logging.info("Data cleaning completed")
        return df

    def _calculate_greeks(self, S: float, K: float, T: float, r: float, sigma: float, 
                         option_type: str) -> Dict[str, float]:
        """计算期权的Greeks
        
        Args:
            S: 标的价格
            K: 行权价
            T: 到期时间（年）
            r: 无风险利率
            sigma: 波动率
            option_type: 期权类型 ('call' or 'put')
            
        Returns:
            包含Greeks的字典
        """
        try:
            from scipy.stats import norm
            
            # d1, d2计算
            d1 = (np.log(S/K) + (r + sigma**2/2)*T) / (sigma*np.sqrt(T))
            d2 = d1 - sigma*np.sqrt(T)
            
            # 计算N(d1)和N(d2)
            Nd1 = norm.cdf(d1)
            Nd2 = norm.cdf(d2)
            
            if option_type == 'call':
                # Call期权的Greeks
                delta = Nd1
                theta = (-S*sigma*np.exp(-d1**2/2)/(2*np.sqrt(2*np.pi*T)) - 
                        r*K*np.exp(-r*T)*Nd2)
            else:
                # Put期权的Greeks
                delta = Nd1 - 1
                theta = (-S*sigma*np.exp(-d1**2/2)/(2*np.sqrt(2*np.pi*T)) + 
                        r*K*np.exp(-r*T)*(1 - Nd2))
            
            # 通用Greeks
            gamma = np.exp(-d1**2/2)/(S*sigma*np.sqrt(2*np.pi*T))
            vega = S*np.sqrt(T)*np.exp(-d1**2/2)/np.sqrt(2*np.pi)
            
            return {
                'delta': delta,
                'gamma': gamma,
                'theta': theta,
                'vega': vega
            }
            
        except Exception as e:
            logging.error(f"Error calculating Greeks: {str(e)}")
            return {
                'delta': np.nan,
                'gamma': np.nan,
                'theta': np.nan,
                'vega': np.nan
            }

    def _calculate_greeks_bcc97(self, S: float, K: float, T: float, r: float, sigma: float,
                               option_type: str, q: float = 0.0) -> Dict[str, float]:
        """使用BCC97模型计算期权Greeks
        
        Args:
            S: 标的价格
            K: 行权价
            T: 到期时间（年）
            r: 无风险利率
            sigma: 波动率
            option_type: 期权类型 ('call' or 'put')
            q: 股息率（默认为0）
            
        Returns:
            包含Greeks的字典
        """
        try:
            from scipy.stats import norm
            
            # BCC97模型的d1和d2计算
            d1 = (np.log(S/K) + (r - q + sigma**2/2)*T) / (sigma*np.sqrt(T))
            d2 = d1 - sigma*np.sqrt(T)
            
            # 计算N(d1)和N(d2)
            Nd1 = norm.cdf(d1)
            Nd2 = norm.cdf(d2)
            nd1 = norm.pdf(d1)  # n(d1)用于计算gamma和vega
            
            # e^(-qT)和e^(-rT)
            e_qt = np.exp(-q*T)
            e_rt = np.exp(-r*T)
            
            if option_type == 'call':
                # Call期权的Greeks
                delta = e_qt * Nd1
                price = S * e_qt * Nd1 - K * e_rt * Nd2
                theta = (-S * sigma * e_qt * nd1 / (2 * np.sqrt(T)) -
                        r * K * e_rt * Nd2 + q * S * e_qt * Nd1)
            else:
                # Put期权的Greeks
                delta = -e_qt * (1 - Nd1)
                price = K * e_rt * (1 - Nd2) - S * e_qt * (1 - Nd1)
                theta = (-S * sigma * e_qt * nd1 / (2 * np.sqrt(T)) +
                        r * K * e_rt * (1 - Nd2) - q * S * e_qt * (1 - Nd1))
            
            # 通用Greeks
            gamma = e_qt * nd1 / (S * sigma * np.sqrt(T))
            vega = S * e_qt * nd1 * np.sqrt(T)
            rho = K * T * e_rt * (Nd2 if option_type == 'call' else -norm.cdf(-d2))
            
            return {
                'price': price,
                'delta': delta,
                'gamma': gamma,
                'theta': theta,
                'vega': vega,
                'rho': rho
            }
            
        except Exception as e:
            logging.error(f"Error calculating BCC97 Greeks: {str(e)}")
            return {
                'price': np.nan,
                'delta': np.nan,
                'gamma': np.nan,
                'theta': np.nan,
                'vega': np.nan,
                'rho': np.nan
            }

    def calculate_option_features(self, df: pd.DataFrame, model: str = 'bs') -> pd.DataFrame:
        """计算期权特定的特征
        
        Args:
            df: 期权数据DataFrame
            model: 使用的期权定价模型，'bs'为Black-Scholes模型，'bcc97'为BCC97模型
        
        Returns:
            添加特征后的DataFrame
        """
        logging.info(f"Calculating option features using {model} model...")
        
        # 1. 基础时间特征
        df['time_to_expiry'] = (pd.to_datetime(df['expiry']) - pd.to_datetime(df['date'])).dt.days / 365.0
        
        # 2. 价格特征
        if 'underlying_price' in df.columns:
            # 期权虚实度
            df['moneyness'] = df.apply(
                lambda x: x['underlying_price'] / x['strike'] if x['option_type'] == 'call'
                else x['strike'] / x['underlying_price'],
                axis=1
            )
            
            # 期权溢价率
            df['premium_ratio'] = df.apply(
                lambda x: x['close'] / x['underlying_price'],
                axis=1
            )
        
        # 3. 波动率特征
        # 日内波动率
        df['intraday_volatility'] = (df['high'] - df['low']) / df['close']
        
        # 如果有隐含波动率数据
        if 'implied_vol' in df.columns:
            # 计算IV的移动平均和变化
            df['iv_ma5'] = df.groupby('symbol')['implied_vol'].transform(lambda x: x.rolling(5).mean())
            df['iv_ma20'] = df.groupby('symbol')['implied_vol'].transform(lambda x: x.rolling(20).mean())
            df['iv_change'] = df.groupby('symbol')['implied_vol'].transform(lambda x: x.pct_change())
        
        # 4. 期权链特征（同一标的、到期日的所有期权）
        if 'underlying' in df.columns and 'expiry' in df.columns:
            # 计算同一期权链的特征
            chain_groups = df.groupby(['date', 'underlying', 'expiry'])
            
            # 距离ATM最近的N个期权的平均IV
            if 'implied_vol' in df.columns and 'moneyness' in df.columns:
                df['chain_near_atm_iv'] = chain_groups.apply(
                    lambda g: g.nsmallest(3, abs(g['moneyness'] - 1))['implied_vol'].mean()
                ).reset_index(level=[0,1,2], drop=True)
            
            # 期权链的交易量分布
            df['chain_volume_ratio'] = df['volume'] / chain_groups['volume'].transform('sum')
            
            # 期权在链中的相对位置（按行权价排序）
            df['chain_strike_rank'] = chain_groups['strike'].transform(
                lambda x: pd.qcut(x, q=5, labels=['deep_otm', 'otm', 'atm', 'itm', 'deep_itm'], 
                                duplicates='drop')
            )
        
        # 5. 流动性特征
        # 计算日内成交量分布
        df['volume_ma5'] = df.groupby('symbol')['volume'].transform(lambda x: x.rolling(5).mean())
        df['volume_ma20'] = df.groupby('symbol')['volume'].transform(lambda x: x.rolling(20).mean())
        df['volume_ratio'] = df['volume'] / df['volume_ma5']
        
        # 如果有bid/ask数据
        if 'bid' in df.columns and 'ask' in df.columns:
            df['spread'] = (df['ask'] - df['bid']) / ((df['ask'] + df['bid'])/2)
            df['spread_ma5'] = df.groupby('symbol')['spread'].transform(lambda x: x.rolling(5).mean())
        
        # 6. 技术指标
        # RSI
        df['rsi14'] = df.groupby('symbol')['close'].transform(
            lambda x: ta.RSI(x, timeperiod=14)
        )
        
        # 布林带
        df['boll_upper'], df['boll_middle'], df['boll_lower'] = ta.BBANDS(
            df.groupby('symbol')['close'].transform('get').values,
            timeperiod=20,
            nbdevup=2,
            nbdevdn=2
        )
        
        # 7. Greeks（如果有必要的数据）
        required_cols = ['underlying_price', 'risk_free_rate', 'implied_vol']
        if model == 'bcc97':
            required_cols.append('dividend_yield')  # BCC97模型需要股息率
            
        if all(col in df.columns for col in required_cols):
            for idx, row in df.iterrows():
                try:
                    if model == 'bcc97':
                        greeks = self._calculate_greeks_bcc97(
                            S=row['underlying_price'],
                            K=row['strike'],
                            T=row['time_to_expiry'],
                            r=row['risk_free_rate'],
                            sigma=row['implied_vol'],
                            option_type=row['option_type'],
                            q=row['dividend_yield']
                        )
                    else:  # 默认使用BS模型
                        greeks = self._calculate_greeks(
                            S=row['underlying_price'],
                            K=row['strike'],
                            T=row['time_to_expiry'],
                            r=row['risk_free_rate'],
                            sigma=row['implied_vol'],
                            option_type=row['option_type']
                        )
                        
                    # 添加模型前缀以区分不同模型的结果
                    prefix = 'bcc_' if model == 'bcc97' else 'bs_'
                    for greek, value in greeks.items():
                        df.at[idx, f'{prefix}{greek}'] = value
                        
                except Exception as e:
                    logging.warning(f"Failed to calculate Greeks for row {idx}: {str(e)}")
                    
        logging.info("Feature calculation completed")
        return df
        
    def process_csv_files(self, csv_dir: str, batch_size: int = 1000, test_mode: bool = False):
        """处理CSV文件并批量上传到DolphinDB
        
        Args:
            csv_dir: CSV文件目录
            batch_size: 批量处理的文件数
            test_mode: 是否测试模式（只处理前3个文件）
        """
        logging.info(f"Searching for CSV files in {csv_dir}...")
        csv_files = glob.glob(os.path.join(csv_dir, "*.csv"))
        total_files = len(csv_files)
        logging.info(f"Found {total_files} CSV files")

        if test_mode:
            csv_files = csv_files[:3]
            logging.info("Running in test mode with first 3 files")

        # 批量处理文件
        for i in range(0, len(csv_files), batch_size):
            batch_files = csv_files[i:i+batch_size]
            batch_data = []
            
            for j, csv_file in enumerate(batch_files, 1):
                try:
                    logging.info(f"Processing [{j}/{len(batch_files)}] {os.path.basename(csv_file)}")
                    
                    # 读取CSV文件
                    logging.info("Reading CSV file...")
                    df = pd.read_csv(csv_file)
                    logging.info(f"Read {len(df)} rows")

                    # 数据清洗和处理
                    df = self.clean_option_data(df)
                    logging.info(f"Cleaned data: {len(df)} rows remaining")

                    # 计算期权特征
                    df = self.calculate_option_features(df)

                    # 提取期权代码
                    symbol = os.path.basename(csv_file).replace('.csv', '')
                    
                    # 转换时间戳
                    logging.info("Converting timestamps...")
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df['date'] = df['timestamp'].dt.date
                    
                    # 准备数据
                    for _, row in df.iterrows():
                        batch_data.append({
                            'symbol': symbol,
                            'date': row['date'],
                            'timestamp': row['timestamp'],
                            'open': row['open'],
                            'high': row['high'],
                            'low': row['low'],
                            'close': row['close'],
                            'volume': row['volume'],
                            'openinterest': row['openinterest']
                        })

                    logging.info(f"Successfully processed {os.path.basename(csv_file)}")

                except Exception as e:
                    logging.error(f"Error processing {csv_file}: {str(e)}")
                    continue

            # 批量上传数据
            if batch_data:
                retry_count = 3
                while retry_count > 0:
                    try:
                        logging.info(f"Uploading batch of {len(batch_data)} records to DolphinDB...")
                        
                        # 准备批量数据
                        symbols = [d['symbol'] for d in batch_data]
                        dates = [d['date'] for d in batch_data]
                        timestamps = [int(d['timestamp'].timestamp() * 1000) for d in batch_data]
                        opens = [d['open'] for d in batch_data]
                        highs = [d['high'] for d in batch_data]
                        lows = [d['low'] for d in batch_data]
                        closes = [d['close'] for d in batch_data]
                        volumes = [d['volume'] for d in batch_data]
                        openinterests = [d['openinterest'] for d in batch_data]

                        # 执行批量插入
                        script = f"""
                        symbols = {symbols};
                        dates = {dates};
                        timestamps = timestamp({timestamps});
                        opens = {opens};
                        highs = {highs};
                        lows = {lows};
                        closes = {closes};
                        volumes = {volumes};
                        openinterests = {openinterests};
                        
                        t = table(symbols as symbol, dates as date, timestamps as timestamp, 
                                opens as open, highs as high, lows as low, closes as close,
                                volumes as volume, openinterests as openinterest)
                        loadTable("dfs://options", "option_60min").append!(t)
                        """
                        self.conn.run(script)
                        logging.info("Batch upload successful")
                        break
                    
                    except Exception as e:
                        retry_count -= 1
                        if retry_count > 0:
                            logging.warning(f"Upload failed, retrying... ({retry_count} attempts left)")
                            time.sleep(2)  # 等待2秒后重试
                        else:
                            logging.error(f"Failed to upload batch after all retries: {str(e)}")

        logging.info("Data import completed successfully!")

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'conn'):
            self.conn.close()

    def get_trading_calendar(self) -> pd.Series:
        """获取交易日历
        
        Returns:
            pd.Series: 交易日期列表
        """
        query = """
        select distinct date from OptionData
        order by date
        """
        calendar = self.conn.run(query)
        return pd.Series(calendar['date'])
        
    def get_option_instruments(self) -> pd.DataFrame:
        """获取期权合约信息
        
        Returns:
            pd.DataFrame: 期权合约信息
        """
        query = """
        select 
            symbol as instrument,
            min(date) as start_time,
            expiry as end_time,
            underlying,
            option_type as type,
            strike
        from OptionData
        group by symbol
        """
        instruments = self.conn.run(query)
        return instruments
        
    def get_option_data(self, symbols: list = None, start_date: str = None, 
                       end_date: str = None) -> pd.DataFrame:
        """获取期权数据
        
        Args:
            symbols: 期权代码列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            pd.DataFrame: 期权数据
        """
        conditions = []
        if symbols:
            symbols_str = ",".join([f"'{s}'" for s in symbols])
            conditions.append(f"symbol in [{symbols_str}]")
        if start_date:
            conditions.append(f"date >= '{start_date}'")
        if end_date:
            conditions.append(f"date <= '{end_date}'")
            
        where_clause = " and ".join(conditions) if conditions else "1=1"
        
        query = f"""
        select * from OptionData
        where {where_clause}
        order by symbol, date
        """
        
        data = self.conn.run(query)
        return data

if __name__ == "__main__":
    handler = None
    try:
        print("Starting data import process...")
        
        # 创建处理器实例
        handler = OptionDataHandler()
        
        # 加载数据
        handler.process_csv_files("/home/luke/optionSource/source/60min")
        
        print("\nData import completed successfully!")
        
    except Exception as e:
        print(f"\nError during data import: {str(e)}")
    finally:
        if handler:
            handler.close()
