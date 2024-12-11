"""
数据库操作工具
"""
import dolphindb as ddb
import pymongo
from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from config import DB_CONFIG, TABLES, MONGODB_CONFIG, COLLECTIONS

class DBConnection:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DBConnection, cls).__new__(cls)
            cls._instance.conn = ddb.session()
            cls._instance.connect()
        return cls._instance
    
    def connect(self):
        """连接到DolphinDB服务器"""
        try:
            self.conn.connect(DB_CONFIG['host'], 
                            DB_CONFIG['port'],
                            DB_CONFIG['username'],
                            DB_CONFIG['password'])
            print("Successfully connected to DolphinDB")
        except Exception as e:
            print(f"Failed to connect to DolphinDB: {str(e)}")
            raise

    def get_option_chain(self, symbol, date):
        """获取特定股票和日期的期权链数据
        
        Args:
            symbol (str): 股票代码
            date (str): 交易日期 'YYYY-MM-DD'
            
        Returns:
            pandas.DataFrame: 期权链数据
        """
        query = f'''
        select * from {TABLES['options']}
        where underlying_symbol = '{symbol}'
        and trade_date = '{date}'
        '''
        return self.conn.run(query)
    
    def get_stock_data(self, symbol, start_date, end_date):
        """获取股票历史数据
        
        Args:
            symbol (str): 股票代码
            start_date (str): 开始日期 'YYYY-MM-DD'
            end_date (str): 结束日期 'YYYY-MM-DD'
            
        Returns:
            pandas.DataFrame: OHLC数据
        """
        query = f'''
        select * from {TABLES['stocks']}
        where symbol = '{symbol}'
        and trade_date between '{start_date}' and '{end_date}'
        '''
        return self.conn.run(query)
    
    def get_expirations(self, symbol, date):
        """获取可用的期权到期日
        
        Args:
            symbol (str): 股票代码
            date (str): 交易日期 'YYYY-MM-DD'
            
        Returns:
            list: 到期日列表
        """
        query = f'''
        select distinct expiration_date from {TABLES['options']}
        where underlying_symbol = '{symbol}'
        and trade_date = '{date}'
        order by expiration_date
        '''
        return self.conn.run(query)

class MongoDBConnection:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDBConnection, cls).__new__(cls)
            cls._instance.client = None
            cls._instance.db = None
            cls._instance.connect()
        return cls._instance
    
    def connect(self):
        """连接到MongoDB服务器"""
        try:
            # 构建MongoDB连接URI
            if MONGODB_CONFIG['username'] and MONGODB_CONFIG['password']:
                uri = f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}"
            else:
                uri = f"mongodb://{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}"
            
            self.client = MongoClient(uri)
            self.db = self.client[MONGODB_CONFIG['database']]
            print("Successfully connected to MongoDB")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {str(e)}")
            raise
    
    def save_stock_data(self, symbol: str, data: pd.DataFrame):
        """保存股票数据
        
        Args:
            symbol: 股票代码
            data: 股票数据DataFrame，包含OHLCV等信息
        """
        collection = self.db[COLLECTIONS['stocks']]
        records = data.to_dict('records')
        for record in records:
            record['symbol'] = symbol
            record['timestamp'] = datetime.now()
            collection.update_one(
                {'symbol': symbol, 'date': record['date']},
                {'$set': record},
                upsert=True
            )
    
    def save_option_data(self, data: pd.DataFrame):
        """保存期权数据
        
        Args:
            data: 期权数据DataFrame
        """
        collection = self.db[COLLECTIONS['options']]
        records = data.to_dict('records')
        for record in records:
            record['timestamp'] = datetime.now()
            collection.update_one(
                {
                    'symbol': record['symbol'],
                    'date': record['date'],
                    'expiration': record['expiration']
                },
                {'$set': record},
                upsert=True
            )
    
    def save_trade(self, trade_data: dict):
        """保存交易记录
        
        Args:
            trade_data: 交易数据字典，包含交易详情
        """
        collection = self.db[COLLECTIONS['trades']]
        trade_data['timestamp'] = datetime.now()
        collection.insert_one(trade_data)
    
    def save_position(self, position_data: dict):
        """保存持仓记录
        
        Args:
            position_data: 持仓数据字典，包含持仓详情
        """
        collection = self.db[COLLECTIONS['positions']]
        position_data['timestamp'] = datetime.now()
        collection.update_one(
            {
                'symbol': position_data['symbol'],
                'account': position_data['account']
            },
            {'$set': position_data},
            upsert=True
        )
    
    def save_signal(self, signal_data: dict):
        """保存交易信号
        
        Args:
            signal_data: 信号数据字典，包含信号详情
        """
        collection = self.db[COLLECTIONS['signals']]
        signal_data['timestamp'] = datetime.now()
        collection.insert_one(signal_data)
    
    def save_backtest_result(self, backtest_data: dict):
        """保存回测结果
        
        Args:
            backtest_data: 回测数据字典，包含回测结果
        """
        collection = self.db[COLLECTIONS['backtest']]
        backtest_data['timestamp'] = datetime.now()
        collection.insert_one(backtest_data)
    
    def get_stock_data(self, symbol: str, start_date: str = None, end_date: str = None):
        """获取股票数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            pd.DataFrame: 股票数据
        """
        collection = self.db[COLLECTIONS['stocks']]
        query = {'symbol': symbol}
        
        if start_date:
            query['date'] = {'$gte': start_date}
        if end_date:
            if 'date' in query:
                query['date']['$lte'] = end_date
            else:
                query['date'] = {'$lte': end_date}
        
        cursor = collection.find(query, {'_id': 0}).sort('date', 1)
        return pd.DataFrame(list(cursor))
    
    def get_option_chain(self, symbol: str, date: str):
        """获取期权链数据
        
        Args:
            symbol: 标的股票代码
            date: 交易日期 (YYYY-MM-DD)
        
        Returns:
            pd.DataFrame: 期权链数据
        """
        collection = self.db[COLLECTIONS['options']]
        cursor = collection.find(
            {
                'underlying_symbol': symbol,
                'date': date
            },
            {'_id': 0}
        )
        return pd.DataFrame(list(cursor))
    
    def get_positions(self, account: str = None):
        """获取持仓记录
        
        Args:
            account: 账户ID，如果为None则获取所有账户的持仓
        
        Returns:
            pd.DataFrame: 持仓数据
        """
        collection = self.db[COLLECTIONS['positions']]
        query = {'account': account} if account else {}
        cursor = collection.find(query, {'_id': 0})
        return pd.DataFrame(list(cursor))
    
    def get_trades(self, symbol: str = None, start_date: str = None, end_date: str = None):
        """获取交易记录
        
        Args:
            symbol: 股票代码，如果为None则获取所有股票的交易
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        
        Returns:
            pd.DataFrame: 交易记录
        """
        collection = self.db[COLLECTIONS['trades']]
        query = {}
        
        if symbol:
            query['symbol'] = symbol
        if start_date:
            query['date'] = {'$gte': start_date}
        if end_date:
            if 'date' in query:
                query['date']['$lte'] = end_date
            else:
                query['date'] = {'$lte': end_date}
        
        cursor = collection.find(query, {'_id': 0}).sort('date', -1)
        return pd.DataFrame(list(cursor))

# 单例模式使用示例
db = DBConnection()
mongodb = MongoDBConnection()

def get_dolphindb_connection():
    """获取DolphinDB连接实例"""
    return db.conn

def get_mongodb_connection():
    """获取MongoDB连接实例"""
    return mongodb.db
