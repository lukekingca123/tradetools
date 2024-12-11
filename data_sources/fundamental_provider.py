"""
基本面数据提供者
提供公司基本面信息、新闻、分析师报告等数据的存储和查询功能
"""
from typing import Dict, List, Optional, Union
import pandas as pd
from datetime import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database

from config import MONGODB_CONFIG, COLLECTIONS

class FundamentalProvider:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FundamentalProvider, cls).__new__(cls)
            cls._instance.client = None
            cls._instance.db = None
            cls._instance.connect()
        return cls._instance
    
    def connect(self):
        """连接到MongoDB数据库"""
        try:
            # 构建MongoDB连接URI
            if MONGODB_CONFIG['username'] and MONGODB_CONFIG['password']:
                uri = f"mongodb://{MONGODB_CONFIG['username']}:{MONGODB_CONFIG['password']}@{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}"
            else:
                uri = f"mongodb://{MONGODB_CONFIG['host']}:{MONGODB_CONFIG['port']}"
            
            self.client = MongoClient(uri)
            self.db = self.client[MONGODB_CONFIG['database']]
            
            # 创建索引
            self._ensure_indexes()
            
            print("Successfully connected to MongoDB")
        except Exception as e:
            print(f"Failed to connect to MongoDB: {str(e)}")
            raise
    
    def _ensure_indexes(self):
        """确保所有必要的索引都已创建"""
        # 公司信息索引
        self.db[COLLECTIONS['company_info']].create_index([('symbol', ASCENDING)], unique=True)
        
        # 财务数据索引
        self.db[COLLECTIONS['financials']].create_index([
            ('symbol', ASCENDING),
            ('date', DESCENDING)
        ])
        
        # 新闻数据索引
        self.db[COLLECTIONS['news']].create_index([
            ('symbol', ASCENDING),
            ('date', DESCENDING)
        ])
        self.db[COLLECTIONS['news']].create_index([('date', DESCENDING)])
        
        # 公告索引
        self.db[COLLECTIONS['announcements']].create_index([
            ('symbol', ASCENDING),
            ('date', DESCENDING)
        ])
        
        # 分析师评级索引
        self.db[COLLECTIONS['ratings']].create_index([
            ('symbol', ASCENDING),
            ('date', DESCENDING)
        ])
        
        # 目标价索引
        self.db[COLLECTIONS['price_targets']].create_index([
            ('symbol', ASCENDING),
            ('date', DESCENDING)
        ])
        
        # ETF持仓索引
        self.db[COLLECTIONS['etf_holdings']].create_index([
            ('etf_symbol', ASCENDING),
            ('date', DESCENDING)
        ])
    
    def save_company_info(self, symbol: str, info: Dict):
        """保存公司基本信息
        
        Args:
            symbol: 股票代码
            info: 公司信息字典
        """
        info['symbol'] = symbol
        info['updated_at'] = datetime.now()
        self.db[COLLECTIONS['company_info']].update_one(
            {'symbol': symbol},
            {'$set': info},
            upsert=True
        )
    
    def get_company_info(self, symbol: str) -> Optional[Dict]:
        """获取公司基本信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            公司信息字典或None
        """
        return self.db[COLLECTIONS['company_info']].find_one(
            {'symbol': symbol},
            {'_id': 0}
        )
    
    def save_financials(self, symbol: str, data: Union[Dict, List[Dict]]):
        """保存财务数据
        
        Args:
            symbol: 股票代码
            data: 财务数据字典或列表
        """
        if isinstance(data, dict):
            data = [data]
        
        for item in data:
            item['symbol'] = symbol
            item['updated_at'] = datetime.now()
            self.db[COLLECTIONS['financials']].update_one(
                {
                    'symbol': symbol,
                    'date': item['date']
                },
                {'$set': item},
                upsert=True
            )
    
    def get_financials(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取财务数据
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            财务数据DataFrame
        """
        query = {'symbol': symbol}
        if start_date or end_date:
            query['date'] = {}
            if start_date:
                query['date']['$gte'] = start_date
            if end_date:
                query['date']['$lte'] = end_date
        
        cursor = self.db[COLLECTIONS['financials']].find(
            query,
            {'_id': 0}
        ).sort('date', DESCENDING)
        
        return pd.DataFrame(list(cursor))
    
    def save_news(self, news_data: Union[Dict, List[Dict]]):
        """保存新闻数据
        
        Args:
            news_data: 新闻数据字典或列表
        """
        if isinstance(news_data, dict):
            news_data = [news_data]
        
        for item in news_data:
            item['updated_at'] = datetime.now()
            self.db[COLLECTIONS['news']].insert_one(item)
    
    def get_news(self, symbol: str = None, start_date: str = None, end_date: str = None,
                limit: int = 100) -> pd.DataFrame:
        """获取新闻数据
        
        Args:
            symbol: 股票代码（可选）
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            limit: 返回的最大记录数
            
        Returns:
            新闻数据DataFrame
        """
        query = {}
        if symbol:
            query['symbol'] = symbol
        if start_date or end_date:
            query['date'] = {}
            if start_date:
                query['date']['$gte'] = start_date
            if end_date:
                query['date']['$lte'] = end_date
        
        cursor = self.db[COLLECTIONS['news']].find(
            query,
            {'_id': 0}
        ).sort('date', DESCENDING).limit(limit)
        
        return pd.DataFrame(list(cursor))
    
    def save_analyst_rating(self, symbol: str, rating_data: Union[Dict, List[Dict]]):
        """保存分析师评级
        
        Args:
            symbol: 股票代码
            rating_data: 评级数据字典或列表
        """
        if isinstance(rating_data, dict):
            rating_data = [rating_data]
        
        for item in rating_data:
            item['symbol'] = symbol
            item['updated_at'] = datetime.now()
            self.db[COLLECTIONS['ratings']].insert_one(item)
    
    def get_analyst_ratings(self, symbol: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """获取分析师评级
        
        Args:
            symbol: 股票代码
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            评级数据DataFrame
        """
        query = {'symbol': symbol}
        if start_date or end_date:
            query['date'] = {}
            if start_date:
                query['date']['$gte'] = start_date
            if end_date:
                query['date']['$lte'] = end_date
        
        cursor = self.db[COLLECTIONS['ratings']].find(
            query,
            {'_id': 0}
        ).sort('date', DESCENDING)
        
        return pd.DataFrame(list(cursor))
    
    def save_etf_holdings(self, etf_symbol: str, holdings_data: Union[Dict, List[Dict]]):
        """保存ETF持仓数据
        
        Args:
            etf_symbol: ETF代码
            holdings_data: 持仓数据字典或列表
        """
        if isinstance(holdings_data, dict):
            holdings_data = [holdings_data]
        
        for item in holdings_data:
            item['etf_symbol'] = etf_symbol
            item['updated_at'] = datetime.now()
            self.db[COLLECTIONS['etf_holdings']].update_one(
                {
                    'etf_symbol': etf_symbol,
                    'date': item['date']
                },
                {'$set': item},
                upsert=True
            )
    
    def get_etf_holdings(self, etf_symbol: str, date: str = None) -> pd.DataFrame:
        """获取ETF持仓数据
        
        Args:
            etf_symbol: ETF代码
            date: 日期 (YYYY-MM-DD)，如果为None则返回最新数据
            
        Returns:
            持仓数据DataFrame
        """
        query = {'etf_symbol': etf_symbol}
        if date:
            query['date'] = date
        
        if date:
            cursor = self.db[COLLECTIONS['etf_holdings']].find(
                query,
                {'_id': 0}
            )
        else:
            # 获取最新日期的数据
            latest_date = self.db[COLLECTIONS['etf_holdings']].find_one(
                {'etf_symbol': etf_symbol},
                sort=[('date', DESCENDING)]
            )
            if latest_date:
                query['date'] = latest_date['date']
                cursor = self.db[COLLECTIONS['etf_holdings']].find(
                    query,
                    {'_id': 0}
                )
            else:
                return pd.DataFrame()
        
        return pd.DataFrame(list(cursor))

# 单例模式使用示例
fundamental_provider = FundamentalProvider()
