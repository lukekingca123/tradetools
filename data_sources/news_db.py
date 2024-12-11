"""
MongoDB新闻数据存储模块
"""
import os
from typing import List, Dict, Optional
from datetime import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

class NewsDatabase:
    def __init__(self):
        self.client = AsyncIOMotorClient(os.getenv('MONGO_URI'))
        self.db = self.client[os.getenv('MONGO_DB_NAME')]
        self.collection = self.db[os.getenv('MONGO_NEWS_COLLECTION')]
        
    async def insert_news(self, news_items: List[Dict]):
        """插入新闻数据"""
        if not news_items:
            return
        
        # 添加时间戳
        for item in news_items:
            item['created_at'] = datetime.utcnow()
            
        await self.collection.insert_many(news_items)
        
    async def get_news(self, 
                      symbol: Optional[str] = None,
                      start_date: Optional[datetime] = None,
                      end_date: Optional[datetime] = None,
                      limit: int = 100) -> List[Dict]:
        """查询新闻数据"""
        query = {}
        
        if symbol:
            query['symbol'] = symbol
            
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query['$gte'] = start_date
            if end_date:
                date_query['$lte'] = end_date
            query['published_date'] = date_query
            
        cursor = self.collection.find(query)
        cursor.sort('published_date', -1).limit(limit)
        
        return await cursor.to_list(length=limit)
    
    async def close(self):
        """关闭数据库连接"""
        self.client.close()
