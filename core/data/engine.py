from datetime import datetime
from typing import List, Optional, Dict, Type
from abc import ABC, abstractmethod

from .news import NewsEngine, NewsData, NewsSource
from ..event.engine import EventEngine, Event

class DataSource(ABC):
    """数据源基类"""
    
    @abstractmethod
    def connect(self) -> None:
        """连接数据源"""
        pass
        
    @abstractmethod
    def disconnect(self) -> None:
        """断开连接"""
        pass
        
    @abstractmethod
    def is_connected(self) -> bool:
        """是否已连接"""
        pass

class DataEngine:
    """数据引擎"""
    
    def __init__(self, event_engine: EventEngine):
        """构造函数"""
        self.event_engine = event_engine
        self.sources: Dict[str, DataSource] = {}
        self.news_engine = NewsEngine()
        
        # 注册事件处理函数
        self.register_handlers()
        
    def register_handlers(self) -> None:
        """注册事件处理函数"""
        self.event_engine.register("NEWS", self.process_news)
        
    def add_data_source(self, name: str, source: DataSource) -> None:
        """添加数据源"""
        self.sources[name] = source
        
    def connect(self, name: str) -> None:
        """连接数据源"""
        if name in self.sources:
            source = self.sources[name]
            source.connect()
            
    def disconnect(self, name: str) -> None:
        """断开数据源"""
        if name in self.sources:
            source = self.sources[name]
            source.disconnect()
            
    def process_news(self, event: Event) -> None:
        """处理新闻事件"""
        news = event.data
        if isinstance(news, NewsData):
            self.news_engine.process_news(news)
            
    def query_historical_data(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime,
        data_type: str,
        source: Optional[str] = None
    ) -> List[Dict]:
        """查询历史数据"""
        if source and source in self.sources:
            source_obj = self.sources[source]
            if hasattr(source_obj, "query_historical_data"):
                return source_obj.query_historical_data(
                    symbol, start_time, end_time, data_type
                )
        return []
        
    def subscribe_market_data(
        self,
        symbols: List[str],
        data_type: str,
        source: Optional[str] = None
    ) -> None:
        """订阅市场数据"""
        if source and source in self.sources:
            source_obj = self.sources[source]
            if hasattr(source_obj, "subscribe_market_data"):
                source_obj.subscribe_market_data(symbols, data_type)
                
    def unsubscribe_market_data(
        self,
        symbols: List[str],
        data_type: str,
        source: Optional[str] = None
    ) -> None:
        """取消订阅市场数据"""
        if source and source in self.sources:
            source_obj = self.sources[source]
            if hasattr(source_obj, "unsubscribe_market_data"):
                source_obj.unsubscribe_market_data(symbols, data_type)
                
    def get_news_engine(self) -> NewsEngine:
        """获取新闻引擎"""
        return self.news_engine
