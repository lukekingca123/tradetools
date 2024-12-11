from datetime import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass
from enum import Enum

class NewsSource(Enum):
    IB = "IB"
    POLYGON = "POLYGON"
    FMP = "FINANCIAL_MODEL_PREP"
    OPENBB = "OPENBB"

@dataclass
class NewsData:
    """标准化新闻数据"""
    source: NewsSource
    timestamp: datetime
    title: str
    content: str
    symbols: List[str]
    url: Optional[str] = None
    sentiment: Optional[float] = None
    language: str = "en"
    additional_info: Dict = None

class NewsEngine:
    """新闻数据引擎"""
    def __init__(self):
        self.sources: Dict[NewsSource, object] = {}
        self.handlers: List[callable] = []
    
    def add_source(self, source: NewsSource, handler: object) -> None:
        """添加新闻数据源"""
        self.sources[source] = handler
    
    def add_handler(self, handler: callable) -> None:
        """添加新闻处理器"""
        self.handlers.append(handler)
    
    def process_news(self, news: NewsData) -> None:
        """处理新闻数据"""
        for handler in self.handlers:
            handler(news)
    
    def query_historical_news(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        sources: Optional[List[NewsSource]] = None
    ) -> List[NewsData]:
        """查询历史新闻"""
        all_news = []
        sources = sources or list(self.sources.keys())
        
        for source in sources:
            if source in self.sources:
                handler = self.sources[source]
                if hasattr(handler, "query_historical_news"):
                    news = handler.query_historical_news(
                        symbols, start_time, end_time
                    )
                    all_news.extend(news)
        
        return sorted(all_news, key=lambda x: x.timestamp)
    
    def subscribe_news(
        self,
        symbols: List[str],
        sources: Optional[List[NewsSource]] = None
    ) -> None:
        """订阅实时新闻"""
        sources = sources or list(self.sources.keys())
        
        for source in sources:
            if source in self.sources:
                handler = self.sources[source]
                if hasattr(handler, "subscribe_news"):
                    handler.subscribe_news(symbols)
    
    def unsubscribe_news(
        self,
        symbols: List[str],
        sources: Optional[List[NewsSource]] = None
    ) -> None:
        """取消订阅实时新闻"""
        sources = sources or list(self.sources.keys())
        
        for source in sources:
            if source in self.sources:
                handler = self.sources[source]
                if hasattr(handler, "unsubscribe_news"):
                    handler.unsubscribe_news(symbols)
