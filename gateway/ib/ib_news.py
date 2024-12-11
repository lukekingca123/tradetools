from datetime import datetime
from typing import List, Optional, Dict
from ibapi.common import NewsProvider

from ...core.data.news import NewsData, NewsSource
from ...core.event.engine import EventEngine

class IbNewsHandler:
    """IB新闻处理器"""
    
    def __init__(self, gateway):
        """构造函数"""
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        self.api = gateway.api
        self.event_engine = gateway.event_engine
        
        # 新闻提供者
        self.providers: Dict[str, NewsProvider] = {}
        # 订阅的合约
        self.subscribed: Dict[str, int] = {}
        
        # 注册回调函数
        self.api.newsProviders = self.newsProviders
        self.api.newsArticle = self.newsArticle
        self.api.historicalNews = self.historicalNews
        
    def query_providers(self) -> None:
        """查询新闻提供者"""
        self.api.reqNewsProviders()
        
    def newsProviders(self, providers: List[NewsProvider]) -> None:
        """新闻提供者回报"""
        for provider in providers:
            self.providers[provider.providerCode] = provider
            self.gateway.write_log(f"新闻提供者：{provider.providerName}")
            
    def subscribe_news(self, symbols: List[str]) -> None:
        """订阅新闻"""
        for symbol in symbols:
            if symbol in self.subscribed:
                continue
                
            # 创建合约对象
            contract = self.api.create_contract(symbol)
            
            # 订阅新闻
            self.api.reqid += 1
            self.subscribed[symbol] = self.api.reqid
            self.api.reqMktData(
                self.api.reqid,
                contract,
                "mdoff,292",  # 292表示新闻订阅
                False,
                False,
                []
            )
            
            self.gateway.write_log(f"订阅{symbol}的新闻")
            
    def unsubscribe_news(self, symbols: List[str]) -> None:
        """取消订阅新闻"""
        for symbol in symbols:
            if symbol not in self.subscribed:
                continue
                
            reqid = self.subscribed[symbol]
            self.api.cancelMktData(reqid)
            del self.subscribed[symbol]
            
            self.gateway.write_log(f"取消订阅{symbol}的新闻")
            
    def query_historical_news(
        self,
        symbols: List[str],
        start_time: datetime,
        end_time: datetime,
        total_results: int = 100
    ) -> List[NewsData]:
        """查询历史新闻"""
        news_list = []
        
        for symbol in symbols:
            contract = self.api.create_contract(symbol)
            
            # 查询历史新闻
            self.api.reqid += 1
            self.api.reqHistoricalNews(
                self.api.reqid,
                contract.conId,
                self.providers.keys(),
                start_time.strftime("%Y%m%d %H:%M:%S"),
                end_time.strftime("%Y%m%d %H:%M:%S"),
                total_results
            )
            
        return news_list
        
    def historicalNews(
        self,
        reqId: int,
        time: str,
        providerCode: str,
        articleId: str,
        headline: str,
    ) -> None:
        """历史新闻回报"""
        # 创建新闻数据
        news = NewsData(
            source=NewsSource.IB,
            timestamp=datetime.strptime(time, "%Y%m%d %H:%M:%S"),
            title=headline,
            content="",  # 需要通过newsArticle获取具体内容
            symbols=[],  # 需要根据reqId找到对应的symbol
            additional_info={
                "provider": providerCode,
                "article_id": articleId
            }
        )
        
        # 请求新闻内容
        self.api.reqNewsArticle(
            reqId,
            providerCode,
            articleId,
            []
        )
        
    def newsArticle(self, reqId: int, articleType: int, articleText: str) -> None:
        """新闻内容回报"""
        # 更新新闻内容
        # TODO: 处理新闻内容，可能需要解析HTML或其他格式
        pass
