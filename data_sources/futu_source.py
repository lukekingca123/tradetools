"""
富途API数据源
"""
import os
from futu import OpenQuoteContext, OpenHKTradeContext, OpenUSTradeContext
from dotenv import load_dotenv

load_dotenv()

class FutuDataSource:
    def __init__(self):
        self.quote_ctx = OpenQuoteContext(host=os.getenv('FUTU_HOST'), 
                                        port=int(os.getenv('FUTU_PORT')))
        
        # 交易上下文 - 按需初始化
        self._hk_trade_ctx = None
        self._us_trade_ctx = None
        
    @property
    def hk_trade_ctx(self):
        if self._hk_trade_ctx is None:
            self._hk_trade_ctx = OpenHKTradeContext(host=os.getenv('FUTU_HOST'),
                                                  port=int(os.getenv('FUTU_PORT')))
        return self._hk_trade_ctx
    
    @property
    def us_trade_ctx(self):
        if self._us_trade_ctx is None:
            self._us_trade_ctx = OpenUSTradeContext(host=os.getenv('FUTU_HOST'),
                                                  port=int(os.getenv('FUTU_PORT')))
        return self._us_trade_ctx
    
    def get_option_chain(self, symbol, start_date=None, end_date=None):
        """获取期权链数据"""
        ret, data = self.quote_ctx.get_option_chain(symbol=symbol,
                                                  start=start_date,
                                                  end=end_date)
        if ret == 0:
            return data
        return None
    
    def get_market_snapshot(self, symbols):
        """获取市场快照"""
        ret, data = self.quote_ctx.get_market_snapshot(symbols)
        if ret == 0:
            return data
        return None
    
    def close(self):
        """关闭所有连接"""
        self.quote_ctx.close()
        if self._hk_trade_ctx:
            self._hk_trade_ctx.close()
        if self._us_trade_ctx:
            self._us_trade_ctx.close()
            
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
