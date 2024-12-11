"""
常量定义模块
"""
from enum import Enum

class Direction(Enum):
    """交易方向"""
    LONG = "多"
    SHORT = "空"
    NET = "净"

class Exchange(Enum):
    """交易所"""
    # 美国
    SMART = "SMART"         # IB智能路由
    NYMEX = "NYMEX"        # 纽约商品交易所
    COMEX = "COMEX"        # 纽约商品交易所期货市场
    GLOBEX = "GLOBEX"      # 芝加哥商业交易所电子交易系统
    IDEALPRO = "IDEALPRO"  # IB外汇交易
    CME = "CME"           # 芝加哥商业交易所
    CBOE = "CBOE"         # 芝加哥期权交易所
    ICE = "ICE"           # 洲际交易所
    AMEX = "AMEX"         # 美国证券交易所
    NYSE = "NYSE"         # 纽约证券交易所
    NASDAQ = "NASDAQ"      # 纳斯达克证券交易所

class Product(Enum):
    """产品类型"""
    EQUITY = "股票"
    FUTURES = "期货"
    OPTION = "期权"
    INDEX = "指数"
    FOREX = "外汇"
    SPOT = "现货"

class OrderType(Enum):
    """委托类型"""
    LIMIT = "限价"
    MARKET = "市价"
    STOP = "止损"
    FAK = "FAK"
    FOK = "FOK"

class Status(Enum):
    """委托状态"""
    SUBMITTING = "提交中"
    NOTTRADED = "未成交"
    PARTTRADED = "部分成交"
    ALLTRADED = "全部成交"
    CANCELLED = "已撤销"
    REJECTED = "拒单"

class Interval(Enum):
    """K线周期"""
    MINUTE = "1m"
    HOUR = "1h"
    DAILY = "d"
    WEEKLY = "w"
