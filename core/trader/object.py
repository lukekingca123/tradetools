"""
基础对象模块
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from .constant import Direction, Exchange, Product, Status, OrderType, Interval

@dataclass
class BaseData:
    """基础数据对象"""
    gateway_name: str             # 接口名称
    symbol: str                   # 代码
    exchange: Exchange            # 交易所
    datetime: datetime           # 时间

    def __post_init__(self):
        """转换时间格式"""
        if self.datetime and isinstance(self.datetime, str):
            self.datetime = datetime.strptime(self.datetime, "%Y-%m-%d %H:%M:%S")

@dataclass
class TickData(BaseData):
    """TICK数据"""
    name: str = ""               # 名称
    volume: float = 0            # 成交量
    turnover: float = 0          # 成交额
    open_interest: float = 0     # 持仓量
    last_price: float = 0        # 最新价
    last_volume: float = 0       # 最新成交量
    limit_up: float = 0          # 涨停价
    limit_down: float = 0        # 跌停价
    
    open_price: float = 0        # 开盘价
    high_price: float = 0        # 最高价
    low_price: float = 0         # 最低价
    pre_close: float = 0         # 昨收价
    bid_price_1: float = 0       # 买一价
    bid_price_2: float = 0       # 买二价
    bid_price_3: float = 0       # 买三价
    bid_price_4: float = 0       # 买四价
    bid_price_5: float = 0       # 买五价
    ask_price_1: float = 0       # 卖一价
    ask_price_2: float = 0       # 卖二价
    ask_price_3: float = 0       # 卖三价
    ask_price_4: float = 0       # 卖四价
    ask_price_5: float = 0       # 卖五价
    bid_volume_1: float = 0      # 买一量
    bid_volume_2: float = 0      # 买二量
    bid_volume_3: float = 0      # 买三量
    bid_volume_4: float = 0      # 买四量
    bid_volume_5: float = 0      # 买五量
    ask_volume_1: float = 0      # 卖一量
    ask_volume_2: float = 0      # 卖二量
    ask_volume_3: float = 0      # 卖三量
    ask_volume_4: float = 0      # 卖四量
    ask_volume_5: float = 0      # 卖五量

@dataclass
class OrderData(BaseData):
    """委托数据"""
    orderid: str                # 委托号
    type: OrderType             # 委托类型
    direction: Direction        # 委托方向
    price: float               # 委托价格
    volume: float              # 委托数量
    traded: float = 0          # 成交数量
    status: Status = Status.SUBMITTING  # 委托状态
    time: str = ""             # 委托时间
    reference: str = ""        # 引用

@dataclass
class TradeData(BaseData):
    """成交数据"""
    orderid: str               # 委托号
    tradeid: str              # 成交号
    direction: Direction       # 成交方向
    price: float              # 成交价格
    volume: float             # 成交数量
    time: str = ""            # 成交时间

@dataclass
class PositionData(BaseData):
    """持仓数据"""
    direction: Direction       # 持仓方向
    volume: float             # 持仓量
    frozen: float = 0         # 冻结量
    price: float = 0          # 持仓均价
    pnl: float = 0           # 持仓盈亏
    yd_volume: float = 0      # 昨持仓

@dataclass
class AccountData(BaseData):
    """账户数据"""
    accountid: str            # 账户代码
    balance: float = 0        # 账户余额
    frozen: float = 0         # 冻结金额
    available: float = 0      # 可用资金

@dataclass
class ContractData(BaseData):
    """合约数据"""
    name: str                 # 合约名称
    product: Product          # 合约类型
    size: float              # 合约大小
    pricetick: float         # 价格最小变动
    min_volume: float = 1    # 最小交易量
    stop_supported: bool = False  # 是否支持停止单
    net_position: bool = False   # 是否净持仓
    history_data: bool = False   # 是否有历史数据

@dataclass
class LogData(BaseData):
    """日志数据"""
    msg: str                     # 日志信息
    level: str = "INFO"          # 日志级别

@dataclass
class OrderRequest:
    """委托请求"""
    symbol: str               # 代码
    exchange: Exchange        # 交易所
    direction: Direction      # 方向
    type: OrderType          # 类型
    volume: float            # 数量
    price: float = 0         # 价格
    reference: str = ""      # 引用

    def create_order_data(self, orderid: str, gateway_name: str) -> OrderData:
        """创建委托数据"""
        order = OrderData(
            gateway_name=gateway_name,
            symbol=self.symbol,
            exchange=self.exchange,
            orderid=orderid,
            type=self.type,
            direction=self.direction,
            price=self.price,
            volume=self.volume,
            reference=self.reference,
            datetime=datetime.now()
        )
        return order

@dataclass
class CancelRequest:
    """撤单请求"""
    orderid: str            # 委托号
    symbol: str            # 代码
    exchange: Exchange     # 交易所
