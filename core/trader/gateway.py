"""
交易接口基类
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from datetime import datetime
from ..event.engine import Event, EventEngine
from .object import (
    TickData, OrderData, TradeData, PositionData,
    AccountData, ContractData, OrderRequest,
    CancelRequest, LogData, Exchange
)

class BaseGateway(ABC):
    """交易接口基类"""

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        self.event_engine: EventEngine = event_engine    # 事件引擎
        self.gateway_name: str = gateway_name            # 接口名称

        # 接口配置
        self.default_setting: Dict[str, Any] = {}
        self.default_name: str = ""

    def on_event(self, type: str, data: Any) -> None:
        """推送事件"""
        event = Event(type, data)
        self.event_engine.put(event)

    def on_tick(self, tick: TickData) -> None:
        """推送行情数据"""
        self.on_event("TICK", tick)

    def on_trade(self, trade: TradeData) -> None:
        """推送成交数据"""
        self.on_event("TRADE", trade)

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.on_event("ORDER", order)

    def on_position(self, position: PositionData) -> None:
        """推送持仓数据"""
        self.on_event("POSITION", position)

    def on_account(self, account: AccountData) -> None:
        """推送账户数据"""
        self.on_event("ACCOUNT", account)

    def on_contract(self, contract: ContractData) -> None:
        """推送合约数据"""
        self.on_event("CONTRACT", contract)

    def write_log(self, msg: str) -> None:
        """记录日志"""
        log = LogData(
            gateway_name=self.gateway_name,
            msg=msg,
            level="INFO",
            datetime=datetime.now(),
            symbol="",
            exchange=Exchange.SMART
        )
        event = Event("LOG", log)
        self.event_engine.put(event)

    @abstractmethod
    def connect(self, setting: Dict[str, str]) -> None:
        """连接交易接口"""
        pass

    @abstractmethod
    def close(self) -> None:
        """关闭接口"""
        pass

    @abstractmethod
    def subscribe(self, req: Dict[str, str]) -> None:
        """订阅行情"""
        pass

    @abstractmethod
    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        pass

    @abstractmethod
    def cancel_order(self, req: CancelRequest) -> None:
        """撤销委托"""
        pass

    def query_account(self) -> None:
        """查询账户资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def get_default_setting(self) -> Dict[str, Any]:
        """获取默认设置"""
        return self.default_setting
