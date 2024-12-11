"""
IB交易接口实现
"""
from copy import copy
from datetime import datetime
from typing import Any, Dict, List, Optional
from decimal import Decimal

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract, ContractDetails
from ibapi.order import Order
from ibapi.order_state import OrderState
from ibapi.execution import Execution
from ibapi.common import BarData as IbBarData
from ibapi.common import OrderId, TickerId, TickAttrib, TickType

from ...core.trader.gateway import BaseGateway
from ...core.trader.object import (
    TickData, OrderData, TradeData, PositionData,
    AccountData, ContractData, OrderRequest,
    CancelRequest, BarData, HistoryRequest
)
from ...core.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from ...core.event.engine import EventEngine

# IB到VT的交易所映射
EXCHANGE_IB2VT = {
    "SMART": Exchange.SMART,
    "NYMEX": Exchange.NYMEX,
    "GLOBEX": Exchange.GLOBEX,
    "IDEALPRO": Exchange.IDEALPRO,
    "CME": Exchange.CME,
    "CBOE": Exchange.CBOE,
    "AMEX": Exchange.AMEX,
    "NYSE": Exchange.NYSE,
    "NASDAQ": Exchange.NASDAQ,
}
EXCHANGE_VT2IB = {v: k for k, v in EXCHANGE_IB2VT.items()}

# IB到VT的产品类型映射
PRODUCT_IB2VT = {
    "STK": Product.EQUITY,
    "FUT": Product.FUTURES,
    "OPT": Product.OPTION,
    "CASH": Product.FOREX,
}

# IB到VT的委托状态映射
STATUS_IB2VT = {
    "Submitted": Status.NOTTRADED,
    "Filled": Status.ALLTRADED,
    "Cancelled": Status.CANCELLED,
    "PendingSubmit": Status.SUBMITTING,
    "PreSubmitted": Status.NOTTRADED,
    "ApiCancelled": Status.CANCELLED,
    "Inactive": Status.REJECTED,
}

# IB到VT的委托类型映射
ORDERTYPE_IB2VT = {
    "LMT": OrderType.LIMIT,
    "MKT": OrderType.MARKET,
    "STP": OrderType.STOP,
}
ORDERTYPE_VT2IB = {v: k for k, v in ORDERTYPE_IB2VT.items()}

# IB到VT的方向类型映射
DIRECTION_IB2VT = {
    "BOT": Direction.LONG,
    "SLD": Direction.SHORT,
}
DIRECTION_VT2IB = {v: k for k, v in DIRECTION_IB2VT.items()}


class IbGateway(BaseGateway):
    """IB接口"""

    default_name: str = "IB"
    
    default_setting = {
        "TWS地址": "127.0.0.1",
        "TWS端口": 7497,
        "客户号": 1,
        "交易账户": ""
    }

    def __init__(self, event_engine: EventEngine, gateway_name: str = "IB") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)
        
        self.api: "IbApi" = IbApi(self)
        
        # 委托号计数器
        self.order_count: int = 0
        
    def connect(self, setting: Dict[str, str]) -> None:
        """连接接口"""
        host = setting["TWS地址"]
        port = int(setting["TWS端口"])
        clientid = int(setting["客户号"])
        account = setting["交易账户"]
        
        self.api.connect(host, port, clientid, account)
        
    def close(self) -> None:
        """关闭接口"""
        self.api.close()
        
    def subscribe(self, req: Dict[str, str]) -> None:
        """订阅行情"""
        self.api.subscribe(req)
        
    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        self.order_count += 1
        orderid = str(self.order_count)
        return self.api.send_order(req, orderid)
    
    def cancel_order(self, req: CancelRequest) -> None:
        """撤销委托"""
        self.api.cancel_order(req)
        
    def query_account(self) -> None:
        """查询账户"""
        self.api.query_account()
        
    def query_position(self) -> None:
        """查询持仓"""
        self.api.query_position()
        
    def query_orders(self) -> None:
        """查询未成交委托"""
        self.api.query_orders()
        
    def query_history(self, req: HistoryRequest) -> None:
        """查询历史数据"""
        self.api.query_history(req)
        
    def start_streaming_data(self, req: Dict[str, str]) -> None:
        """开始订阅实时数据"""
        self.api.start_streaming_data(req)
        
    def stop_streaming_data(self, symbol: str) -> None:
        """停止订阅实时数据"""
        self.api.stop_streaming_data(symbol)


class IbApi(EWrapper, EClient):
    """IB API实现"""
    
    def __init__(self, gateway: IbGateway) -> None:
        """构造函数"""
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        
        self.gateway: IbGateway = gateway
        self.gateway_name: str = gateway.gateway_name
        
        self.status: bool = False  # 连接状态
        
        self.reqid: int = 0  # 请求编号
        self.orderid: int = 0  # 委托编号
        self.account: str = ""  # 账户
        
        self.ticks: Dict[int, TickData] = {}  # Tick缓存
        self.orders: Dict[str, OrderData] = {}  # 委托缓存
        self.accounts: Dict[str, AccountData] = {}  # 账户缓存
        self.subscribed: Dict[str, Contract] = {}  # 已订阅合约
        
        self.contracts: Dict[str, ContractData] = {}  # 合约缓存
        self.symbol_contract_map: Dict[str, Contract] = {}  # 代码合约映射
        self.trades: Dict[int, str] = {}  # 成交缓存
        self.history_req: Dict[int, str] = {}  # 历史数据请求缓存

    def connect(self, host: str, port: int, clientid: int, account: str) -> None:
        """连接服务器"""
        self.clientid = clientid
        self.account = account
        
        # 连接IB服务器
        self.connect_async(host, port, clientid)
        self.run()

    def close(self) -> None:
        """关闭连接"""
        if self.status:
            self.status = False
            self.disconnect()

    def connectAck(self) -> None:
        """连接成功回报"""
        self.status = True
        self.gateway.write_log(f"{self.gateway_name}接口连接成功")
        
        # 查询账户
        self.reqAccountUpdates(True, self.account)
        
        # 查询合约信息
        self.init_contracts()

    def connectionClosed(self) -> None:
        """连接断开回报"""
        self.status = False
        self.gateway.write_log(f"{self.gateway_name}接口连接断开")

    def init_contracts(self) -> None:
        """初始化合约信息"""
        # 设置美股合约请求
        req = Contract()
        req.symbol = "SPY"  # 使用SPY作为示例
        req.secType = "STK"
        req.currency = "USD"
        req.exchange = "SMART"
        
        self.reqid += 1
        self.reqContractDetails(self.reqid, req)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:
        """合约信息回报"""
        ib_contract = contractDetails.contract
        symbol = ib_contract.symbol
        exchange = EXCHANGE_IB2VT.get(ib_contract.exchange, None)
        
        if not exchange:
            return
        
        # 构建合约对象
        contract = ContractData(
            gateway_name=self.gateway_name,
            symbol=symbol,
            exchange=exchange,
            name=contractDetails.longName,
            product=PRODUCT_IB2VT.get(ib_contract.secType, Product.EQUITY),
            size=ib_contract.multiplier,
            pricetick=contractDetails.minTick,
            net_position=True,
            history_data=True,
            stop_supported=True,
            datetime=datetime.now()
        )
        
        # 缓存合约信息
        self.contracts[contract.symbol] = contract
        self.symbol_contract_map[contract.symbol] = ib_contract
        
        # 推送合约信息
        self.gateway.on_contract(contract)

    def contractDetailsEnd(self, reqId: int) -> None:
        """合约信息查询结束"""
        self.gateway.write_log("合约信息查询完成")

    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str) -> None:
        """账户更新回报"""
        if accountName != self.account:
            return
        
        # 构建账户数据
        account = self.accounts.get(accountName, None)
        if not account:
            account = AccountData(
                gateway_name=self.gateway_name,
                accountid=accountName,
                datetime=datetime.now(),
                symbol="",
                exchange=Exchange.SMART
            )
            self.accounts[accountName] = account
        
        # 更新账户数据
        if key == "NetLiquidation":
            account.balance = float(val)
        elif key == "AvailableFunds":
            account.available = float(val)
        elif key == "MaintMarginReq":
            account.frozen = float(val)
        
        # 推送账户数据
        self.gateway.on_account(account)

    def updatePortfolio(
        self,
        contract: Contract,
        position: Decimal,
        marketPrice: float,
        marketValue: float,
        averageCost: float,
        unrealizedPNL: float,
        realizedPNL: float,
        accountName: str,
    ) -> None:
        """持仓更新回报"""
        if accountName != self.account:
            return
            
        symbol = contract.symbol
        exchange = EXCHANGE_IB2VT.get(contract.exchange, None)
        if not exchange:
            return
            
        # 构建持仓数据
        pos = PositionData(
            gateway_name=self.gateway_name,
            symbol=symbol,
            exchange=exchange,
            direction=Direction.NET,
            volume=float(position),
            price=averageCost,
            pnl=unrealizedPNL,
            datetime=datetime.now()
        )
        
        # 推送持仓数据
        self.gateway.on_position(pos)

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:
        """错误回报"""
        # 过滤掉IB API中常见的无关紧要的提示信息
        if errorCode in {2104, 2106, 2158}:  # 这些是连接成功、断开等提示信息
            return
            
        msg = f"错误代码：{errorCode}，错误信息：{errorString}"
        self.gateway.write_log(msg)
        
        # 委托错误处理
        if reqId in self.orders:
            order = self.orders[reqId]
            order.status = Status.REJECTED
            self.gateway.on_order(copy(order))

    def write_log(self, msg: str) -> None:
        """输出日志"""
        self.gateway.write_log(f"{self.gateway_name}：{msg}")

    def subscribe(self, req: Dict[str, str]) -> None:
        """订阅行情"""
        if req["symbol"] in self.subscribed:
            self.gateway.write_log(f"重复订阅行情：{req['symbol']}")
            return
            
        # 获取合约信息
        symbol = req["symbol"]
        exchange = Exchange(req["exchange"])
        currency = req.get("currency", "USD")
        
        # 创建IB合约对象
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"  # 股票
        contract.currency = currency
        contract.exchange = EXCHANGE_VT2IB[exchange]
        
        # 订阅行情
        self.reqid += 1
        self.subscribed[symbol] = contract
        
        # 创建TICK对象
        tick = TickData(
            gateway_name=self.gateway_name,
            symbol=symbol,
            exchange=exchange,
            datetime=datetime.now()
        )
        self.ticks[self.reqid] = tick
        
        # 订阅市场数据
        self.reqMktData(self.reqid, contract, "", False, False, [])
        self.gateway.write_log(f"订阅行情：{symbol}")

    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float, attrib: TickAttrib) -> None:
        """行情价格推送"""
        tick = self.ticks.get(reqId, None)
        if not tick:
            return
            
        if tickType == 1:  # Bid Price
            tick.bid_price_1 = price
        elif tickType == 2:  # Ask Price
            tick.ask_price_1 = price
        elif tickType == 4:  # Last Price
            tick.last_price = price
        elif tickType == 6:  # High
            tick.high_price = price
        elif tickType == 7:  # Low
            tick.low_price = price
        elif tickType == 9:  # Close
            tick.pre_close = price
        elif tickType == 14:  # Open
            tick.open_price = price
            
        # 推送行情更新
        if tick.last_price:
            self.gateway.on_tick(copy(tick))

    def tickSize(self, reqId: TickerId, tickType: TickType, size: Decimal) -> None:
        """行情数量推送"""
        tick = self.ticks.get(reqId, None)
        if not tick:
            return
            
        if tickType == 0:  # Bid Size
            tick.bid_volume_1 = float(size)
        elif tickType == 3:  # Ask Size
            tick.ask_volume_1 = float(size)
        elif tickType == 5:  # Last Size
            tick.last_volume = float(size)
        elif tickType == 8:  # Volume
            tick.volume = float(size)
            
        # 推送行情更新
        if tick.last_price:
            self.gateway.on_tick(copy(tick))

    def tickString(self, reqId: TickerId, tickType: TickType, value: str) -> None:
        """行情字符串推送"""
        tick = self.ticks.get(reqId, None)
        if not tick:
            return
            
        if tickType == 45:  # Last Timestamp
            tick.datetime = datetime.fromtimestamp(int(value))
            
        # 推送行情更新
        if tick.last_price:
            self.gateway.on_tick(copy(tick))

    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float) -> None:
        """行情其他数据推送"""
        tick = self.ticks.get(reqId, None)
        if not tick:
            return
            
        # 处理其他类型的行情数据
        # 可以根据需要添加更多的tickType处理
            
        # 推送行情更新
        if tick.last_price:
            self.gateway.on_tick(copy(tick))

    def send_order(self, req: OrderRequest, orderid: str) -> str:
        """委托下单"""
        # 生成委托号
        self.orderid += 1
        
        # 创建IB合约对象
        contract = Contract()
        contract.symbol = req.symbol
        contract.secType = "STK"  # 股票
        contract.currency = "USD"
        contract.exchange = EXCHANGE_VT2IB[req.exchange]
        
        # 创建IB委托对象
        order = Order()
        order.orderId = self.orderid
        order.clientId = self.clientid
        order.action = DIRECTION_VT2IB[req.direction]
        order.orderType = ORDERTYPE_VT2IB[req.type]
        order.totalQuantity = req.volume
        order.lmtPrice = req.price
        order.account = self.account
        
        # 创建委托数据对象
        order_data = OrderData(
            gateway_name=self.gateway_name,
            symbol=req.symbol,
            exchange=req.exchange,
            orderid=orderid,
            type=req.type,
            direction=req.direction,
            price=req.price,
            volume=req.volume,
            status=Status.SUBMITTING,
            datetime=datetime.now()
        )
        self.orders[orderid] = order_data
        
        # 发送委托请求
        self.placeOrder(self.orderid, contract, order)
        self.gateway.on_order(copy(order_data))
        
        return orderid
        
    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        orderid = int(req.orderid)
        self.cancelOrder(orderid)
        
    def orderStatus(
        self,
        orderId: OrderId,
        status: str,
        filled: Decimal,
        remaining: Decimal,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ) -> None:
        """委托状态更新"""
        orderid = str(orderId)
        order = self.orders.get(orderid, None)
        if not order:
            return
            
        # 更新委托状态
        order.traded = float(filled)
        order.status = STATUS_IB2VT[status]
        
        self.gateway.on_order(copy(order))
        
    def execDetails(
        self,
        reqId: int,
        contract: Contract,
        execution: Execution
    ) -> None:
        """成交数据更新"""
        # 过滤重复的成交推送
        if execution.orderId in self.trades:
            return
        self.trades[execution.orderId] = execution.execId
        
        # 创建成交对象
        trade_data = TradeData(
            gateway_name=self.gateway_name,
            symbol=contract.symbol,
            exchange=EXCHANGE_IB2VT[contract.exchange],
            orderid=str(execution.orderId),
            tradeid=execution.execId,
            direction=DIRECTION_IB2VT[execution.side],
            price=execution.price,
            volume=execution.shares,
            datetime=datetime.strptime(execution.time, "%Y%m%d  %H:%M:%S"),
        )
        
        self.gateway.on_trade(trade_data)

    def query_account(self) -> None:
        """查询账户资金"""
        self.reqAccountUpdates(True, self.account)

    def query_position(self) -> None:
        """查询持仓"""
        self.reqPositions()

    def query_orders(self) -> None:
        """查询未成交委托"""
        self.reqAllOpenOrders()

    def nextValidId(self, orderId: int) -> None:
        """获取下一个有效的委托编号"""
        super().nextValidId(orderId)
        self.orderid = orderId
        
        # 连接成功后自动初始化查询
        self.gateway.write_log(f"{self.gateway_name}连接成功")
        
        # 查询数据
        self.query_account()
        self.query_position()
        self.query_orders()

    def contractDetails(self, reqId: int, contractDetails: ContractDetails) -> None:
        """合约查询回报"""
        contract = contractDetails.contract
        
        data = ContractData(
            gateway_name=self.gateway_name,
            symbol=contract.symbol,
            exchange=EXCHANGE_IB2VT[contract.exchange],
            name=contractDetails.longName,
            product=Product.EQUITY,
            size=1,
            pricetick=contractDetails.minTick,
            net_position=True
        )
        
        self.gateway.on_contract(data)
        self.contracts[contract.symbol] = data
        
    def historicalData(self, reqId: int, bar: IbBarData) -> None:
        """历史数据回报"""
        symbol = self.history_req.get(reqId, None)
        if not symbol:
            return
            
        dt = datetime.strptime(bar.date, "%Y%m%d %H:%M:%S")
        
        bar_data = BarData(
            gateway_name=self.gateway_name,
            symbol=symbol,
            exchange=Exchange.SMART,
            datetime=dt,
            interval=Interval.MINUTE,
            volume=bar.volume,
            open_price=bar.open,
            high_price=bar.high,
            low_price=bar.low,
            close_price=bar.close,
        )
        self.gateway.on_bar(bar_data)

    def query_history(self, req: HistoryRequest) -> None:
        """查询历史数据"""
        self.reqid += 1
        self.history_req[self.reqid] = req.symbol
        
        ib_contract = Contract()
        ib_contract.symbol = req.symbol
        ib_contract.secType = "STK"
        ib_contract.exchange = EXCHANGE_VT2IB[req.exchange]
        ib_contract.currency = "USD"
        
        # 计算时间
        end = req.end.strftime("%Y%m%d %H:%M:%S")
        delta = req.end - req.start
        days = min(delta.days, 180)  # IB只支持查询180天的分钟数据
        
        # 查询历史数据
        self.reqHistoricalData(
            self.reqid,
            ib_contract,
            end,
            f"{days} D",
            "1 min",
            "TRADES",
            1,
            1,
            False,
            []
        )

    def start_streaming_data(self, req: Dict[str, str]) -> None:
        """开始订阅实时数据"""
        if req["symbol"] in self.subscribed:
            self.gateway.write_log(f"已经订阅了{req['symbol']}的实时数据")
            return
            
        contract = Contract()
        contract.symbol = req["symbol"]
        contract.secType = "STK"
        contract.currency = "USD"
        contract.exchange = EXCHANGE_VT2IB[Exchange(req["exchange"])]
        
        self.reqid += 1
        self.subscribed[req["symbol"]] = self.reqid
        
        # 订阅Level 1市场数据
        self.reqMktData(self.reqid, contract, "", False, False, [])
        
        # 订阅Level 2市场数据（深度数据）
        self.reqMktDepth(self.reqid, contract, 5, False, [])
        
        self.gateway.write_log(f"开始订阅{req['symbol']}的实时数据")

    def stop_streaming_data(self, symbol: str) -> None:
        """停止订阅实时数据"""
        if symbol not in self.subscribed:
            return
            
        reqid = self.subscribed[symbol]
        
        # 取消Level 1市场数据
        self.cancelMktData(reqid)
        
        # 取消Level 2市场数据
        self.cancelMktDepth(reqid, False)
        
        del self.subscribed[symbol]
        self.gateway.write_log(f"停止订阅{symbol}的实时数据")
