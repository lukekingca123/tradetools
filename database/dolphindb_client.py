"""
DolphinDB数据库客户端

提供DolphinDB的基础连接和数据操作功能，针对金融数据优化
"""

import dolphindb as ddb
import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Union, Tuple
from datetime import datetime
import logging
from ..utils.logger import setup_logger

logger = setup_logger('dolphindb_client')

class DolphinDBClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8848,
        username: str = "admin",
        password: str = "123456"
    ):
        """初始化DolphinDB客户端
        
        Args:
            host: DolphinDB服务器地址
            port: 端口号
            username: 用户名
            password: 密码
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.conn = None
        
    def connect(self) -> bool:
        """建立数据库连接"""
        try:
            self.conn = ddb.session()
            self.conn.connect(self.host, self.port, self.username, self.password)
            # 设置为美东时区 (UTC-4/UTC-5)
            self.execute("setTimeZone(-4)")  # 夏令时为-4，冬令时为-5
            logger.info(f"Successfully connected to DolphinDB at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to DolphinDB: {str(e)}")
            return False
            
    def disconnect(self):
        """关闭数据库连接"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Disconnected from DolphinDB")
            except Exception as e:
                logger.error(f"Error disconnecting from DolphinDB: {str(e)}")
                
    def execute(self, script: str) -> Optional[Union[pd.DataFrame, dict]]:
        """执行DolphinDB脚本
        
        Args:
            script: DolphinDB脚本
            
        Returns:
            执行结果，可能是DataFrame或字典
        """
        try:
            result = self.conn.run(script)
            return result
        except Exception as e:
            logger.error(f"Error executing script: {str(e)}")
            return None
            
    def init_database(self):
        """初始化数据库结构"""
        scripts = [
            # 创建市场数据库
            """
            if(!exists("dfs://market")){
                db = database("dfs://market", VALUE, ["US", "CN"])
            }
            """,
            
            # 创建分钟级行情表
            """
            if(!exists("dfs://market", "minute_bars")){
                schema = table(
                    1:0, `symbol`market`timestamp`open`high`low`close`volume`amount`vwap,
                    [SYMBOL, SYMBOL, TIMESTAMP, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, DOUBLE, DOUBLE]
                )
                db = database("dfs://market")
                db.createPartitionedTable(
                    schema, `minute_bars,
                    `market`symbol`timestamp
                )
            }
            """,
            
            # 创建Tick数据表
            """
            if(!exists("dfs://market", "ticks")){
                schema = table(
                    1:0, `symbol`market`timestamp`price`volume`bid_price`ask_price`bid_volume`ask_volume,
                    [SYMBOL, SYMBOL, TIMESTAMP, DOUBLE, LONG, DOUBLE, DOUBLE, LONG, LONG]
                )
                db = database("dfs://market")
                db.createPartitionedTable(
                    schema, `ticks,
                    `market`symbol`timestamp
                )
            }
            """,
            
            # 创建期权数据表
            """
            if(!exists("dfs://market", "options")){
                schema = table(
                    1:0, `symbol`underlying`market`timestamp`strike`expiry`type`price`volume`iv`delta`gamma`vega`theta,
                    [SYMBOL, SYMBOL, SYMBOL, TIMESTAMP, DOUBLE, DATE, SYMBOL, DOUBLE, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE]
                )
                db = database("dfs://market")
                db.createPartitionedTable(
                    schema, `options,
                    `market`underlying`timestamp
                )
            }
            """
        ]
        
        for script in scripts:
            if self.execute(script) is None:
                return False
        return True
        
    def save_minute_bars(
        self,
        symbol: str,
        market: str,
        data: pd.DataFrame
    ) -> bool:
        """保存分钟级行情数据
        
        Args:
            symbol: 证券代码
            market: 市场（US/CN）
            data: 行情数据，需包含必要字段
        """
        try:
            # 添加symbol和market列
            data['symbol'] = symbol
            data['market'] = market
            
            # 计算VWAP
            if 'amount' in data.columns and 'volume' in data.columns:
                data['vwap'] = data['amount'] / data['volume']
            else:
                data['vwap'] = data['close']
                
            # 上传数据
            self.conn.upload({'data': data})
            
            script = """
            try {
                loadTable("dfs://market", "minute_bars").append!(data)
                return true
            } catch(ex) {
                print(ex)
                return false
            }
            """
            result = self.execute(script)
            return result if result is not None else False
            
        except Exception as e:
            logger.error(f"Error saving minute bars: {str(e)}")
            return False
            
    def save_ticks(
        self,
        symbol: str,
        market: str,
        data: pd.DataFrame
    ) -> bool:
        """保存Tick数据
        
        Args:
            symbol: 证券代码
            market: 市场（US/CN）
            data: Tick数据
        """
        try:
            data['symbol'] = symbol
            data['market'] = market
            
            self.conn.upload({'data': data})
            
            script = """
            try {
                loadTable("dfs://market", "ticks").append!(data)
                return true
            } catch(ex) {
                print(ex)
                return false
            }
            """
            result = self.execute(script)
            return result if result is not None else False
            
        except Exception as e:
            logger.error(f"Error saving ticks: {str(e)}")
            return False
            
    def save_options(
        self,
        underlying: str,
        market: str,
        data: pd.DataFrame
    ) -> bool:
        """保存期权数据
        
        Args:
            underlying: 标的证券代码
            market: 市场（US/CN）
            data: 期权数据
        """
        try:
            data['market'] = market
            
            self.conn.upload({'data': data})
            
            script = """
            try {
                loadTable("dfs://market", "options").append!(data)
                return true
            } catch(ex) {
                print(ex)
                return false
            }
            """
            result = self.execute(script)
            return result if result is not None else False
            
        except Exception as e:
            logger.error(f"Error saving options: {str(e)}")
            return False
            
    def query_bars(
        self,
        symbol: str,
        market: str,
        start_time: datetime,
        end_time: datetime,
        freq: str = "1m"
    ) -> Optional[pd.DataFrame]:
        """查询K线数据
        
        Args:
            symbol: 证券代码
            market: 市场（US/CN）
            start_time: 开始时间
            end_time: 结束时间
            freq: 频率（1m/5m/15m/30m/1h/1d）
        """
        try:
            if freq == "1m":
                table = "minute_bars"
            else:
                # 使用timeBar聚合
                resample_map = {
                    "5m": 5,
                    "15m": 15,
                    "30m": 30,
                    "1h": 60,
                    "1d": 1440
                }
                minutes = resample_map.get(freq)
                if not minutes:
                    raise ValueError(f"Unsupported frequency: {freq}")
                    
                script = f"""
                select 
                    first(timestamp) as timestamp,
                    first(open) as open,
                    max(high) as high,
                    min(low) as low,
                    last(close) as close,
                    sum(volume) as volume,
                    sum(amount) as amount,
                    wavg(vwap, volume) as vwap
                from loadTable("dfs://market", "minute_bars")
                where 
                    symbol = '{symbol}'
                    and market = '{market}'
                    and timestamp between {start_time.strftime('%Y.%m.%d %H:%M:%S')} : {end_time.strftime('%Y.%m.%d %H:%M:%S')}
                group by bar(timestamp, {minutes})
                order by timestamp
                """
            
            return self.execute(script)
            
        except Exception as e:
            logger.error(f"Error querying bars: {str(e)}")
            return None
            
    def query_ticks(
        self,
        symbol: str,
        market: str,
        start_time: datetime,
        end_time: datetime
    ) -> Optional[pd.DataFrame]:
        """查询Tick数据"""
        try:
            script = f"""
            select * from loadTable("dfs://market", "ticks")
            where 
                symbol = '{symbol}'
                and market = '{market}'
                and timestamp between {start_time.strftime('%Y.%m.%d %H:%M:%S')} : {end_time.strftime('%Y.%m.%d %H:%M:%S')}
            order by timestamp
            """
            return self.execute(script)
        except Exception as e:
            logger.error(f"Error querying ticks: {str(e)}")
            return None
            
    def query_options(
        self,
        underlying: str,
        market: str,
        start_time: datetime,
        end_time: datetime,
        min_strike: float = None,
        max_strike: float = None,
        min_expiry: datetime = None,
        max_expiry: datetime = None,
        option_type: str = None
    ) -> Optional[pd.DataFrame]:
        """查询期权数据"""
        try:
            conditions = [
                f"underlying = '{underlying}'",
                f"market = '{market}'",
                f"timestamp between {start_time.strftime('%Y.%m.%d %H:%M:%S')} : {end_time.strftime('%Y.%m.%d %H:%M:%S')}"
            ]
            
            if min_strike:
                conditions.append(f"strike >= {min_strike}")
            if max_strike:
                conditions.append(f"strike <= {max_strike}")
            if min_expiry:
                conditions.append(f"expiry >= {min_expiry.strftime('%Y.%m.%d')}")
            if max_expiry:
                conditions.append(f"expiry <= {max_expiry.strftime('%Y.%m.%d')}")
            if option_type:
                conditions.append(f"type = '{option_type}'")
                
            script = f"""
            select * from loadTable("dfs://market", "options")
            where {' and '.join(conditions)}
            order by timestamp, strike, expiry
            """
            return self.execute(script)
            
        except Exception as e:
            logger.error(f"Error querying options: {str(e)}")
            return None
            
    def calculate_vwap(
        self,
        symbol: str,
        market: str,
        start_time: datetime,
        end_time: datetime,
        window: str = "1d"
    ) -> Optional[pd.DataFrame]:
        """计算VWAP
        
        Args:
            window: 计算窗口（1d/1w/1m）
        """
        try:
            window_map = {
                "1d": "1d",
                "1w": "7d",
                "1m": "30d"
            }
            window_size = window_map.get(window, "1d")
            
            script = f"""
            select 
                timestamp,
                symbol,
                market,
                sum(price * volume) / sum(volume) as vwap,
                sum(volume) as total_volume,
                sum(price * volume) as total_amount
            from loadTable("dfs://market", "ticks")
            where 
                symbol = '{symbol}'
                and market = '{market}'
                and timestamp between {start_time.strftime('%Y.%m.%d %H:%M:%S')} : {end_time.strftime('%Y.%m.%d %H:%M:%S')}
            group by date(timestamp)
            order by timestamp
            """
            return self.execute(script)
            
        except Exception as e:
            logger.error(f"Error calculating VWAP: {str(e)}")
            return None
