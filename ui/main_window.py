"""
主窗口UI
"""
from typing import Dict, Optional
from PyQt5.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QTabWidget
from vnpy.trader.ui import QtWidgets
from vnpy.trader.ui.widget import (
    TickMonitor,
    OrderMonitor,
    TradeMonitor,
    PositionMonitor,
    AccountMonitor,
    LogMonitor,
)

class MainWindow(QMainWindow):
    """主窗口"""
    
    def __init__(self):
        super().__init__()
        
        # 设置窗口标题
        self.setWindowTitle("TradeTools")
        
        # 创建中心窗口
        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        
        # 创建布局
        self.layout = QVBoxLayout()
        self.central_widget.setLayout(self.layout)
        
        # 创建标签页
        self.tab_widget = QTabWidget()
        self.layout.addWidget(self.tab_widget)
        
        # 初始化各个监控组件
        self.init_monitors()
        
        # 添加自定义组件
        self.init_custom_widgets()
        
    def init_monitors(self):
        """初始化vnpy监控组件"""
        # 行情监控
        self.tick_monitor = TickMonitor()
        self.tab_widget.addTab(self.tick_monitor, "行情")
        
        # 委托监控
        self.order_monitor = OrderMonitor()
        self.tab_widget.addTab(self.order_monitor, "委托")
        
        # 成交监控
        self.trade_monitor = TradeMonitor()
        self.tab_widget.addTab(self.trade_monitor, "成交")
        
        # 持仓监控
        self.position_monitor = PositionMonitor()
        self.tab_widget.addTab(self.position_monitor, "持仓")
        
        # 账户监控
        self.account_monitor = AccountMonitor()
        self.tab_widget.addTab(self.account_monitor, "账户")
        
        # 日志监控
        self.log_monitor = LogMonitor()
        self.tab_widget.addTab(self.log_monitor, "日志")
        
    def init_custom_widgets(self):
        """初始化自定义组件"""
        # 期权链面板
        from .option_chain import OptionChainWidget
        self.option_chain = OptionChainWidget()
        self.tab_widget.addTab(self.option_chain, "期权链")
        
        # 新闻面板
        from .news_panel import NewsPanel
        self.news_panel = NewsPanel()
        self.tab_widget.addTab(self.news_panel, "新闻")
