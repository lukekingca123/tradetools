"""
新闻面板组件
"""
from typing import Dict, List
from datetime import datetime
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget,
    QTableWidgetItem, QLabel, QPushButton, QLineEdit
)
from PyQt5.QtCore import Qt, pyqtSignal, QTimer

class NewsPanel(QWidget):
    """新闻面板组件"""
    
    # 信号定义
    symbol_changed = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
        
        # 自动刷新定时器
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.refresh_data)
        
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # 控制面板
        control_layout = QHBoxLayout()
        layout.addLayout(control_layout)
        
        # 股票代码输入
        control_layout.addWidget(QLabel("股票代码:"))
        self.symbol_input = QLineEdit()
        self.symbol_input.textChanged.connect(self.on_symbol_changed)
        control_layout.addWidget(self.symbol_input)
        
        # 刷新按钮
        self.refresh_btn = QPushButton("刷新")
        self.refresh_btn.clicked.connect(self.refresh_data)
        control_layout.addWidget(self.refresh_btn)
        
        # 自动刷新选项
        self.auto_refresh_btn = QPushButton("自动刷新")
        self.auto_refresh_btn.setCheckable(True)
        self.auto_refresh_btn.toggled.connect(self.toggle_auto_refresh)
        control_layout.addWidget(self.auto_refresh_btn)
        
        # 新闻表格
        self.news_table = QTableWidget()
        self.init_news_table()
        layout.addWidget(self.news_table)
        
    def init_news_table(self):
        """初始化新闻表格"""
        headers = [
            "时间",
            "标题",
            "来源",
            "类型"
        ]
        
        self.news_table.setColumnCount(len(headers))
        self.news_table.setHorizontalHeaderLabels(headers)
        self.news_table.verticalHeader().setVisible(False)
        
        # 设置列宽
        self.news_table.setColumnWidth(0, 150)  # 时间列
        self.news_table.setColumnWidth(1, 400)  # 标题列
        self.news_table.setColumnWidth(2, 100)  # 来源列
        self.news_table.setColumnWidth(3, 100)  # 类型列
        
    def on_symbol_changed(self, symbol: str):
        """股票代码改变处理"""
        self.symbol_changed.emit(symbol)
        self.refresh_data()
        
    def toggle_auto_refresh(self, checked: bool):
        """切换自动刷新"""
        if checked:
            self.refresh_timer.start(60000)  # 每分钟刷新
        else:
            self.refresh_timer.stop()
            
    def refresh_data(self):
        """刷新新闻数据"""
        # TODO: 从数据源获取新闻数据并更新表格
        pass
        
    def update_news_table(self, news_data: List[Dict]):
        """更新新闻表格数据"""
        self.news_table.setRowCount(len(news_data))
        
        for row, news in enumerate(news_data):
            # 设置表格项
            time_item = QTableWidgetItem(
                datetime.fromtimestamp(news.get("timestamp", 0))
                .strftime("%Y-%m-%d %H:%M:%S")
            )
            title_item = QTableWidgetItem(news.get("title", ""))
            source_item = QTableWidgetItem(news.get("source", ""))
            type_item = QTableWidgetItem(news.get("type", ""))
            
            items = [time_item, title_item, source_item, type_item]
            
            for col, item in enumerate(items):
                if col == 1:  # 标题列左对齐
                    item.setTextAlignment(Qt.AlignLeft | Qt.AlignVCenter)
                else:
                    item.setTextAlignment(Qt.AlignCenter | Qt.AlignVCenter)
                self.news_table.setItem(row, col, item)
