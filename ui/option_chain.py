"""
期权链显示组件
"""
from typing import Dict, List, Optional
from datetime import datetime
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, 
    QTableWidgetItem, QComboBox, QLabel, QPushButton
)
from PyQt5.QtCore import Qt, pyqtSignal

class OptionChainWidget(QWidget):
    """期权链组件"""
    
    # 信号定义
    symbol_changed = pyqtSignal(str)
    expiry_changed = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
        
        self.init_ui()
        
    def init_ui(self):
        """初始化UI"""
        layout = QVBoxLayout()
        self.setLayout(layout)
        
        # 控制面板
        control_layout = QHBoxLayout()
        layout.addLayout(control_layout)
        
        # 股票代码选择
        control_layout.addWidget(QLabel("股票代码:"))
        self.symbol_combo = QComboBox()
        self.symbol_combo.setEditable(True)
        self.symbol_combo.currentTextChanged.connect(self.on_symbol_changed)
        control_layout.addWidget(self.symbol_combo)
        
        # 到期日选择
        control_layout.addWidget(QLabel("到期日:"))
        self.expiry_combo = QComboBox()
        self.expiry_combo.currentTextChanged.connect(self.on_expiry_changed)
        control_layout.addWidget(self.expiry_combo)
        
        # 刷新按钮
        self.refresh_btn = QPushButton("刷新")
        self.refresh_btn.clicked.connect(self.refresh_data)
        control_layout.addWidget(self.refresh_btn)
        
        # 期权表格
        table_layout = QHBoxLayout()
        layout.addLayout(table_layout)
        
        # 看涨期权表格
        self.call_table = QTableWidget()
        self.init_option_table(self.call_table)
        table_layout.addWidget(self.call_table)
        
        # 看跌期权表格
        self.put_table = QTableWidget()
        self.init_option_table(self.put_table)
        table_layout.addWidget(self.put_table)
        
    def init_option_table(self, table: QTableWidget):
        """初始化期权表格"""
        headers = [
            "执行价",
            "最新价",
            "涨跌幅",
            "成交量",
            "持仓量",
            "隐含波动率",
            "Delta",
            "Gamma",
            "Theta",
            "Vega"
        ]
        
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().setVisible(False)
        
    def on_symbol_changed(self, symbol: str):
        """股票代码改变处理"""
        self.symbol_changed.emit(symbol)
        self.refresh_expiry_dates()
        
    def on_expiry_changed(self, expiry: str):
        """到期日改变处理"""
        self.expiry_changed.emit(expiry)
        self.refresh_data()
        
    def refresh_expiry_dates(self):
        """刷新到期日列表"""
        # TODO: 从数据源获取到期日列表
        pass
        
    def refresh_data(self):
        """刷新期权数据"""
        # TODO: 从数据源获取期权数据并更新表格
        pass
        
    def update_option_table(self, table: QTableWidget, data: List[Dict]):
        """更新期权表格数据"""
        table.setRowCount(len(data))
        
        for row, option in enumerate(data):
            # 设置表格项
            items = [
                QTableWidgetItem(str(option.get("strike_price", ""))),
                QTableWidgetItem(str(option.get("last_price", ""))),
                QTableWidgetItem(f"{option.get('change_percent', '')}%"),
                QTableWidgetItem(str(option.get("volume", ""))),
                QTableWidgetItem(str(option.get("open_interest", ""))),
                QTableWidgetItem(f"{option.get('implied_volatility', '')}%"),
                QTableWidgetItem(str(option.get("delta", ""))),
                QTableWidgetItem(str(option.get("gamma", ""))),
                QTableWidgetItem(str(option.get("theta", ""))),
                QTableWidgetItem(str(option.get("vega", "")))
            ]
            
            for col, item in enumerate(items):
                item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                table.setItem(row, col, item)
