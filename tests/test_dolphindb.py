"""
DolphinDB功能测试模块
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ..database.dolphindb_client import DolphinDBClient

class TestDolphinDB(unittest.TestCase):
    def setUp(self):
        """测试前准备"""
        self.client = DolphinDBClient()
        self.connected = self.client.connect()
        
    def tearDown(self):
        """测试后清理"""
        if self.connected:
            self.client.disconnect()
            
    def test_connection(self):
        """测试数据库连接"""
        self.assertTrue(self.connected)
        
    def test_create_table(self):
        """测试创建数据表"""
        if not self.connected:
            self.skipTest("Database not connected")
        result = self.client.create_market_data_table()
        self.assertIsNone(result)  # 创建表成功应返回None
        
    def test_save_and_query_data(self):
        """测试数据保存和查询"""
        if not self.connected:
            self.skipTest("Database not connected")
            
        # 创建测试数据
        dates = pd.date_range(start='2023-01-01', end='2023-01-10', freq='D')
        test_data = pd.DataFrame({
            'timestamp': dates,
            'open': np.random.rand(len(dates)) * 100 + 100,
            'high': np.random.rand(len(dates)) * 100 + 110,
            'low': np.random.rand(len(dates)) * 100 + 90,
            'close': np.random.rand(len(dates)) * 100 + 100,
            'volume': np.random.randint(1000, 10000, len(dates))
        })
        
        # 保存数据
        success = self.client.save_market_data('AAPL', test_data)
        self.assertTrue(success)
        
        # 查询数据
        result = self.client.query_market_data(
            'AAPL',
            datetime(2023, 1, 1),
            datetime(2023, 1, 10)
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result), len(test_data))
        
    def test_error_handling(self):
        """测试错误处理"""
        if not self.connected:
            self.skipTest("Database not connected")
            
        # 测试无效的查询
        result = self.client.query_market_data(
            'INVALID_SYMBOL',
            datetime.now(),
            datetime.now()
        )
        self.assertIsNone(result)
        
        # 测试无效的数据保存
        invalid_data = pd.DataFrame({
            'invalid_column': [1, 2, 3]
        })
        success = self.client.save_market_data('AAPL', invalid_data)
        self.assertFalse(success)
        
def run_tests():
    """运行所有测试"""
    unittest.main()
