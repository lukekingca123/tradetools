"""
DolphinDB数据源
提供从DolphinDB读取期权数据的基本功能
"""

import logging
import pandas as pd
import dolphindb

class DolphinDBSource:
    """DolphinDB数据源"""
    
    def __init__(self, host="localhost", port=8848, username="admin", password="123456"):
        """初始化连接"""
        self.conn = dolphindb.session()
        try:
            self.conn.connect(host, port, username, password)
            logging.info("成功连接到DolphinDB")
        except Exception as e:
            logging.error(f"连接DolphinDB失败: {str(e)}")
            raise
            
    def _execute_query(self, script):
        """执行查询并返回DataFrame"""
        try:
            result = self.conn.run(script)
            return pd.DataFrame(result) if result is not None else pd.DataFrame()
        except Exception as e:
            logging.error(f"查询执行失败: {str(e)}")
            return pd.DataFrame()
    
    def get_data_by_date(self, date):
        """获取指定日期的期权数据"""
        script = f"""
        t = loadTable("dfs://optiondb", "options")
        select * from t where date = '{date}'
        """
        return self._execute_query(script)
        
    def get_data_by_symbol(self, symbol, date=None):
        """获取指定期权代码的数据"""
        where_clause = f"where symbol = '{symbol}'"
        if date:
            where_clause += f" and date = '{date}'"
            
        script = f"""
        t = loadTable("dfs://optiondb", "options")
        select * from t {where_clause}
        """
        return self._execute_query(script)
        
    def get_options_chain(self, date, underlying=None):
        """获取期权链数据"""
        where_clause = f"where date = '{date}'"
        if underlying:
            where_clause += f" and symbol like '{underlying}%'"
            
        script = f"""
        t = loadTable("dfs://optiondb", "options")
        select 
            symbol,
            type,
            strike,
            expiry,
            close,
            volume,
            oi
        from t 
        {where_clause}
        order by strike, expiry
        """
        return self._execute_query(script)
        
    def get_active_options(self, date, min_volume=1000):
        """获取活跃期权"""
        script = f"""
        t = loadTable("dfs://optiondb", "options")
        select * from t 
        where date = '{date}' and volume >= {min_volume}
        order by volume desc
        """
        return self._execute_query(script)

def test_connection():
    """测试连接和基本查询"""
    try:
        # 创建数据源实例
        db = DolphinDBSource()
        
        # 测试查询
        date = "2023-12-11"  # 使用当前日期
        data = db.get_data_by_date(date)
        print(f"查询到 {len(data)} 条记录")
        
        if not data.empty:
            print("\n数据示例:")
            print(data.head())
            
    except Exception as e:
        print(f"测试失败: {str(e)}")

if __name__ == "__main__":
    test_connection()
