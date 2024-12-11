"""
从DolphinDB查询期权数据的脚本
实现基本的查询功能
"""

import logging
import pandas as pd
import dolphindb as ddb
from datetime import datetime, timedelta

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class OptionsDataReader:
    """期权数据读取器"""
    
    def __init__(self, host="localhost", port=8848):
        """初始化连接"""
        try:
            self.conn = ddb.session()
            self.conn.connect(host, port)
            # 登录
            self.conn.run("login('admin', '123456')")
            logging.info("成功连接到DolphinDB")
        except Exception as e:
            logging.error(f"连接失败: {str(e)}")
            raise
            
    def get_data_by_date(self, date):
        """按日期获取数据"""
        try:
            script = f"""
            t = loadTable("dfs://optiondb", "options")
            select * from t where date = date({date})
            """
            return self.conn.run(script)
        except Exception as e:
            logging.error(f"按日期查询失败: {str(e)}")
            return pd.DataFrame()
            
    def get_data_by_symbol(self, symbol, start_date=None, end_date=None):
        """按期权代码获取数据"""
        try:
            script = f"""
            t = loadTable("dfs://optiondb", "options")
            select * from t where symbol = '{symbol}'
            """
            if start_date and end_date:
                script += f" and date between {start_date} and {end_date}"
            return self.conn.run(script)
        except Exception as e:
            logging.error(f"按代码查询失败: {str(e)}")
            return pd.DataFrame()
            
    def get_data_by_expiry(self, expiry):
        """按到期日获取数据"""
        try:
            script = f"""
            t = loadTable("dfs://optiondb", "options")
            select * from t where expiry = {expiry}
            """
            return self.conn.run(script)
        except Exception as e:
            logging.error(f"按到期日查询失败: {str(e)}")
            return pd.DataFrame()
            
    def get_data_by_type(self, option_type, date=None):
        """按期权类型获取数据"""
        try:
            script = f"""
            t = loadTable("dfs://optiondb", "options")
            select * from t where type = '{option_type}'
            """
            if date:
                script += f" and date = date({date})"
            return self.conn.run(script)
        except Exception as e:
            logging.error(f"按类型查询失败: {str(e)}")
            return pd.DataFrame()
            
    def get_daily_summary(self, date):
        """获取每日汇总数据"""
        try:
            script = f"""
            t = loadTable("dfs://optiondb", "options")
            select 
                symbol,
                type,
                strike,
                expiry,
                sum(volume) as total_volume,
                avg(close) as avg_price,
                max(high) as high,
                min(low) as low
            from t
            where date = date({date})
            group by symbol, type, strike, expiry
            """
            return self.conn.run(script)
        except Exception as e:
            logging.error(f"获取每日汇总失败: {str(e)}")
            return pd.DataFrame()
            
    def get_active_options(self, min_volume=1000, date=None):
        """获取活跃期权"""
        try:
            script = f"""
            t = loadTable("dfs://optiondb", "options")
            select 
                symbol,
                type,
                strike,
                expiry,
                sum(volume) as total_volume,
                avg(close) as avg_price
            from t
            """
            if date:
                script += f" where date = date({date})"
            script += f" group by symbol, type, strike, expiry having sum(volume) >= {min_volume}"
            return self.conn.run(script)
        except Exception as e:
            logging.error(f"获取活跃期权失败: {str(e)}")
            return pd.DataFrame()
            
    def get_options_chain(self, date, underlying=None):
        """获取期权链"""
        try:
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
            where date = date({date})
            """
            if underlying:
                script += f" and symbol = '{underlying}'"
            script += " order by symbol, type, strike"
            return self.conn.run(script)
        except Exception as e:
            logging.error(f"获取期权链失败: {str(e)}")
            return pd.DataFrame()

def main():
    """主函数：演示如何使用OptionsDataReader"""
    try:
        # 创建数据读取器
        reader = OptionsDataReader()
        
        # 直接尝试查询数据
        print("\n尝试直接查询数据:")
        try:
            data = reader.conn.run("""
            t = loadTable("dfs://optiondb", "options")
            select top 5 * from t
            """)
            print("\n数据示例:")
            print(data)
            
            count = reader.conn.run("""
            t = loadTable("dfs://optiondb", "options")
            select count(*) as count from t
            """)
            print(f"\n总记录数: {count[0][0]}")
            
            date_range = reader.conn.run("""
            t = loadTable("dfs://optiondb", "options")
            select 
                min(date) as min_date,
                max(date) as max_date 
            from t
            """)
            print(f"\n日期范围: {date_range['min_date'][0]} - {date_range['max_date'][0]}")
            
            # 测试各个查询方法
            test_date = date_range['min_date'][0]
            print(f"\n测试日期 {test_date} 的数据:")
            
            # 1. 按日期查询
            daily_data = reader.get_data_by_date(test_date)
            print(f"\n当日数据记录数: {len(daily_data)}")
            
            # 2. 按期权类型查询
            calls = reader.get_data_by_type("CALL", test_date)
            puts = reader.get_data_by_type("PUT", test_date)
            print(f"\n认购期权数: {len(calls)}")
            print(f"认沽期权数: {len(puts)}")
            
            # 3. 查询活跃期权
            active = reader.get_active_options(min_volume=100, date=test_date)
            print(f"\n活跃期权数: {len(active)}")
            
            # 4. 查询期权链
            chain = reader.get_options_chain(test_date)
            print(f"\n期权链记录数: {len(chain)}")
            
        except Exception as e:
            print(f"查询失败: {str(e)}")
            
    except Exception as e:
        logging.error(f"主程序执行失败: {str(e)}")
        raise

if __name__ == "__main__":
    main()
