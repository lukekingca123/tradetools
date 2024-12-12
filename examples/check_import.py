import dolphindb as ddb
import logging

logging.basicConfig(level=logging.INFO)

def check_import():
    try:
        # 连接到DolphinDB
        conn = ddb.session()
        conn.connect("127.0.0.1", 8848, "admin", "123456")
        
        # 检查数据库是否存在
        db_exists = conn.run("existsDatabase('dfs://optiondb')")
        logging.info(f"数据库存在: {db_exists}")
        
        if db_exists:
            # 检查表是否存在
            table_exists = conn.run("existsTable('dfs://optiondb', 'options')")
            logging.info(f"表存在: {table_exists}")
            
            if table_exists:
                # 获取表大小和最新数据
                count = conn.run("select count(*) from loadTable('dfs://optiondb', 'options')")
                logging.info(f"表中总记录数: {count}")
                
                # 获取一些样本数据
                sample = conn.run("select top 5 * from loadTable('dfs://optiondb', 'options')")
                logging.info(f"样本数据:\n{sample}")
                
                # 获取数据的时间范围
                time_range = conn.run("""
                select min(expiry) as min_date, max(expiry) as max_date 
                from loadTable('dfs://optiondb', 'options')
                """)
                logging.info(f"数据时间范围: {time_range}")
    except Exception as e:
        logging.error(f"检查失败: {str(e)}")

if __name__ == "__main__":
    check_import()
