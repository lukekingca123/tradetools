import dolphindb as ddb
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

def test_connection():
    config = load_config()['dolphindb']  # 修改这里，使用dolphindb配置
    conn = ddb.session()
    
    try:
        # 连接到数据库
        conn.connect(config['host'], config['port'])
        conn.login(config['username'], config['password'])
        logging.info("成功连接到DolphinDB服务器")
        
        # 测试基本运算
        result = conn.run("1 + 1")
        logging.info(f"基本运算测试: 1 + 1 = {result}")
        
        # 检查数据库是否存在
        db_exists = conn.run("existsDatabase('dfs://optiondb')")
        logging.info(f"数据库存在: {db_exists}")
        
        if db_exists:
            # 检查表是否存在
            table_exists = conn.run("existsTable('dfs://optiondb', 'options')")
            logging.info(f"表存在: {table_exists}")
            
            if table_exists:
                # 获取表的记录数
                count = conn.run("select count(*) from loadTable('dfs://optiondb', 'options')")
                logging.info(f"表中记录数: {count}")
                
                # 获取最新的记录
                latest = conn.run("select top 1 * from loadTable('dfs://optiondb', 'options') order by timestamp desc")
                if len(latest) > 0:
                    logging.info(f"最新记录时间戳: {latest['timestamp'][0]}")
                
    except Exception as e:
        logging.error(f"测试失败: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_connection()
