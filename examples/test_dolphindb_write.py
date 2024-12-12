import dolphindb as ddb
import logging

logging.basicConfig(level=logging.INFO)

def test_write():
    try:
        # 连接到DolphinDB
        conn = ddb.session()
        conn.connect("127.0.0.1", 8848, "admin", "123456")
        
        # 测试基本运算
        result = conn.run("1 + 1")
        logging.info(f"基本运算测试: 1 + 1 = {result}")
        
        # 测试创建内存表
        conn.run("""
        t = table(1 2 3 as id, `a`b`c as name)
        share t as test_table
        """)
        logging.info("创建内存表成功")
        
        # 测试写入数据库
        conn.run("""
        if(existsDatabase('dfs://test_db')){
            dropDatabase('dfs://test_db')
        }
        db = database('dfs://test_db', RANGE, 1 10 20 30)
        pt = db.createPartitionedTable(t, `test_table, `id)
        pt.append!(t)
        """)
        logging.info("数据库写入测试成功")
        
        # 查询写入的数据
        result = conn.run("select * from loadTable('dfs://test_db', 'test_table')")
        logging.info(f"查询结果:\n{result}")
        
        # 清理测试数据
        conn.run("dropDatabase('dfs://test_db')")
        logging.info("清理测试数据成功")
        
    except Exception as e:
        logging.error(f"测试失败: {str(e)}")

if __name__ == "__main__":
    test_write()
