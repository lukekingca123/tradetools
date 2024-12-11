"""
测试DolphinDB连接和数据的脚本
"""
import dolphindb as ddb

def test_connection(host: str = "localhost", port: int = 8848):
    """
    测试DolphinDB连接并查询数据
    """
    try:
        # 连接DolphinDB
        print(f"Connecting to DolphinDB at {host}:{port}...")
        conn = ddb.session()
        conn.connect(host, port, "admin", "123456")
        
        # 检查数据库是否存在
        exists = conn.run("existsDatabase('dfs://market')")
        print(f"Market database exists: {exists}")
        
        if exists:
            # 检查表是否存在
            exists_table = conn.run("existsTable('dfs://market', 'stock_daily')")
            print(f"Stock daily table exists: {exists_table}")
            
            if exists_table:
                # 查询数据示例
                print("\nQuerying sample data...")
                df = conn.run("""
                    select top 5 * from loadTable('dfs://market', 'stock_daily')
                    order by date desc
                """)
                print("\nLatest 5 records:")
                print(df)
                
                # 查询数据统计
                count = conn.run("""
                    select count(*) from loadTable('dfs://market', 'stock_daily')
                """)
                print(f"\nTotal records in database: {count}")
        
        print("\nConnection test completed successfully!")
        
    except Exception as e:
        print(f"Error testing connection: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    test_connection()
