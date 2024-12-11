import dolphindb as ddb

# 创建连接
conn = ddb.session()
try:
    # 连接到服务器
    conn.connect("localhost", 8848, "admin", "123456")
    print("Successfully connected to DolphinDB")
    
    # 执行简单查询
    result = conn.run("1 + 1")
    print(f"Test query result: {result}")
    
except Exception as e:
    print(f"Error: {str(e)}")
finally:
    conn.close()
