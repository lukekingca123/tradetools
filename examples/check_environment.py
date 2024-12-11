import dolphindb as ddb
from pymongo import MongoClient
import sys
import os

def check_dolphindb():
    """检查DolphinDB连接和基本功能"""
    try:
        # 检查进程
        if os.system("pgrep -x dolphindb > /dev/null") != 0:
            print("❌ DolphinDB 进程未运行")
            return False
            
        # 测试连接
        s = ddb.session()
        s.connect("localhost", 8848, "admin", "123456")
        result = s.run("1 + 1")
        
        print("✅ DolphinDB 运行正常")
        print(f"  - 连接成功")
        print(f"  - 测试查询结果: {result}")
        
        # 获取数据库列表
        try:
            dbs = s.run("exec name from objs(true) where type='DATABASE'")
            print(f"  - 现有数据库数量: {len(dbs)}")
        except:
            print("  - 无法获取数据库列表")
        
        return True
    except Exception as e:
        print(f"❌ DolphinDB 异常: {str(e)}")
        return False
    finally:
        try:
            s.close()
        except:
            pass

def check_mongodb():
    """检查MongoDB连接和基本功能"""
    try:
        # 检查服务状态
        if os.system("systemctl is-active --quiet mongod") != 0:
            print("❌ MongoDB 服务未运行")
            return False
            
        # 测试连接
        client = MongoClient('mongodb://localhost:27017/')
        db = client.admin
        result = db.command('ping')
        
        # 获取服务器状态
        status = db.command('serverStatus')
        
        print("✅ MongoDB 运行正常")
        print(f"  - 连接成功")
        print(f"  - 版本: {status['version']}")
        print(f"  - 连接数: {status['connections']['current']}")
        
        return True
    except Exception as e:
        print(f"❌ MongoDB 异常: {str(e)}")
        return False

def main():
    """主函数：运行所有检查"""
    print("开始环境检查...\n")
    
    # 检查DolphinDB
    print("检查 DolphinDB:")
    ddb_ok = check_dolphindb()
    print()
    
    # 检查MongoDB
    print("检查 MongoDB:")
    mongo_ok = check_mongodb()
    print()
    
    # 总结
    if ddb_ok and mongo_ok:
        print("✅ 所有服务运行正常")
        sys.exit(0)
    else:
        print("❌ 部分服务异常")
        sys.exit(1)

if __name__ == "__main__":
    main()
