import dolphindb as ddb
import sys
import os

def test_connection(host="localhost", port=8848):
    """测试DolphinDB连接
    
    Returns:
        bool: 连接是否成功
    """
    try:
        # 创建连接
        print("正在连接DolphinDB...")
        s = ddb.session()
        s.connect(host, port, "admin", "123456")
        
        # 执行简单查询
        print("执行测试查询...")
        result = s.run("1 + 1")
        print(f"查询结果: {result}")
        
        # 获取服务器信息
        print("\nDolphinDB服务器信息:")
        try:
            # 系统资源使用情况
            cpu = s.run("getSystemCpuUsage()")
            load = s.run("getSystemLoadAvg()")
            print(f"CPU使用率: {cpu:.1f}%")
            print(f"系统负载: {load:.2f}")
            
            # 获取更多系统信息
            try:
                node = s.run("rpc(getNodeAlias(), getNodeHost)")
                print(f"节点主机: {node}")
            except:
                pass
                
            try:
                workers = s.run("rpc(getNodeAlias(), getWorkerCount)")
                print(f"工作线程数: {workers}")
            except:
                pass
        except Exception as e:
            print(f"获取服务器信息失败: {str(e)}")
        
        # 列出所有数据库
        print("\n现有数据库:")
        try:
            dbs = s.run("select name, owner, shared from objs(true) where type='DATABASE'")
            if len(dbs) > 0:
                for i in range(len(dbs['name'])):
                    print(f"- {dbs['name'][i]} (拥有者: {dbs['owner'][i]}, 共享: {dbs['shared'][i]})")
            else:
                print("暂无数据库")
        except Exception as e:
            print(f"获取数据库列表失败: {str(e)}")
            
        # 获取DolphinDB版本信息
        try:
            version = s.run("version()")
            print(f"\nDolphinDB版本: {version}")
        except:
            pass
        
        print("\n连接测试成功!")
        return True
        
    except Exception as e:
        print(f"\n连接失败: {str(e)}")
        return False
    finally:
        try:
            s.close()
        except:
            pass

if __name__ == "__main__":
    test_connection()
