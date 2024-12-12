import dolphindb as ddb
import logging

logging.basicConfig(level=logging.INFO)

def test_connection():
    try:
        # Create connection
        conn = ddb.session()
        conn.connect('127.0.0.1', 8848, 'admin', '123456')
        
        # Test basic operation
        result = conn.run("1 + 1")
        logging.info(f"Basic operation test result: {result}")
        
        # Test database access
        result = conn.run("existsDatabase('dfs://options')")
        logging.info(f"Database 'options' exists: {result}")
        
        # Test memory usage
        mem_info = conn.run("getMaxMemSize(), getTotalMemSize(), getFreeDiskSpace()")
        logging.info(f"Memory Info - Max: {mem_info[0]}, Total: {mem_info[1]}, Free Disk: {mem_info[2]}")
        
        # Test connection status
        status = conn.run("getSessionMemoryStat()")
        logging.info(f"Session Memory Stats: {status}")
        
        return True
    except Exception as e:
        logging.error(f"Connection test failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection()
