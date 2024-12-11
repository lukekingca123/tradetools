from pymongo import MongoClient, ASCENDING
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_database():
    """初始化MongoDB数据库，创建必要的集合和索引"""
    try:
        # 加载环境变量
        load_dotenv()
        
        # 连接MongoDB
        client = MongoClient(os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'))
        db = client['twitter_tracker']
        
        # 创建tweets集合的索引
        tweets = db['tweets']
        tweets.create_index([("id", ASCENDING)], unique=True)
        tweets.create_index([("username", ASCENDING)])
        tweets.create_index([("created_at", ASCENDING)])
        logger.info("Created indexes for tweets collection")
        
        # 创建users集合的索引
        users = db['users']
        users.create_index([("username", ASCENDING)], unique=True)
        users.create_index([("id", ASCENDING)])
        logger.info("Created indexes for users collection")
        
        # 验证索引是否创建成功
        tweet_indexes = tweets.list_indexes()
        user_indexes = users.list_indexes()
        
        logger.info("Tweet collection indexes:")
        for index in tweet_indexes:
            logger.info(f"  - {index['name']}: {index['key']}")
            
        logger.info("User collection indexes:")
        for index in user_indexes:
            logger.info(f"  - {index['name']}: {index['key']}")
            
        return True
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False

if __name__ == "__main__":
    if init_database():
        logger.info("Database initialization completed successfully")
    else:
        logger.error("Database initialization failed")
