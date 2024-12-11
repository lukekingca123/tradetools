import tweepy
from typing import List, Dict, Any
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import pandas as pd
from pymongo import MongoClient

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TwitterTracker:
    def __init__(self):
        load_dotenv()
        
        # Twitter API认证
        self.client = tweepy.Client(
            bearer_token=os.getenv('TWITTER_BEARER_TOKEN'),
            consumer_key=os.getenv('TWITTER_API_KEY'),
            consumer_secret=os.getenv('TWITTER_API_SECRET'),
            access_token=os.getenv('TWITTER_ACCESS_TOKEN'),
            access_token_secret=os.getenv('TWITTER_ACCESS_TOKEN_SECRET')
        )
        
        # MongoDB连接
        self.mongo_client = MongoClient(os.getenv('MONGODB_URI'))
        self.db = self.mongo_client['twitter_tracker']
        self.tweets_collection = self.db['tweets']
        self.users_collection = self.db['users']

    def add_user(self, username: str) -> bool:
        """添加要跟踪的用户"""
        try:
            user = self.client.get_user(username=username)
            if user.data:
                user_data = {
                    'id': user.data.id,
                    'username': username,
                    'added_at': datetime.utcnow(),
                    'is_active': True
                }
                self.users_collection.update_one(
                    {'username': username},
                    {'$set': user_data},
                    upsert=True
                )
                logger.info(f"Successfully added user: {username}")
                return True
        except Exception as e:
            logger.error(f"Error adding user {username}: {str(e)}")
        return False

    def fetch_user_tweets(self, username: str, max_results: int = 100) -> List[Dict[str, Any]]:
        """获取用户的最新推文"""
        try:
            tweets = self.client.get_users_tweets(
                id=self.users_collection.find_one({'username': username})['id'],
                max_results=max_results,
                tweet_fields=['created_at', 'public_metrics', 'text']
            )
            
            if not tweets.data:
                return []
                
            processed_tweets = []
            for tweet in tweets.data:
                tweet_data = {
                    'id': tweet.id,
                    'username': username,
                    'text': tweet.text,
                    'created_at': tweet.created_at,
                    'metrics': tweet.public_metrics,
                    'fetched_at': datetime.utcnow()
                }
                processed_tweets.append(tweet_data)
                
                # 存储到MongoDB
                self.tweets_collection.update_one(
                    {'id': tweet.id},
                    {'$set': tweet_data},
                    upsert=True
                )
                
            logger.info(f"Successfully fetched {len(processed_tweets)} tweets from {username}")
            return processed_tweets
        except Exception as e:
            logger.error(f"Error fetching tweets for {username}: {str(e)}")
            return []

    def get_user_analytics(self, username: str) -> Dict[str, Any]:
        """获取用户推文的分析数据"""
        tweets = list(self.tweets_collection.find({'username': username}))
        if not tweets:
            return {}
            
        df = pd.DataFrame(tweets)
        
        # 基础分析
        analytics = {
            'total_tweets': len(tweets),
            'avg_likes': df['metrics'].apply(lambda x: x.get('like_count', 0)).mean(),
            'avg_retweets': df['metrics'].apply(lambda x: x.get('retweet_count', 0)).mean(),
            'avg_replies': df['metrics'].apply(lambda x: x.get('reply_count', 0)).mean(),
            'most_recent_tweet': df['created_at'].max(),
            'oldest_tweet': df['created_at'].min()
        }
        
        return analytics

    def get_all_tracked_users(self) -> List[str]:
        """获取所有正在跟踪的用户列表"""
        return [user['username'] for user in self.users_collection.find({'is_active': True})]

    def remove_user(self, username: str) -> bool:
        """停止跟踪某个用户"""
        try:
            result = self.users_collection.update_one(
                {'username': username},
                {'$set': {'is_active': False}}
            )
            success = result.modified_count > 0
            if success:
                logger.info(f"Successfully removed user: {username}")
            return success
        except Exception as e:
            logger.error(f"Error removing user {username}: {str(e)}")
            return False
