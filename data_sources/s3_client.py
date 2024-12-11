"""
AWS S3客户端，用于下载和上传文件
"""
import os
import boto3
from typing import Optional
import logging

class S3Client:
    def __init__(self, 
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 region_name: Optional[str] = None):
        """
        初始化S3客户端
        
        Args:
            aws_access_key_id: AWS访问密钥ID，如果为None则从环境变量AWS_ACCESS_KEY_ID获取
            aws_secret_access_key: AWS秘密访问密钥，如果为None则从环境变量AWS_SECRET_ACCESS_KEY获取
            region_name: AWS区域名称，如果为None则从环境变量AWS_DEFAULT_REGION获取，默认为'us-east-1'
        """
        self.aws_access_key_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region_name = region_name or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        if not self.aws_access_key_id or not self.aws_secret_access_key:
            raise ValueError("AWS credentials are required. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables or pass them directly.")
        
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )
        
    def download_file(self, bucket: str, key: str, local_path: str) -> bool:
        """
        从S3下载文件
        
        Args:
            bucket: S3存储桶名称
            key: S3对象键（文件路径）
            local_path: 本地保存路径
            
        Returns:
            bool: 下载是否成功
        """
        try:
            # 确保目标目录存在
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # 下载文件
            self.s3.download_file(bucket, key, local_path)
            logging.info(f"Successfully downloaded s3://{bucket}/{key} to {local_path}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to download s3://{bucket}/{key}: {str(e)}")
            return False
    
    def download_directory(self, bucket: str, prefix: str, local_dir: str) -> bool:
        """
        从S3下载整个目录
        
        Args:
            bucket: S3存储桶名称
            prefix: S3目录前缀
            local_dir: 本地目录路径
            
        Returns:
            bool: 是否所有文件都下载成功
        """
        try:
            # 确保本地目录存在
            os.makedirs(local_dir, exist_ok=True)
            
            # 列出所有对象
            paginator = self.s3.get_paginator('list_objects_v2')
            all_success = True
            
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    # 获取相对路径
                    rel_path = obj['Key'][len(prefix):].lstrip('/')
                    if not rel_path:  # 跳过目录对象
                        continue
                        
                    # 构建本地路径
                    local_path = os.path.join(local_dir, rel_path)
                    
                    # 下载文件
                    success = self.download_file(bucket, obj['Key'], local_path)
                    all_success = all_success and success
            
            return all_success
            
        except Exception as e:
            logging.error(f"Failed to download directory s3://{bucket}/{prefix}: {str(e)}")
            return False
