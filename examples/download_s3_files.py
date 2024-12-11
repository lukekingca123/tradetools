"""
从S3下载文件的示例脚本
"""
import os
import sys
from dotenv import load_dotenv

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 加载环境变量
load_dotenv()

from data_sources.s3_client import S3Client

def main():
    # 初始化S3客户端
    # AWS凭证将从环境变量中读取：
    # - AWS_ACCESS_KEY_ID
    # - AWS_SECRET_ACCESS_KEY
    # - AWS_DEFAULT_REGION (可选，默认为us-east-1)
    client = S3Client()
    
    # 示例：下载单个文件
    bucket = "your-bucket-name"
    key = "path/to/your/file.csv"
    local_path = "data/downloaded/file.csv"
    
    success = client.download_file(bucket, key, local_path)
    if success:
        print(f"Successfully downloaded file to {local_path}")
    else:
        print("Failed to download file")
    
    # 示例：下载整个目录
    prefix = "path/to/your/directory/"
    local_dir = "data/downloaded/directory"
    
    success = client.download_directory(bucket, prefix, local_dir)
    if success:
        print(f"Successfully downloaded directory to {local_dir}")
    else:
        print("Failed to download directory")

if __name__ == "__main__":
    main()
