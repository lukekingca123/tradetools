# 数据库架构设计与管理 - 2024-01-26

## 1. 数据架构设计

### 1.1 DolphinDB（时间序列数据）
用于存储和处理高频时间序列数据：
- 股票价格数据
- 期权数据
- 指数数据
- 期货数据

### 1.2 MongoDB（非结构化和半结构化数据）
用于存储和管理：
- 基本面数据
  - 公司基本信息
  - 财务数据
  - 股东数据
  - 行业数据
  - ESG数据
- 新闻和事件
  - 新闻数据
  - 公告信息
  - 重大事件
- 分析师数据
  - 研究报告
  - 评级数据
  - 目标价
  - 盈利预测
- 市场情绪
  - 情绪数据
  - 社交媒体数据
- 其他数据
  - ETF持仓
  - 内部交易
  - 空头兴趣

## 2. 代码实现

### 2.1 配置文件更新
更新了 `config.py`，明确区分了两种数据库的配置：
```python
# DolphinDB配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 8848,
    'username': 'admin',
    'password': '123456'
}

# MongoDB配置
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'tradetools',
    'username': '',
    'password': ''
}
```

### 2.2 基本面数据提供者
创建了 `FundamentalProvider` 类（`fundamental_provider.py`），提供以下功能：
- 公司基本信息管理
- 财务数据管理
- 新闻数据管理
- 分析师评级管理
- ETF持仓数据管理

主要特点：
- 单例模式确保只有一个数据库连接
- 自动创建必要的索引
- 支持日期范围查询
- 自动添加时间戳
- 使用 upsert 避免重复数据
- 返回 pandas DataFrame 格式

### 2.3 数据库管理脚本
创建了 `scripts/start_databases.sh` 用于管理两个数据库：

功能：
- start：启动数据库
- stop：停止数据库
- status：检查状态
- restart：重启数据库

配置：
- DolphinDB 路径：`/home/luke/ddb/server`
- DolphinDB 配置文件：`dolphindb.cfg`
- MongoDB 使用系统服务管理

## 3. 使用说明

### 3.1 数据库管理
```bash
# 检查数据库状态
./scripts/start_databases.sh status

# 启动数据库
./scripts/start_databases.sh start

# 停止数据库
./scripts/start_databases.sh stop

# 重启数据库
./scripts/start_databases.sh restart
```

### 3.2 基本面数据操作
```python
from data_sources.fundamental_provider import fundamental_provider

# 保存公司信息
company_info = {
    'name': 'Apple Inc.',
    'industry': 'Technology',
    'sector': 'Consumer Electronics',
    'description': 'Apple Inc. designs, manufactures, and markets smartphones...'
}
fundamental_provider.save_company_info('AAPL', company_info)

# 获取公司信息
info = fundamental_provider.get_company_info('AAPL')

# 保存新闻
news = {
    'symbol': 'AAPL',
    'date': '2024-01-15',
    'title': 'Apple Announces New iPhone',
    'content': 'Apple today announced...',
    'source': 'Reuters'
}
fundamental_provider.save_news(news)

# 获取新闻
recent_news = fundamental_provider.get_news('AAPL', start_date='2024-01-01')
```

## 4. 下一步计划

1. 数据采集
   - 实现基本面数据的自动采集
   - 添加新闻数据的自动抓取
   - 实现分析师报告的定期更新

2. 数据分析
   - 开发基本面分析工具
   - 实现新闻情感分析
   - 创建分析师观点汇总报告

3. 系统优化
   - 添加数据验证机制
   - 实现缓存层
   - 优化查询性能
   - 添加数据备份功能
