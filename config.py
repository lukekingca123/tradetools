"""
配置文件
"""

# DolphinDB配置 - 用于时间序列数据（股价、期权等）
DB_CONFIG = {
    'host': 'localhost',
    'port': 8848,
    'username': 'admin',
    'password': '123456'
}

# DolphinDB表名配置
TABLES = {
    'stocks': 'stocks',              # 股票价格数据
    'options': 'options',            # 期权数据
    'index': 'index',               # 指数数据
    'futures': 'futures'            # 期货数据
}

# MongoDB配置 - 用于基本面、新闻等数据
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'database': 'tradetools',
    'username': '',
    'password': ''
}

# MongoDB集合配置
COLLECTIONS = {
    # 基本面数据
    'company_info': 'company_info',          # 公司基本信息
    'financials': 'financials',              # 财务数据
    'ownership': 'ownership',                # 股东数据
    'industry': 'industry',                  # 行业数据
    'esg': 'esg',                           # ESG数据
    
    # 新闻和事件
    'news': 'news',                         # 新闻数据
    'announcements': 'announcements',        # 公告信息
    'events': 'events',                     # 重大事件
    
    # 分析师数据
    'research': 'research',                 # 研究报告
    'ratings': 'ratings',                   # 评级数据
    'price_targets': 'price_targets',       # 目标价
    'earnings_forecasts': 'earnings_forecasts', # 盈利预测
    
    # 市场情绪
    'sentiment': 'sentiment',               # 情绪数据
    'social_media': 'social_media',         # 社交媒体数据
    
    # 其他数据
    'etf_holdings': 'etf_holdings',         # ETF持仓数据
    'insider_trades': 'insider_trades',     # 内部交易
    'short_interest': 'short_interest',     # 空头兴趣
    
    # 系统数据
    'signals': 'signals',                   # 交易信号
    'backtest': 'backtest',                # 回测结果
    'trades': 'trades',                    # 交易记录
    'positions': 'positions'               # 持仓记录
}
