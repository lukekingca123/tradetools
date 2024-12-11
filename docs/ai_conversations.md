# AI 对话记录

## 2024-01-07 期权预测模型开发

### 1. QLib与DolphinDB集成

#### 现有功能
- 已实现DolphinDB数据提供者
- 支持股票日线数据和因子数据
- 已有基本的预测模型框架（XGBoost、LightGBM、LSTM）

#### 待开发功能
1. 期权数据支持
   - 添加期权数据表结构
   - 支持期权特定字段（strike、expiry、option_type等）
   - 添加期权特征计算（隐含波动率、Greeks等）

2. 模型改进方向
   - 特征重要性分析
   - 集成方法
   - 交叉验证支持
   - 超参数调优
   - 基于预测置信度的仓位管理

### 2. 现有期权数据处理系统

#### 数据库结构
- DolphinDB作为存储引擎
- 按日期分区的表结构
- 存储60分钟级别的期权数据
- 主要字段：symbol, date, timestamp, OHLC, volume, openinterest

#### 数据处理功能
1. 数据清洗
   - 缺失值处理（价格使用前值填充，交易量用0填充）
   - 异常值检测（使用5个标准差的Z-score方法）
   - 价格合理性检查（OHLC关系验证）
   - 时间戳处理（确保单调性，处理重复值）
   - 流动性指标计算

2. 数据导入
   - 支持批量处理CSV文件
   - 包含错误处理和重试机制
   - 详细的日志记录

#### 代码结构
```python
class OptionDataHandler:
    def __init__(self, host="localhost", port=8848, ...):
        # 初始化DolphinDB连接
        
    def initialize_database(self):
        # 创建数据库和表结构
        
    def clean_option_data(self, df: pd.DataFrame):
        # 数据清洗和处理
        
    def process_csv_files(self, csv_dir: str, batch_size: int = 1000):
        # CSV文件批量处理和导入
```

### 3. 期权数据清洗改进

#### 缺失值处理优化
- 不再简单使用前值填充
- 连续缺失超过3个周期的数据标记为无效
- 短期缺失使用同组期权（相同到期日和行权价）的中位数填充
- 只在没有期权具体信息时才使用前值填充

#### 异常值检测优化
- 使用价格变化率而不是绝对价格检测异常
- 异常值阈值从5个标准差提高到10个标准差
- 按期权合约分组进行检测
- 标记异常值但不自动修正

#### 流动性评分改进
- 使用分位数方法评估交易量
- 加入价差因素（如有bid/ask数据）
- 更细致的流动性评分系统（0-8分）

### 代码片段

#### 期权数据表结构
```python
def _init_schema(self):
    # 添加期权数据表
    self.conn.run("""
    if(!existsTable("dfs://market", "options_daily")){
        schema = table(
            1:0, 
            `symbol`date`strike`expiry`option_type`underlying`open`high`low`close`volume`open_interest`implied_vol`delta`gamma`theta`vega,
            [SYMBOL, DATE, DOUBLE, DATE, SYMBOL, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, LONG, LONG, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE]
        )
        db = database("dfs://market", VALUE, 2010.01.01..2030.12.31)
        createPartitionedTable(db, schema, `options_daily, `date)
    }
    """)
```

### 下一步计划
1. 将现有的期权数据处理系统与QLib集成
2. 添加期权特定的特征计算
3. 开发预测模型

### 代码示例
```python
def detect_option_outliers(group):
    """基于期权特性的异常值检测"""
    if len(group) < 5:  # 样本太少不做检测
        return pd.Series(False, index=group.index)
        
    # 使用价格变化率而不是绝对价格来检测异常
    returns = group.pct_change()
    # 期权允许更大的价格波动，使用10个标准差
    mean_ret = returns.mean()
    std_ret = returns.std()
    if std_ret == 0:
        return pd.Series(False, index=group.index)
        
    z_scores = abs((returns - mean_ret) / std_ret)
    return z_scores > 10
```

### 注意事项
- 需要确保数据清洗规则适合期权交易的特点
- 考虑添加更多期权特定的数据验证规则
- 可能需要调整批处理大小以优化性能
- 需要确保DolphinDB中的期权数据质量
- 特征计算需要考虑期权的特殊性质
- 模型评估需要针对期权交易的特点进行调整
- 期权数据的特殊性要求更谨慎的数据处理
- 需要考虑期权的分组特征（到期日、行权价）
- 异常值检测需要适应期权的高波动性
- 流动性评分需要综合考虑多个因素
