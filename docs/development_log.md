# 开发日志

## 2024-01-09 期权数据导入和查询优化

### 问题描述
在将期权数据导入DolphinDB并进行查询时遇到了一些问题：
1. 数据导入后查询返回零记录
2. 时间戳和期权类型的处理可能存在问题
3. 缺乏详细的调试信息

### 改进措施

#### 1. 重构 `import_option_csv` 函数
改进了期权数据导入功能，主要包括以下优化：

##### 数据读取和解析
- 添加了CSV原始数据和数据类型的详细日志
- 改进了期权代码解析，提取underlying、expiry、type和strike信息
- 添加了解析结果的验证和打印

##### 数据处理和转换
- 确保timestamp列为int64类型
- 正确处理期权相关字段（symbol、type、strike、expiry）
- 统一了列名（close -> price等）
- 选择和验证必需的列

##### 数据导入到DolphinDB
```sql
schema = table(
    1:0, `timestamp`symbol`type`strike`expiry`price`volume`open_interest,
    [TIMESTAMP, SYMBOL, SYMBOL, DOUBLE, TIMESTAMP, DOUBLE, INT, INT]
)
```
- 使用temporalAdd正确处理时间戳
- 添加了导入验证，包括行数和时间范围检查

#### 2. 优化 `get_option_data` 函数
改进了期权数据查询功能：

##### 查询条件优化
- 使用正确的时间戳比较
- 改进了期权类型过滤
- 添加了详细的查询调试信息

##### 调试信息
- 打印表结构和现有数据
- 显示查询条件和SQL语句
- 输出查询结果统计

### 代码示例

#### 数据导入过程
```python
# 1. 读取CSV文件
df = pd.read_csv(csv_file)
print("\nCSV原始数据:")
print("前5行:", df.head())
print("数据类型:", df.dtypes)

# 2. 解析期权信息
filename = os.path.basename(csv_file)
symbol = os.path.splitext(filename)[0]
underlying, expiry_date, option_type, strike = self._parse_option_symbol(symbol)

# 3. 准备数据
df['timestamp'] = df['timestamp'].astype('int64')
df['symbol'] = symbol
df['type'] = option_type
df['strike'] = strike
df['expiry'] = int(expiry_date.timestamp() * 1000)

# 4. 导入数据
self.conn.upload({'data': df})
self.conn.run("""
data = select 
    temporalAdd(timestamp, 0, 'ms') as timestamp,
    symbol,
    type,
    strike,
    temporalAdd(expiry, 0, 'ms') as expiry,
    price,
    volume,
    open_interest
from data
options = loadTable('dfs://options', 'options')
options.append!(data)
""")
```

#### 数据查询过程
```python
# 构建查询
query = f"""
select * from loadTable('dfs://options', 'options') 
where symbol like '{symbol}%'
and timestamp between {start_timestamp} and {end_timestamp}
"""

# 如果指定了期权类型，添加类型过滤
if option_type:
    query += f" and type = '{option_type.upper()}'"
```

### 后续工作
1. 监控数据导入的性能
2. 考虑添加数据完整性检查
3. 可能需要添加索引以提升查询性能
4. 考虑添加数据压缩以节省存储空间

### 注意事项
1. 时间戳必须使用毫秒级的整数
2. 期权类型必须是大写的'C'或'P'
3. 所有数值类型必须正确转换（如strike为DOUBLE）

## 2024-01-09 扩展股票代码支持范围

### 改进内容
1. 移除了对特定股票代码的限制（原先仅支持AAPL、MSFT、AMZN、NVDA、GOOGL）
2. 改进了期权代码解析和验证：
   - 验证标的代码格式（必须是字母）
   - 添加日期解析的错误处理
   - 验证期权类型（必须是'C'或'P'）
   - 验证行权价（必须是正数）

### 代码示例
```python
def _parse_option_symbol(self, symbol: str) -> tuple:
    """解析期权代码"""
    pattern = r"([A-Z]+)(\d{6})([CP])(\d+)"
    match = re.match(pattern, symbol)
    if not match:
        raise ValueError(f"无效的期权代码: {symbol}")
    
    underlying, date_str, option_type, strike_str = match.groups()
    
    # 验证标的代码格式
    if not underlying.isalpha():
        raise ValueError(f"无效的标的代码: {underlying}")
    
    # 解析日期
    try:
        expiry_date = datetime.strptime(date_str, "%y%m%d").date()
    except ValueError:
        raise ValueError(f"无效的到期日: {date_str}")
    
    # 验证期权类型和行权价
    if option_type not in ['C', 'P']:
        raise ValueError(f"无效的期权类型: {option_type}")
    
    try:
        strike_price = float(strike_str) / 1000.0
        if strike_price <= 0:
            raise ValueError(f"无效的行权价: {strike_price}")
    except ValueError:
        raise ValueError(f"无效的行权价格字符串: {strike_str}")
    
    return underlying, expiry_date, option_type, strike_price
```

### 改进效果
1. 可以处理任何符合格式的期权代码
2. 提供更详细的错误信息，有助于问题诊断
3. 保持了数据质量，通过严格的验证确保数据有效性

### 后续工作
1. 考虑添加更多的数据验证规则
2. 可能需要支持不同市场的期权代码格式
3. 监控新增股票的数据质量

## 2024-01-10 DolphinDB查询优化

### 问题描述
在优化DolphinDB查询功能时遇到了以下问题：
1. 时间戳查询语法不正确
2. 数据重复导入问题
3. 分区范围设置不当

### 解决方案

#### 1. 修复时间戳查询语法
- 尝试了多种查询方式：
  1. `temporalRange`函数（不支持）
  2. `between`函数（参数不匹配）
  3. 最终使用正确的语法：`timestamp(timestamp) between timestamp(start) and timestamp(end)`

#### 2. 解决数据重复问题
- 每次导入前检查并删除已存在的数据库：
```python
if(existsDatabase('dfs://options')){
    dropDatabase('dfs://options')
}
```

#### 3. 修正分区范围
- 将分区范围从`2023.01M..2024.12M`改为`2020.01M..2021.12M`以匹配数据时间范围
```python
db = database('dfs://options', VALUE, 2020.01M..2021.12M)
```

### 功能验证
1. 数据导入：
   - 成功导入多个期权数据文件
   - 验证无数据重复
   - 数据时间范围正确（2020年10月至12月）

2. 数据查询：
   - 支持按时间范围查询
   - 支持按期权类型过滤（看涨/看跌）
   - 支持按股票代码模糊查询

### 经验总结
1. DolphinDB的时间戳处理需要特别注意：
   - 使用`timestamp()`函数进行时间转换
   - 正确处理毫秒级时间戳
2. 分区设计要考虑实际数据的时间范围
3. 每次导入前清理旧数据，避免数据重复

## 2024-01-27 UI和可视化系统实现

### 系统架构
实现了两套UI和可视化系统：

#### 1. 主交易界面 (PyQt5)
- 使用PyQt5构建桌面应用界面
- 主要功能模块：
  - 行情监控
  - 委托监控
  - 成交监控
  - 持仓监控
  - 账户监控
  - 日志监控
  - 期权链面板
  - 新闻面板

#### 2. 回测和分析系统 (Plotly + Dash)

##### 回测可视化
- 使用Plotly构建交互式图表
- 主要图表：
  - 权益曲线（带回撤阴影）
  - 收益分析（分布、波动率等）
  - 因子分析图
  - 风险指标图

##### 期权分析仪表板
- 使用Dash构建Web界面
- 主要功能：
  - 期权链分析
  - 波动率分析
  - Greeks分析
  - 事件分析
- 交互式控制：
  - 股票选择器
  - 日期范围选择器
  - 分析模式选择器

### 技术选型说明

#### PyQt5 (主交易界面)
- 优点：性能好，原生桌面体验
- 适合实时交易场景
- 主要用于：交易操作、实时监控

#### Plotly (回测结果可视化)
- 优点：交互性强，图表美观
- 支持导出为HTML和图片
- 主要用于：回测结果展示、策略分析

#### Dash (分析仪表板)
- 优点：易于构建Web界面
- 适合复杂的数据分析展示
- 主要用于：期权分析、市场研究

### 后续计划
1. 添加更多自定义图表和分析工具
2. 优化UI响应速度和用户体验
3. 增加更多交互式分析功能
4. 完善数据导出和报告生成功能
