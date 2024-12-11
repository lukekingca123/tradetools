# 交易策略设计文档

## 1. 项目结构

```
tradetools/
├── strategies/           # 策略实现
│   ├── __init__.py
│   ├── base_strategy.py     # 策略基类
│   ├── momentum_strategy.py # 动量策略
│   ├── etf_rotation_strategy.py  # ETF轮动策略
│   ├── mean_reversion_strategy.py # 均值回归策略
│   ├── kelly_option_strategy.py  # 凯利期权策略
│   └── volatility_arbitrage_strategy.py  # 波动率套利策略
├── data/                # 数据模块
├── backtest/           # 回测框架
└── risk/               # 风险管理
```

## 2. 策略框架设计

### 2.1 基础策略类 (BaseStrategy)

所有具体策略的基类，定义了策略的基本接口和功能：

```python
class BaseStrategy:
    def __init__(self, name: str):
        self.name = name
        self.position = {}  # 当前持仓
        self.params = {}    # 策略参数
    
    def initialize(self, **kwargs):
        """初始化策略参数"""
        pass
    
    def generate_signals(self, data: pd.DataFrame) -> Dict[str, float]:
        """生成交易信号"""
        pass
    
    def calculate_position_size(self, signals: Dict[str, float],
                              total_capital: float,
                              max_position_size: float = 0.1) -> Dict[str, float]:
        """计算持仓规模"""
        pass
```

### 2.2 动量策略 (MomentumStrategy)

基于过去收益率排序的动量策略：

- 主要参数：
  - lookback_period: 回看期
  - holding_period: 持有期
  - n_top: 选择数量
  - allow_short: 是否允许做空

- 核心逻辑：
  1. 计算各资产过去收益率
  2. 按收益率排序
  3. 做多表现最好的资产
  4. 可选做空表现最差的资产

### 2.3 ETF轮动策略 (ETFRotationStrategy)

基于相对强度和动量的ETF轮动策略：

- 主要参数：
  - momentum_period: 动量计算周期
  - volatility_period: 波动率计算周期
  - n_top: 选择数量
  - risk_adjusted: 是否使用风险调整后收益

- 核心逻辑：
  1. 计算动量和波动率指标
  2. 计算风险调整后收益（可选）
  3. 选择排名靠前的ETF
  4. 应用市场趋势过滤

### 2.4 均值回归策略 (MeanReversionStrategy)

基于价格偏离程度的均值回归策略：

- 主要参数：
  - lookback_period: 回看期
  - entry_zscore: 进场阈值
  - exit_zscore: 出场阈值
  - max_holding_period: 最大持仓期

- 核心逻辑：
  1. 计算收益率的z-score
  2. 在显著偏离时逆向建仓
  3. 在回归到均值或超过最大持仓期时平仓

### 2.5 凯利期权策略 (KellyOptionStrategy)

基于凯利公式优化期权组合的策略：

- 主要参数：
  - lookback_period: 历史数据回看期（默认252天）
  - min_win_rate: 最小胜率要求（默认40%）
  - min_profit_ratio: 最小盈亏比要求（默认1.5）
  - max_portfolio_iv: 最大组合隐含波动率（默认50%）
  - risk_free_rate: 无风险利率（默认2%）

- 核心功能：
  1. 期权筛选
     - 基于历史胜率筛选
     - 基于盈亏比筛选
     - 控制组合隐含波动率

  2. 仓位优化
     - 使用修正的凯利公式
     - 考虑胜率和盈亏比
     - 根据隐含波动率调整

  3. 风险控制
     - 单个期权仓位限制
     - 组合波动率控制
     - 期权到期管理

  4. 指标计算
     - Black-Scholes定价
     - 历史胜率统计
     - 隐含波动率计算

- 待开发功能：
  1. Greeks管理
     - Delta中性对冲
     - Gamma风险控制
     - Vega敞口管理
     - Theta衰减监控

  2. 波动率分析
     - 波动率期限结构
     - 波动率微笑/偏斜
     - 历史波动率vs隐含波动率

  3. 组合优化
     - 自动展期机制
     - 组合Delta对冲
     - 跨期权种类配置

  4. 风险管理
     - VaR计算
     - 压力测试
     - 情景分析

## 波动率套利策略设计

### 1. 策略概述

波动率套利策略(`VolatilityArbitrageStrategy`)主要基于以下几个核心假设：
1. 期权隐含波动率在特定事件前后存在可预测的模式
2. 市场对某些事件（如盈利公告）的波动率预期可能存在系统性偏差
3. 通过delta中性的期权组合可以纯粹地交易波动率

### 2. 核心组件

#### 2.1 事件识别
- 盈利公告事件筛选
  ```python
  def filter_earnings_events(self, min_market_cap: float = 1e9) -> pd.DataFrame:
  ```
- 重要经济数据发布
  ```python
  def filter_economic_events(self, event_types: List[str]) -> pd.DataFrame:
  ```

#### 2.2 波动率分析
- 历史波动率计算
  - 使用对数收益率
  - 支持不同时间窗口
- 隐含波动率分析
  - 期权链数据处理
  - 平价期权筛选
- 波动率锥计算
  - 多周期分位数分析
  - 偏度和峰度统计

#### 2.3 信号生成
1. 事件驱动信号
   - 基于历史相似事件的波动率表现
   - 考虑市场环境和公司特征
   
2. 统计套利信号
   - 波动率期限结构分析
   - 跨期/跨执行价套利机会

#### 2.4 仓位管理
1. 期权组合构建
   - Delta中性
   - Gamma中性（可选）
   - Vega最大化

2. 风险控制
   - 单一持仓限制
   - 组合VaR控制
   - 止损止盈设置

### 3. 实现细节

#### 3.1 数据需求
1. 实时数据
   - 期权报价和Greeks
   - 标的价格和成交量
   
2. 历史数据
   - 期权历史交易数据
   - 历史波动率数据
   - 事件日历

#### 3.2 回测框架
1. 事件回测模式
   - 基于历史事件的回测
   - 考虑期权流动性约束
   
2. 性能评估
   - 收益率分析
   - 风险指标计算
   - 交易成本分析

### 4. 待优化项目

- [ ] 改进事件相似度计算方法
- [ ] 优化期权组合构建算法
- [ ] 增加更多风险控制手段
- [ ] 实现实时监控和调整机制

### 5. 使用示例

```python
# 初始化策略
strategy = VolatilityArbitrageStrategy(
    capital=1000000,
    universe=['AAPL', 'GOOGL', 'META'],
    risk_limit=0.02
)

# 设置事件过滤器
strategy.set_event_filters(
    min_market_cap=5e9,
    min_option_volume=1000,
    earnings_only=True
)

# 运行策略
strategy.run(
    start_date='2023-01-01',
    end_date='2023-12-31',
    rebalance_freq='1d'
)
```

## 数据管理更新

### 盈利公告日历功能

在 `DataManager` 类中新增了获取盈利公告日历的功能：

```python
def get_earnings_calendar(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
```

该方法使用 Financial Modeling Prep API 获取指定时间范围内的公司盈利公告日期。返回的DataFrame包含以下信息：
- 公司代码
- 公告日期
- 预期EPS
- 实际EPS
- 其他相关财务数据

### 经济日历功能

同时新增了获取经济日历的功能：

```python
def get_economic_calendar(self, start_date: datetime, end_date: datetime, events: List[str] = None) -> pd.DataFrame:
```

该方法用于获取重要经济事件日历，支持：
- 自定义时间范围
- 事件类型过滤（如FOMC会议、CPI公布等）
- 返回标准化的DataFrame格式数据

### 使用要求

1. 需要设置Financial Modeling Prep API密钥：
   ```bash
   export FMP_API_KEY='your_api_key_here'
   ```

2. 安装新增依赖：
   ```bash
   pip install financialmodelingprep>=0.1.0
   ```

### 应用场景

这些新功能主要用于支持：
1. 波动率套利策略中的事件驱动交易
2. 基于盈利公告的期权策略
3. 宏观经济事件对市场影响的分析

### 后续计划

- [ ] 添加更多数据源支持
- [ ] 优化数据缓存机制
- [ ] 增加数据质量检验
- [ ] 实现实时数据更新

## 3. 待开发模块

### 3.1 数据模块
- 市场数据获取和处理
- 数据清洗和对齐
- 实时数据更新

### 3.2 回测框架
- 历史数据回测
- 性能评估
- 交易成本模拟

### 3.3 风险管理
- 持仓限制
- 止损止盈
- 风险敞口控制

### 3.4 策略组合管理
- 多策略组合
- 资金分配
- 再平衡机制

## 4. 实现计划

1. 第一阶段：基础框架搭建
   - 完成策略基类
   - 实现数据获取模块
   - 搭建简单回测框架

2. 第二阶段：策略实现
   - 实现动量策略
   - 实现ETF轮动策略
   - 实现均值回归策略
   - 实现凯利期权策略

3. 第三阶段：功能完善
   - 完善回测框架
   - 添加风险管理
   - 实现策略组合

4. 第四阶段：优化和测试
   - 策略参数优化
   - 回测性能测试
   - 实盘模拟测试
