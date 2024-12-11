# DolphinDB 性能优化指南

## 1. 数据导入优化

### 1.1 批量导入策略
```python
# 推荐的批量大小
BATCH_SIZE = 1000  # 一般情况
BATCH_SIZE = 5000  # 大数据量情况

# 批量导入示例
def batch_import(df, table_name):
    for i in range(0, len(df), BATCH_SIZE):
        batch = df[i:i+BATCH_SIZE]
        s.upload({'data': batch})
        s.run(f"{table_name}.append!(data)")
```

### 1.2 异步写入
```sql
-- 启用异步写入
trades.append!(data, ,true)

-- 控制并发写入数
setMaxConnections(20)
```

### 1.3 数据预处理
```python
def preprocess_data(df):
    # 1. 类型转换
    df['timestamp'] = pd.to_datetime(df['timestamp']).astype('int64') // 10**9
    
    # 2. 数据压缩
    df = df.astype({
        'symbol': 'category',
        'price': 'float32'  # 如果精度允许
    })
    
    return df
```

## 2. 查询优化

### 2.1 分区优化
```sql
-- 好的分区策略
-- 1. 时间+标的分区
db = database("dfs://trades", VALUE, [`AAPL, `GOOGL])
addValuePartitions(db, ["MSFT", "AMZN"])

-- 2. 范围分区
db = database("dfs://trades", RANGE, [2020.01.01, 2021.01.01, 2022.01.01])
```

### 2.2 索引优化
```sql
-- 创建索引
create index idx_symbol on trades(symbol)

-- 复合索引
create index idx_symbol_date on trades(symbol, date)
```

### 2.3 查询优化
```sql
-- 使用分区剪枝
select * from trades 
where date between 2024.01.01 and 2024.01.10
and symbol in `AAPL`GOOGL

-- 避免使用 * 
select symbol, price, volume from trades
```

## 3. 内存优化

### 3.1 内存配置
```sql
-- 设置内存限制
setMaxMemSize(64)  -- 64GB

-- 设置缓存大小
setMaxCacheSize(32)  -- 32GB
```

### 3.2 内存监控
```python
# 监控内存使用
def monitor_memory():
    mem_info = s.run("""
    select 
        getAllocatedMemory() as allocated,
        getMaxMemSize() as max_size,
        getCacheMemory() as cache
    """)
    return mem_info

# 定期清理
def cleanup():
    s.run("clearAllCache()")
    s.run("clearTablePersistence()")
```

## 4. 并发优化

### 4.1 连接池
```python
# 创建连接池
class ConnectionPool:
    def __init__(self, size=10):
        self.pool = []
        for _ in range(size):
            conn = ddb.session()
            conn.connect("localhost", 8848)
            self.pool.append(conn)
    
    def get_connection(self):
        return self.pool.pop()
    
    def return_connection(self, conn):
        self.pool.append(conn)
```

### 4.2 并发控制
```sql
-- 设置最大连接数
setMaxConnections(100)

-- 设置每个查询的最大内存
setMaxMemoryPerQuery(4)  -- 4GB
```

## 5. 监控和维护

### 5.1 性能监控
```python
def monitor_performance():
    # 查询延迟
    start = time.time()
    s.run("select * from trades limit 1")
    latency = time.time() - start
    
    # 系统状态
    status = s.run("""
    select 
        getMaxMemSize() as max_mem,
        getAllocatedMemory() as used_mem,
        getMaxConnections() as max_conn,
        getConnectionCount() as curr_conn
    """)
    
    return {'latency': latency, 'status': status}
```

### 5.2 定期维护
```python
def maintenance():
    # 1. 优化表
    s.run("optimize table trades")
    
    # 2. 更新统计信息
    s.run("update stats trades")
    
    # 3. 清理缓存
    s.run("clearAllCache()")
    
    # 4. 碎片整理
    s.run("defragment table trades")
```

## 6. 最佳实践

### 6.1 数据导入最佳实践
```python
def optimal_import(df, table_name):
    # 1. 预处理
    df = preprocess_data(df)
    
    # 2. 批量导入
    with Timer() as t:
        batch_import(df, table_name)
    
    # 3. 验证
    count = s.run(f"select count(*) from {table_name}")
    
    return {
        'time': t.elapsed,
        'records': len(df),
        'verified_count': count
    }
```

### 6.2 查询最佳实践
```python
def optimal_query(symbol, start_date, end_date):
    # 1. 参数化查询
    query = """
    select symbol, date, avg(price) as avg_price
    from trades
    where symbol = ?
    and date between ? and ?
    group by symbol, date
    order by date
    """
    
    # 2. 使用异步执行
    future = s.run(query, symbol, start_date, end_date, async_=True)
    
    # 3. 处理结果
    result = future.result()
    return result
```

### 6.3 系统维护最佳实践
```python
def system_maintenance():
    # 1. 检查系统状态
    status = monitor_performance()
    
    # 2. 需要维护时执行
    if status['latency'] > threshold:
        maintenance()
    
    # 3. 更新监控指标
    update_metrics(status)
```
