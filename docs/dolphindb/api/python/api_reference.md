# DolphinDB Python API 参考

## 1. 基础连接

### 1.1 建立连接
```python
import dolphindb as ddb

# 创建连接
s = ddb.session()
s.connect("localhost", 8848)
s.login("admin", "123456")

# 连接配置
s.enableStreaming(port=8849)  # 启用流数据
s.setTimeOut(timeout=10)      # 设置超时
```

### 1.2 错误处理
```python
try:
    s.connect("localhost", 8848)
except ddb.ConnectionException as e:
    print(f"连接失败: {str(e)}")
```

## 2. 数据操作

### 2.1 上传数据
```python
# 上传DataFrame
s.upload({'data': df})

# 上传字典
data = {
    'symbol': ['AAPL', 'GOOGL'],
    'price': [150.5, 2800.0]
}
s.upload(data)

# 指定类型上传
s.upload({
    'data': df
}, types={
    'symbol': ddb.SYMBOL,
    'price': ddb.DOUBLE
})
```

### 2.2 执行查询
```python
# 基础查询
result = s.run("select * from trades where date=2024.01.10")

# 参数化查询
date = "2024.01.10"
result = s.run("select * from trades where date=?", date)

# 多参数查询
symbol, date = "AAPL", "2024.01.10"
result = s.run("select * from trades where symbol=? and date=?", symbol, date)
```

### 2.3 数据类型转换
```python
# Python类型到DolphinDB类型
import numpy as np
import pandas as pd

# 时间戳转换
timestamp = pd.Timestamp.now()
dolphindb_ts = ddb.Timestamp(timestamp)

# 数值类型转换
price = np.float64(100.5)
dolphindb_price = ddb.Double(price)
```

## 3. 批量操作

### 3.1 批量导入
```python
def batch_import(df, table_name, batch_size=1000):
    """批量导入数据
    
    Args:
        df: pandas DataFrame
        table_name: 目标表名
        batch_size: 每批数据量
    """
    total_rows = len(df)
    for i in range(0, total_rows, batch_size):
        batch = df[i:i+batch_size]
        s.upload({'data': batch})
        s.run(f"{table_name}.append!(data)")
```

### 3.2 异步操作
```python
# 异步执行
future = s.run("long_running_query", async_=True)
result = future.result()  # 等待结果

# 批量异步
futures = []
for query in queries:
    future = s.run(query, async_=True)
    futures.append(future)

results = [f.result() for f in futures]
```

## 4. 高级特性

### 4.1 流数据订阅
```python
def handler(msg):
    print(f"收到数据: {msg}")

# 订阅流表
s.subscribe(
    host="localhost",
    port=8849,
    handler=handler,
    tableName="trades",
    actionName="sub1"
)
```

### 4.2 性能优化
```python
# 使用压缩
s.enableCompression(true)

# 设置缓冲区大小
s.setBufferSize(64)  # MB

# 批量写入优化
s.run("""
trades.append!(data, ,true)  # 异步写入
""")
```

### 4.3 会话管理
```python
# 检查连接状态
if s.isConnected():
    print("连接正常")

# 重连机制
def ensure_connection():
    if not s.isConnected():
        try:
            s.connect("localhost", 8848)
        except Exception as e:
            print(f"重连失败: {str(e)}")
```

## 5. 常见问题解决

### 5.1 连接问题
```python
# 连接超时处理
s.setTimeOut(30)  # 设置更长的超时时间

# 保持连接
def keep_alive():
    try:
        s.run("1+1")  # 心跳查询
    except Exception:
        ensure_connection()
```

### 5.2 数据类型问题
```python
# 检查数据类型
def check_types(df):
    type_map = {
        'int64': ddb.INT,
        'float64': ddb.DOUBLE,
        'datetime64[ns]': ddb.TIMESTAMP
    }
    return {col: type_map.get(str(dtype)) 
            for col, dtype in df.dtypes.items()}

# 类型转换
def convert_types(df):
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].astype('int64') // 10**9
    return df
```

### 5.3 内存管理
```python
# 清理内存
def cleanup():
    s.run("clearAllCache()")
    gc.collect()

# 大数据集处理
def process_large_data(df):
    batch_size = 1000
    for chunk in np.array_split(df, len(df) // batch_size + 1):
        process_chunk(chunk)
        cleanup()
```
