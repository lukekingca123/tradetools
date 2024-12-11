# DolphinDB 数据库操作手册

## 1. 数据库基础

### 1.1 创建数据库
```python
# 创建分区数据库
db = database("dfs://db_name", VALUE, 2020.01M..2024.12M)

# 创建复合分区
db = database("dfs://db_name", RANGE, [2020.01.01, 2021.01.01, 2022.01.01])
```

### 1.2 分区表
```python
# 创建分区表
schema = table(1:0, `symbol`timestamp`price`volume, [SYMBOL,TIMESTAMP,DOUBLE,LONG])
db.createPartitionedTable(schema, `trades, `symbol`date)

# 按时间分区
db.createPartitionedTable(schema, `trades, `date)
```

## 2. 数据导入

### 2.1 批量导入
```python
# Python API
data = {'symbol': symbols, 'price': prices, 'volume': volumes}
s.upload(data)
s.run("trades.append!(data)")

# 优化写入性能
s.run("""
trades.append!(data, [SYMBOL, DOUBLE, LONG])
""")
```

### 2.2 导入注意事项

1. 数据类型匹配
- 确保上传数据类型与表schema匹配
- 使用合适的类型转换函数

2. 批量大小
- 建议每批1000-5000条记录
- 避免过大批量导致内存压力

3. 错误处理
```python
try:
    s.run("trades.append!(data)")
except Exception as e:
    print(f"Import failed: {str(e)}")
    # 实现重试逻辑
```

## 3. 性能优化

### 3.1 分区优化
- 选择合适的分区粒度
- 避免分区过小或过大
- 考虑查询模式选择分区键

### 3.2 写入优化
```python
# 使用批量写入
s.run("""
trades.append!(data, ,true)  # 第三个参数true表示异步写入
""")

# 预分配内存
s.run("""
trades.resize(n)  # n为预期记录数
""")
```

### 3.3 内存管理
```python
# 定期清理缓存
s.run("clearAllCache()")

# 设置内存限制
s.run("setMaxMemSize(64)")  # 单位为GB
```

## 4. 常见问题

### 4.1 导入失败
常见原因：
1. 数据类型不匹配
2. 内存不足
3. 网络连接问题
4. 分区键缺失

解决方案：
```python
# 检查数据类型
print(df.dtypes)

# 验证分区键
assert 'date' in df.columns

# 实现重试机制
max_retries = 3
while retries < max_retries:
    try:
        s.run("trades.append!(data)")
        break
    except Exception as e:
        retries += 1
        time.sleep(2)
```

### 4.2 性能问题
优化建议：
1. 使用合适的分区策略
2. 批量导入而不是逐条插入
3. 定期维护和优化
4. 监控系统资源使用

## 5. 最佳实践

### 5.1 数据导入
```python
def import_data(df, table_name):
    # 1. 数据预处理
    df = preprocess_data(df)
    
    # 2. 类型转换
    df = convert_types(df)
    
    # 3. 分批导入
    batch_size = 1000
    for i in range(0, len(df), batch_size):
        batch = df[i:i+batch_size]
        retry_import(batch, table_name)
        
def retry_import(batch, table_name):
    retries = 3
    while retries > 0:
        try:
            s.upload({'data': batch})
            s.run(f"{table_name}.append!(data)")
            break
        except Exception as e:
            retries -= 1
            time.sleep(2)
```

### 5.2 监控和维护
```python
# 检查表状态
s.run("""
select count(*) from trades
group by date
order by date desc
""")

# 检查分区状态
s.run("select * from getTables()")

# 优化表
s.run("optimize table trades")
```
