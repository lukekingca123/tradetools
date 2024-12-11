# TradeTools - 期权分析工具

基于历史期权交易数据和股票行情数据的分析工具，提供期权链查看和分析功能。

## 功能特点

- 期权链(Option Chain)可视化展示
- 支持多个到期日的期权数据展示
- 提供隐含波动率(IV)和Greeks计算
- 与DolphinDB数据库集成
- 历史数据分析和回测功能

## 环境要求

- Python 3.8+
- DolphinDB 服务器
- 相关Python包 (见requirements.txt)

## 安装说明

1. 克隆项目
2. 安装依赖：
```bash
pip install -r requirements.txt
```

## 配置说明

需要配置DolphinDB连接信息，包括：
- 服务器地址
- 端口
- 用户名密码
- 数据表名称

## 使用说明

1. 配置数据库连接
2. 运行Web界面：
```bash
python app.py
```
3. 访问本地界面：http://localhost:8050
