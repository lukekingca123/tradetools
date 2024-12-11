"""
使用DolphinDB作为数据源的QLib预测示例
"""
import qlib
from qlib.config import REG_US
from qlib.contrib.model.gbdt import LGBModel
from qlib.contrib.data.handler import Alpha158
from qlib.utils import init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from ..ml_predict.dolphin_provider import DolphinDBProvider, DolphinDBDataHandler

def prepare_data():
    """准备示例数据"""
    provider = DolphinDBProvider()
    
    # 准备股票日线数据
    daily_data = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'GOOGL', 'GOOGL'],
        'date': [datetime(2023, 1, 1), datetime(2023, 1, 2),
                datetime(2023, 1, 1), datetime(2023, 1, 2)],
        'open': [150.0, 151.0, 90.0, 91.0],
        'high': [152.0, 153.0, 92.0, 93.0],
        'low': [149.0, 150.0, 89.0, 90.0],
        'close': [151.0, 152.0, 91.0, 92.0],
        'volume': [1000000, 1100000, 500000, 550000],
        'amount': [151000000.0, 152000000.0, 45500000.0, 46000000.0],
        'factor': [1.0, 1.0, 1.0, 1.0]
    })
    provider.insert_stock_daily(daily_data)
    
    # 准备因子数据
    factor_data = pd.DataFrame({
        'symbol': ['AAPL', 'AAPL', 'GOOGL', 'GOOGL'],
        'date': [datetime(2023, 1, 1), datetime(2023, 1, 2),
                datetime(2023, 1, 1), datetime(2023, 1, 2)],
        'factor_name': ['momentum', 'momentum', 'momentum', 'momentum'],
        'value': [0.1, 0.2, 0.15, 0.25]
    })
    provider.insert_factors(factor_data)
    
    return provider

def get_data_handler_config(start_time='2023-01-01', end_time='2023-12-31'):
    """获取数据处理器配置"""
    handler_config = {
        "class": "DolphinDBDataHandler",
        "module_path": "tradetools.ml_predict.dolphin_provider",
        "kwargs": {
            "start_time": start_time,
            "end_time": end_time,
            "fit_start_time": start_time,
            "fit_end_time": end_time,
            "instruments": ["AAPL", "GOOGL"],
            "infer_processors": [
                {"class": "RobustZScoreNorm", "kwargs": {"fields_group": "feature"}},
                {"class": "Fillna", "kwargs": {"fields_group": "feature"}}
            ],
            "learn_processors": [
                {"class": "DropnaLabel"},
                {"class": "CSRankNorm", "kwargs": {"fields_group": "label"}}
            ],
            "label": ["Ref($close, -2) / Ref($close, -1) - 1"]
        }
    }
    return handler_config

def get_model_config():
    """获取模型配置"""
    model_config = {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {
            "loss": "mse",
            "colsample_bytree": 0.8879,
            "learning_rate": 0.0421,
            "subsample": 0.8789,
            "lambda_l1": 205.6999,
            "lambda_l2": 580.9768,
            "max_depth": 8,
            "num_leaves": 210,
            "num_threads": 20
        }
    }
    return model_config

def get_dataset_config(handler_config):
    """获取数据集配置"""
    dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": handler_config,
            "segments": {
                "train": ("2023-01-01", "2023-06-30"),
                "valid": ("2023-07-01", "2023-09-30"),
                "test": ("2023-10-01", "2023-12-31")
            }
        }
    }
    return dataset_config

def train_model():
    """训练模型"""
    # 准备数据
    provider = prepare_data()
    
    # 初始化QLib
    qlib.init(provider_uri="DolphinDB://localhost:8848", region=REG_US)
    
    # 获取配置
    handler_config = get_data_handler_config()
    dataset_config = get_dataset_config(handler_config)
    model_config = get_model_config()
    
    # 创建数据集
    dataset = init_instance_by_config(dataset_config)
    train_dataset, valid_dataset, test_dataset = dataset.prepare(
        ["train", "valid", "test"],
        col_set=["feature", "label"],
        data_key=DataHandlerLP.DK_L
    )
    
    # 创建并训练模型
    model = init_instance_by_config(model_config)
    model.fit(train_dataset)
    
    # 预测
    pred_test = model.predict(test_dataset)
    
    # 计算IC值
    ic = pd.Series(pred_test.squeeze()).corr(test_dataset.label.squeeze())
    print(f"Test IC: {ic:.4f}")
    
    return model, pred_test

def backtest(model, dataset):
    """回测模型"""
    # 创建投资组合策略
    strategy_config = {
        "topk": 2,  # 由于示例只有两只股票
        "n_drop": 0,
        "signal": model.predict(dataset)
    }
    
    # 运行回测
    backtest_config = {
        "start_time": "2023-10-01",
        "end_time": "2023-12-31",
        "account": 100000000,
        "benchmark": "AAPL",  # 使用AAPL作为基准
        "exchange_kwargs": {
            "limit_threshold": 0.095,
            "deal_price": "close",
            "open_cost": 0.0005,
            "close_cost": 0.0015,
            "min_cost": 5
        }
    }
    
    # 记录回测结果
    recorder = PortAnaRecord(model, dataset, strategy_config)
    recorder.generate(**backtest_config)
    
    # 获取回测指标
    analysis = recorder.get_portfolio_analysis()
    print("\nBacktest Results:")
    print(f"Annual Return: {analysis['annual_return']:.2%}")
    print(f"Max Drawdown: {analysis['max_drawdown']:.2%}")
    print(f"Sharpe Ratio: {analysis['sharp']:.2f}")
    
    return analysis

if __name__ == "__main__":
    model, pred = train_model()
    analysis = backtest(model, test_dataset)
