"""
使用QLib进行股票预测的示例
"""
import qlib
from qlib.config import REG_CN
from qlib.contrib.model.gbdt import LGBModel
from qlib.contrib.data.handler import Alpha158
from qlib.utils import init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
import pandas as pd
import numpy as np

def init_qlib():
    """初始化QLib"""
    # 设置日志级别
    qlib.init(provider_uri='~/.qlib/qlib_data/cn_data', region=REG_CN)

def get_data_handler_config(start_time='2018-01-01', end_time='2023-01-01'):
    """获取数据处理器配置"""
    handler_config = {
        "start_time": start_time,
        "end_time": end_time,
        "fit_start_time": start_time,
        "fit_end_time": end_time,
        "instruments": "csi300",
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
            "handler": {
                "class": "Alpha158",
                "module_path": "qlib.contrib.data.handler",
                "kwargs": handler_config
            },
            "segments": {
                "train": ("2018-01-01", "2021-12-31"),
                "valid": ("2022-01-01", "2022-06-30"),
                "test": ("2022-07-01", "2023-01-01")
            }
        }
    }
    return dataset_config

def train_model():
    """训练模型"""
    # 初始化QLib
    init_qlib()
    
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
        "topk": 50,
        "n_drop": 5,
        "signal": model.predict(dataset)
    }
    
    # 运行回测
    backtest_config = {
        "start_time": "2022-07-01",
        "end_time": "2023-01-01",
        "account": 100000000,
        "benchmark": "SH000300",
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
