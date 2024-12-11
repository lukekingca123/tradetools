"""
基于QLib的股价预测模型
"""
from typing import Dict, List, Optional, Union, Tuple
import numpy as np
import pandas as pd
from datetime import datetime
import qlib
from qlib.data import D
from qlib.config import REG_CN
from qlib.contrib.model.gbdt import LGBModel, XGBModel
from qlib.contrib.model.pytorch.lstm import LSTMModel
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP
from qlib.workflow import R
from qlib.workflow.record_temp import SignalRecord, PortAnaRecord
import torch
import torch.nn as nn
from dataclasses import dataclass

@dataclass
class ModelConfig:
    """模型配置"""
    learning_rate: float = 0.01
    num_epochs: int = 200
    batch_size: int = 256
    early_stop_rounds: int = 50
    
class StockPredictor:
    """股票预测器"""
    
    def __init__(self, 
                 provider_uri: str = '~/.qlib/qlib_data/cn_data',
                 region: str = 'cn'):
        """
        Args:
            provider_uri: 数据源路径
            region: 地区，'cn'为中国市场，'us'为美国市场
        """
        # 初始化QLib
        qlib.init(provider_uri=provider_uri, region=region)
        
        # 定义特征
        self.features = {
            # 价格特征
            'OPEN': '$open',
            'HIGH': '$high',
            'LOW': '$low',
            'CLOSE': '$close',
            'VOLUME': '$volume',
            
            # 技术指标
            'MA5': 'Mean($close, 5)',
            'MA10': 'Mean($close, 10)',
            'MA20': 'Mean($close, 20)',
            'MA60': 'Mean($close, 60)',
            
            'VOL5': 'Mean($volume, 5)',
            'VOL10': 'Mean($volume, 10)',
            'VOL20': 'Mean($volume, 20)',
            'VOL60': 'Mean($volume, 60)',
            
            'MACD': 'EMA($close, 12) - EMA($close, 26)',
            'SIGNAL': 'EMA(EMA($close, 12) - EMA($close, 26), 9)',
            
            'RSI6': 'RSI($close, 6)',
            'RSI12': 'RSI($close, 12)',
            'RSI24': 'RSI($close, 24)',
            
            'BOLL_UPPER': 'BBand($close, 20, 2, 0)',
            'BOLL_MIDDLE': 'BBand($close, 20, 2, 1)',
            'BOLL_LOWER': 'BBand($close, 20, 2, 2)',
            
            'ROC5': 'ROC($close, 5)',
            'ROC10': 'ROC($close, 10)',
            'ROC20': 'ROC($close, 20)',
            
            # 动量指标
            'MOM5': 'Ref($close, -5)/$close - 1',
            'MOM10': 'Ref($close, -10)/$close - 1',
            'MOM20': 'Ref($close, -20)/$close - 1',
            
            # 波动率指标
            'STD5': 'Std($close, 5)',
            'STD10': 'Std($close, 10)',
            'STD20': 'Std($close, 20)',
            
            # 目标变量：未来5日收益率
            'LABEL': 'Ref($close, 5)/$close - 1'
        }
        
        self.models = {}
        
    def prepare_data(self,
                    instruments: Union[str, List[str]],
                    start_time: str,
                    end_time: str,
                    train_end_time: Optional[str] = None) -> Tuple[DatasetH, DatasetH]:
        """准备训练和测试数据
        
        Args:
            instruments: 股票代码列表或'all'
            start_time: 开始时间
            end_time: 结束时间
            train_end_time: 训练集结束时间，如果为None则使用end_time的80%
            
        Returns:
            训练集和测试集
        """
        # 获取特征数据
        df = D.features(
            instruments=instruments,
            fields=self.features,
            start_time=start_time,
            end_time=end_time,
            freq='day'
        )
        
        # 处理缺失值
        df = df.fillna(method='ffill').fillna(0)
        
        # 分割训练集和测试集
        if train_end_time is None:
            train_len = int(len(df) * 0.8)
            train_end_time = df.index[train_len][0]
            
        handler = {'class': 'Alpha360', 'module_path': 'qlib.contrib.data.handler'}
        segments = {
            'train': (start_time, train_end_time),
            'test': (train_end_time, end_time)
        }
        
        # 创建数据集
        dataset = DatasetH(
            handler=handler,
            segments=segments,
            fetch_kwargs={
                'data': df,
                'col_set': ['feature', 'label'],
                'data_key': 'raw'
            }
        )
        
        return dataset.prepare('train'), dataset.prepare('test')
        
    def train_xgboost(self,
                      train_dataset: DatasetH,
                      valid_dataset: Optional[DatasetH] = None,
                      config: Optional[ModelConfig] = None) -> XGBModel:
        """训练XGBoost模型
        
        Args:
            train_dataset: 训练数据集
            valid_dataset: 验证数据集
            config: 模型配置
        """
        if config is None:
            config = ModelConfig()
            
        # 模型参数
        model_params = {
            'learning_rate': config.learning_rate,
            'n_estimators': 200,
            'max_depth': 8,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'early_stopping_rounds': config.early_stop_rounds
        }
        
        # 创建模型
        model = XGBModel(**model_params)
        
        # 训练模型
        evals_result = {}
        model.fit(
            train_dataset,
            valid_dataset,
            evals_result=evals_result
        )
        
        self.models['xgboost'] = model
        return model
        
    def train_lightgbm(self,
                       train_dataset: DatasetH,
                       valid_dataset: Optional[DatasetH] = None,
                       config: Optional[ModelConfig] = None) -> LGBModel:
        """训练LightGBM模型"""
        if config is None:
            config = ModelConfig()
            
        # 模型参数
        model_params = {
            'learning_rate': config.learning_rate,
            'num_leaves': 31,
            'max_depth': 8,
            'n_estimators': 200,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'early_stopping_rounds': config.early_stop_rounds
        }
        
        # 创建模型
        model = LGBModel(**model_params)
        
        # 训练模型
        evals_result = {}
        model.fit(
            train_dataset,
            valid_dataset,
            evals_result=evals_result
        )
        
        self.models['lightgbm'] = model
        return model
        
    def train_lstm(self,
                   train_dataset: DatasetH,
                   valid_dataset: Optional[DatasetH] = None,
                   config: Optional[ModelConfig] = None) -> LSTMModel:
        """训练LSTM模型"""
        if config is None:
            config = ModelConfig()
            
        # 模型参数
        model_params = {
            'input_size': len(self.features) - 1,  # 减去label
            'hidden_size': 64,
            'num_layers': 2,
            'dropout': 0.2,
            'batch_size': config.batch_size,
            'n_epochs': config.num_epochs,
            'lr': config.learning_rate,
            'early_stop': config.early_stop_rounds
        }
        
        # 创建模型
        model = LSTMModel(**model_params)
        
        # 训练模型
        evals_result = {}
        model.fit(
            train_dataset,
            valid_dataset,
            evals_result=evals_result
        )
        
        self.models['lstm'] = model
        return model
        
    def predict(self,
               model_name: str,
               dataset: DatasetH) -> pd.Series:
        """使用模型进行预测
        
        Args:
            model_name: 模型名称 ('xgboost', 'lightgbm', 'lstm')
            dataset: 测试数据集
        """
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not trained yet")
            
        model = self.models[model_name]
        return model.predict(dataset)
        
    def evaluate(self,
                model_name: str,
                dataset: DatasetH) -> Dict:
        """评估模型性能
        
        Args:
            model_name: 模型名称
            dataset: 测试数据集
        """
        pred = self.predict(model_name, dataset)
        label = dataset.fetch(col_set='label')
        
        # 计算评估指标
        metrics = {}
        
        # MSE
        metrics['mse'] = np.mean((pred - label) ** 2)
        
        # MAE
        metrics['mae'] = np.mean(np.abs(pred - label))
        
        # 方向准确率
        pred_dir = (pred > 0).astype(int)
        label_dir = (label > 0).astype(int)
        metrics['dir_acc'] = np.mean(pred_dir == label_dir)
        
        # IC
        metrics['ic'] = np.corrcoef(pred, label)[0, 1]
        
        # ICIR
        ic_series = pd.Series(pred).rolling(20).corr(pd.Series(label))
        metrics['icir'] = ic_series.mean() / ic_series.std()
        
        return metrics
        
    def save_model(self, model_name: str, path: str):
        """保存模型"""
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")
            
        self.models[model_name].save(path)
        
    def load_model(self, model_name: str, path: str):
        """加载模型"""
        if model_name == 'xgboost':
            model = XGBModel()
        elif model_name == 'lightgbm':
            model = LGBModel()
        elif model_name == 'lstm':
            model = LSTMModel()
        else:
            raise ValueError(f"Unknown model type: {model_name}")
            
        model.load(path)
        self.models[model_name] = model
