"""
期权预测模型模块
"""
import numpy as np
import pandas as pd
from typing import Text, Union
import torch
import torch.nn as nn
from qlib.model.base import Model
from qlib.data.dataset import DatasetH
from qlib.data.dataset.handler import DataHandlerLP
from torch.utils.data import DataLoader, Dataset
from .dolphin_provider import DolphinDBProvider  # 使用相对导入

class OptionDataset(Dataset):
    """期权数据集"""
    
    def __init__(self, features: pd.DataFrame, labels: pd.DataFrame = None,
                 window_size: int = 20):
        """
        初始化数据集
        
        Args:
            features: 特征数据
            labels: 标签数据
            window_size: 时间窗口大小
        """
        self.features = features
        self.labels = labels
        self.window_size = window_size
        
        # 按合约分组，确保每个序列的连续性
        self.groups = list(features.groupby(level=0))
        
    def __len__(self):
        return len(self.features) - self.window_size
        
    def __getitem__(self, idx):
        # 获取当前合约和时间点
        instrument = self.features.index[idx][0]
        timestamp = self.features.index[idx][1]
        
        # 获取历史数据窗口
        group_data = self.features.loc[instrument]
        start_idx = group_data.index.get_loc(timestamp)
        window_data = group_data.iloc[start_idx:start_idx + self.window_size].values
        
        # 如果有标签数据，获取对应的标签
        if self.labels is not None:
            label = self.labels.loc[(instrument, timestamp)]
            return torch.FloatTensor(window_data), torch.FloatTensor([label])
        
        return torch.FloatTensor(window_data)

class OptionPricePredictor(nn.Module):
    """期权价格预测模型"""
    
    def __init__(self, input_size: int, hidden_size: int = 128, num_layers: int = 2,
                 dropout: float = 0.2):
        """
        初始化模型
        
        Args:
            input_size: 输入特征维度
            hidden_size: LSTM隐藏层大小
            num_layers: LSTM层数
            dropout: Dropout比率
        """
        super().__init__()
        
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            dropout=dropout,
            batch_first=True
        )
        
        self.attention = nn.MultiheadAttention(
            embed_dim=hidden_size,
            num_heads=4,
            dropout=dropout
        )
        
        self.fc_layers = nn.Sequential(
            nn.Linear(hidden_size, 64),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(32, 1)
        )
        
    def forward(self, x):
        # LSTM层
        lstm_out, _ = self.lstm(x)  # [batch_size, seq_len, hidden_size]
        
        # 注意力层
        attn_out, _ = self.attention(
            lstm_out.permute(1, 0, 2),  # [seq_len, batch_size, hidden_size]
            lstm_out.permute(1, 0, 2),
            lstm_out.permute(1, 0, 2)
        )
        attn_out = attn_out.permute(1, 0, 2)  # [batch_size, seq_len, hidden_size]
        
        # 取最后一个时间步的输出
        last_hidden = attn_out[:, -1, :]  # [batch_size, hidden_size]
        
        # 全连接层
        out = self.fc_layers(last_hidden)  # [batch_size, 1]
        
        return out

class OptionPredictor(Model):
    """QLib模型接口"""
    
    def __init__(self, model_config: dict = None, dataset_config: dict = None):
        """
        初始化预测器
        
        Args:
            model_config: 模型配置
            dataset_config: 数据集配置
        """
        self.model_config = model_config or {}
        self.dataset_config = dataset_config or {}
        
        # 模型参数
        self.input_size = self.model_config.get('input_size', 20)
        self.hidden_size = self.model_config.get('hidden_size', 128)
        self.num_layers = self.model_config.get('num_layers', 2)
        self.dropout = self.model_config.get('dropout', 0.2)
        self.learning_rate = self.model_config.get('learning_rate', 0.001)
        self.batch_size = self.model_config.get('batch_size', 64)
        self.num_epochs = self.model_config.get('num_epochs', 100)
        
        # 数据集参数
        self.window_size = self.dataset_config.get('window_size', 20)
        
        # 初始化模型
        self.model = None
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
    def _init_model(self, input_size: int):
        """初始化模型"""
        self.model = OptionPricePredictor(
            input_size=input_size,
            hidden_size=self.hidden_size,
            num_layers=self.num_layers,
            dropout=self.dropout
        ).to(self.device)
        
        self.optimizer = torch.optim.Adam(
            self.model.parameters(),
            lr=self.learning_rate
        )
        
        self.criterion = nn.MSELoss()
        
    def fit(self, dataset: DatasetH):
        """训练模型
        
        Args:
            dataset: QLib数据集
        """
        df_train = dataset.prepare("train")
        x_train, y_train = df_train["feature"], df_train["label"]
        
        # 初始化模型
        if self.model is None:
            self._init_model(input_size=x_train.shape[1])
        
        # 创建数据集和数据加载器
        train_dataset = OptionDataset(
            features=x_train,
            labels=y_train,
            window_size=self.window_size
        )
        train_loader = DataLoader(
            train_dataset,
            batch_size=self.batch_size,
            shuffle=True
        )
        
        # 训练循环
        self.model.train()
        for epoch in range(self.num_epochs):
            total_loss = 0
            for batch_x, batch_y in train_loader:
                batch_x = batch_x.to(self.device)
                batch_y = batch_y.to(self.device)
                
                # 前向传播
                self.optimizer.zero_grad()
                pred = self.model(batch_x)
                loss = self.criterion(pred, batch_y)
                
                # 反向传播
                loss.backward()
                self.optimizer.step()
                
                total_loss += loss.item()
            
            # 打印训练进度
            if (epoch + 1) % 10 == 0:
                print(f"Epoch [{epoch+1}/{self.num_epochs}], "
                      f"Loss: {total_loss/len(train_loader):.4f}")
                
    def predict(self, dataset: DatasetH) -> Union[pd.Series, pd.DataFrame]:
        """预测
        
        Args:
            dataset: QLib数据集
            
        Returns:
            预测结果
        """
        df_test = dataset.prepare("test")
        x_test = df_test["feature"]
        
        # 创建测试数据集
        test_dataset = OptionDataset(
            features=x_test,
            window_size=self.window_size
        )
        test_loader = DataLoader(
            test_dataset,
            batch_size=self.batch_size,
            shuffle=False
        )
        
        # 预测
        self.model.eval()
        predictions = []
        with torch.no_grad():
            for batch_x in test_loader:
                batch_x = batch_x.to(self.device)
                pred = self.model(batch_x)
                predictions.extend(pred.cpu().numpy())
        
        # 转换为pandas Series
        predictions = pd.Series(
            predictions,
            index=x_test.index[self.window_size:],
            name="prediction"
        )
        
        return predictions
