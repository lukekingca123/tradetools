"""
股价预测示例
"""
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

from ..ml_predict.stock_predictor import StockPredictor, ModelConfig

def main():
    # 创建预测器
    predictor = StockPredictor(
        provider_uri='~/.qlib/qlib_data/cn_data',
        region='cn'
    )
    
    # 准备数据
    instruments = ['SH600000']  # 浦发银行
    start_time = '2018-01-01'
    end_time = '2023-01-01'
    train_end_time = '2022-01-01'
    
    train_dataset, test_dataset = predictor.prepare_data(
        instruments=instruments,
        start_time=start_time,
        end_time=end_time,
        train_end_time=train_end_time
    )
    
    # 配置模型
    config = ModelConfig(
        learning_rate=0.01,
        num_epochs=200,
        batch_size=256,
        early_stop_rounds=50
    )
    
    # 训练模型
    print("Training XGBoost model...")
    xgb_model = predictor.train_xgboost(
        train_dataset=train_dataset,
        valid_dataset=test_dataset,
        config=config
    )
    
    print("Training LightGBM model...")
    lgb_model = predictor.train_lightgbm(
        train_dataset=train_dataset,
        valid_dataset=test_dataset,
        config=config
    )
    
    print("Training LSTM model...")
    lstm_model = predictor.train_lstm(
        train_dataset=train_dataset,
        valid_dataset=test_dataset,
        config=config
    )
    
    # 评估模型
    models = ['xgboost', 'lightgbm', 'lstm']
    results = {}
    
    for model_name in models:
        print(f"\nEvaluating {model_name}...")
        metrics = predictor.evaluate(model_name, test_dataset)
        results[model_name] = metrics
        
        print(f"MSE: {metrics['mse']:.6f}")
        print(f"MAE: {metrics['mae']:.6f}")
        print(f"Direction Accuracy: {metrics['dir_acc']:.2%}")
        print(f"IC: {metrics['ic']:.4f}")
        print(f"ICIR: {metrics['icir']:.4f}")
        
    # 绘制预测结果
    plt.figure(figsize=(15, 10))
    
    # 获取真实值
    label = test_dataset.fetch(col_set='label')
    
    # 绘制每个模型的预测值
    for model_name in models:
        pred = predictor.predict(model_name, test_dataset)
        plt.plot(pred, label=f'{model_name} prediction')
        
    plt.plot(label, label='True value', color='black', linestyle='--')
    plt.title('Stock Price Prediction')
    plt.xlabel('Time')
    plt.ylabel('Return')
    plt.legend()
    plt.grid(True)
    
    # 保存图像
    plt.savefig('prediction_results.png')
    print("\nPrediction results have been saved to 'prediction_results.png'")
    
    # 保存模型
    save_dir = Path('models')
    save_dir.mkdir(exist_ok=True)
    
    for model_name in models:
        model_path = save_dir / f'{model_name}_model'
        predictor.save_model(model_name, str(model_path))
        print(f"Saved {model_name} model to {model_path}")
        
if __name__ == '__main__':
    main()
