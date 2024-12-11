"""
导入期权数据到 DolphinDB
"""
from ml_predict.dolphin_provider import DolphinDBProvider
import os
import traceback
from collections import defaultdict

def main():
    # 初始化 DolphinDB 提供者
    provider = DolphinDBProvider()
    
    # 设置数据源目录
    data_dir = "/home/luke/optionSource/source/60min"
    
    # 获取所有CSV文件
    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    total_files = len(files)
    
    print(f"找到 {total_files} 个CSV文件")
    
    # 记录导入结果
    success_count = 0
    skip_count = 0
    error_files = defaultdict(list)  # 按错误类型分类记录失败的文件
    
    # 导入所有数据
    for i, file in enumerate(files, 1):
        file_path = os.path.join(data_dir, file)
        print(f"\n正在处理 [{i}/{total_files}]: {file}")
        try:
            provider.import_option_csv(file_path)
            success_count += 1
        except ValueError as e:
            if "数据已存在" in str(e):
                print(f"跳过已存在的数据: {file}")
                skip_count += 1
            else:
                error_files["数据格式错误"].append((file, str(e)))
                print(f"数据格式错误: {str(e)}")
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            error_files[error_type].append((file, error_msg))
            print(f"导入失败: {error_type} - {error_msg}")
            print("详细错误信息:")
            print(traceback.format_exc())
    
    # 打印导入统计
    print("\n导入统计:")
    print(f"总文件数: {total_files}")
    print(f"成功导入: {success_count}")
    print(f"已存在跳过: {skip_count}")
    print(f"导入失败: {sum(len(files) for files in error_files.values())}")
    
    # 打印错误详情
    if error_files:
        print("\n错误详情:")
        for error_type, files in error_files.items():
            print(f"\n{error_type}:")
            for file, msg in files:
                print(f"- {file}: {msg}")

if __name__ == "__main__":
    main()
