# filter.py
#
# 筛选交易品种，依据为：
#  1. 成交量超过指定数量，且
#  2. 持仓量超过指定数量
#
# 输入：
#  1. csv文件名
#  2. 成交量阀值
#  3. 持仓量阀值
#  4. 生成csv文件名
# 
# 输入: 
#  1. 生成指定文件名的CSV文件
#
# 使用方法：
# 
#      filter <input_file_name> <volume_threshold> <OI_threshold> <output_file_name>
#
# Copyright(C) by Shenzhen Jupiter Fund 2025-
#

import pandas as pd
import numpy as np
import argparse

def filter_contracts(file_name, volume_threshold, oi_threshold):
    # read the CSV file
    df = pd.read_csv(file_name)
    
    # 初始化一个空列表，存放符合条件的记录
    selected_array = []

    for index, row in df.iterrows():
        volume = row['volume']  # 获取 volume 列的数据
        open_interest = row['open_interest']  # 获取 open_interest 列的数据

        if volume > volume_threshold and open_interest > oi_threshold :
            selected_array.append(row.to_numpy())  # 将符合条件的记录添加到 selected_records

    # 将 selected_records 转换为 numpy 数组
    selected_array = np.array(selected_records)

    # 返回符合条件的记录数组
    return selected_array

def save_selected_contracts(selected_array, output_file):
    # 如果 selected_array 中有数据，将其保存到 CSV 文件
    if selected_array.size > 0:
        # 转换为 DataFrame
        columns = ['date', 'exchange', 'contract', 'open', 'close', 'high', 'low', 'volume', 'open_interest']
        df_selected = pd.DataFrame(selected_array, columns=columns)

        # 保存为 CSV 文件
        df_selected.to_csv(output_file, index=False)

# 读取CSV文件，CSV格式如下：
# 
#   day, exchange, contract, open, close, high, low, volume, open_interest
#  20250210, DCE, i2505, 2222.0, 2202.0, 2222.0, 2201.0, 10000, 20000
#

if __name__ == "__main__":
    # 设置命令行参数
    parser = argparse.ArgumentParser(description="比较CSV文件中的数据与阀值")
    parser.add_argument('in_fn', type=str, help="CSV文件名")
    parser.add_argument('volume_threshold', type=int, help="成交量")
    parser.add_argument('oi_threshold', type=int, help="持仓阀值")
    parser.add_argument('out_fn', type=str, help="输出过滤后的文件名")

    解析命令行参数
    args = parser.parse_args()

    # 调用函数进行比较
    selected_array = filter_contracts(args.in_fn, args.volume_threshold, args.oi_threshold)

    # 保存符合条件的记录到 CSV 文件
    save_selected_contracts(selected_array, args.out_fn)

