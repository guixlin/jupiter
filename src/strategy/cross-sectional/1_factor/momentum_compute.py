# momentum_compute.py
#
# 计算交易品种的动量因子值，依据为：
#  1. N天的收益率， N为输入参数 days， 范围为：1-30
#  2. 计算公式为： 
#        ratio = (close_price - close_price.shift(days)) / close_price.shift(days)
# 
# 输入：
#     1. csv文件名
#     2. 计算动量因子的天数
#     3. 生成csv文件名
#
# 输出：
#     1. 生成指定文件名的CSV文件
#
# 使用方法：
#
#      factor_compute <input_file_name> <days> <output_file_name>
#

import pandas as pd
import numpy as np
import argparse

def compute_momentum(file_name, days):
    # read the CSV file
    df = pd.read_csv(file_name)
    
    # 初始化一个空列表，存放动量因子值
    momentum_array = []
    
    for index, row in df.iterrows():
        close_price = row['close']  # 获取 close 列的数据
        momentum = (close_price - df['close'].shift(days)) / df['close'].shift(days)
        momentum_array.append(momentum)
    
    # 将 momentum_array 转换为 numpy 数组
    momentum_array = np.array(momentum_array)
    
    # 返回动量因子值数组
    return momentum_array

def save_momentum(momentum_array, output_file):
    # 如果 momentum_array 中有数据，将其保存到 CSV 文件
    if momentum_array.size > 0:
	# 转换为 DataFrame
    	columns = ['momentum']
	df_momentum = pd.DataFrame(momentum_array, columns=columns)
    
    # 保存为 CSV 文件
    df_momentum.to_csv(output_file, index=False)
    
# 读取CSV文件，CSV格式如下：
#
#   day, exchange, contract, open, close, high, low, volume, open_interest
#  20250210, DCE, i2505, 2222.0, 2202.0, 2222.0, 2201.0, 10000, 20000
#
# 读取动量因子的天数
parser = argparse.ArgumentParser()
parser.add_argument("file_name", help="input file name")
parser.add_argument("days", help="momentum days")
parser.add_argument("output_file", help="output file name")
args = parser.parse_args()

# 调用函数进行计算
momentum_array = compute_momentum(args.file_name, args.days)

# 保存动量因子值到 CSV 文件
save_momentum(momentum_array, args.output_file)

