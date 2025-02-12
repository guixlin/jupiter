# cffex2csv.py
#
# 将Excel文件转换为CSV文件
#
# 输入：
#     1. csv文件名
#     2. 生成CSV文件名
#
# 输出：
#     1. 生成指定文件名的CSV文件
#
# 使用方法：
#
#      cffex2csv <input_file_name> <output_file_name>
#
# CZCE daily BAR file format:
#   合约代码,今开盘价,最高价,最低价, 成交量,成交金额,持仓量,持仓变化,今收盘,今结算,前结算,涨跌1,涨跌2,Delta 
#   contract, open, high, low, volume, amount, open_interest, oi_change, close, settlement, pre_settlement, change1, change2, delta
#

import pandas as pd
import argparse
import re
import os

def get_product(text):
    # 使用正则表达式匹配合约，直到遇到第一个数字
    result = re.match(r'^[^\d]*', text)
    if result:
        return result.group(0)
    return ""

def is_contract(text):
    # 检查字符串是否非空且第一个字符是字母
    return text and text[0].isalpha()

# 读取参数
parser = argparse.ArgumentParser()
parser.add_argument("file_name", help="input file name")
parser.add_argument("output_file", help="output file name")
args = parser.parse_args()

# 加载CSV文件
try:
    df = pd.read_csv(args.file_name, encoding='utf-8')
except UnicodeDecodeError:
    df = pd.read_csv(args.file_name, encoding='gb18030')  # 适用于中文

df.columns=['contract', 'open', 'high', 'low', 'volume', 'amount', 'open_interest', 'oi_change', 'close', 'settlement', 'pre_settlement', 'change1', 'change2', 'delta']

# 删除最后一列
# df = df.iloc[:, :-1]

# 去掉所有"小计"行
df = df[df['contract'].astype(str).str.match(r'^[A-Za-z0-9\s]+$', na=False)]

# 去掉dataframe中所有的空格
df = df.map(lambda x: x.replace(' ', '') if isinstance(x, str) else x)

# 计算每一行的产品名称
df['product'] = df.apply(lambda row: get_product(row['contract']), axis=1)

# 将新列插入到最前面
df.insert(0, 'product', df.pop('product'))

# 添加日期列
# 提取文件名（不带目录）
filename = os.path.basename(args.file_name)

day_value = filename.split('_')[0]
if day_value != '':
    df.insert(2, 'day', day_value)

df.to_csv(args.output_file, index=False)


