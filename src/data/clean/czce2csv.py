# czce2csv.py
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
#      czce2csv <input_file_name> <output_file_name>
#
# CZCE daily BAR file format:
#    交易日期 |合约代码|昨结算 |今开盘  |最高价  |最低价  |今收盘  |今结算  |涨跌1  |涨跌2  |成交量(手)|持仓量  |增减量  |成交额(万元)|交割结算价
#

import pandas as pd
import argparse
import re

def get_product(text):
    # 使用正则表达式匹配合约，直到遇到第一个数字
    result = re.match(r'^[^\d]*', text)
    if result:
        return result.group(0)
    return ""

def is_contract(text):
    # 检查字符串是否非空且第一个字符是字母
    return text and text[0].isalpha()

def format_contract(contract, day):
    # 提取年份的第一个数字（例如 '2025' 中的 '2'）
    year_first_digit = day.split('-')[0][0]

    # 使用正则表达式找到字符串中的第一个数字的索引
    match = re.search(r'\d', contract)

    if match:
        # 获取第一个数字的位置
        index = match.start()

        # 在第一个数字前插入年份的第一个数字
        formal_contract = contract[:index] + year_first_digit + contract[index:]
        return formal_contract
    else:
        # 如果没有找到数字，直接返回原始字符串
        return contract

# 读取参数
parser = argparse.ArgumentParser()
parser.add_argument("file_name", help="input file name")
parser.add_argument("output_file", help="output file name")
args = parser.parse_args()

# 加载CSV文件
df = pd.read_csv(args.file_name, sep='|', skiprows=2)

df.columns=['day', 'contract', 'pre_settlement', 'open', 'high', 'low', 'close', 'settlement', 'change1', 'change2', 'volume', 'open_interest', 'delta', 'amount', 'delivery']

# 删除最后一列
df = df.iloc[:, :-1]

# 把合约放在第一列
df.insert(0, 'contract', df.pop('contract'))

# 格式化合约
df['contract'] = df.apply(lambda row: format_contract(row['contract'], row['day']), axis=1)

# 计算每一行的产品名称
df['product'] = df.apply(lambda row: get_product(row['contract']), axis=1)

# 将新列插入到最前面
df.insert(0, 'product', df.pop('product'))

# 去掉dataframe中所有的空格
# 去掉所有的空格
df = df.map(lambda x: x.replace(' ', '') if isinstance(x, str) else x)
df = df.map(lambda x: x.replace('"', '') if isinstance(x, str) else x)

df.to_csv(args.output_file, index=False)


