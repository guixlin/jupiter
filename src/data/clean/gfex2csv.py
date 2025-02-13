# gfex2csv.py
#
# 将Excel文件转换为CSV文件
#
# 输入：
#     1. Excel文件名
#     2. 生成CSV文件名
#
# 输出：
#     1. 生成指定文件名的CSV文件
#
# 使用方法：
#
#      gfex2csv <input_file_name> <output_file_name>
#
# CSV文件格式：
#    交易日期,品种名称,交割月份,合约代码,前结算价,开盘价,最高价,最低价,收盘价,结算价,涨跌,涨跌1,成交量,持仓量,持仓量变化,成交额
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

# 读取交易所名称
parser = argparse.ArgumentParser()
parser.add_argument("file_name", help="input file name")
parser.add_argument("output_file", help="output file name")
args = parser.parse_args()
#

df = pd.read_csv(args.file_name, skiprows=2)

# 转换为 DataFrame 进行后续处理
df.columns=['day', 'product_cn', 'delivery_month','contract','pre_settlement','open','high','low','close','settlement','change1','change2','volume','open_interest', 'oi_change', 'amount']

# 删除最后一列
#df = df.iloc[:, :-1]

# 删除第一列和第二列
# df = df.iloc[:, 2:]
# 删除第二和第三列
df = df.drop(df.columns[1],axis=1)
df = df.drop(df.columns[1],axis=1)

# 将contract列插入到最前面
df.insert(0, 'contract', df.pop('contract'))

# to csv file
# 计算每一行的产品名称
df['product'] = df.apply(lambda row: get_product(row['contract']), axis=1)

# 将新列插入到最前面
df.insert(0, 'product', df.pop('product'))

df.to_csv(args.output_file, index=False)


# 读取Excel文件，Excel格式如下：
#  GFEX:
#     contract, day, pre_close, pre_settlement, open, high, low, close, settlement, change1, change2, volume, amount, open_interest
#
