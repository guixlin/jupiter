# shfe2csv.py
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
#      dce2csv <input_file_name> <output_file_name>
#

import pandas as pd
import xlrd
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
# 加载工作簿和活动工作表
# 打开 .xls 文件
workbook = xlrd.open_workbook(args.file_name)

# 选择第一个工作表
sheet = workbook.sheet_by_index(0)

# header = sheet.row_values(2)
data = []

# 从第二行开始遍历（跳过表头）
for row in range(1, sheet.nrows):
    data.append(sheet.row_values(row))

# 转换为 DataFrame 进行后续处理
df = pd.DataFrame(data, 
                  columns=['no1','no2','contract','day','pre_close','pre_settlement','open','high','low','close','settlement','change1','change2','volume','amount','open_interest'])

# 删除最后一列
df = df.iloc[:, :-1]

# 删除第一列和第二列
df = df.iloc[:, 2:]

# to csv file
# 计算每一行的产品名称
df['product'] = df.apply(lambda row: get_product(row['contract']), axis=1)

# 将新列插入到最前面
df.insert(0, 'product', df.pop('product'))

df.to_csv(args.output_file, index=False)


# 读取Excel文件，Excel格式如下：
#  SHFE:
#     contract, day, pre_close, pre_settlement, open, high, low, close, settlement, change1, change2, volume, amount, open_interest
#
