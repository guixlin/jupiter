# merge_year.py
#
# 以交易所为单位，将交易所一年的行情数据合并到一个文件里面。
#
# 输入：
#    1. 目录名
#
# 输出：
#    1. 以交易所为目录，年份为文件名的csv文件，如: SHFE/2023.csv
#
# 命令:
#
#    merge_year.py <src_dir> <dest_dir>
#

import os
import sys
import pandas as pd
import argparse

def create_dir(dest, exchange):
    # 构造完整目录
    full_path = os.path.join(dest, exchange)

    # 检测目录是否存在，如果不存在即创建目录
    if not os.path.exists(full_path):
        os.makedirs(full_path)

# 读取参数
parser = argparse.ArgumentParser()
parser.add_argument("s_dir", help="source market data dir")
parser.add_argument("exchange", help="exchange's name")
parser.add_argument("d_dir", help="destionation data dir")
args = parser.parse_args()

create_dir(args.d_dir, args.exchange)

if not os.path.exists(args.s_dir):
    print(f"Directory '{args.directory}' does not exist. Exiting the program.")
    sys.exit()

for item in os.listdir(args.s_dir):
    item_path = os.path.join(args.s_dir, item)

    # 检查当前文件是否目录
    if os.path.isfile(item_path):
        # 读取文件到dataframe中

    # 检查子目录文件是否存在
    else:
        for subitem in os.listdir(item_path):
            subitem_path = os.path.join(item_path, subitem)

            # 只有是文件的时候才打开
            if os.path.isfile(subitem_path):
                df = pd.read_csv(subitem_path, skiprows=1)
