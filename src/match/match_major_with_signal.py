import pandas as pd
import numpy as np
import re

# 读取数据
signals_df = pd.read_csv('strategy_result.csv', parse_dates=['date'])
main_contracts_df = pd.read_csv('all_majors.csv', parse_dates=['date'])

# 提取产品名函数
def extract_product(contract_code):
    match = re.match(r'([a-zA-Z]+)', contract_code)
    return match.group(1).upper() if match else None

main_contracts_df['product'] = main_contracts_df['contract'].apply(extract_product)
signals_df['product'] = signals_df['product'].str.upper()

# 匹配开仓日合约价格
matched_df = pd.merge(
    signals_df,
    main_contracts_df[['date', 'product', 'contract', 'close']],
    on=['date', 'product'],
    how='left'
).rename(columns={'close': 'open_price', 'contract': 'open_contract'})

# 计算开仓数量（取整）
matched_df['open_quantity'] = np.floor(matched_df['amount'] / matched_df['open_price']).astype(int)

# 确定平仓日期（10天后）
matched_df['close_date'] = matched_df['date'] + pd.Timedelta(days=10)

# 匹配10天后平仓日的收盘价格
close_price_df = main_contracts_df[['date', 'product', 'close']].rename(columns={'date': 'close_date', 'close': 'close_price'})
matched_df = pd.merge(
    matched_df,
    close_price_df,
    on=['close_date', 'product'],
    how='left'
)

# 计算每手盈利
matched_df['profit_per_unit'] = np.where(
    matched_df['position'].str.lower() == 'long',
    matched_df['close_price'] - matched_df['open_price'],  # 多头盈利
    matched_df['open_price'] - matched_df['close_price']   # 空头盈利
)

# 计算总盈利
matched_df['total_profit'] = matched_df['profit_per_unit'] * matched_df['open_quantity']

# 最终结果排序展示清晰
matched_df = matched_df[
    ['date', 'product', 'position', 'open_contract', 'amount', 'open_price',
     'open_quantity', 'close_date', 'close_price', 'profit_per_unit', 'total_profit']
]

# 结果查看
print(matched_df.head(10))

# 保存至csv文件
matched_df.to_csv('matched_signals_with_profit.csv', index=False)

