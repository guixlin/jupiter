import pandas as pd
import numpy as np
import re

# 读取数据
signals_df = pd.read_csv('strategy_result.csv', parse_dates=['date'])
main_contracts_df = pd.read_csv('all_majors.csv', parse_dates=['date'])

# 提取产品名称
def extract_product(contract):
    match = re.match(r'([a-zA-Z]+)', contract)
    return match.group(1).upper() if match else None

main_contracts_df['product'] = main_contracts_df['contract'].apply(extract_product)
signals_df['product'] = signals_df['product'].str.upper()

# 合并开仓数据
open_df = pd.merge(
    signals_df,
    main_contracts_df[['date', 'product', 'contract', 'close', 'settlement']],
    on=['date', 'product'],
    how='left'
).rename(columns={
    'close': 'open_price',
    'settlement': 'open_settlement',
    'contract': 'open_contract'
})

# 计算开仓数量（取整）
open_df['open_quantity'] = (open_df['amount'] / open_df['open_price']).astype(int)

# 交易日历（去重）
trading_days = main_contracts_df['date'].drop_duplicates().sort_values().reset_index(drop=True)

# 获取10个交易日后的平仓日期
def get_close_date(open_date):
    idx = trading_days[trading_days == open_date].index
    if not idx.empty and idx[0] + 10 < len(trading_days):
        return trading_days.iloc[idx[0] + 10]
    else:
        return pd.NaT

open_df['close_date'] = open_df['date'].apply(get_close_date)

# 获取平仓日的结算价
settlement_prices = main_contracts_df[['date', 'product', 'settlement']].rename(columns={'date': 'close_date', 'settlement': 'close_settlement'})
open_df = pd.merge(open_df, settlement_prices, on=['close_date', 'product'], how='left')

# 计算每手盈亏与总盈亏
open_df['profit_per_unit'] = np.where(
    open_df['position'].str.lower() == 'long',
    open_df['close_settlement'] - open_df['open_price'],
    open_df['open_price'] - open_df['close_settlement']
)
open_df['total_profit'] = open_df['profit_per_unit'] * open_df['open_quantity']

# 计算每天持仓盈亏（用每日结算价）
daily_pnl_records = []

for _, row in open_df.iterrows():
    if pd.isna(row['close_date']):
        continue
    
    hold_period = trading_days[
        (trading_days >= row['date']) & (trading_days <= row['close_date'])
    ]
    
    for day in hold_period:
        daily_settlement = main_contracts_df[
            (main_contracts_df['date'] == day) &
            (main_contracts_df['product'] == row['product'])
        ]['settlement'].values
        
        if daily_settlement.size == 0:
            continue
        
        daily_settlement_price = daily_settlement[0]
        
        # 每日盈亏 (结算价 - 开仓价)
        if row['position'].lower() == 'long':
            daily_pnl = (daily_settlement_price - row['open_price']) * row['open_quantity']
        else:
            daily_pnl = (row['open_price'] - daily_settlement_price) * row['open_quantity']
        
        daily_pnl_records.append({
            'open_date': row['date'],
            'date': day,
            'product': row['product'],
            'position': row['position'],
            'open_contract': row['open_contract'],
            'open_price': row['open_price'],
            'open_quantity': row['open_quantity'],
            'daily_settlement': daily_settlement_price,
            'daily_pnl': daily_pnl
        })

daily_pnl_df = pd.DataFrame(daily_pnl_records)

# 保存结果
open_df.to_csv('matched_signals_with_settlement_profit.csv', index=False)
daily_pnl_df.to_csv('daily_position_settlement_pnl.csv', index=False)

# 输出前几行查看
print(open_df.head())
print(daily_pnl_df.head(10))

