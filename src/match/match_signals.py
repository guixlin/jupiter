import pandas as pd
import numpy as np
import re

# 读取数据
signals_df = pd.read_csv('strategy_result.csv', parse_dates=['date'])
main_df = pd.read_csv('all_majors.csv', parse_dates=['date'])

# 提取产品名
main_df['product'] = main_df['contract'].str.extract('([a-zA-Z]+)').iloc[:,0].str.upper()
signals_df['product'] = signals_df['product'].str.upper()

# 交易日历
trading_days = sorted(main_df['date'].unique())

positions = []  # 存放所有仓位信息

# 初始化每日盈亏记录
daily_pnl_records = []

for idx, signal in signals_df.iterrows():
    open_date = signal['date']
    product = signal['product']
    position = signal['position'].lower()
    amount = signal['amount']

    # 获取开仓信息
    open_row = main_df[(main_df['date'] == open_date) & main_df['contract'].str.startswith(product.lower())]
    if open_row.empty:
        continue

    open_contract = open_row['contract'].iloc[0]
    open_settlement = open_row['settlement'].iloc[0]
    open_quantity = int(amount // open_settlement)

    # 找到平仓日
    open_idx = trading_days.index(open_date)
    close_date = trading_days[open_idx + 10] if open_idx + 10 < len(trading_days) else trading_days[-1]

    positions.append({
        'open_date': open_date,
        'product': product,
        'position': position,
        'open_contract': open_contract,
        'open_settlement': open_settlement,
        'quantity': open_quantity,
        'close_date': close_date
    })

# 每日盈亏跟踪
for day in trading_days:
    daily_positions = [pos for pos in positions if pos['open_date'] <= day < pos['close_date']]

    for pos in daily_positions:
        day_row = main_df[(main_df['date']==day) & (main_df['contract'].str.startswith(pos['open_contract'][:2]))]
        if day_row.empty:
            continue

        day_contract = day_row['contract'].iloc[0]
        day_settlement = day_row['settlement'].iloc[0]

        # 每日盈亏
        direction = 1 if pos['position'] == 'long' else -1
        daily_pnl = direction * (day_settlement - pos['open_settlement']) * pos['quantity']

        daily_pnl_records.append({
            'date': day,
            'product': pos['product'],
            'contract': day_contract,
            'position': pos['position'],
            'quantity': pos['quantity'],
            'daily_settlement': day_settlement,
            'daily_pnl': daily_pnl
        })

# 汇总数据
daily_pnl_df = pd.DataFrame(daily_pnl_records)
daily_pnl_df.to_csv('daily_pnl_tracking.csv', index=False)

print(daily_pnl_df.head(20))

