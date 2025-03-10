import pandas as pd
import numpy as np
import re

# 读取数据
signals_df = pd.read_csv('signals.csv', parse_dates=['date'])
main_df = pd.read_csv('main_contracts.csv', parse_dates=['date'])

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
    open_price = open_row['close'].iloc[0]
    open_quantity = int(amount // open_price)

    # 找到平仓日
    open_idx = trading_days.index(open_date)
    close_date = trading_days[open_idx + 10] if open_idx + 10 < len(trading_days) else trading_days[-1]

    positions.append({
        'open_date': open_date,
        'product': product,
        'position': position,
        'open_contract': open_contract,
        'open_price': open_price,
        'quantity': open_quantity,
        'close_date': close_date
    })

# 每日盈亏跟踪
for day in trading_days:
    daily_positions = [pos for pos in positions if pos['open_date'] <= day <= pos['close_date']]

    daily_summary = {}

    for pos in daily_positions:
        day_row = main_df[(main_df['date']==day) & (main_df['contract'].str.startswith(pos['open_contract'][:2]))]
        if day_row.empty:
            continue

        day_contract = day_row['contract'].iloc[0]
        day_settlement = day_row['settlement'].iloc[0]

        direction = 1 if pos['position'] == 'long' else -1
        pnl = direction * (day_settlement - pos['open_price']) * pos['quantity']

        key = (day, pos['product'])

        if key not in daily_summary:
            daily_summary[key] = {'total_pnl':0, 'holding_pnl':0, 'closing_pnl':0, 'long_pnl':0, 'short_pnl':0, 'total_quantity':0}

        daily_summary[key]['total_quantity'] += pos['quantity']

        if day < pos['close_date']:
            daily_summary[key]['holding_pnl'] += pnl
        else:
            daily_summary[key]['closing_pnl'] += pnl

        if pos['position'] == 'long':
            daily_summary[key]['long_pnl'] += pnl
        else:
            daily_summary[key]['short_pnl'] += pnl

        daily_summary[key]['total_pnl'] += pnl

    for (date, product), vals in daily_summary.items():
        daily_pnl_records.append({
            'date': date,
            'product': product,
            'total_profit': vals['total_pnl'],
            'holding_profit': vals['holding_pnl'],
            'closing_profit': vals['closing_pnl'],
            'long_profit': vals['long_pnl'],
            'short_profit': vals['short_pnl'],
            'profit_per_unit': vals['total_pnl'] / vals['total_quantity'] if vals['total_quantity'] else 0
        })

# 汇总数据
daily_pnl_df = pd.DataFrame(daily_pnl_records)
daily_pnl_df.to_csv('daily_pnl_tracking.csv', index=False)

print(daily_pnl_df.head(20))

