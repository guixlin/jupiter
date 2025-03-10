import pandas as pd
import numpy as np

# 横截面策略完整实现
def cross_section_strategy(data, strength_pct, ref_days, vol_threshold, oi_threshold, trade_amount):
    """
    参数说明：
    data: DataFrame, 必须包含列['product', 'date', 'open', 'high', 'low', 'close', 'volume_index', 'oi_index']
    strength_pct: 强弱百分比，用于确定多空头合约数量
    ref_days: 涨跌参考日期的交易日数
    vol_threshold: 成交量指数阈值
    oi_threshold: 持仓量指数阈值
    trade_amount: 每个主力合约指定的交易金额
    """

    data = data.sort_values(['product', 'date'])

    # 计算参考日期收盘价
    data['ref_close'] = data.groupby('product')['close'].shift(ref_days)

    # 计算涨跌幅
    data['return'] = data['close'] / data['ref_close'] - 1

    data.dropna(subset=['return'], inplace=True)

    result_records = []

    for current_date, daily_data in data.groupby('date'):
        # 筛选符合阈值条件的品种
        filtered = daily_data[(daily_data['volume_index'] >= vol_threshold) &
                              (daily_data['oi_index'] >= oi_threshold)]

        if filtered.empty:
            continue

        # 计算截面内选取的合约数量
        n_contracts = max(int(len(filtered) * strength_pct), 1)

        # 按涨跌幅排序
        sorted_data = filtered.sort_values('return')

        # 多头（涨幅最低）
        longs = sorted_data.head(n=n=n)
        # 空头（涨幅最高）
        shorts = sorted_data.tail(n=n=n)

        # 记录交易明细
        for _, row in longs.iterrows():
            result_records.append({
                'date': current_date,
                'product': row['product'],
                'volume_open_index': row['volume_open_index'],
                'volume_high_index': row['volume_high_index'],
                'volume_low_index': row['volume_low_index'],
                'volume_close_index': row['volume_close_index'],
                'position': 'long',
                'trade_amount': trade_amount
            })

        for _, row in shorts.iterrows():
            result_records.append({
                'date': current_date,
                'product': row['product'],
                'volume_open_index': row['volume_open_index'],
                'volume_high_index': row['volume_high_index'],
                'volume_low_index': row['volume_low_index'],
                'volume_close_index': row['volume_close_index'],
                'position': 'short',
                'trade_amount': trade_amount
            })

    return pd.DataFrame(result_records)

# 示例调用
if __name__ == '__main__':
    # 假设data为数据集
    data = pd.read_csv('futures_index.csv')

    strength_pct = 0.1        # 强弱百分比，10%
    ref_days = 5              # 涨跌参考交易日数
    vol_threshold = 1000      # 成交量指数阈值
    oi_threshold = 500        # 持仓量指数阈值
    trade_amount = 100000     # 指定每个主力合约交易金额

    # 执行策略
    strategy_df = cross_section_strategy(data, strength_pct, ref_days,
                                         vol_threshold, oi_threshold, trade_amount)

    # 输出结果至文件
    strategy_df.to_csv('strategy_results.csv', index=False)

