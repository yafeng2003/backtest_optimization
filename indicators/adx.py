import pandas as pd
import numpy as np

# adx: 趋势强度
# di-：判断空头
# di+：判断多头
# 判断趋势和区间：
#     adx < 20 为区间
#     adx > 20 可能开始趋势
# 趋势里：
#     涨势开始：di+穿过di-; adx穿过20并大幅上涨;价格涨过布林带中轨
#     跌势开始：di-穿过di+; adx穿过20并大幅上涨;价格跌破布林带中轨
#     趋势放缓或结束：adx停止上涨甚至下跌+布林带分析

def compute_adx(df, column, period):
    high = df['High']
    low = df['Low']
    price = df[column]

    df['H-L'] = high - low
    df['H-Cp'] = abs(high - price.shift(1))
    df['L-Cp'] = abs(low - price.shift(1))
    df['TR'] = df[['H-L', 'H-Cp', 'L-Cp']].max(axis=1)

    df['+DM'] = np.where((high - high.shift(1)) > (low.shift(1) - low),
                         np.maximum(high - high.shift(1), 0), 0)
    df['-DM'] = np.where((low.shift(1) - low) > (high - high.shift(1)),
                         np.maximum(low.shift(1) - low, 0), 0)

    df['TR_smooth'] = df['TR'].rolling(period).sum()
    df['+DM_smooth'] = df['+DM'].rolling(period).sum()
    df['-DM_smooth'] = df['-DM'].rolling(period).sum()

    for i in range(period, len(df)):
        df.at[i, 'TR_smooth'] = df.at[i-1, 'TR_smooth'] - (df.at[i-1, 'TR_smooth'] / period) + df.at[i, 'TR']
        df.at[i, '+DM_smooth'] = df.at[i-1, '+DM_smooth'] - (df.at[i-1, '+DM_smooth'] / period) + df.at[i, '+DM']
        df.at[i, '-DM_smooth'] = df.at[i-1, '-DM_smooth'] - (df.at[i-1, '-DM_smooth'] / period) + df.at[i, '-DM']

    df['ATR'] = df['TR_smooth']
    df['+DI'] = 100 * (df['+DM_smooth'] / df['TR_smooth'])
    df['-DI'] = 100 * (df['-DM_smooth'] / df['TR_smooth'])

    df['DX'] = 100 * abs(df['+DI'] - df['-DI']) / (df['+DI'] + df['-DI'])

    df['ADX'] = np.nan
    df.at[period-1, 'ADX'] = df['DX'].iloc[:period].mean()

    for i in range(period, len(df)):
        df.at[i, 'ADX'] = (df.at[i-1, 'ADX'] * (period - 1) + df.at[i, 'DX']) / period

    df.drop(['H-L', 'H-Cp', 'L-Cp', 'TR', '+DM', '-DM', 'TR_smooth', '+DM_smooth', '-DM_smooth', 'DX'], axis=1, inplace=True)
    return df
