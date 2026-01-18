import pandas as pd

# 区间：>70 超买 <30 超卖
# RSI 背离：
    # 趋势转变：
        # 上升趋势： 股价高点抬升但RSI高点下降
        # 下跌趋势： 股价低点下降到RSI低点上升
    # 趋势延续：
        # 上升趋势： 股价低点抬升但RSI低点却下降
        # 下跌趋势： 股价高点降低但RSI高点上升
def compute_rsi(df, column, period):
    rsi_column = f"rsi{period}"
    delta = df[column].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - 100 / (1 + rs)

    df[rsi_column] = rsi

    
    return df
