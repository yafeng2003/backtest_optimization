import pandas as pd

# bollinger:
# 中轨显示当前趋势
# 上升：中轨支撑上轨阻力
# 走平：上轨阻力下轨支撑
# 下跌：中轨阻力下轨支撑
# 趋势强度：开口方向代表方向延续且较强，收口代表进入震荡阶段

def compute_bollinger(df, column, period=20, k=2):
    sma_column = f"sma{period}"
    upper_column = f"upper{period}"
    lower_column = f"lower{period}"

    df[sma_column] = df[column].rolling(window=period).mean()
    std = df[column].rolling(window=period).std(ddof=0)
    
    df[upper_column] = df[sma_column] + k * std
    df[lower_column] = df[sma_column] - k * std

    return df
