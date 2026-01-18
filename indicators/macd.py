import pandas as pd
from .ema import compute_ema

def compute_macd(df, column, fast_period=12, slow_period=26, signal_period=9):

    df = compute_ema(df, column, fast_period)
    df = compute_ema(df, column, slow_period)

    fast_col = f"ema{fast_period}"
    slow_col = f"ema{slow_period}"

    df["macd"] = df[fast_col] - df[slow_col]

    df = compute_ema(df, "macd", signal_period)
    df.rename(columns={f"ema{signal_period}": "signal"}, inplace=True)

    df["histogram"] = df["macd"] - df["signal"]

    return df