import pandas as pd
from .ema import compute_ema

def compute_atr(df, period=14):
    """
    计算 ATR (Average True Range)
    """
    prev_close = df["Close"].shift(1)

    # True Range 的三种情况
    tr1 = df["High"] - df["Low"]
    tr2 = (df["High"] - prev_close).abs()
    tr3 = (df["Low"] - prev_close).abs()

    df["tr"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    df = compute_ema(df, "tr", period)
    df.rename(columns={f"ema{period}": f"atr{period}"}, inplace=True)

    return df
