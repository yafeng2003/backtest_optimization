import pandas as pd

def compute_ema(df, column, period):
    ema_column = f"ema{period}"
    df[ema_column] = df[column].ewm(span=period, adjust=False).mean()

    return df