import pandas as pd
from .ema import compute_ema

def compute_ema_50_100_200(df, column):
    df = compute_ema(df, column, 50)
    df = compute_ema(df, column, 100)
    df = compute_ema(df, column, 200)

    return df
