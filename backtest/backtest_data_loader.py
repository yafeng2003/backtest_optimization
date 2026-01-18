# backtest/data_loader.py

import pandas as pd
from datetime import datetime
from indicator_loader import load_indicators

class BacktestDataLoader:
    def __init__(self):
        pass 

    def get_stock_data(self, stockno):
        return load_indicators(stockno)

    def get_next_trade_day_price(self, stockno, date):
        """
        找到 stockno 在 date 之后的第一个有价格的交易日
        """
        df = self.get_stock_data(stockno)
        date = pd.to_datetime(date)
        
        df = df[pd.to_datetime(df["Datetime"]) > date]
        df = df.sort_values("Datetime").reset_index(drop=True)
        
        valid_idx = df["Open"].first_valid_index()
        if valid_idx is not None:
            row = df.iloc[valid_idx]
            trade_date = pd.to_datetime(row["Datetime"]).strftime("%Y-%m-%d")
            return trade_date, row["Open"]
            
        return None, None

    def get_recent_price(self, stockno, date):
        """
        找到 stockno 在 date 及之前的最新价格
        """
        df = self.get_stock_data(stockno)
        date = pd.to_datetime(date)
        
        df = df[pd.to_datetime(df["Datetime"]) <= date]
        # 降序
        df = df.sort_values("Datetime", ascending=False).reset_index(drop=True)

        valid_idx = df["Close"].first_valid_index()
        if valid_idx is not None:
            return df.loc[valid_idx, "Close"]
            
        return None