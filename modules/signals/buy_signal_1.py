import pandas as pd
import numpy as np

from modules.signals.base_signal import BaseSignal, SignalResult
from indicator_loader import load_indicators_until_date, load_hsi

class BuySignal1(BaseSignal):
    """
    基于 RSI 超卖恢复与价格低位过滤的买入信号。

    该策略结合了指数趋势过滤（HSI < MA200）、RSI 指标从超卖区回升（穿越 30）
    以及个股自身价格处于近期历史低位（分位数判断）的逻辑。

    Parameters
    ----------
    rsi_recovery_lookback : int, default 2
        RSI 从超卖区回升的观察窗口天数。
    rsi_oversold_threshold : float, default 30.0
        RSI 超卖界限。
    price_lookback_days : int, default 90
        计算个股历史价格分位数的追溯天数。
    price_low_quantile : float, default 0.2
        价格低位判断的分位数阈值（0.2 表示处于过去 90 天的最低 20% 区域）。
    """

    def __init__(
        self,
        rsi_recovery_lookback: int = 2,
        rsi_oversold_threshold: float = 30.0,
        price_lookback_days: int = 90,
        price_low_quantile: float = 0.2
    ):
        self.rsi_recovery_lookback = rsi_recovery_lookback
        self.rsi_oversold_threshold = rsi_oversold_threshold
        self.price_lookback_days = price_lookback_days
        self.price_low_quantile = price_low_quantile

    def check_signal(self, stockno: str, date: str) -> SignalResult:
        """
        检查指定日期及股票是否触发买入信号。

        1. 过滤环境：仅在恒生指数 (HSI) 低于 200 日均线时操作。
        2. RSI 修复：检查 HSI 的 RSI 是否在近期从 30 以下回升至 30 以上。
        3. 价格低位：检查个股当前收盘价是否处于近期 X 天的低分位数区间。

        Parameters
        ----------
        stockno : str
            股票代码。
        date : str
            查询日期（格式通常为 'YYYY-MM-DD'）。

        Returns
        -------
        SignalResult
            包含买入信号 (1 为触发, 0 为未触发) 和相关信息的信号结果对象。
        """
        buy, info = 0, {}

        # --- 1. 加载 HSI 数据并获取指标位置 ---
        hsi = load_hsi()
        rsi_col = hsi.columns.get_loc("RSI")
        close_col = hsi.columns.get_loc("Close")
        ma200_col = hsi.columns.get_loc("MA200")    
        dt_col = hsi.columns.get_loc("Datetime")

        dt = pd.to_datetime(date)
        pos = hsi["Datetime"].searchsorted(dt, side="right") - 1

        if pos < 0:
            return None
        
        # --- 2. 市场环境过滤：仅在 HSI 低于 MA200 时继续 (弱势市场反弹逻辑) ---
        close_price_hsi = hsi.iat[pos, close_col]
        ma200_hsi = hsi.iat[pos, ma200_col]
        
        if pd.isna(ma200_hsi) or close_price_hsi > ma200_hsi:
            return None

        # --- 3. RSI 超卖恢复判断 ---
        # 寻找在指定窗口期内，RSI 是否完成了“由下向上穿越阈值”的动作
        rsi_oversold_recovery = False
        start = max(1, pos - self.rsi_recovery_lookback)

        for i in range(start, pos + 1):
            prev_rsi = hsi.iat[i - 1, rsi_col]
            curr_rsi = hsi.iat[i, rsi_col]

            # 穿越逻辑：前一值 < 阈值，当前值 > 阈值
            if (
                prev_rsi < self.rsi_oversold_threshold
                and curr_rsi > self.rsi_oversold_threshold
            ):
                rsi_oversold_recovery = True
                break

        # --- 4. 个股价格低位判断 ---
        if rsi_oversold_recovery:
            # 加载个股历史指标
            stock = load_indicators_until_date(stockno, date)
            if stock.empty or len(stock) < self.price_lookback_days:
                return None

            # 获取近期价格序列并计算分位数阈值
            recent_prices = stock["Close"].tail(self.price_lookback_days)
            current_stock_price = recent_prices.iloc[-1]
            low_price_threshold = recent_prices.quantile(self.price_low_quantile)

            # 如果当前价格低于或等于历史低位阈值，则触发买入
            if current_stock_price <= low_price_threshold:
                buy = 1

        return SignalResult(buy=buy, sell=0, info=info)