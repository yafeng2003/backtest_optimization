import numpy as np

from modules.signals.base_signal import BaseSignal, SignalResult
from indicator_loader import load_indicators_until_date


class BuySignal2(BaseSignal):
    """
    基于 MACD 低位金叉和上升 EMA 趋势的买入信号策略。

    该策略的核心逻辑是：
    1. 价格位于上升的 EMA 均线之上。
    2. MACD 在数值较低（绝对值处于历史低分位数）时发生金叉。

    Parameters
    ----------
    macd_history_days : int, default 250
        用于计算 MACD 绝对值分位数的历史回顾天数。
    ema_length : int, default 50
        用于判断长期趋势的指数移动平均线 (EMA) 周期。
    macd_low_quantile : int, default 25
        定义“低位”的百分比分位数（0-100）。
    """

    def __init__(
        self,
        macd_history_days: int = 250,
        ema_length: int = 50,
        macd_low_quantile: int = 25
    ):
        self.macd_history_days = macd_history_days
        self.ema_length = ema_length
        self.macd_low_quantile = macd_low_quantile

    def check_signal(self, stockno: str, date: str) -> SignalResult:
        """
        根据 MACD 和 EMA 条件检测买入信号。

        算法步骤：
        1. 加载历史指标数据。
        2. 计算 MACD 历史绝对值的指定分位数作为“低位”阈值。
        3. 验证今日是否满足 MACD 金叉且处于低位。
        4. 验证今日价格是否在上升的 EMA 之上。

        Parameters
        ----------
        stockno : str
            股票代码。
        date : str
            查询日期，格式为 'YYYY-MM-DD'。

        Returns
        -------
        SignalResult or None
            如果数据不足返回 None；否则返回包含买入决策和调试信息的 SignalResult 对象。
        """
        buy, info = 0, {}

        # 加载截至指定日期的指标数据
        df = load_indicators_until_date(stockno, date)

        # 确保数据量足够计算 EMA 和历史分位数
        if df is None or len(df) < max(self.ema_length, 5):
            return None

        today = df.iloc[-1]
        yesterday = df.iloc[-2]

        ema_column = f"ema{self.ema_length}"
        
        # 提取指标数值
        macd_today = today["macd"]
        signal_today = today["signal"]
        macd_prev = yesterday["macd"]
        signal_prev = yesterday["signal"]

        close_today = today["Close"]
        ema_today = today[ema_column]

        # 1. 计算 MACD 低位阈值（基于历史绝对值的百分位数）
        macd_history = df.tail(self.macd_history_days)
        macd_threshold = np.percentile(
            macd_history["macd"].abs(),
            self.macd_low_quantile
        )

        # 2. 判断低位金叉：MACD > Signal 且昨日 MACD <= Signal，且 MACD 数值处于低位区间
        is_low_macd_golden_cross = (
            (0 < macd_today < macd_threshold)
            and macd_today > signal_today
            and macd_prev <= signal_prev
        )

        # 3. 判断价格是否在上升趋势中：价格高于 EMA 且 EMA 较 5 日前上升
        ema_5_days_ago = df.iloc[-5][ema_column]
        is_price_above_rising_ema = (
            close_today > ema_today
            and ema_today > ema_5_days_ago
        )

        # 综合判断
        if is_low_macd_golden_cross and is_price_above_rising_ema:
            buy = 1

        return SignalResult(buy=buy, sell=0, info=info)