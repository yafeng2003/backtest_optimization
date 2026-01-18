import numpy as np
import pandas as pd

from modules.signals.base_signal import BaseSignal, SignalResult
from indicator_loader import load_indicators_until_date


class SellSignal2(BaseSignal):
    """
    基于趋势破位和追踪止损的卖出信号策略。

    该信号通过两个维度保护利润并控制风险：
    1. 趋势过滤：当股价跌破指定的长期 EMA 均线时，进行止损/止盈。
    2. 追踪止损：从买入后的最高价下跌超过指定比例时，进行动态止损/止盈。

    Parameters
    ----------
    ema_length : int, default 50
        用于判断趋势支撑的 EMA 周期长度。
    trailing_stop_rate : float, default 0.08
        追踪止损比例（如 0.08 表示从高点回撤 8% 时触发卖出）。
    """

    def __init__(self, ema_length: int = 50, trailing_stop_rate: float = 0.08):
        self.ema_length = ema_length
        self.trailing_stop_rate = trailing_stop_rate

    def check_signal(self, stockno: str, date: str, position_info: dict) -> SignalResult:
        """
        根据当前行情和持仓信息判断是否触发卖出信号。

        算法逻辑：
        - 计算买入日至今的最高收盘价。
        - 计算动态止损位 = 最高价 * (1 - trailing_stop_rate)。
        - 比较当前价格与 EMA 均线以及动态止损位。

        Parameters
        ----------
        stockno : str
            股票代码。
        date : str
            当前查询日期。
        position_info : dict
            持仓信息字典，必须包含 'buy_date' 键，值为买入日期（str 或 datetime）。

        Returns
        -------
        SignalResult
            包含卖解决策（sell=1 或 0）及相关计算数值的信号对象。
        """
        sell, info = 0, {}

        # 1. 加载历史数据
        df = load_indicators_until_date(stockno, date)
        if df is None or len(df) < 20:
            return SignalResult(buy=0, sell=0, info={})

        today = df.iloc[-1]

        # 2. 提取当前行情指标
        close_t = today["Close"]
        ema_column = f"ema{self.ema_length}"
        ema_t = today[ema_column]

        # 3. 计算追踪止损逻辑
        # 确保买入日期格式正确，并筛选出持仓期间的所有价格数据
        buy_date = pd.to_datetime(position_info["buy_date"])
        prices_since_buy = df.loc[df["Datetime"] >= buy_date, "Close"]
        
        # 如果无法获取持仓期数据，则至少取今日价格
        highest_price_since_buy = prices_since_buy.max()

        # 计算止损触发线 (移动止损价)
        stop_loss_price = highest_price_since_buy * (1 - self.trailing_stop_rate)

        # 4. 卖出条件判定
        cond_trend_break = close_t < ema_t              # 条件 A: 跌破均线支撑
        cond_trailing_stop = close_t < stop_loss_price  # 条件 B: 触发动态回撤止损

        if cond_trend_break or cond_trailing_stop:
            sell = 1

        return SignalResult(
            buy=0,
            sell=sell,
            info=info
        )