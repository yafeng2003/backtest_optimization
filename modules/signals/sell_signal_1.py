from typing import Dict, Any
import pandas as pd
from modules.signals.base_signal import BaseSignal, SignalResult
from indicator_loader import load_indicators_until_date, load_hsi

class SellSignal1(BaseSignal):
    """
    移动止损策略信号类。

    该策略通过追踪自买入后的最高收盘价，当股价从最高点回撤超过指定比例时触发卖出信号。

    Parameters
    ----------
    trailing_stop_rate : float, default 0.08
        回撤触发比例。例如 0.08 表示当股价从最高位下跌 8% 时卖出。
    """

    def __init__(self, trailing_stop_rate: float = 0.08):
        super().__init__()
        self.trailing_stop_rate = trailing_stop_rate

    def check_signal(self, stockno: str, date: str, position_info: Dict[str, Any]) -> SignalResult:
        """
        检查当前日期是否满足移动止损条件。

        Parameters
        ----------
        stockno : str
            股票代码。
        date : str
            当前检测日期 (YYYY-MM-DD)。
        position_info : Dict[str, Any]
            持仓信息，必须包含 'buy_date' 键。

        Returns
        -------
        SignalResult
            包含卖出决策 (sell=1) 和相关信息的信号结果对象。
        """
        sell, info = 0, {}

        # 加载截至当前的行情数据
        stock = load_indicators_until_date(stockno, date)
        if stock.empty:
            return None

        # 获取今日收盘价
        today = stock.iloc[-1]
        close_t = today["Close"]

        # 获取买入以来的价格区间并计算最高价
        buy_date = pd.to_datetime(position_info["buy_date"])
        prices_since_buy = stock.loc[stock["Datetime"]>=buy_date, "Close"]
        highest_price_since_buy = prices_since_buy.max()

        # 计算止损触发线 (移动止损价)
        stop_loss_price = highest_price_since_buy * (1 - self.trailing_stop_rate)

        # 逻辑判断：若今日收盘价低于止损线，则触发卖出
        if close_t < stop_loss_price:
            sell = 1

        return SignalResult(buy=0, sell=sell, info=info)