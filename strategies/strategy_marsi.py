from typing import Dict, Any, Union
import pandas as pd

from strategies.base_strategy import BaseStrategy, StrategyResult
from modules.signals.buy_signal_1 import BuySignal1
from modules.signals.buy_signal_2 import BuySignal2 
from modules.signals.buy_signal_3 import BuySignal3 
from modules.signals.sell_signal_1 import SellSignal1
from modules.signals.sell_signal_2 import SellSignal2 
from indicator_loader import load_indicators_until_date, load_hsi


class StrategyMarsi(BaseStrategy):
    """
    基于 MACD、RSI 的 Marsi 交易策略。

    Parameters
    ----------
    buy_signal1 : BuySignal1
        RSI 买入信号模块。
    buy_signal2 : BuySignal2
        MACD 买入信号模块（MACD低位金叉+EMA过滤）。
    buy_signal3 : BuySignal3
        布林带 买入信号模块（上穿布林带下轨+不能长期处于下轨附近）。
    sell_signal1 : SellSignal1
        RSI 和 布林带 卖出信号模块。
    sell_signal2 : SellSignal2
        MACD 卖出信号模块（趋势破位+追踪止损）。

    Raises
    ------
    TypeError
        如果传入的信号实例类型不符合规范。
    """

    def __init__(
        self,
        buy_signal1: BuySignal1,
        buy_signal2: BuySignal2,
        buy_signal3: BuySignal3,
        sell_signal1: SellSignal1,
        sell_signal2: SellSignal2
    ):
        # 参数类型检查
        if not isinstance(buy_signal1, BuySignal1):
            raise TypeError("buy_signal1 must be BuySignal1 instance")
        if not isinstance(buy_signal2, BuySignal2):
            raise TypeError("buy_signal2 must be BuySignal2 instance")
        if not isinstance(buy_signal3, BuySignal3):
            raise TypeError("buy_signal3 must be BuySignal3 instance")
        if not isinstance(sell_signal1, SellSignal1):
            raise TypeError("sell_signal1 must be SellSignal1 instance")
        if not isinstance(sell_signal2, SellSignal2):
            raise TypeError("sell_signal2 must be SellSignal2 instance")

        self.buy_signal1 = buy_signal1
        self.buy_signal2 = buy_signal2
        self.buy_signal3 = buy_signal3
        self.sell_signal1 = sell_signal1
        self.sell_signal2 = sell_signal2

    def check_buy(self, stockno: str, date: str) -> StrategyResult:
        buy, info = 0, {}

        hsi = load_hsi()
        close_col = hsi.columns.get_loc("Close")
        ma200_col = hsi.columns.get_loc("MA200")   
        rsi_col = hsi.columns.get_loc("RSI") 

        dt = pd.to_datetime(date)
        pos = hsi["Datetime"].searchsorted(dt, side="right") - 1

        if pos < 0:
            return None
        
        close_price_hsi = hsi.iat[pos, close_col]
        ma200_hsi = hsi.iat[pos, ma200_col]
        rsi_hsi = hsi.iat[pos, rsi_col]
        if pd.isna(ma200_hsi) or pd.isna(rsi_hsi):
            return None

        if close_price_hsi < ma200_hsi:
            res1 = self.buy_signal1.check_signal(stockno, date)
            if res1 and res1.buy:
                buy = 1
                info["buy_signal"] = self.buy_signal1.__class__.__name__
        else:
            res3 = self.buy_signal3.check_signal(stockno, date)
            if res3 and res3.buy:
                buy = 1
                info["buy_signal"] = self.buy_signal3.__class__.__name__

        return StrategyResult(buy=buy, sell=0, info=info)


    def check_sell(
        self,
        stockno: str,
        date: str,
        position_info: Dict[str, Any]
    ) -> StrategyResult:

        sell, info = 0, {}

        if position_info["buy_signal"] == self.buy_signal1.__class__.__name__:
            res1 = self.sell_signal1.check_signal(stockno, date, position_info)
            if res1 and res1.sell:
                sell = 1
        elif position_info["buy_signal"] == self.buy_signal3.__class__.__name__:
            res1 = self.sell_signal1.check_signal(stockno, date, position_info)
            if res1 and res1.sell:
                sell = 1

        return StrategyResult(buy=0, sell=sell, info=info)