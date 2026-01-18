import numpy as np

from modules.signals.base_signal import BaseSignal, SignalResult
from indicator_loader import load_indicators_until_date


class BuySignal3(BaseSignal):
    def __init__(
        self,
        lookback_days: int = 15,
        max_below_sma_ratio: float = 0.8,
    ) -> None:
        self.lookback_days = lookback_days
        self.max_below_sma_ratio = max_below_sma_ratio

    def check_signal(self, stockno: str, date: str) -> SignalResult | None:
        buy_signal = 0
        signal_info = {}

        dataframe = load_indicators_until_date(stockno, date)
        if len(dataframe) < 2:
            return None

        sma_column = "sma20"
        lower_band_column = "lower20"

        current_row = dataframe.iloc[-1]
        previous_row = dataframe.iloc[-2]

        crossed_above_lower_band = (
            current_row["Close"] > current_row[lower_band_column]
            and previous_row["Close"] < previous_row[lower_band_column]
        )

        lookback_window = dataframe.tail(self.lookback_days)
        below_sma_count = 0

        for _, row in lookback_window.iterrows():
            if row["Close"] < row[sma_column]:
                below_sma_count += 1

        below_sma_ratio = below_sma_count / self.lookback_days
        sufficient_closes_above_sma = below_sma_ratio < self.max_below_sma_ratio

        if crossed_above_lower_band and sufficient_closes_above_sma:
            buy_signal = 1

        # if date=="2025-01-10" and stockno=="02333":
        #     print(f"=== BuySignal3 date:{date} stockno:{stockno} ===")
        #     print(f"crossed_above_lower_band:{crossed_above_lower_band},sufficient_closes_above_sma:{sufficient_closes_above_sma}")

        return SignalResult(buy=buy_signal, sell=0, info=signal_info)
