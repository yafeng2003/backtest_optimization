import os
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from datetime import timedelta
from tqdm import tqdm  
from typing import Dict, List, Any, Optional, Tuple, Union 
from indicator_loader import load_indicators


my_color = mpf.make_marketcolors(
    up='g',
    down='r',
    edge='inherit',
    wick='inherit',
    volume='inherit'
)

my_style = mpf.make_mpf_style(
    marketcolors=my_color,
    figcolor='#D1D3D6',
    gridcolor='#D1D3D6'
)


class BacktestChartGenerator:
    def __init__(
        self,
        strategy_name: str,
        hkt_only: bool = False,
        hkt_output_dir: Optional[str] = None,
    ):
        self.strategy_name = strategy_name
        self.hkt_only = hkt_only
        self.hkt_output_dir = hkt_output_dir

        # 获取项目根目录
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.abspath(os.path.join(current_script_dir, os.pardir))
    
    def clear_chart_dir(self, chart_dir):
        for file_name in os.listdir(chart_dir):
            file_path = os.path.join(chart_dir, file_name)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(f"无法删除文件 {file_path}: {e}")

    def plot_single_trade(
        self,
        stockno,
        buy_date,
        sell_date,
        lk=90,
        rk=30,
        save_path=None
    ):
        df = load_indicators(stockno)
        df = df.copy()

        buy_date = pd.to_datetime(buy_date)
        sell_date = pd.to_datetime(sell_date)
        start_date = buy_date - timedelta(days=lk)
        end_date = sell_date + timedelta(days=rk)

        df_window = df[
            (df["Datetime"] >= start_date) &
            (df["Datetime"] <= end_date)
        ].copy()
        if df_window.empty:
            print(f"{stockno} 在 {buy_date} 到 {sell_date} 内无数据。")
            return
        df_window.set_index("Datetime", inplace=True)

        # 买卖点标记
        addplots = []
        buy_markers = pd.Series(index=df_window.index, dtype=float)
        if buy_date in df_window.index:
            buy_markers.loc[buy_date] = df_window.loc[buy_date, "Low"] * 0.995
            addplots.append(
                mpf.make_addplot(
                    buy_markers,
                    type="scatter",
                    marker="^",
                    markersize=80,
                    color="green"
                )
            )
        sell_markers = pd.Series(index=df_window.index, dtype=float)
        if sell_date in df_window.index:
            sell_markers.loc[sell_date] = df_window.loc[sell_date, "High"] * 1.005
            addplots.append(
                mpf.make_addplot(
                    sell_markers,
                    type="scatter",
                    marker="v",
                    markersize=80,
                    color="red"
                )
            )

        # ===== MACD 指标 =====
        macd_apds = []
        # MACD 线
        macd_apds.append(
            mpf.make_addplot(
                df_window["macd"],
                panel=1,
                color="blue",
                width=1,
                ylabel="MACD"
            )
        )
        # Signal 线
        macd_apds.append(
            mpf.make_addplot(
                df_window["signal"],
                panel=1,
                color="orange",
                width=1
            )
        )
        # Histogram（柱状图，红绿区分）
        hist_colors = [
            "red" if v >= 0 else "green"
            for v in df_window["histogram"].values
        ]
        macd_apds.append(
            mpf.make_addplot(
                df_window["histogram"],
                type="bar",
                panel=1,
                color=hist_colors,
                alpha=0.6
            )
        )

        # # ===== RSI(5) 指标 =====
        # rsi_apds = []
        # # RSI 线
        # rsi_apds.append(
        #     mpf.make_addplot(
        #         df_window["rsi5"],
        #         panel=1,                 # 放在新的 panel（MACD 是 panel=1）
        #         color="purple",
        #         width=1,
        #         ylabel="RSI(5)"
        #     )
        # )
        # # RSI = 30 参考线
        # rsi_apds.append(
        #     mpf.make_addplot(
        #         [30] * len(df_window),
        #         panel=1,
        #         color="gray",
        #         linestyle="--",
        #         width=1
        #     )
        # )
        # # RSI = 70 参考线
        # rsi_apds.append(
        #     mpf.make_addplot(
        #         [70] * len(df_window),
        #         panel=1,
        #         color="gray",
        #         linestyle="--",
        #         width=1
        #     )
        # )

        # ===== 布林带（Bollinger Bands）=====
        bb_apds = []
        # 上轨
        bb_apds.append(
            mpf.make_addplot(
                df_window["upper20"],
                panel=0,
                color="gray",
                width=1
            )
        )
        # 中轨（20日均线）
        bb_apds.append(
            mpf.make_addplot(
                df_window["sma20"],
                panel=0,
                color="blue",
                width=1
            )
        )
        # 下轨
        bb_apds.append(
            mpf.make_addplot(
                df_window["lower20"],
                panel=0,
                color="gray",
                width=1
            )
        )

        # 合并所有的 addplots
        all_addplots = addplots + bb_apds

        # 绘图
        fig, axlist = mpf.plot(
            df_window,
            type="candle",
            style=my_style,
            title=f"{stockno} {buy_date.date()} {sell_date.date()}",
            figsize=(12, 9),
            addplot=all_addplots,
            returnfig=True,
            volume=False,
            # panel_ratios=(2, 1)  # 主图 : 指标 = 3 : 1
        )

        for ax in axlist:
            ax.xaxis.set_major_locator(ticker.MaxNLocator(10))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

        # 保存或显示
        if save_path:
            fig.savefig(save_path, dpi=200, bbox_inches="tight")
            plt.close(fig)
        else:
            plt.show()


    def plot_trade_record(
            self, 
            df_records: Optional[pd.DataFrame] = None,
            records_file: Optional[str] = None,
            lk: int = 90,
            rk: int = 30,
        ):
            if df_records is not None:
                df = df_records
            elif records_file:
                try:
                    df = pd.read_csv(records_file)
                except FileNotFoundError:
                    return
            else:
                return

            if df.empty:
                return

            df["ope_date"] = pd.to_datetime(df["ope_date"])
            df["stockno"] = df["stockno"].astype(str).str.zfill(5)

            # 输出目录处理
            if self.hkt_only:
                base_dir = os.path.join(self.project_root, "backtest_results_hkt", self.strategy_name, "trade_charts")
            else:
                base_dir = os.path.join(self.project_root, "backtest_results", self.strategy_name, "trade_charts")
            if self.hkt_only and self.hkt_output_dir:
                base_dir = os.path.join(self.hkt_output_dir if os.path.isabs(self.hkt_output_dir) else os.path.join(self.project_root, self.hkt_output_dir), "trade_charts")
            os.makedirs(base_dir, exist_ok=True)
            self.clear_chart_dir(base_dir)

            for stockno, df_stock in df.groupby("stockno"):
                df_stock = df_stock.sort_values("ope_date")
                
                buy_date = None
                buy_price = None

                for i, (_, row) in enumerate(df_stock.iterrows()):
                    # 1. 处理买入信号
                    if row["buyorsell"] == 1:
                        buy_date = row["ope_date"]
                        buy_price = row["price"]

                    # 2. 处理卖出信号
                    elif row["buyorsell"] == -1 and buy_date is not None:
                        sell_date = row["ope_date"]
                        sell_price = row["price"]
                        
                        # 计算收益并生成文件名
                        profit = round((sell_price / buy_price - 1) * 100, 2)
                        file_name = f"{profit}%-{stockno}.png"
                        save_path = os.path.join(base_dir, file_name)

                        self._safe_plot(stockno, buy_date, sell_date, lk, rk, save_path)
                        
                        # 重置状态
                        buy_date = None
                        buy_price = None

                # 3. 处理循环结束后仍持仓的情况 (buyorsell=0 或 未出现 -1)
                if buy_date is not None:
                    # 使用该股票记录中的最后一天作为结束日期
                    last_date = df_stock["ope_date"].max()
                    last_price = df_stock.iloc[-1]["price"]
                    
                    profit = round((last_price / buy_price - 1) * 100, 2)
                    file_name = f"HOLDING_{profit}%-{stockno}.png"
                    save_path = os.path.join(base_dir, file_name)
                    
                    self._safe_plot(stockno, buy_date, last_date, lk, rk, save_path)

    def _safe_plot(self, stockno, buy_date, sell_date, lk, rk, save_path):
        """辅助函数：执行绘图并捕获异常"""
        try:
            self.plot_single_trade(
                stockno=stockno,
                buy_date=buy_date,
                sell_date=sell_date,
                lk=lk,
                rk=rk,
                save_path=save_path
            )
        except Exception as e:
            print(f"绘图失败 {stockno} {buy_date} -> {sell_date}: {e}")