# backtest/backtest_result_calculator.py

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple, Union 

class BacktestResultCalculator:
    """
    回测结果计算器。
    
    Attributes
    ----------
    strategy_name : str
        策略名称，用于保存文件。
    daily_returns : list of dict
        计算得到的每日资产和收益记录列表。
    """
    
    def __init__(self, strategy_name: str):
        """
        初始化结果计算器。

        Parameters
        ----------
        strategy_name : str
            策略名称，用于保存文件。
        """
        # 项目根目录
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.abspath(os.path.join(current_script_dir, os.pardir))
        self.strategy_name = strategy_name
        self.daily_returns: List[Dict[str, Any]] = []
        # HKT 输出配置
        self.hkt_only = False
        self.hkt_output_dir = None

    def calculate_returns(
        self, 
        df_records: Optional[pd.DataFrame] = None, 
        initial_capital: float = 25_000_000, 
        tax: float = 0.002, 
        records_file: Optional[str] = None
    ):
        """
        根据交易记录计算每日总资产和收益。

        Parameters
        ----------
        df_records : pandas.DataFrame, optional
            直接传入交易/持仓记录 DataFrame，优先级最高。
        initial_capital : float, optional
            初始资金额。默认值: 25,000,000。
        tax : float, optional
            交易税费率（买入和卖出时应用）。默认值: 0.002 (0.2%)。
        records_file : str, optional
            如果提供了此参数，则从指定文件加载记录进行计算（优先级次之）。
        """
        
        if df_records is not None:
            # 优先级 1: 使用传入的 DataFrame
            df = df_records
        elif records_file:
            # 优先级 2: 从文件加载
            try:
                df = pd.read_csv(records_file)
            except FileNotFoundError:
                return
        else:
            self.daily_returns = [] 
            return

        if df.empty:
            self.daily_returns = [] 
            return

        df['ope_date'] = pd.to_datetime(df['ope_date'])
        dates = sorted(df['ope_date'].unique())

        cash = initial_capital
        # stockno -> (shares, last_price)
        holdings: Dict[str, Tuple[int, float]] = {} 
        daily_records: List[Dict[str, Any]] = []

        for date in dates:
            df_day = df[df['ope_date'] == date]
            
            # 1. 估算当前总资产 (用于确定买入量)
            total_asset_estimate = cash + sum(s * p for s, p in holdings.values())

            # 2. 处理当天的交易 (买入/卖出)
            for _, row in df_day.iterrows():
                stock = row["stockno"]
                price = row["price"]
                action = row["buyorsell"]

                if action == 1: # BUY
                    # 按照权重买入
                    weight = row["weight"]
                    trade_shares = int((total_asset_estimate * weight) // price) 
                    # trade_shares = int((initial_capital * weight) // price) 
                    
                    if trade_shares > 0:
                        cost = trade_shares * price * (1 + tax)
                        # 现金校验
                        if cost > cash: 
                            trade_shares = int(cash // (price * (1 + tax))) # 用最大剩余资金买入
                            cost = trade_shares * price * (1 + tax)
                            
                        if trade_shares > 0:
                            cash -= cost
                            prev_shares = holdings[stock][0] if stock in holdings else 0
                            holdings[stock] = (prev_shares + trade_shares, price) 

                elif action == -1: # SELL
                    if stock in holdings:
                        shares, _ = holdings.pop(stock)
                        cash += shares * price * (1 - tax)

            # 3. 处理当天的持仓价格更新 (action == 0, HOLD)
            for _, row in df_day[df_day['buyorsell'] == 0].iterrows():
                stock = row["stockno"]
                price = row["price"] 
                
                if stock in holdings:
                    shares, _ = holdings[stock]
                    holdings[stock] = (shares, price)

            # 4. 每天记录总资产
            total_asset = cash + sum(s * p for s, p in holdings.values())
            daily_records.append({
                "date": date,
                "cash": cash,
                "holdings_value": sum(s * p for s, p in holdings.values()),
                "total_asset": total_asset
            })

        self.daily_returns = daily_records

    def save_returns(self, output_path: Optional[str] = None):
        """
        保存每日收益记录到 CSV 文件。

        Parameters
        ----------
        output_path : str, optional
            完整的输出文件路径。
            如果为 None (默认)，则保存到: 
            {project_root}/backtest_results/{StrategyName}/{StrategyName}_daily_returns.csv
        """
        if not self.daily_returns:
            return

        if output_path:
            final_output_path = output_path
        else:
            if self.hkt_only:
                output_dir = os.path.join(self.project_root, "backtest_results_hkt", self.strategy_name)
            else:
                output_dir = os.path.join(self.project_root, "backtest_results", self.strategy_name)
            if self.hkt_only and self.hkt_output_dir:
                output_dir = self.hkt_output_dir if os.path.isabs(self.hkt_output_dir) else os.path.join(self.project_root, self.hkt_output_dir)
            os.makedirs(output_dir, exist_ok=True)
            final_output_path = os.path.join(output_dir, f"{self.strategy_name}_daily_returns.csv")
            
        pd.DataFrame(self.daily_returns).to_csv(final_output_path, index=False, encoding="utf-8-sig")
        print(f"{self.strategy_name} returns saved to {final_output_path}")

    def get_cumulative_return(self) -> float:
        """
        获取回测的累计收益率（Cumulative Return）。

        Returns
        -------
        float
            累计收益率，例如 0.25 表示 25%。
            若无有效回测结果，返回 0.0。
        """
        if not self.daily_returns or len(self.daily_returns) < 2:
            return 0.0

        initial_asset = self.daily_returns[0]["total_asset"]
        final_asset = self.daily_returns[-1]["total_asset"]

        if initial_asset <= 0:
            return 0.0

        return final_asset / initial_asset - 1
    

    def _add_trade_record(self, trades_list, pos, stock, exit_date, exit_price, tax, is_forced):
        """内部辅助函数：计算单笔盈亏并存入列表"""
        buy_cost_factor = (1 + tax)
        sell_gain_factor = (1 - tax)
        
        profit_rate = (exit_price * sell_gain_factor) / (pos["entry_price"] * buy_cost_factor) - 1
        hold_days = (pd.to_datetime(exit_date) - pd.to_datetime(pos["entry_date"])).days
        
        trades_list.append({
            "stockno": stock,
            "entry_date": pos["entry_date"],
            "entry_price": pos["entry_price"],
            "exit_date": exit_date,
            "exit_price": exit_price,
            "hold_days": hold_days,
            "profit_rate": profit_rate,
            "max_float_rate": (pos["max_price"] / pos["entry_price"]) - 1,
            "max_float_date": pos["max_date"],
            "min_float_rate": (pos["min_price"] / pos["entry_price"]) - 1,
            "min_float_date": pos["min_date"],
            "exit_type": "Forced_At_End" if is_forced else "Normal_Sell"
        })

    def analyze_transactions(self, df_records: pd.DataFrame, tax: float = 0.002, output_path: Optional[str] = None):
        """
        分析交易记录，包括已平仓交易、回测结束时尚未平仓的交易，以及每日持仓数统计。
        """
        if df_records is None or df_records.empty:
            return

        # 预处理：按时间排序
        df = df_records.sort_values(['ope_date', 'stockno']).copy()
        last_date = df['ope_date'].max()
        
        trades = []
        active_positions = {}
        # 用于记录每日持仓数量: {date: count}
        daily_pos_counts = {}

        # 获取所有交易日期，确保统计完整性
        all_dates = sorted(df['ope_date'].unique())

        for date in all_dates:
            df_day = df[df['ope_date'] == date]
            
            # 处理当天的操作
            for _, row in df_day.iterrows():
                stock = row['stockno']
                price = row['price']
                action = row['buyorsell']

                if action == 1:  # BUY
                    active_positions[stock] = {
                        "entry_date": date,
                        "entry_price": price,
                        "max_price": price,
                        "max_date": date,
                        "min_price": price,
                        "min_date": date,
                        "is_closed": False
                    }
                
                elif action == 0:  # HOLD
                    if stock in active_positions:
                        pos = active_positions[stock]
                        if price > pos["max_price"]:
                            pos["max_price"], pos["max_date"] = price, date
                        if price < pos["min_price"]:
                            pos["min_price"], pos["min_date"] = price, date
                
                elif action == -1:  # SELL
                    if stock in active_positions:
                        pos = active_positions.pop(stock)
                        self._add_trade_record(trades, pos, stock, date, price, tax, is_forced=False)

            # --- 关键修改：记录当天交易处理完毕后的持仓总数 ---
            daily_pos_counts[date] = len(active_positions)

        # --- 处理回测结束时仍未平仓的股票 ---
        for stock, pos in active_positions.items():
            final_price = df[df['stockno'] == stock]['price'].iloc[-1]
            self._add_trade_record(trades, pos, stock, last_date, final_price, tax, is_forced=True)

        # 转换为 DataFrame 方便统计
        df_analysis = pd.DataFrame(trades)
        
        # 将持仓数统计转换为 Series
        pos_series = pd.Series(daily_pos_counts)

        # 保存与打印报告
        self._save_and_report(df_analysis, pos_series, output_path)

    def _save_and_report(self, df: pd.DataFrame, pos_series: pd.Series, output_path: Optional[str]):
        """修改后的报告函数，包含持仓数统计"""
        if df.empty: return

        # 1. 保存交易明细 CSV
        if not output_path:
            if self.hkt_only:
                output_dir = os.path.join(self.project_root, "backtest_results_hkt", self.strategy_name)
            else:
                output_dir = os.path.join(self.project_root, "backtest_results", self.strategy_name)
            if self.hkt_only and self.hkt_output_dir:
                output_dir = self.hkt_output_dir if os.path.isabs(self.hkt_output_dir) else os.path.join(self.project_root, self.hkt_output_dir)
            os.makedirs(output_dir, exist_ok=True)
            output_path = os.path.join(output_dir, f"{self.strategy_name}_trade_analysis.csv")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        # 2. 统计计算
        wins = df[df['profit_rate'] > 0]
        losses = df[df['profit_rate'] <= 0]
        
        # 3. 打印详细报告
        print(f"\n### {self.strategy_name} 详细交易统计 ###")
        print(f"| 指标 | 盈利交易 (Win) | 亏损交易 (Loss) | 总体 (Total) |")
        print(f"| :--- | :--- | :--- | :--- |")
        print(f"| 数量 | {len(wins)} | {len(losses)} | {len(df)} |")
        print(f"| 平均收益率 | {wins['profit_rate'].mean():.2%} | {losses['profit_rate'].mean():.2%} | {df['profit_rate'].mean():.2%} |")
        print(f"| 平均持仓天数 | {wins['hold_days'].mean():.1f} | {losses['hold_days'].mean():.1f} | {df['hold_days'].mean():.1f} |")
        
        print(f"\n--- 持仓统计 ---")
        print(f"最大持仓: {pos_series.max()} |平均持仓: {pos_series.mean():.2f} ")

        print(f"\n胜率: {len(wins)/len(df):.2%} | 盈亏比: {abs(wins['profit_rate'].mean()/losses['profit_rate'].mean()) if not losses.empty else 0:.2f}")
        print(f"结果已写入: {output_path}")


    def get_metrics(self, start_date: str = None, end_date: str = None) -> dict:
        """
        计算指定时间区间内的指标：
        总收益、年化收益、最大回撤。
        """

        # ========= 1. 按日期过滤 =========
        records = self.daily_returns

        if start_date:
            records = [r for r in records if r["date"] >= start_date]
        if end_date:
            records = [r for r in records if r["date"] <= end_date]

        # 基础校验
        if not records or len(records) < 2:
            return {
                "total_return": 0.0,
                "annual_return": 0.0,
                "max_drawdown": 0.0,
            }

        # ========= 2. 资产与收益率 =========
        assets = np.array([r["total_asset"] for r in records], dtype=float)
        returns = assets[1:] / assets[:-1] - 1

        initial_asset = assets[0]
        final_asset = assets[-1]

        if initial_asset <= 0:
            return {
                "total_return": 0.0,
                "annual_return": 0.0,
                "max_drawdown": 0.0,
            }

        # ========= 3. 区间总收益 =========
        total_return = final_asset / initial_asset - 1

        # ========= 4. 年化收益 =========
        total_days = len(assets)
        annual_return = (final_asset / initial_asset) ** (365 / total_days) - 1

        # ========= 5. 最大回撤 =========
        running_max = np.maximum.accumulate(assets)
        drawdowns = (running_max - assets) / running_max
        max_drawdown = np.max(drawdowns)

        return {
            "total_return": round(total_return, 4),
            "annual_return": round(annual_return, 4),
            "max_drawdown": round(max_drawdown, 4),
        }
