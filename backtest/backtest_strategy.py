from datetime import datetime, timedelta
import os
import pandas as pd
from tqdm import tqdm
import random
from indicator_caculator import calculate_indicators
from backtest.backtest_data_loader import BacktestDataLoader
from typing import Dict, List, Any, Optional, Tuple 

class BacktestStrategy:
    """
    回测策略执行器。

    Attributes
    ----------
    strategy : object
        包含策略逻辑的对象。
    pool_file : str
        股票池文件的路径。
    start_date : str
        回测开始日期 (YYYY-MM-DD)。
    end_date : str
        回测结束日期 (YYYY-MM-DD)。
    max_hold : int
        最大持仓数量。
    min_hold : int
        最小持仓数量。
    candidate_n : int
        每日候选池中的股票数量上限。
    core_n : int
        核心目标的数量。
    holdings : dict
        当前的持仓状态。
    pending_orders : list
        未成交的挂单列表。
    records : list
        所有交易（买入/卖出）和持仓（HOLD）的记录列表。
    daily_returns : list
        每日总资产和收益记录。
    df : pandas.DataFrame
        从 `pool_file` 加载的股票池数据。
    project_root : str
        项目根目录的绝对路径。
    data_loader : BacktestDataLoader
        数据加载器实例。
    """
    def __init__(
        self, 
        strategy: Any, 
        pool_file: str, 
        start_date: str = "2024-01-01", 
        end_date: str = "2024-12-31",
        max_hold: int = 20,
        min_hold: int = 15,
        candidate_n: int = 100,
        core_n: int = 30,
        detail_tables: Optional[List[str]] = None,
        hkt_only: bool = False,
        hkt_pool_file: Optional[str] = None,
        hkt_output_dir: Optional[str] = None
    ):
        """
        初始化回测策略执行器。

        Parameters
        ----------
        strategy : object
            策略对象（必须包含 check_buy/check_sell 方法）。
        pool_file : str
            股票池文件路径。
        start_date : str, optional
            回测开始日期 (YYYY-MM-DD)。
        end_date : str, optional
            回测结束日期 (YYYY-MM-DD)。
        max_hold : int, optional
            最大持仓数量。
        min_hold : int, optional
            最小持仓数量。
        candidate_n : int, optional
            候选池数量。
        core_n : int, optional
            核心目标的数量。
        """
        self.strategy = strategy
        self.pool_file = pool_file
        self.start_date = start_date
        self.end_date = end_date
        self.max_hold = max_hold
        self.min_hold = min_hold
        self.candidate_n = candidate_n
        self.core_n = core_n
        self.detail_tables = detail_tables or []
        # 港股通开关及路径
        self.hkt_only = hkt_only
        self.hkt_pool_file = hkt_pool_file
        self.hkt_output_dir = hkt_output_dir

        # 状态
        self.holdings = {}
        self.pending_orders = []
        self.records = []
        self.detail_records = {}
        self.full_position_candidates = []  # 记录满仓时的所有候选股票

        # 项目根目录（用于解析相对路径）
        current_script_dir = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.abspath(os.path.join(current_script_dir, os.pardir))

        # 读取股票池（支持相对路径）
        if not os.path.isabs(pool_file):
            pool_file_path = os.path.join(self.project_root, pool_file)
        else:
            pool_file_path = pool_file
        self.df = pd.read_csv(pool_file_path, dtype={"stockno": str})

        # 如果启用港股通模式，则读取港股通名单并过滤股票池
        if self.hkt_only:
            # 默认港股通名单文件位于项目根目录
            if not self.hkt_pool_file:
                hkt_file = os.path.join(self.project_root, "港股通股票代码.csv")
            else:
                hkt_file = self.hkt_pool_file if os.path.isabs(self.hkt_pool_file) else os.path.join(self.project_root, self.hkt_pool_file)
            try:
                hkt_df = pd.read_csv(hkt_file, dtype=str)
                # 支持不同列名，优先使用'证券代码'
                if '证券代码' in hkt_df.columns:
                    codes = hkt_df['证券代码'].astype(str).str.zfill(5)
                else:
                    codes = hkt_df.iloc[:, 0].astype(str).str.zfill(5)
                codes_set = set(codes.tolist())
                self.df['stockno'] = self.df['stockno'].astype(str).str.zfill(5)
                self.df = self.df[self.df['stockno'].isin(codes_set)].copy()
                print(f"HKT mode enabled: filtered pool to {len(self.df)} records using {hkt_file}")
            except Exception as e:
                print(f"Failed to load HKT pool file '{hkt_file}': {e}")

        # 实例化组件
        self.data_loader = BacktestDataLoader()

    def get_records(self) -> pd.DataFrame:
        """
        获取所有交易记录。

        Returns
        -------
        pd.DataFrame
            包含所有记录的 DataFrame。如果 self.records 为空，则返回一个空 DataFrame。
        """
        if not self.records:
            return pd.DataFrame()
        
        return pd.DataFrame(self.records)

    def get_config_params(self) -> Dict[str, Any]:
        """
        获取回测策略的配置参数。

        Returns
        -------
        Dict[str, Any]
            包含 start_date, end_date, max_hold, min_hold, candidate_n, core_n 的字典。
        """
        return {
            "strategy_name": self.strategy.__class__.__name__,
            "pool_file": self.pool_file,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "max_hold": self.max_hold,
            "min_hold": self.min_hold,
            "candidate_n": self.candidate_n,
            "core_n": self.core_n,
        }

    def generate_date_list(self) -> List[str]:
        """
        生成回测需要的日期范围列表。

        Returns
        -------
        list of str
            从 `start_date` 到 `end_date` 每天的日期列表 (YYYY-MM-DD)。
        """
        start = datetime.strptime(self.start_date, "%Y-%m-%d")
        end = datetime.strptime(self.end_date, "%Y-%m-%d")
        result = []
        d = start
        while d <= end:
            result.append(d.strftime("%Y-%m-%d"))
            d += timedelta(days=1)
        return result

    def upgrade_records(
        self,
        action: str,
        stockno: str,
        ope_date: str,
        price: float,
        update_time: str,
        buy_signal: Optional[str] = None,
    ):
        """
        更新交易或持仓记录表。

        Parameters
        ----------
        action : {'BUY', 'SELL', 'HOLD'}
            操作类型。
        stockno : str
            股票代码。
        ope_date : str
            操作发生的日期 (YYYY-MM-DD)。
        price : float
            操作时的价格。
        update_time : str
            订单请求的日期 (YYYY-MM-DD)。
        """
        if action == "BUY":
            buyorsell = 1
            target_shareholding = 100
            current_shareholding = 0
        elif action == "SELL":
            buyorsell = -1
            target_shareholding = 0
            current_shareholding = 100
        else:  # 持有
            buyorsell = 0
            target_shareholding = 100
            current_shareholding = 100

        weight = 1 / self.max_hold
        target_weight = weight
        current_weight = weight

        self.records.append({
            "buyorsell": buyorsell,
            "ope_date": ope_date,
            "weight": weight,
            "target_shareholding": target_shareholding,
            "current_shareholding": current_shareholding,
            "target_weight": target_weight,
            "current_weight": current_weight,
            "price": price,
            "stockno": stockno,
            "buy_signal": buy_signal,
            "operation_succ": 1,
            "update_time": update_time,
            "version": f"{self.strategy.__class__.__name__}",
            "gainvol": 0,
            "gaincash": 0
        })

    def update_detail_record(self, table: str, record: Dict[str, Any]):
        """
        保存不同信号的详细记录，每个 table 独立存储。
        """
        if table not in self.detail_records:
            self.detail_records[table] = []  # 新建一个表

        self.detail_records[table].append(record)


    def run(self):
        """
        每日流程：
        1. 处理所有未成交的挂单。
        2. 生成已持仓股票的持仓记录（更新价格）。
        3. 检查所有持仓股票是否满足策略的卖出条件，生成新的卖出挂单。
        4. 在核心候选池中，检查买入机会，生成新的买入挂单，直到达到 `max_hold`。
        5. 如果持仓数量不足 `min_hold`，则在非核心候选池中检查买入机会，生成买入挂单。
        """
        dates = self.generate_date_list()

        for date in tqdm(dates, desc=f"Running {self.strategy.__class__.__name__}"):
            df_date = self.df[self.df["date"] == date].copy()
            # next_date_str = (pd.to_datetime(date) + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
            # df_date = self.df[self.df["date"] == next_date_str].copy()
            candidate_pool = df_date.nsmallest(self.candidate_n, "rank")
            candidate_stocknos = candidate_pool["stockno"].astype(str).str.zfill(5).tolist()

            # 记录昨天持仓，用于计算今日的持仓记录
            yesterday_holdings = self.holdings.copy()

            # 1. 先处理未成交交易
            still_pending = []
            for order in self.pending_orders:
                stockno = order["stockno"]
                action = order["action"]
                info = order["info"]
                trade_date, trade_price = self.data_loader.get_next_trade_day_price(stockno, order["request_date"])
                if trade_date and pd.to_datetime(trade_date) <= pd.to_datetime(date):
                    # 成交
                    if action == "SELL":
                        buy_signal = self.holdings.get(stockno, {}).get("buy_signal")
                    else:
                        buy_signal = info.get("buy_signal")
                    self.upgrade_records(
                        action,
                        stockno,
                        trade_date,
                        trade_price,
                        order["request_date"],
                        buy_signal=buy_signal,
                    )
                    for table in self.detail_tables: # 存入详细信息
                        extra = order["info"].get(table, {})
                        self.update_detail_record(table,{
                            "action": action,
                            "stockno": stockno,
                            "trade_date": trade_date,
                            "trade_price": trade_price,
                            "request_date": order["request_date"],
                            **extra
                        })
                    if action == "BUY":
                        self.holdings[stockno] = {
                            "buy_date": trade_date,
                            "buy_price": trade_price,
                            "buy_signal": info["buy_signal"]
                            # 方便后续扩展添加字段
                        }
                    elif action == "SELL":
                        if stockno in self.holdings:
                            del self.holdings[stockno]
                else:
                    still_pending.append(order)  # 仍未成交
            self.pending_orders = still_pending

            # 2. 生成持仓记录，只针对昨天及之前买入的股票
            for stockno in yesterday_holdings.keys():
                # 如果今天卖出成交了，就不生成持仓记录
                if stockno not in self.holdings:
                    continue
                recent_price = self.data_loader.get_recent_price(stockno, date)
                self.upgrade_records(
                    "HOLD",
                    stockno,
                    date,
                    recent_price,
                    date,
                    buy_signal=self.holdings[stockno].get("buy_signal"),
                )

            # 3. 卖出
            for stockno in self.holdings.keys():
                if any(o["stockno"] == stockno and o["action"] == "SELL" for o in self.pending_orders):
                    continue
                calculate_indicators(stockno)
                result = self.strategy.check_sell(stockno, date, self.holdings[stockno])
                if result and result.sell:
                    self.pending_orders.append({"stockno": stockno, "action": "SELL", "request_date": date, "info": result.info})

            import numpy as np
            # npy_arr = np.load("ggt_codes_formatted.npy")  # dtype='<U5'
            # npy_set = set(npy_arr)  # 转成 set 查询更快
            # 4. 买入
            is_full_position = False  # 标记是否已经满仓
            actually_bought = []  # 记录实际买入的股票
            full_position_recorded = False  # 标记是否已经记录过满仓候选
            
            for stockno in candidate_stocknos[:self.core_n]:
                # if stockno not in npy_set:
                    # continue
                if stockno in self.holdings or any(o["stockno"] == stockno for o in self.pending_orders):
                    continue
                calculate_indicators(stockno)
                result = self.strategy.check_buy(stockno, date)
                if result and result.buy:
                    # 检查是否已经满仓
                    current_positions = len(self.holdings) + sum(1 for o in self.pending_orders if o["action"] == "BUY")
                    if current_positions < self.max_hold:
                        # 还没满仓，正常买入
                        self.pending_orders.append({"stockno": stockno, "action": "BUY", "request_date": date, "info": result.info})
                        actually_bought.append(stockno)
                    else:
                        # 刚达到满仓，第一次记录时需要把之前买入的也加入
                        if not full_position_recorded:
                            is_full_position = True
                            full_position_recorded = True
                            # 先记录所有已经买入的股票
                            for bought_stockno in actually_bought:
                                stock_info = df_date[df_date["stockno"].astype(str).str.zfill(5) == bought_stockno]
                                if not stock_info.empty:
                                    score = stock_info.iloc[0].get("point", None)
                                    rank = stock_info.iloc[0].get("rank", None)
                                    self.full_position_candidates.append({
                                        "date": date,
                                        "stockno": bought_stockno,
                                        "score": score,
                                        "rank": rank,
                                        "actually_bought": True
                                    })
                    
                    # 如果满仓，记录当前及后续所有符合条件的股票
                    if is_full_position:
                        # 从股票池中获取该股票的score和rank
                        stock_info = df_date[df_date["stockno"].astype(str).str.zfill(5) == stockno]
                        if not stock_info.empty:
                            score = stock_info.iloc[0].get("point", None)
                            rank = stock_info.iloc[0].get("rank", None)
                            self.full_position_candidates.append({
                                "date": date,
                                "stockno": stockno,
                                "score": score,
                                "rank": rank,
                                "actually_bought": stockno in actually_bought
                            })

            # 5. 保证最少持仓
            if len(self.holdings) + sum(1 for o in self.pending_orders if o["action"] == "BUY") < self.min_hold:
                for stockno in candidate_stocknos[self.core_n:]:
                    # if stockno not in npy_set:
                        # continue
                    if len(self.holdings) + sum(1 for o in self.pending_orders if o["action"] == "BUY") >= self.min_hold:
                        break
                    if stockno in self.holdings or any(o["stockno"] == stockno for o in self.pending_orders):
                        continue
                    calculate_indicators(stockno)
                    result = self.strategy.check_buy(stockno, date)
                    if result and result.buy:
                        self.pending_orders.append({"stockno": stockno, "action": "BUY", "request_date": date, "info": result.info})

    def save_records(self, output_path: Optional[str] = None):
        """
        保存交易/持仓记录到 CSV 文件。

        Parameters
        ----------
        output_path : str, optional
            完整的输出文件路径。
            如果为 None (默认)，则保存到: 
            {project_root}/backtest_results/{StrategyName}/{StrategyName}_records.csv
        """
        if not self.records:
            print("No records!")
            return
        
        if output_path:
            final_output_path = output_path
        else:
            strategy_name = self.strategy.__class__.__name__
            if self.hkt_only:
                output_dir = os.path.join(self.project_root, "backtest_results_hkt", strategy_name)
            else:
                output_dir = os.path.join(self.project_root, "backtest_results", strategy_name)
            # 如果用户提供了自定义 hkt_output_dir，则覆盖
            if self.hkt_only and self.hkt_output_dir:
                output_dir = self.hkt_output_dir if os.path.isabs(self.hkt_output_dir) else os.path.join(self.project_root, self.hkt_output_dir)
            os.makedirs(output_dir, exist_ok=True)
            final_output_path = os.path.join(output_dir, f"{strategy_name}_records.csv")

        pd.DataFrame(self.records).to_csv(final_output_path, index=False, encoding="utf-8-sig")
        print(f"{strategy_name} records saved to {final_output_path}.")
            
    def save_all_detail_records(self, output_dir: Optional[str] = None):
        """
        保存所有 detail_records 到多个 CSV 文件。

        Parameters
        ----------
        output_dir : str, optional
            完整的输出目录。
            若为 None，则默认保存到:
            {project_root}/backtest_results/{StrategyName}/
        """
        if not self.detail_records:
            return

        if output_dir:
            final_output_dir = output_dir
        else:
            strategy_name = self.strategy.__class__.__name__
            if self.hkt_only:
                final_output_dir = os.path.join(self.project_root, "backtest_results_hkt", strategy_name)
            else:
                final_output_dir = os.path.join(self.project_root, "backtest_results", strategy_name)
            if self.hkt_only and self.hkt_output_dir:
                final_output_dir = self.hkt_output_dir if os.path.isabs(self.hkt_output_dir) else os.path.join(self.project_root, self.hkt_output_dir)

        os.makedirs(final_output_dir, exist_ok=True)

        for table_name, records in self.detail_records.items():
            if not records:
                continue  # 跳过空表

            file_path = os.path.join(final_output_dir, f"{strategy_name}_{table_name}.csv")
            pd.DataFrame(records).to_csv(file_path, index=False, encoding="utf-8-sig")
            print(f"{strategy_name} detail table '{table_name}' saved to {file_path}.")

    def save_full_position_candidates(self, output_path: Optional[str] = None):
        """
        保存满仓时的所有候选股票信息到 CSV 文件。

        Parameters
        ----------
        output_path : str, optional
            完整的输出文件路径。
            如果为 None (默认)，则保存到: 
            {project_root}/backtest_results/{StrategyName}/{StrategyName}_full_position_candidates.csv
        """
        if not self.full_position_candidates:
            print("No full position candidates to save!")
            return
        
        if output_path:
            final_output_path = output_path
        else:
            strategy_name = self.strategy.__class__.__name__
            if self.hkt_only:
                output_dir = os.path.join(self.project_root, "backtest_results_hkt", strategy_name)
            else:
                output_dir = os.path.join(self.project_root, "backtest_results", strategy_name)
            if self.hkt_only and self.hkt_output_dir:
                output_dir = self.hkt_output_dir if os.path.isabs(self.hkt_output_dir) else os.path.join(self.project_root, self.hkt_output_dir)
            os.makedirs(output_dir, exist_ok=True)
            final_output_path = os.path.join(output_dir, f"{strategy_name}_full_position_candidates.csv")

        pd.DataFrame(self.full_position_candidates).to_csv(final_output_path, index=False, encoding="utf-8-sig")
        print(f"{strategy_name} full position candidates saved to {final_output_path}.")
