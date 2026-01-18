from strategies.strategy_marsi import StrategyMarsi 
from modules.signals.buy_signal_1 import BuySignal1
from modules.signals.sell_signal_1 import SellSignal1
from modules.signals.buy_signal_2 import BuySignal2
from modules.signals.sell_signal_2 import SellSignal2
from modules.signals.buy_signal_3 import BuySignal3

from backtest.backtest_strategy import BacktestStrategy
from backtest.backtest_result_calculator import BacktestResultCalculator
from backtest.backtest_chart_generator import BacktestChartGenerator


strategy = StrategyMarsi(
    buy_signal1=BuySignal1(),
    sell_signal1=SellSignal1(trailing_stop_rate=0.08),
    buy_signal2=BuySignal2(),
    sell_signal2=SellSignal2(),
    buy_signal3=BuySignal3(lookback_days=15, max_below_sma_ratio=0.8)
)

# 开关：当 `HKT_ONLY` 为 True 时，仅在港股通股票池内回测，并把结果保存到新的路径
# 修改为 False 则保持原有行为
HKT_ONLY = False
HKT_POOL_FILE = "./港股通股票代码.csv"  # 相对于项目根的路径
HKT_OUTPUT_DIR = None  # 可设置为自定义输出目录，例如 "backtest_results_hkt"

bts = BacktestStrategy(
    strategy=strategy, 
    pool_file="./universes/id_ltsmhk_online_300_v3.csv",
    # pool_file="./universes/GWN59_367_test_testtopk_0.1206644144144144_s22.csv",
    start_date="2022-01-01",
    end_date="2026-01-01",
    max_hold=15,          
    min_hold=15,     
    candidate_n=400,            
    core_n=400,          
    detail_tables=[],
    hkt_only=HKT_ONLY,
    hkt_pool_file=HKT_POOL_FILE,
    hkt_output_dir=HKT_OUTPUT_DIR
)
btrc = BacktestResultCalculator(strategy_name=strategy.__class__.__name__)
# 将 HKT 设置传递给结果/图表生成器，确保输出目录一致
btrc.hkt_only = HKT_ONLY
btrc.hkt_output_dir = HKT_OUTPUT_DIR

bts.run()
bts.save_records()
bts.save_full_position_candidates()  # 保存满仓时的候选股票信息
btrc.calculate_returns(bts.get_records())
btrc.save_returns()
cumulative_return = btrc.get_cumulative_return()
print(f"\n{strategy.__class__.__name__} 总收益率:{cumulative_return:.2%} ")
print(btrc.get_metrics())
btrc.analyze_transactions(bts.get_records())

btcg = BacktestChartGenerator(strategy_name=strategy.__class__.__name__, hkt_only=HKT_ONLY, hkt_output_dir=HKT_OUTPUT_DIR)


