import pandas as pd
import matplotlib.pyplot as plt

def plot_strategy_vs_hsi(
    strategy_csv,
    hsi_csv,
    start_date,
    end_date
):
    # =====================
    # 1. 读取数据
    # =====================
    strategy = pd.read_csv(strategy_csv)
    hsi = pd.read_csv(hsi_csv)

    # =====================
    # 2. 处理日期
    # =====================
    strategy['date'] = pd.to_datetime(strategy['date'])
    hsi['Datetime'] = pd.to_datetime(hsi['Datetime'])

    strategy.set_index('date', inplace=True)
    hsi.set_index('Datetime', inplace=True)

    # =====================
    # 3. 按日期区间筛选
    # =====================
    strategy = strategy.loc[start_date:end_date]
    hsi = hsi.loc[start_date:end_date]

    # =====================
    # 4. 以 HSI 交易日为基准，不做交集
    # =====================
    data = pd.DataFrame(index=hsi.index)
    data['hsi_close'] = hsi['Close']
    data['strategy_asset'] = strategy['total_asset']

    # =====================
    # 5. 处理策略前期无数据的情况
    # =====================
    first_asset = strategy['total_asset'].iloc[0]
    data['strategy_asset'] = data['strategy_asset'].fillna(method='ffill')
    data['strategy_asset'] = data['strategy_asset'].fillna(first_asset)

    # =====================
    # 6. 计算累计收益（归一化）
    # =====================
    data['strategy_return'] = data['strategy_asset'] / first_asset
    data['hsi_return'] = data['hsi_close'] / data['hsi_close'].iloc[0]

    # =====================
    # 7. 画图
    # =====================
    plt.figure(figsize=(12, 6))
    plt.plot(data.index, data['strategy_return'], label='Strategy', linewidth=2)
    plt.plot(data.index, data['hsi_return'], label='HSI', linewidth=2)

    plt.title(f"Strategy vs HSI Performance\n({start_date} to {end_date})")
    plt.xlabel("Date")
    plt.ylabel("Cumulative Return (Start = 1.0)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()


# =====================
# 示例调用
# =====================
plot_strategy_vs_hsi(
    strategy_csv="./backtest_results/StrategyMarsi/StrategyMarsi_daily_returns.csv",
    hsi_csv="./data/HSI.HK_1d.csv",
    start_date="2022-01-01",
    end_date="2023-12-31"
)
