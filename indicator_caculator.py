import os
import pandas as pd
from tqdm import tqdm 
from indicators.ema_50_100_200 import compute_ema_50_100_200
from indicators.macd import compute_macd
from indicators.rsi import compute_rsi
from indicators.bollinger import compute_bollinger


def calculate_indicators(stockno, indicators="all"):
    """
    计算输入 stockno 对应的指标
    """
    # 输入输出目录
    current_file_path = os.path.abspath(__file__)
    root_dir = os.path.dirname(current_file_path)
    raw_dir = os.path.join(root_dir, "data")
    processed_dir = os.path.join(root_dir, "processed_data")

    # 判断是否存在
    output_path = os.path.join(processed_dir, f"{stockno}.indicators.csv")
    if os.path.exists(output_path):
        return

    # 读取股价数据
    input_csv = os.path.join(raw_dir, f"{stockno}.HK_1d.csv")
    if not os.path.exists(input_csv):
        print(f"股价数据不存在：{input_csv}")
        return
    df = pd.read_csv(input_csv)

    # 指标任务
    tasks = {
        "ema": {
            "func": lambda df: compute_ema_50_100_200(df, column="Close")
        },
        "macd": {
            "func": lambda df: compute_macd(df, column="Close")
        },
        "bollinger": {
            "func": lambda df: compute_bollinger(df, column="Close", period=20, k=2)
        },
    }

    # 筛选指标
    selected_tasks = []
    if indicators == "all":
        selected_tasks = tasks
    else:
        selected_tasks = {k: tasks[k] for k in indicators if k in tasks}

    # 逐个执行
    for name, task in selected_tasks.items():
        df = task["func"](df)
    df.to_csv(output_path, index=False)
