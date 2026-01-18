from typing import Union

import pandas as pd
import os

# 全局缓存：减少磁盘 I/O 和重复计算，提升性能
_INDICATOR_CACHE = {}
_HSI_CACHE = {}


def _get_file_path(stockno):
    """
    获取指定股票指标文件的绝对路径。

    Parameters
    ----------
    stockno : str or int
        股票代码或编号。

    Returns
    -------
    str
        指标 CSV 文件的完整绝对路径。
    """
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    return os.path.join(root_dir, 'processed_data', f"{stockno}.indicators.csv")


def _get_hsi_path(stockno=None):
    """
    获取恒生指数（HSI）原始数据文件的路径。

    Parameters
    ----------
    stockno : any, optional
        预留参数，暂未使用，默认为 None。

    Returns
    -------
    str
        HSI 价格数据的绝对路径。
    """
    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    return os.path.join(root_dir, 'data', 'HSI.HK_1d.csv')


def load_hsi(rsi_period: int = 14, ma_period: int = 200):
    """
    加载恒生指数数据并集成计算技术指标（RSI 和 MA）。

    该函数执行以下操作：
    1. 检查缓存以避免重复计算。
    2. 加载 CSV 文件并进行时间序列预处理。
    3. 计算相对强弱指数 (RSI)。
    4. 计算 200 日简单移动平均线 (MA200)。
    5. 将结果存入缓存并返回。

    Parameters
    ----------
    rsi_period : int, optional
        RSI 的计算周期，默认为 14。
    ma_period : int, optional
        移动平均线的计算周期，默认为 200。

    Returns
    -------
    pd.DataFrame
        包含原始价格、'RSI' 和 'MA200' 列的 DataFrame。
        索引为 Datetime。

    Raises
    ------
    FileNotFoundError
        如果 HSI 数据文件不存在。
    KeyError
        如果 CSV 文件中缺少 'Close' 或 'Datetime' 列。
    """
    # 构造唯一的缓存键，防止参数变化时读取错误的缓存
    cache_key = f'HSI_RSI{rsi_period}_MA{ma_period}'
    if cache_key in _HSI_CACHE:
        return _HSI_CACHE[cache_key]

    hsi_path = _get_hsi_path()
    if not os.path.exists(hsi_path):
        raise FileNotFoundError(f"HSI 数据文件不存在: {hsi_path}")

    df = pd.read_csv(hsi_path)
    
    # 1. 基础预处理
    if 'Datetime' not in df.columns:
        raise KeyError("数据中缺少 'Datetime' 列")
    
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df = df.sort_values('Datetime').reset_index(drop=True)
    df['_pos'] = df.index  # 记录原始顺序位置

    # 2. 计算 RSI (使用 Wilder's Smoothing 方法)
    if 'Close' not in df.columns:
        raise KeyError("数据中缺少 'Close' 列，无法计算指标")
        
    delta = df['Close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / rsi_period, adjust=False).mean()

    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # 3. 计算 MA200 (简单移动平均)
    df[f'MA{ma_period}'] = df['Close'].rolling(window=ma_period).mean()

    # 4. 设置索引并缓存
    # df.set_index('Datetime', inplace=True)
    _HSI_CACHE[cache_key] = df

    return df


def load_indicators(stockno: str) -> Union[pd.DataFrame, None]:
    """
    从本地 CSV 文件加载特定股票的预处理指标数据并缓存。

    采用延迟加载模式，首次读取后将 DataFrame 存入全局缓存 `_INDICATOR_CACHE`。
    自动将 'Datetime' 列转换为日期时间格式并按升序排列。

    Parameters
    ----------
    stockno : str
        股票代码（如 '00001'），用于检索对应的本地文件名。

    Returns
    -------
    pd.DataFrame or None
        包含技术指标的时间序列数据；若文件缺失、格式错误或为空则返回 None。
    """
    if stockno in _INDICATOR_CACHE:
        return _INDICATOR_CACHE[stockno]

    try:
        file_path = _get_file_path(stockno)
        df = pd.read_csv(file_path)
    except (FileNotFoundError, pd.errors.EmptyDataError) as e:
        # 文件不存在或为空
        return None
    except Exception as e:
        # 其他读取错误
        return None

    # 基本数据处理
    try:
        df["Datetime"] = pd.to_datetime(df["Datetime"])
        df = df.sort_values("Datetime")
    except Exception:
        return None

    _INDICATOR_CACHE[stockno] = df
    return df


def load_indicators_until_date(stockno: str, date: Union[str, pd.Timestamp]) -> Union[pd.DataFrame, None]:
    """
    截取指定截止日期（含）之前的股票指标历史数据。

    常用于回测模拟，确保策略在特定时间点只能访问到历史观测值，防止“未来函数”干扰。

    Parameters
    ----------
    stockno : str
        股票代码。
    date : str or pd.Timestamp
        截止日期，支持标准日期字符串（如 '2023-01-01'）或 Timestamp 对象。

    Returns
    -------
    pd.DataFrame or None
        过滤后的数据副本；若该股票无可用数据则返回 None。
    """
    df = load_indicators(stockno)
    if df is None:
        return None

    date = pd.to_datetime(date)
    return df[df["Datetime"] <= date].copy()