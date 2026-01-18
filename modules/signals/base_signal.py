from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class SignalResult:
    """
    交易信号判定结果的封装类。

    Attributes
    ----------
    buy : int
        买入信号标识：1 表示触发买入，0 表示无信号。
    sell : int
        卖出信号标识：1 表示触发卖出，0 表示无信号。
    info : Dict[str, Any], optional
        可选的扩展信息字典，用于存放该信号的辅助数据（如指标数值、触发原因等）。
    """
    buy: int
    sell: int
    info: Dict[str, Any] = field(default_factory=dict)

class BaseSignal(ABC):
    """
    交易信号生成器的抽象基类。

    所有具体的信号逻辑（如 KDJ、RSI 或自定义因子）均需继承此类并实现信号检查方法。
    """

    @abstractmethod
    def check_signal(self, stockno: str, date: str) -> SignalResult:
        """
        检查指定标的在特定日期的信号触发情况。

        Parameters
        ----------
        stockno : str
            股票代码或标的唯一标识符。
        date : str
            待检查的日期（建议格式为 'YYYY-MM-DD'）。

        Returns
        -------
        SignalResult
            包含买卖触发状态及附加元数据的信号结果对象。
        """
        pass