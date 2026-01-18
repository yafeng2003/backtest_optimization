from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, Any

@dataclass
class StrategyResult:
    """
    策略判定结果的封装数据类。

    Attributes
    ----------
    buy : int
        买入信号：1 表示触发买入，0 表示不触发。
    sell : int
        卖出信号：1 表示触发卖出，0 表示不触发。
    info : Dict[str, Any]
        可选的附加信息字典，用于存储触发信号时的技术指标数值或调试信息。
    """
    buy: int
    sell: int
    info: Dict[str, Any] = field(default_factory=dict)

class BaseStrategy(ABC):
    """
    量化交易策略的抽象基类。
    
    所有具体策略均需继承此类并实现买入与卖出的核心逻辑。
    """

    @abstractmethod
    def check_buy(self, stockno: str, date: str) -> StrategyResult:
        """
        检查指定股票在特定日期是否满足买入条件。

        Parameters
        ----------
        stockno : str
            股票代码（例如 '00001'）。
        date : str
            查询日期（格式通常为 'YYYY-MM-DD'）。

        Returns
        -------
        StrategyResult
            包含买入判定结果及其相关元数据。
        """
        pass

    @abstractmethod
    def check_sell(self, stockno: str, date: str, position_info: Dict[str, Any]) -> StrategyResult:
        """
        检查指定股票在特定日期是否满足卖出条件。

        Parameters
        ----------
        stockno : str
            股票代码（例如 '00001'）。
        date : str
            查询日期（格式通常为 'YYYY-MM-DD'）。
        position_info : Dict[str, Any]
            当前持仓信息字典。

        Returns
        -------
        StrategyResult
            包含卖出判定结果及其相关元数据。
        """
        pass