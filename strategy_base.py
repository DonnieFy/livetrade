# -*- coding: utf-8 -*-
"""
Livetrade — 策略基类与注册器

所有策略继承 BaseStrategy，并使用 @register_strategy 装饰器自动注册。
策略文件放在 strategies/ 包中，__init__.py 自动发现并导入。
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import pandas as pd

from context import MarketContext, StrategyContext

logger = logging.getLogger(__name__)

# ============================================================
# Alert 数据类
# ============================================================


@dataclass
class Alert:
    """策略触发的提示信号。"""

    code: str               # 股票代码，如 "sh600000"
    name: str               # 股票名称
    strategy_slug: str       # 策略唯一标识
    strategy_name: str       # 策略中文名
    message: str             # 提示信息
    level: str = "info"      # "info" | "warn" | "important"
    time: str = ""           # 触发时间 "HH:MM:SS"

    def format_line(self) -> str:
        """格式化为输出文件中的一行文本。"""
        return f"[{self.strategy_name}] {self.code} {self.name} | {self.message}"


# ============================================================
# 策略注册表
# ============================================================

_STRATEGY_REGISTRY: dict[str, type["BaseStrategy"]] = {}


def register_strategy(cls: type["BaseStrategy"]) -> type["BaseStrategy"]:
    """装饰器：将策略类注册到全局注册表。"""
    if not hasattr(cls, "slug") or not cls.slug:
        raise ValueError(f"策略类 {cls.__name__} 必须定义 slug 属性")
    if cls.slug in _STRATEGY_REGISTRY:
        logger.warning(f"策略 slug '{cls.slug}' 重复注册，覆盖旧策略")
    _STRATEGY_REGISTRY[cls.slug] = cls
    logger.debug(f"注册策略: {cls.slug} ({cls.__name__})")
    return cls


def get_registered_strategies() -> dict[str, type["BaseStrategy"]]:
    """获取所有已注册的策略类。"""
    return dict(_STRATEGY_REGISTRY)


# ============================================================
# 策略基类
# ============================================================


class BaseStrategy(ABC):
    """所有实盘策略的基类。"""

    slug: str = ""              # 唯一标识，如 "trend_breakout"
    name: str = ""              # 中文名，如 "产业趋势突破"
    description: str = ""       # 策略说明

    def prepare(self, ctx: StrategyContext) -> None:
        """启动时初始化（只调用一次）。

        可在此做重型操作：
        - 读取 k-lines 日线数据筛选昨日涨停股
        - 查询 stock-vector-knowledge 筛选板块关联股
        - 预计算基线指标等

        结果存入 ctx.state 供 on_tick 使用。
        """

    @abstractmethod
    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        """每个 tick 帧调用。

        参数:
            frame: 当帧 DataFrame（已按候选股池过滤，含 pct_chg 等衍生列）
            ctx: 策略上下文（包含 market 全局状态、tick_history、params、state）

        返回:
            触发的 Alert 列表（无信号返回空列表）
        """
        ...

    def on_phase_start(self, phase: str, ctx: StrategyContext) -> None:
        """阶段开始回调（可选覆盖）。"""

    def on_phase_end(self, phase: str, ctx: StrategyContext) -> None:
        """阶段结束回调（可选覆盖）。"""

    def __repr__(self) -> str:
        return f"<Strategy:{self.slug} '{self.name}'>"
