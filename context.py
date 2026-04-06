# -*- coding: utf-8 -*-
"""
Livetrade — 策略上下文

双层数据结构：
- StockSnapshot: 每只股票的静态/缓变字段（name, close, open, high, low），仅存一份
- tick_history: 瘦时序 DataFrame（code, now, volume, time, bid/ask），用于滚动计算

三层上下文结构：
- MarketContext: 全局盘面累积状态（所有策略共享，engine 每帧更新）
- StrategyContext: 单策略执行上下文（含 params、candidates、tick_history、私有 state）
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import pandas as pd
from review.runtime import ReviewData

logger = logging.getLogger(__name__)

# ============================================================
# Tick 时序列定义（瘦 DataFrame）
# ============================================================

# 始终保留的时序列
TICK_TS_COLUMNS = [
    "code",           # 股票代码
    "now",            # 当前价
    "volume",         # 成交额
    "buy",            # 买一价
    "sell",           # 卖一价
    "bid1_volume",    # 买一量
    "ask1_volume",    # 卖一量
    "time",           # 时间
]

# 竞价阶段额外保留的列（买卖差额在 bid2/ask2 上）
TICK_TS_AUCTION_EXTRA = [
    "bid1",           # 买一价（竞价阶段等于 buy）
    "bid2_volume", "bid2",
    "ask2_volume", "ask2",
]

# 时序数值列（用于类型转换）
TICK_TS_NUMERIC = ["now", "volume", "buy", "sell", "bid1_volume", "ask1_volume",
                   "bid1", "bid2_volume", "bid2", "ask2_volume", "ask2"]


# ============================================================
# 每股静态/缓变快照（engine 维护，不放入 tick_history）
# ============================================================

@dataclass
class StockSnapshot:
    """每只股票的日内静态/缓变数据（仅存一份）。"""
    code: str = ""
    name: str = ""
    close: float = 0.0      # 昨收盘价（当日不变）
    open: float = 0.0       # 开盘价（第一笔 tick 后不变）
    high: float = 0.0       # 当日最高（running max）
    low: float = 999999.0   # 当日最低（running min）
    turnover: float = 0.0   # 最新成交量（累计，每帧更新）
    pct_chg: float = 0.0    # 最新涨跌幅
    is_limit_up: bool = False
    is_limit_down: bool = False
    limit_up_price: float = 0.0
    limit_down_price: float = 0.0


# ============================================================
# MarketContext
# ============================================================

@dataclass
class MarketContext:
    """全局盘面累积状态（所有策略共享）。"""

    date: str = ""
    review: ReviewData = field(default_factory=lambda: ReviewData(trade_date=""))
    current_time: str = ""
    current_phase: str = ""
    tick_count: int = 0
    total_tick_count: int = 0

    # 全市场实时统计
    market_up_count: int = 0
    market_down_count: int = 0
    market_flat_count: int = 0
    market_limit_up_count: int = 0
    market_limit_down_count: int = 0
    market_total_volume: float = 0.0    # 全市场成交额累计
    market_avg_pct_chg: float = 0.0

    def update_from_snapshots(self, snapshots: dict[str, StockSnapshot],
                              phase: str, tick_time: str) -> None:
        """根据全量股票快照更新全局统计。"""
        self.current_time = tick_time
        self.current_phase = phase
        self.tick_count += 1
        self.total_tick_count += 1

        if not snapshots:
            return

        up = down = flat = limit_up = limit_down = 0
        total_vol = 0.0
        pct_sum = 0.0
        n = 0

        for snap in snapshots.values():
            if snap.close <= 0:
                continue
            n += 1
            pct_sum += snap.pct_chg
            total_vol += snap.volume  # volume 是成交额

            if snap.pct_chg > 0:
                up += 1
            elif snap.pct_chg < 0:
                down += 1
            else:
                flat += 1

            if snap.is_limit_up:
                limit_up += 1
            if snap.is_limit_down:
                limit_down += 1

        self.market_up_count = up
        self.market_down_count = down
        self.market_flat_count = flat
        self.market_limit_up_count = limit_up
        self.market_limit_down_count = limit_down
        self.market_total_volume = total_vol
        self.market_avg_pct_chg = round(pct_sum / n, 4) if n > 0 else 0.0

    def reset_phase(self, phase: str) -> None:
        self.current_phase = phase
        self.tick_count = 0


# ============================================================
# StrategyContext
# ============================================================

@dataclass
class StrategyContext:
    """单策略执行上下文。"""

    market: MarketContext
    review: ReviewData = field(default_factory=lambda: ReviewData(trade_date=""))
    params: dict = field(default_factory=dict)
    candidates: set[str] | None = None

    # 每股静态快照（engine 维护）
    stock_snapshots: dict[str, StockSnapshot] = field(default_factory=dict)

    # 瘦时序历史（仅 TICK_TS_COLUMNS，按需加 auction 列）
    tick_history: pd.DataFrame = field(
        default_factory=lambda: pd.DataFrame(columns=TICK_TS_COLUMNS)
    )

    # 策略私有状态
    state: dict = field(default_factory=dict)

    def filter_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if self.candidates is None or len(self.candidates) == 0:
            return frame
        return frame[frame["code"].isin(self.candidates)]

    def get_snapshot(self, code: str) -> StockSnapshot | None:
        return self.stock_snapshots.get(code)
