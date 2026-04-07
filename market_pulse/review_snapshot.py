# -*- coding: utf-8 -*-
"""
盘面脉搏 — 昨日复盘快照

从 review/daily/{prev_date}/machine.json 加载昨日分组数据，
封装为统一对象供策略层使用，避免到处写深层字典路径。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class StockRef:
    """股票引用（来自复盘数据）。"""
    symbol: str       # 6 位纯数字
    name: str
    pct_chg: float = 0.0
    close: float = 0.0
    board_count: int = 0
    amount_yi: float = 0.0
    prev_board_count: int = 0


@dataclass
class SectorRef:
    """题材/板块引用。"""
    name: str
    stock_count: int = 0
    limit_up_count: int = 0
    max_height: int = 0
    total_amount_yi: float = 0.0
    avg_pct_chg: float = 0.0
    top_stocks: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class ReviewSnapshot:
    """昨日复盘数据快照。"""
    review_date: str = ""

    # 昨日股票分组
    limit_ups: list[StockRef] = field(default_factory=list)
    broken_boards: list[StockRef] = field(default_factory=list)
    board_breakers: list[StockRef] = field(default_factory=list)

    # 连板梯队 {板数: [StockRef]}
    board_ladder: dict[int, list[StockRef]] = field(default_factory=dict)

    # 昨日热门题材
    hot_sectors: list[SectorRef] = field(default_factory=list)

    # 昨日主线/副线题材名称（从 analyst.yaml 获取）
    main_themes: list[str] = field(default_factory=list)
    secondary_themes: list[str] = field(default_factory=list)

    # 昨日涨停股 symbol 集合（快速查找）
    limit_up_symbols: set[str] = field(default_factory=set)
    broken_board_symbols: set[str] = field(default_factory=set)

    # 昨日高标列表（最高板和次高板）
    high_boards: list[StockRef] = field(default_factory=list)

    @property
    def available(self) -> bool:
        return bool(self.review_date)

    def has_limit_ups(self) -> bool:
        return len(self.limit_ups) > 0

    def has_broken_boards(self) -> bool:
        return len(self.broken_boards) > 0

    def has_board_breakers(self) -> bool:
        return len(self.board_breakers) > 0

    def has_board_ladder(self) -> bool:
        return len(self.board_ladder) > 0


def _parse_stock_ref(item: dict[str, Any]) -> StockRef:
    """从 machine.json 的股票条目解析 StockRef。"""
    return StockRef(
        symbol=str(item.get("symbol", "")).zfill(6),
        name=str(item.get("name", "")),
        pct_chg=float(item.get("pct_chg", 0) or 0),
        close=float(item.get("close", 0) or 0),
        board_count=int(item.get("board_count", 0) or 0),
        amount_yi=float(item.get("amount_yi", 0) or 0),
        prev_board_count=int(item.get("prev_board_count", 0) or 0),
    )


def _parse_sector_ref(item: dict[str, Any]) -> SectorRef:
    """从 action_analysis.sectors 解析 SectorRef。"""
    return SectorRef(
        name=str(item.get("name", "")),
        stock_count=int(item.get("stock_count", 0) or 0),
        limit_up_count=int(item.get("limit_up_count", 0) or 0),
        max_height=int(item.get("max_height", 0) or 0),
        total_amount_yi=float(item.get("total_amount_yi", 0) or 0),
        avg_pct_chg=float(item.get("avg_pct_chg", 0) or 0),
        top_stocks=item.get("top_stocks", []),
    )


def _extract_high_boards(board_ladder: dict[int, list[StockRef]]) -> list[StockRef]:
    """从连板梯队中提取高标（最高板 + 次高板）。"""
    if not board_ladder:
        return []
    sorted_heights = sorted(board_ladder.keys(), reverse=True)
    result = []
    for height in sorted_heights[:2]:
        result.extend(board_ladder[height])
    return result


def load_review_snapshot(
    machine_data: dict[str, Any],
    analyst_data: dict[str, Any] | None = None,
    review_date: str = "",
) -> ReviewSnapshot:
    """从 machine.json 和 analyst.yaml 数据加载昨日复盘快照。

    参数:
        machine_data: machine.json 解析后的字典
        analyst_data: analyst.yaml 解析后的字典（可选）
        review_date: 复盘日期
    """
    snap = ReviewSnapshot(review_date=review_date)

    if not machine_data:
        return snap

    # ---- 昨日涨停 ----
    for item in machine_data.get("stocks", {}).get("limit_up", []):
        ref = _parse_stock_ref(item)
        snap.limit_ups.append(ref)
        snap.limit_up_symbols.add(ref.symbol)

    # ---- 昨日炸板 ----
    for item in machine_data.get("stocks", {}).get("broken_board", []):
        ref = _parse_stock_ref(item)
        snap.broken_boards.append(ref)
        snap.broken_board_symbols.add(ref.symbol)

    # ---- 昨日断板 ----
    for item in machine_data.get("stocks", {}).get("board_breakers", []):
        snap.board_breakers.append(_parse_stock_ref(item))

    # ---- 连板梯队 ----
    ladder = machine_data.get("board_stats", {}).get("consecutive_board_ladder", {})
    for height_str, stocks in ladder.items():
        height = int(height_str)
        refs = [_parse_stock_ref(s) for s in stocks]
        snap.board_ladder[height] = refs

    snap.high_boards = _extract_high_boards(snap.board_ladder)

    # ---- 昨日热门题材 ----
    sectors = machine_data.get("themes", {}).get("action_analysis", {}).get("sectors", [])
    for item in sectors:
        snap.hot_sectors.append(_parse_sector_ref(item))

    # ---- 主线/副线题材（从 analyst.yaml）----
    if analyst_data:
        for theme in analyst_data.get("main_themes", []):
            if isinstance(theme, dict):
                name = theme.get("name", "")
                if name:
                    snap.main_themes.append(name)
            elif isinstance(theme, str):
                snap.main_themes.append(theme)

        for theme in analyst_data.get("secondary_themes", []):
            if isinstance(theme, dict):
                name = theme.get("name", "")
                if name:
                    snap.secondary_themes.append(name)
            elif isinstance(theme, str):
                snap.secondary_themes.append(theme)

    logger.info(
        f"[ReviewSnapshot] 加载完成 — 日期: {review_date}, "
        f"涨停: {len(snap.limit_ups)}, 炸板: {len(snap.broken_boards)}, "
        f"断板: {len(snap.board_breakers)}, 连板梯队层数: {len(snap.board_ladder)}, "
        f"热门题材: {len(snap.hot_sectors)}"
    )

    return snap
