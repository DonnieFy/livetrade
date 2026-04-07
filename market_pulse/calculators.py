# -*- coding: utf-8 -*-
"""
盘面脉搏 — 纯计算逻辑

所有函数都是纯函数，便于单元测试。
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any

from .theme_resolver import ThemeResolver, ThemeBucket


@dataclass
class GroupPremiumResult:
    """一组股票的溢价统计结果。"""
    group_name: str
    sample_count: int = 0
    avg_pct: float = 0.0
    median_pct: float = 0.0
    high_open_count: int = 0     # 高开数 (pct > 0)
    flat_open_count: int = 0     # 平开数 (pct == 0)
    low_open_count: int = 0      # 低开数 (pct < 0)
    top3: list[dict[str, Any]] = field(default_factory=list)    # 最强3只
    bottom3: list[dict[str, Any]] = field(default_factory=list) # 最弱3只


def _pct_from_close(close: float, current: float) -> float:
    """计算涨跌幅百分比。"""
    if close <= 0:
        return 0.0
    return round((current / close - 1) * 100, 2)


def _code_from_symbol(symbol: str) -> str:
    """6位 symbol 或已带前缀 code 转为标准带前缀 code。"""
    if symbol and len(symbol) > 2 and symbol[:2] in ("sh", "sz", "bj"):
        return symbol
    pure = symbol.zfill(6)
    if pure.startswith(("4", "8", "92")):
        return f"bj{pure}"
    if pure.startswith(("0", "1", "2", "3")):
        return f"sz{pure}"
    return f"sh{pure}"


def _symbol_from_code(code: str) -> str:
    """带前缀 code 转 6位 symbol。"""
    if code and len(code) > 2 and code[:2] in ("sh", "sz", "bj"):
        return code[2:]
    return code.zfill(6)


def calc_group_premium(
    symbols: list[str],
    snapshots: dict[str, dict[str, Any]],
    group_name: str = "",
) -> GroupPremiumResult:
    """计算一组股票相对昨收的溢价统计。

    参数:
        symbols: 6位 symbol 列表
        snapshots: {code: snapshot_dict} 全市场快照
        group_name: 分组名称

    返回:
        GroupPremiumResult
    """
    result = GroupPremiumResult(group_name=group_name)
    entries = []  # [(symbol, pct_chg, name), ...]

    for sym in symbols:
        code = _code_from_symbol(sym)
        snap = snapshots.get(code)
        if snap is None:
            continue

        close = snap.get("close", 0)
        current = snap.get("pct_chg", 0)  # 已是百分比
        name = snap.get("name", sym)

        # 如果有 pct_chg 直接用，否则从 now / close 算
        if current != 0 or close == 0:
            pct = float(current)
        else:
            now = snap.get("volume", 0)  # 不对，应该用 pct_chg
            pct = float(current)

        result.sample_count += 1
        entries.append((sym, pct, name))

    if not entries:
        return result

    pcts = [e[1] for e in entries]

    result.avg_pct = round(statistics.mean(pcts), 2)
    result.median_pct = round(statistics.median(pcts), 2)

    for pct in pcts:
        if pct > 0:
            result.high_open_count += 1
        elif pct < 0:
            result.low_open_count += 1
        else:
            result.flat_open_count += 1

    # 排序取最强/最弱
    sorted_entries = sorted(entries, key=lambda x: x[1], reverse=True)
    for sym, pct, name in sorted_entries[:3]:
        result.top3.append({"symbol": sym, "name": name, "pct": pct})
    for sym, pct, name in sorted_entries[-3:]:
        result.bottom3.append({"symbol": sym, "name": name, "pct": pct})

    return result


def calc_board_ladder_premium(
    board_ladder: dict[int, list],
    snapshots: dict[str, dict[str, Any]],
) -> dict[int, GroupPremiumResult]:
    """计算连板梯队每层的溢价。

    参数:
        board_ladder: {板数: [StockRef, ...]}
        snapshots: 全市场快照

    返回:
        {板数: GroupPremiumResult}
    """
    results = {}
    for height, stocks in board_ladder.items():
        symbols = [s.symbol for s in stocks]
        results[height] = calc_group_premium(
            symbols, snapshots,
            group_name=f"{height}板",
        )
    return results


def calc_theme_performance(
    sector_names: list[str],
    theme_resolver: ThemeResolver,
    snapshots: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """计算昨日热门题材今日表现。

    参数:
        sector_names: 题材名称列表
        theme_resolver: 题材解析器
        snapshots: 全市场快照

    返回:
        [{"name": str, "red_count": int, "total_count": int, ...}, ...]
    """
    results = []
    for name in sector_names:
        red_count = 0
        total_count = 0
        pct_sum = 0.0
        top_stocks = []

        for code, snap in snapshots.items():
            sym = _symbol_from_code(code)
            themes = theme_resolver.resolve_themes(sym)
            if name not in themes:
                continue

            pct = float(snap.get("pct_chg", 0))
            total_count += 1
            pct_sum += pct
            if pct > 0:
                red_count += 1
            top_stocks.append({"symbol": sym, "name": snap.get("name", ""), "pct": pct})

        if total_count == 0:
            continue

        top_stocks.sort(key=lambda x: x["pct"], reverse=True)
        results.append({
            "name": name,
            "red_count": red_count,
            "total_count": total_count,
            "red_rate": round(red_count / total_count * 100, 1),
            "avg_pct": round(pct_sum / total_count, 2),
            "top3": top_stocks[:3],
        })

    results.sort(key=lambda x: x["avg_pct"], reverse=True)
    return results


def top_n_by_volume(
    snapshots: dict[str, dict[str, Any]],
    n: int = 50,
) -> list[tuple[str, float]]:
    """按成交额排序取 TOP-N。

    返回:
        [(code, volume), ...] 按成交额降序
    """
    entries = []
    for code, snap in snapshots.items():
        vol = float(snap.get("volume", 0))
        if vol > 0:
            entries.append((code, vol))
    entries.sort(key=lambda x: x[1], reverse=True)
    return entries[:n]


def top_n_by_pct_chg(
    snapshots: dict[str, dict[str, Any]],
    n: int = 50,
    ascending: bool = False,
) -> list[tuple[str, float]]:
    """按涨跌幅排序取 TOP-N。

    参数:
        ascending: True=跌幅榜，False=涨幅榜

    返回:
        [(code, pct_chg), ...]
    """
    entries = []
    for code, snap in snapshots.items():
        close = float(snap.get("close", 0))
        if close <= 0:
            continue
        pct = float(snap.get("pct_chg", 0))
        entries.append((code, pct))
    entries.sort(key=lambda x: x[1], reverse=not ascending)
    return entries[:n]


def calc_market_breadth(snapshots: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """计算市场宽度/温度计。

    返回:
        {
            "up": int, "down": int, "flat": int,
            "limit_up": int, "limit_down": int,
            "total_volume": float,
            "total_count": int,
            "red_rate": float,
            "up_down_ratio": float,
            "avg_pct": float,
        }
    """
    up = down = flat = limit_up = limit_down = 0
    total_vol = 0.0
    pct_sum = 0.0
    n = 0

    for snap in snapshots.values():
        close = float(snap.get("close", 0))
        if close <= 0:
            continue
        n += 1
        pct = float(snap.get("pct_chg", 0))
        pct_sum += pct
        total_vol += float(snap.get("volume", 0))

        if pct > 0:
            up += 1
        elif pct < 0:
            down += 1
        else:
            flat += 1

        if snap.get("is_limit_up"):
            limit_up += 1
        if snap.get("is_limit_down"):
            limit_down += 1

    return {
        "up": up,
        "down": down,
        "flat": flat,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "total_volume": round(total_vol, 0),
        "total_count": n,
        "red_rate": round(up / n * 100, 1) if n > 0 else 0,
        "up_down_ratio": round(up / down, 2) if down > 0 else float(up),
        "avg_pct": round(pct_sum / n, 2) if n > 0 else 0,
    }


def calc_open_slippage(
    prev_snapshot: dict[str, dict[str, Any]],
    curr_snapshot: dict[str, dict[str, Any]],
    threshold: float = -2.0,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    """计算竞价 vs 开盘偏离（假高开检测）。

    参数:
        prev_snapshot: T1(09:25) 竞价快照
        curr_snapshot: T2(09:30) 开盘快照
        threshold: 偏离阈值（百分点），低于此值视为假高开
        top_n: 返回前 N 个

    返回:
        [{"symbol": str, "name": str, "auction_pct": float, "open_pct": float, "slippage": float}, ...]
    """
    results = []

    for code, curr in curr_snapshot.items():
        prev = prev_snapshot.get(code)
        if prev is None:
            continue

        prev_close = float(prev.get("close", 0))
        if prev_close <= 0:
            continue

        # 竞价时的涨跌幅（从 T1 快照取 pct_chg）
        auction_pct = float(prev.get("pct_chg", 0))
        # 开盘时的涨跌幅（从 T2 快照取）
        open_pct = float(curr.get("pct_chg", 0))

        # 偏离 = 开盘涨幅 - 竞价涨幅（负值表示回落）
        slippage = round(open_pct - auction_pct, 2)

        # 只关注竞价高开但开盘回落的
        if auction_pct > 1.0 and slippage < threshold:
            results.append({
                "symbol": _symbol_from_code(code),
                "name": curr.get("name", ""),
                "auction_pct": auction_pct,
                "open_pct": open_pct,
                "slippage": slippage,
            })

    results.sort(key=lambda x: x["slippage"])
    return results[:top_n]


def calc_big_face_risk(
    snapshots: dict[str, dict[str, Any]],
    risk_symbols: set[str],
    threshold_pct: float = -5.0,
    top_n: int = 10,
) -> list[dict[str, Any]]:
    """大面股监控：跌幅超过阈值且属于风险池的股票。

    参数:
        snapshots: 全市场快照
        risk_symbols: 风险池 symbol 集合（昨日涨停/炸板/高位股）
        threshold_pct: 跌幅阈值（百分比）
        top_n: 返回前 N 个

    返回:
        [{"symbol": str, "name": str, "pct": float, "tag": str}, ...]
    """
    results = []

    for code, snap in snapshots.items():
        sym = _symbol_from_code(code)
        if sym not in risk_symbols:
            continue

        pct = float(snap.get("pct_chg", 0))
        if pct <= threshold_pct:
            results.append({
                "symbol": sym,
                "name": snap.get("name", ""),
                "pct": pct,
            })

    results.sort(key=lambda x: x["pct"])
    return results[:top_n]


def calc_high_board_reception(
    high_boards: list,
    snapshots: dict[str, dict[str, Any]],
    prev_snapshot: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """高标承接观察。

    参数:
        high_boards: 高标 StockRef 列表
        snapshots: 当前全市场快照
        prev_snapshot: T1 竞价快照（可选，用于对比）

    返回:
        [{"symbol": str, "name": str, "board_count": int,
          "current_pct": float, "auction_pct": float|None, "status": str}, ...]
    """
    results = []

    for ref in high_boards:
        code = _code_from_symbol(ref.symbol)
        snap = snapshots.get(code)
        if snap is None:
            continue

        current_pct = float(snap.get("pct_chg", 0))
        auction_pct = None
        if prev_snapshot:
            prev = prev_snapshot.get(code)
            if prev:
                auction_pct = float(prev.get("pct_chg", 0))

        # 判断状态
        if current_pct > 5:
            status = "加速"
        elif current_pct > 0:
            status = "承接"
        elif current_pct > -3:
            status = "分歧"
        else:
            status = "补跌"

        results.append({
            "symbol": ref.symbol,
            "name": ref.name,
            "board_count": ref.board_count,
            "current_pct": current_pct,
            "auction_pct": auction_pct,
            "status": status,
        })

    return results


def calc_mainline_continuation(
    main_theme_names: list[str],
    theme_resolver: ThemeResolver,
    snapshots: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """主线延续度。

    参数:
        main_theme_names: 主线题材名称列表
        theme_resolver: 题材解析器
        snapshots: 全市场快照

    返回:
        [{"name": str, "red_count": int, "total_count": int,
          "red_rate": float, "limit_up_count": int, "leader": dict}, ...]
    """
    results = []

    for name in main_theme_names:
        red_count = 0
        total_count = 0
        limit_up_count = 0
        leader = None
        leader_pct = -999

        for code, snap in snapshots.items():
            sym = _symbol_from_code(code)
            themes = theme_resolver.resolve_themes(sym)
            if name not in themes:
                continue

            total_count += 1
            pct = float(snap.get("pct_chg", 0))
            if pct > 0:
                red_count += 1
            if snap.get("is_limit_up"):
                limit_up_count += 1
            if pct > leader_pct:
                leader_pct = pct
                leader = {"symbol": sym, "name": snap.get("name", ""), "pct": pct}

        if total_count == 0:
            continue

        results.append({
            "name": name,
            "red_count": red_count,
            "total_count": total_count,
            "red_rate": round(red_count / total_count * 100, 1),
            "limit_up_count": limit_up_count,
            "leader": leader,
        })

    return results
