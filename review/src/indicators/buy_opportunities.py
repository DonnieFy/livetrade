"""
买入机会分析模块 — 分析当日最佳买入机会

优先级：
1. 连板涨停股（>=2板）
2. 首板涨停股
3. 日内低吸反转股（低点到收盘涨幅 >= 10%）
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from pathlib import Path

import config
from src.indicators.emotion_cycle import (
    _get_limit_price,
    _filter_st,
    _filter_new_stocks,
)


# ---------------------------------------------------------------------------
# 公开入口
# ---------------------------------------------------------------------------

def compute_buy_opportunities(dc, board_stats: dict, emotion_cycle: dict) -> dict:
    """
    计算买入机会分析。

    Args:
        dc: DataCollector 实例
        board_stats: compute_board_stats() 输出
        emotion_cycle: compute_emotion_cycle() 输出
    """
    target_date = dc.date
    day_data = dc.get_day_klines()

    result = _empty_result(target_date)

    if day_data.empty:
        return result

    day_data = _filter_st(day_data, dc)
    day_data = _filter_new_stocks(day_data, target_date, dc.klines)

    # ── Stage A: 候选股识别 ──
    ladder = emotion_cycle.get("consecutive_board", {}).get("ladder", {})
    limit_up_list = emotion_cycle.get("limit_up_stocks", [])

    consecutive_symbols = _extract_consecutive_symbols(ladder)
    consecutive_info = {
        s["symbol"]: s["board_count"]
        for bucket in ladder.values()
        for s in bucket
    }

    first_board_symbols = _extract_first_board_symbols(limit_up_list, consecutive_symbols)

    # 日内低吸反转候选
    surge_candidates = _find_intraday_surge(day_data, dc)

    # ── Stage B: 加载 tick 数据 ──
    all_candidate_symbols = consecutive_symbols | first_board_symbols
    all_candidate_symbols |= {s["symbol"] for s in surge_candidates}

    tick_map = _load_candidate_ticks(target_date, all_candidate_symbols)
    tick_available = bool(tick_map)

    # ── Stage C: 逐股分析 ──
    for stock in limit_up_list:
        sym = stock["symbol"]
        board_count = consecutive_info.get(sym, 1)
        tick_df = tick_map.get(sym)

        analysis = _analyze_limit_up_stock(
            symbol=sym,
            name=stock["name"],
            board_count=board_count,
            close=stock["close"],
            pct_chg=stock["pct_chg"],
            tick_df=tick_df,
            day_data=day_data,
            dc=dc,
        )

        if board_count >= 2:
            result["consecutive_board"].append(analysis)
        else:
            result["first_board"].append(analysis)

    for s in surge_candidates:
        sym = s["symbol"]
        tick_df = tick_map.get(sym)
        analysis = _analyze_intraday_surge_stock(
            symbol=sym,
            name=s["name"],
            close=s["close"],
            pct_chg=s["pct_chg"],
            low=s["low"],
            tick_df=tick_df,
            day_data=day_data,
            dc=dc,
        )
        result["intraday_surge"].append(analysis)

    # 排序
    result["consecutive_board"].sort(key=lambda x: x["board_count"], reverse=True)
    result["first_board"].sort(key=lambda x: x["pct_chg"], reverse=True)
    result["intraday_surge"].sort(key=lambda x: x["return_metrics"]["low_to_close_pct"], reverse=True)

    result["summary"] = {
        "consecutive_board_count": len(result["consecutive_board"]),
        "first_board_count": len(result["first_board"]),
        "intraday_surge_count": len(result["intraday_surge"]),
        "tick_data_available": tick_available,
    }

    return result


# ---------------------------------------------------------------------------
# 候选股识别
# ---------------------------------------------------------------------------

def _extract_consecutive_symbols(ladder: dict) -> set[str]:
    symbols = set()
    for bucket in ladder.values():
        for s in bucket:
            symbols.add(s["symbol"])
    return symbols


def _extract_first_board_symbols(limit_up_list: list[dict], consecutive: set[str]) -> set[str]:
    return {
        s["symbol"] for s in limit_up_list
        if s["symbol"] not in consecutive
    }


def _find_intraday_surge(day_data: pd.DataFrame, dc) -> list[dict]:
    """筛选非涨停股中 (close - low) / low >= 0.10 的标的"""
    if day_data.empty or "pre_close" not in day_data.columns:
        return []

    # 排除涨停股
    limit_price = _get_limit_price(day_data["symbol"], day_data["pre_close"], "up")
    not_limit = day_data[day_data["close"] < limit_price - 0.011].copy()

    if not_limit.empty:
        return []

    not_limit = not_limit[not_limit["low"] > 0]  # 排除 low=0
    not_limit["low_to_close_pct"] = (
        (not_limit["close"] - not_limit["low"]) / not_limit["low"] * 100
    )

    surge = not_limit[not_limit["low_to_close_pct"] >= 10.0]

    results = []
    for _, row in surge.iterrows():
        sym = str(row["symbol"]).zfill(6)
        results.append({
            "symbol": sym,
            "name": dc.get_stock_name(sym),
            "close": round(float(row["close"]), 2),
            "pct_chg": round(float(row["pct_chg"]), 2),
            "low": round(float(row["low"]), 2),
            "low_to_close_pct": round(float(row["low_to_close_pct"]), 2),
        })

    return sorted(results, key=lambda x: x["low_to_close_pct"], reverse=True)


# ---------------------------------------------------------------------------
# Tick 数据加载
# ---------------------------------------------------------------------------

def _load_candidate_ticks(date: str, candidate_symbols: set[str]) -> dict[str, pd.DataFrame]:
    """
    分块加载 tick 数据，仅保留候选股。

    Returns:
        {symbol_6digit: DataFrame}
    """
    if not candidate_symbols:
        return {}

    tick_path = Path(config.TICKS_DATA_DIR) / date / f"{date}_trading.csv.gz"
    if not tick_path.exists():
        return {}

    # 纯6位代码集合，用于快速过滤
    candidate_pure = {s.zfill(6) for s in candidate_symbols}

    chunks = []
    reader = pd.read_csv(
        tick_path,
        header=None,
        compression="gzip",
        chunksize=500_000,
        dtype={0: str},  # code 列保持字符串
        encoding="utf-8",
    )

    for chunk in reader:
        # chunk[0] 是 code 列，格式如 "sz000001"
        mask = chunk[0].str[2:].isin(candidate_pure)
        if mask.any():
            chunks.append(chunk[mask].copy())

    if not chunks:
        return {}

    filtered = pd.concat(chunks, ignore_index=True)
    filtered.columns = list(config.CSV_COLUMNS)

    for col in config.NUMERIC_COLUMNS:
        if col in filtered.columns:
            filtered[col] = pd.to_numeric(filtered[col], errors="coerce")

    filtered["_pure"] = filtered["code"].str[2:]

    result = {}
    for code, group in filtered.groupby("_pure"):
        result[str(code).zfill(6)] = (
            group.sort_values("time").reset_index(drop=True)
        )

    return result


# ---------------------------------------------------------------------------
# 涨停股分析
# ---------------------------------------------------------------------------

def _analyze_limit_up_stock(
    *,
    symbol: str,
    name: str,
    board_count: int,
    close: float,
    pct_chg: float,
    tick_df: pd.DataFrame | None,
    day_data: pd.DataFrame,
    dc,
) -> dict:
    """分析单只涨停股的买入机会。"""
    if tick_df is None or tick_df.empty:
        return _degraded_limit_up(symbol, name, board_count, close, pct_chg, day_data, dc)

    prev_close = _get_prev_close(symbol, day_data)
    limit_up_price = _calc_limit_up_price(symbol, prev_close)

    limit_up_type = _classify_limit_up_type(tick_df, limit_up_price)
    entry = _analyze_entry(tick_df, limit_up_price, limit_up_type, prev_close)
    seal = _analyze_seal_info(tick_df, limit_up_price)
    volume = _analyze_volume(symbol, tick_df, day_data, dc)
    ret = _calc_return_metrics(tick_df, close, prev_close)

    return {
        "symbol": symbol,
        "name": name,
        "board_count": board_count,
        "close": close,
        "pct_chg": pct_chg,
        "limit_up_type": limit_up_type,
        "entry_analysis": entry,
        "return_metrics": ret,
        "volume_analysis": volume,
        "seal_info": seal,
        "tick_data_available": True,
    }


def _classify_limit_up_type(tick_df: pd.DataFrame, limit_up_price: float) -> str:
    """分类涨停类型：一字板 / T字板 / 换手板 / 打开回封"""
    if tick_df.empty:
        return "unknown"

    open_price = float(tick_df.iloc[0]["open"])
    low_price = float(tick_df["now"].min())

    is_at_limit = tick_df["now"] >= limit_up_price - 0.011
    transitions = int((is_at_limit != is_at_limit.shift()).sum()) - 1

    if open_price >= limit_up_price - 0.011 and low_price >= limit_up_price - 0.011:
        return "一字板"
    elif open_price >= limit_up_price - 0.011 and low_price < limit_up_price - 0.011:
        return "T字板"
    elif transitions >= 4:
        return "打开回封"
    else:
        return "换手板"


def _analyze_entry(
    tick_df: pd.DataFrame,
    limit_up_price: float,
    limit_up_type: str,
    prev_close: float,
) -> dict:
    """分析入场时机。"""
    if limit_up_type == "一字板":
        return {
            "best_entry_price": round(float(tick_df.iloc[0]["open"]), 2),
            "best_entry_time": str(tick_df.iloc[0].get("time", "")),
            "entry_difficulty": "impossible",
            "difficulty_reason": "一字板全天未打开，无实际买入机会",
        }

    if limit_up_type == "T字板":
        # 找到跌破涨停价的窗口
        below = tick_df[tick_df["now"] < limit_up_price - 0.011]
        if below.empty:
            return _unknown_entry(tick_df)

        entry_price = round(float(below["now"].min()), 2)
        entry_row = below.loc[below["now"].idxmin()]
        entry_time = str(entry_row.get("time", ""))
        window_seconds = _estimate_window_seconds(below, tick_df)
        difficulty, reason = _difficulty_from_window(window_seconds, "T字板短暂开板")

        return {
            "best_entry_price": entry_price,
            "best_entry_time": entry_time,
            "entry_difficulty": difficulty,
            "difficulty_reason": reason,
        }

    if limit_up_type == "打开回封":
        # 找最后一次封板前的开板区间
        is_at_limit = tick_df["now"] >= limit_up_price - 0.011
        last_seal_idx = _find_last_seal_start(is_at_limit)
        if last_seal_idx is None or last_seal_idx == 0:
            return _unknown_entry(tick_df)

        # 最后封板前的区间
        pre_seal = tick_df.iloc[:last_seal_idx]
        if pre_seal.empty:
            return _unknown_entry(tick_df)

        entry_price = round(float(pre_seal["now"].min()), 2)
        entry_row = pre_seal.loc[pre_seal["now"].idxmin()]
        entry_time = str(entry_row.get("time", ""))
        window_seconds = _estimate_window_seconds(pre_seal[pre_seal["now"] < limit_up_price - 0.011], tick_df)
        difficulty, reason = _difficulty_from_window(window_seconds, "多次开板后回封")

        return {
            "best_entry_price": entry_price,
            "best_entry_time": entry_time,
            "entry_difficulty": difficulty,
            "difficulty_reason": reason,
        }

    # 换手板：首次触及涨停前的最低点
    first_limit_idx = tick_df[tick_df["now"] >= limit_up_price - 0.011].index.min()
    if pd.isna(first_limit_idx) or first_limit_idx == 0:
        return _unknown_entry(tick_df)

    pre_limit = tick_df.iloc[:first_limit_idx]
    entry_price = round(float(pre_limit["now"].min()), 2)
    entry_row = pre_limit.loc[pre_limit["now"].idxmin()]
    entry_time = str(entry_row.get("time", ""))

    # 估算入场窗口
    near_low = pre_limit[pre_limit["now"] <= entry_price * 1.005]
    window_seconds = _estimate_window_seconds(near_low, tick_df)
    difficulty, reason = _difficulty_from_window(window_seconds, "换手板封板前低点")

    return {
        "best_entry_price": entry_price,
        "best_entry_time": entry_time,
        "entry_difficulty": difficulty,
        "difficulty_reason": reason,
    }


def _analyze_seal_info(tick_df: pd.DataFrame, limit_up_price: float) -> dict:
    """分析封板信息。"""
    is_at_limit = tick_df["now"] >= limit_up_price - 0.011

    at_limit_times = tick_df.loc[is_at_limit, "time"]
    first_seal = str(at_limit_times.iloc[0]) if not at_limit_times.empty else None
    last_seal = str(at_limit_times.iloc[-1]) if not at_limit_times.empty else None

    # 封板时长（分钟）
    seal_duration = 0
    if first_seal and last_seal:
        try:
            t1 = _parse_time(first_seal)
            t2 = _parse_time(last_seal)
            seal_duration = int((t2 - t1).total_seconds() / 60)
        except (ValueError, TypeError):
            pass

    # 开板次数
    transitions = int((is_at_limit != is_at_limit.shift()).sum()) - 1
    breaks = max(0, transitions // 2)

    return {
        "first_seal_time": first_seal,
        "last_seal_time": last_seal,
        "seal_duration_minutes": seal_duration,
        "board_breaks_count": breaks,
    }


def _analyze_volume(symbol: str, tick_df: pd.DataFrame, day_data: pd.DataFrame, dc) -> dict:
    """分析成交量。"""
    amount_yi = 0.0
    if "volume" in tick_df.columns:
        amount_yi = round(float(tick_df["volume"].max()) / 100_000_000, 2)

    # 与前日对比
    prev_ratio = None
    prev_day = dc.get_prev_day_klines()
    if not prev_day.empty and "amount" in prev_day.columns:
        prev_row = prev_day[prev_day["symbol"] == symbol.zfill(6)]
        if not prev_row.empty:
            prev_amount = float(prev_row.iloc[0].get("amount", 0))
            today_amount = float(day_data[day_data["symbol"] == symbol.zfill(6)].iloc[0].get("amount", 0)) if not day_data[day_data["symbol"] == symbol.zfill(6)].empty else 0
            if prev_amount > 0:
                prev_ratio = round(today_amount / prev_amount, 2)

    return {
        "amount_yi": amount_yi,
        "volume_vs_prev_day_ratio": prev_ratio,
        "volume_at_entry_increasing": None,  # 需要更细粒度分析，暂留 None
    }


def _calc_return_metrics(tick_df: pd.DataFrame, close: float, prev_close: float) -> dict:
    """计算收益率指标。"""
    low_price = float(tick_df["now"].min())
    open_price = float(tick_df.iloc[0]["open"])

    low_to_close = round((close - low_price) / low_price * 100, 2) if low_price > 0 else 0
    open_to_close = round((close - open_price) / open_price * 100, 2) if open_price > 0 else 0
    prev_to_close = round((close - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0

    return {
        "low_to_close_pct": low_to_close,
        "open_to_close_pct": open_to_close,
        "prev_close_to_close_pct": prev_to_close,
    }


# ---------------------------------------------------------------------------
# 日内低吸反转分析
# ---------------------------------------------------------------------------

def _analyze_intraday_surge_stock(
    *,
    symbol: str,
    name: str,
    close: float,
    pct_chg: float,
    low: float,
    tick_df: pd.DataFrame | None,
    day_data: pd.DataFrame,
    dc,
) -> dict:
    """分析日内低吸反转股。"""
    prev_close = _get_prev_close(symbol, day_data)

    if tick_df is None or tick_df.empty:
        return {
            "symbol": symbol,
            "name": name,
            "close": close,
            "pct_chg": pct_chg,
            "entry_analysis": {
                "best_entry_price": low,
                "best_entry_time": None,
                "entry_difficulty": "unknown",
                "difficulty_reason": "无tick数据",
            },
            "return_metrics": {
                "low_to_close_pct": round((close - low) / low * 100, 2) if low > 0 else 0,
                "open_to_close_pct": None,
                "prev_close_to_close_pct": pct_chg,
            },
            "volume_analysis": {
                "amount_yi": None,
                "volume_at_entry_increasing": None,
            },
            "tick_data_available": False,
        }

    # 找到 now 序列最低点
    low_idx = tick_df["now"].idxmin()
    low_row = tick_df.loc[low_idx]
    entry_price = round(float(low_row["now"]), 2)
    entry_time = str(low_row.get("time", ""))

    # 入场窗口：价格在最低点附近 ±0.5% 的时间跨度
    near_low = tick_df[tick_df["now"] <= entry_price * 1.005]
    window_seconds = _estimate_window_seconds(near_low, tick_df)
    difficulty, reason = _difficulty_from_window(window_seconds, "日内低吸")

    # 量价分析
    amount_yi = 0.0
    if "volume" in tick_df.columns:
        amount_yi = round(float(tick_df["volume"].max()) / 100_000_000, 2)

    # 入场点附近放量判断
    volume_increasing = None
    if "turnover" in tick_df.columns and len(tick_df) > 20:
        low_pos = tick_df.index.get_loc(low_idx) if low_idx in tick_df.index else 0
        before_start = max(0, low_pos - 20)
        after_end = min(len(tick_df), low_pos + 20)
        vol_before = float(tick_df.iloc[before_start:low_pos]["turnover"].sum()) if low_pos > before_start else 0
        vol_after = float(tick_df.iloc[low_pos:after_end]["turnover"].sum()) if after_end > low_pos else 0
        if vol_before > 0:
            volume_increasing = vol_after > vol_before * 1.2

    return {
        "symbol": symbol,
        "name": name,
        "close": close,
        "pct_chg": pct_chg,
        "entry_analysis": {
            "best_entry_price": entry_price,
            "best_entry_time": entry_time,
            "entry_difficulty": difficulty,
            "difficulty_reason": reason,
        },
        "return_metrics": {
            "low_to_close_pct": round((close - entry_price) / entry_price * 100, 2) if entry_price > 0 else 0,
            "open_to_close_pct": round(
                (close - float(tick_df.iloc[0]["open"])) / float(tick_df.iloc[0]["open"]) * 100, 2
            ) if float(tick_df.iloc[0]["open"]) > 0 else 0,
            "prev_close_to_close_pct": pct_chg,
        },
        "volume_analysis": {
            "amount_yi": amount_yi,
            "volume_at_entry_increasing": volume_increasing,
        },
        "tick_data_available": True,
    }


# ---------------------------------------------------------------------------
# 降级结果（无 tick 数据）
# ---------------------------------------------------------------------------

def _degraded_limit_up(
    symbol: str, name: str, board_count: int,
    close: float, pct_chg: float,
    day_data: pd.DataFrame, dc,
) -> dict:
    """无 tick 数据时的降级输出。"""
    prev_close = _get_prev_close(symbol, day_data)
    low_price = _get_day_low(symbol, day_data)
    open_price = _get_day_open(symbol, day_data)

    return {
        "symbol": symbol,
        "name": name,
        "board_count": board_count,
        "close": close,
        "pct_chg": pct_chg,
        "limit_up_type": "unknown",
        "entry_analysis": {
            "best_entry_price": round(low_price, 2) if low_price else close,
            "best_entry_time": None,
            "entry_difficulty": "unknown",
            "difficulty_reason": "无tick数据，无法分析入场时机",
        },
        "return_metrics": {
            "low_to_close_pct": round((close - low_price) / low_price * 100, 2) if low_price and low_price > 0 else 0,
            "open_to_close_pct": round((close - open_price) / open_price * 100, 2) if open_price and open_price > 0 else 0,
            "prev_close_to_close_pct": pct_chg,
        },
        "volume_analysis": {
            "amount_yi": _get_day_amount_yi(symbol, day_data),
            "volume_vs_prev_day_ratio": None,
            "volume_at_entry_increasing": None,
        },
        "seal_info": {
            "first_seal_time": None,
            "last_seal_time": None,
            "seal_duration_minutes": None,
            "board_breaks_count": None,
        },
        "tick_data_available": False,
    }


# ---------------------------------------------------------------------------
# 辅助函数
# ---------------------------------------------------------------------------

def _empty_result(date: str) -> dict:
    return {
        "date": date,
        "consecutive_board": [],
        "first_board": [],
        "intraday_surge": [],
        "summary": {
            "consecutive_board_count": 0,
            "first_board_count": 0,
            "intraday_surge_count": 0,
            "tick_data_available": False,
        },
    }


def _unknown_entry(tick_df: pd.DataFrame) -> dict:
    return {
        "best_entry_price": round(float(tick_df["now"].min()), 2),
        "best_entry_time": str(tick_df.loc[tick_df["now"].idxmin()].get("time", "")),
        "entry_difficulty": "unknown",
        "difficulty_reason": "无法确定入场时机",
    }


def _calc_limit_up_price(symbol: str, prev_close: float) -> float:
    """计算涨停价（复用 emotion_cycle 的逻辑）。"""
    from decimal import Decimal, ROUND_HALF_UP
    sym = symbol.zfill(6)
    is_gem_star = sym.startswith(("300", "301")) or sym.startswith(("688",))
    is_bse = sym.startswith(("4", "8", "920"))
    if is_bse:
        price = prev_close * 1.3
    elif is_gem_star:
        price = prev_close * 1.2
    else:
        price = prev_close * 1.1
    return float(Decimal(str(price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _get_prev_close(symbol: str, day_data: pd.DataFrame) -> float:
    row = day_data[day_data["symbol"] == symbol.zfill(6)]
    if row.empty or "pre_close" not in row.columns:
        return 0
    return float(row.iloc[0]["pre_close"])


def _get_day_low(symbol: str, day_data: pd.DataFrame) -> float | None:
    row = day_data[day_data["symbol"] == symbol.zfill(6)]
    if row.empty or "low" not in row.columns:
        return None
    return float(row.iloc[0]["low"])


def _get_day_open(symbol: str, day_data: pd.DataFrame) -> float | None:
    row = day_data[day_data["symbol"] == symbol.zfill(6)]
    if row.empty or "open" not in row.columns:
        return None
    return float(row.iloc[0]["open"])


def _get_day_amount_yi(symbol: str, day_data: pd.DataFrame) -> float | None:
    row = day_data[day_data["symbol"] == symbol.zfill(6)]
    if row.empty or "amount" not in row.columns:
        return None
    return round(float(row.iloc[0]["amount"]) / 100_000, 2)


def _find_last_seal_start(is_at_limit: pd.Series) -> int | None:
    """找到最后一次持续封板的起始索引。"""
    if is_at_limit.empty or not is_at_limit.iloc[-1]:
        return None
    # 从末尾往前找，找到第一个 False→True 的跳变点
    for i in range(len(is_at_limit) - 1, 0, -1):
        if is_at_limit.iloc[i] and not is_at_limit.iloc[i - 1]:
            return i
    return 0 if is_at_limit.iloc[0] else None


def _estimate_window_seconds(window_df: pd.DataFrame, full_df: pd.DataFrame) -> float:
    """估算入场窗口时长（秒）。"""
    if window_df.empty or len(window_df) < 2:
        return 0
    times = window_df["time"].dropna()
    if len(times) < 2:
        return 0
    try:
        t_start = _parse_time(str(times.iloc[0]))
        t_end = _parse_time(str(times.iloc[-1]))
        return max(0, (t_end - t_start).total_seconds())
    except (ValueError, TypeError):
        return 0


def _difficulty_from_window(seconds: float, context: str = "") -> tuple[str, str]:
    """根据窗口时长判定入场难度。"""
    if seconds <= 0:
        return "hard", f"{context}，窗口极短"
    elif seconds < 30:
        return "hard", f"{context}，窗口仅{int(seconds)}秒"
    elif seconds < 300:
        return "moderate", f"{context}，窗口约{int(seconds / 60)}分钟"
    else:
        return "easy", f"{context}，窗口约{int(seconds / 60)}分钟，有充分时间"


def _parse_time(time_str: str):
    """解析时间字符串 HH:MM:SS。"""
    from datetime import datetime as dt
    return dt.strptime(time_str[:8], "%H:%M:%S")
