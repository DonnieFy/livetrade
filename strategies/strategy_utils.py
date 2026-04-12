# -*- coding: utf-8 -*-
"""
策略共享工具函数

提取各策略重复的日线加载、涨跌停计算等逻辑，统一维护。
"""

from __future__ import annotations

import logging
import os

import numpy as np
import pandas as pd

import config

logger = logging.getLogger(__name__)


# ============================================================
# 日线数据加载
# ============================================================


def load_klines() -> pd.DataFrame | None:
    """加载全市场日线数据（klines_daily.csv.gz）。"""
    path = config.KLINES_DAILY_FILE
    if not os.path.exists(path):
        logger.warning(f"日线数据文件不存在: {path}")
        return None
    try:
        df = pd.read_csv(path, compression="gzip", dtype={"symbol": str})
        df["date"] = df["date"].astype(str)
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        return df
    except Exception as e:
        logger.error(f"加载日线数据失败: {e}")
        return None


def get_sorted_dates(klines: pd.DataFrame) -> list[str]:
    """获取排序后的交易日列表。"""
    return sorted(klines["date"].unique())


def get_prev_dates(sorted_dates: list[str], date: str, *, strict: bool = True) -> list[str]:
    """获取 date 之前的交易日列表。"""
    if strict:
        return [d for d in sorted_dates if d < date]
    return [d for d in sorted_dates if d <= date]


# ============================================================
# 涨跌停计算
# ============================================================


def calc_limit_ratio(symbol: str) -> float:
    """根据股票代码返回涨跌停比例。"""
    s = str(symbol).zfill(6)
    if s.startswith(("300", "301", "688")):
        return 0.20
    return 0.10


def calc_limit_up_price(pre_close: float, symbol: str) -> float:
    """计算涨停价。"""
    return round(pre_close * (1 + calc_limit_ratio(symbol)), 2)


def calc_limit_down_price(pre_close: float, symbol: str) -> float:
    """计算跌停价。"""
    return round(pre_close * (1 - calc_limit_ratio(symbol)), 2)


def is_limit_up(close: float, pre_close: float, symbol: str) -> bool:
    """判断是否涨停。"""
    return close >= calc_limit_up_price(pre_close, symbol)


def is_limit_down(close: float, pre_close: float, symbol: str) -> bool:
    """判断是否跌停。"""
    return close <= calc_limit_down_price(pre_close, symbol)


def is_one_word_limit_up(
    open_: float, high: float, low: float, close: float,
    limit_up_price: float, tol: float = 0.015,
) -> bool:
    """判断是否一字涨停（开盘=最高=最低=收盘≈涨停价）。"""
    return (
        abs(close - limit_up_price) < tol
        and abs(high - low) < tol
        and abs(open_ - close) < tol
    )


# ============================================================
# 个股数据提取
# ============================================================


def get_stock_klines(
    klines: pd.DataFrame,
    symbol: str,
    end_date: str,
    lookback: int,
) -> pd.DataFrame:
    """获取单只股票 end_date 之前最近 lookback 天的日线数据。"""
    mask = (klines["symbol"] == symbol) & (klines["date"] <= end_date)
    subset = klines[mask].sort_values("date").tail(lookback).copy()
    return subset


def calc_moving_averages(closes: np.ndarray, periods: list[int]) -> dict[int, float]:
    """从收盘价序列计算移动平均线。"""
    result = {}
    for p in periods:
        if len(closes) >= p:
            result[p] = float(closes[-p:].mean())
    return result


# ============================================================
# 形态检测
# ============================================================


def detect_rally(
    closes: np.ndarray,
    min_gain_pct: float = 80.0,
    max_days: int = 15,
    lookback: int = 60,
) -> dict | None:
    """在收盘价序列末尾搜索大幅拉升区间。

    返回 {"start", "end", "gain_pct"} 或 None。
    start/end 是数组下标，end > start。
    """
    n = len(closes)
    start_idx = max(0, n - lookback)
    best = None

    for end in range(n - 1, start_idx, -1):
        if closes[end] <= 0:
            continue
        for start in range(max(start_idx, end - max_days), end):
            if closes[start] <= 0:
                continue
            gain_pct = (closes[end] / closes[start] - 1) * 100
            if gain_pct >= min_gain_pct:
                span = end - start + 1
                if best is None or span < (best["end"] - best["start"] + 1):
                    best = {
                        "start": start,
                        "end": end,
                        "gain_pct": gain_pct,
                        "span": span,
                    }
                break  # 找到最短窗口就跳出

    return best


def ensure_pre_close(klines_df: pd.DataFrame) -> None:
    """确保 DataFrame 包含 pre_close 列（原地修改）。"""
    if "pre_close" in klines_df.columns:
        return
    if "change" in klines_df.columns:
        klines_df["pre_close"] = klines_df["close"] - klines_df["change"]
    else:
        klines_df["pre_close"] = klines_df["close"] / (1 + klines_df["pct_chg"] / 100)


# ============================================================
# 趋势判断
# ============================================================


def is_bullish_ma_alignment(closes: np.ndarray, periods: list[int] | None = None) -> bool:
    """判断均线多头排列（短>中>长）。

    默认检查 MA5 > MA20 > MA60。
    """
    if periods is None:
        periods = [5, 20, 60]
    if len(closes) < max(periods):
        return False
    mas = []
    for p in periods:
        ma = float(closes[-p:].mean())
        mas.append(ma)
    # 从短到长递减即为多头排列
    return all(mas[i] > mas[i + 1] for i in range(len(mas) - 1))


def is_bottom_strengthening(
    lows: np.ndarray, days: int = 5,
) -> bool:
    """判断低点是否在抬高（底部走强）。

    检查最近 days 天的低点序列是否整体呈上升趋势。
    """
    if len(lows) < days:
        return False
    recent = lows[-days:]
    # 至少有3天低点在抬高（允许1天例外）
    rising_count = sum(1 for i in range(1, len(recent)) if recent[i] >= recent[i - 1])
    return rising_count >= len(recent) - 2


def count_consecutive_boards(
    klines_df: pd.DataFrame, symbol: str,
) -> int:
    """从最近一天往前数连续涨停天数（含当日）。

    返回连板数（0 表示最后一天不是涨停）。
    """
    sym_data = klines_df[klines_df["symbol"] == symbol].sort_values("date")
    if sym_data.empty:
        return 0
    ensure_pre_close(sym_data)
    limit_ratio = calc_limit_ratio(symbol)

    count = 0
    for _, row in sym_data.iloc[::-1].iterrows():
        pre_close = row.get("pre_close", 0)
        if pre_close <= 0:
            break
        limit_up = round(pre_close * (1 + limit_ratio), 2)
        if row["close"] >= limit_up:
            count += 1
        else:
            break
    return count


def calc_sector_strength(
    snapshots: dict, theme_resolver, threshold: float = 0.0,
) -> dict[str, float]:
    """计算各板块/题材的平均涨幅。

    返回 {theme_name: avg_pct_chg}，仅包含涨幅超过 threshold 的板块。
    """
    if not theme_resolver or not theme_resolver.loaded:
        return {}

    # 按题材聚合
    theme_pcts: dict[str, list[float]] = {}
    for code, snap in snapshots.items():
        if snap.close <= 0:
            continue
        pure = code[2:] if len(code) > 2 else code
        themes = theme_resolver.resolve_themes(pure)
        for theme in themes:
            if theme not in theme_pcts:
                theme_pcts[theme] = []
            theme_pcts[theme].append(snap.pct_chg)

    result = {}
    for theme, pcts in theme_pcts.items():
        if len(pcts) >= 3:  # 至少3只股票才算板块
            avg = sum(pcts) / len(pcts)
            if avg > threshold:
                result[theme] = round(avg, 2)
    return result


# ============================================================
# Tick 级 Realized Volatility
# ============================================================

def calc_tick_rv(prices: np.ndarray) -> float:
    """计算 tick 级 Realized Volatility。

    RV = sqrt(Σ(ln(P_t / P_{t-1}))²)
    衡量价格在连续 tick 间的微观波动强度。

    参数:
        prices: 连续 tick 的价格数组
    返回:
        RV 值，数据不足返回 0.0
    """
    if prices is None or len(prices) < 2:
        return 0.0
    valid = prices[prices > 0]
    if len(valid) < 2:
        return 0.0
    log_returns = np.diff(np.log(valid))
    return float(np.sqrt(np.sum(log_returns ** 2)))


def calc_tick_rv_for_code(
    tick_history: pd.DataFrame,
    code: str,
    window: int = 20,
) -> float:
    """从 tick_history 提取指定股票最近 N 个 tick 的 RV。

    参数:
        tick_history: 全市场 tick 历史 DataFrame
        code: 股票代码 (如 "sz002491")
        window: 回看 tick 数
    返回:
        RV 值
    """
    mask = tick_history["code"] == code
    prices = tick_history.loc[mask, "now"].values
    n = len(prices)
    if n == 0:
        return 0.0
    w = min(window, n)
    return calc_tick_rv(prices[-w:])
