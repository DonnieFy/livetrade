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
