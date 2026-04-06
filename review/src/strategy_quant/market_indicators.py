"""全市场量化指标库 — 策略环境与买点判定。

从五位公众号大V文章中提炼的核心量化指标，
适配 ashares-k-lines ``load_klines()`` 返回格式。

字段要求: symbol, date, open, close, high, low, volume, amount, pct_chg, change
"""

from __future__ import annotations

import pandas as pd
import numpy as np


# ==================================================================
#  涨停溢价率
# ==================================================================

def calc_limit_up_premium(klines: pd.DataFrame, date: str) -> pd.DataFrame:
    """统计昨日涨停股在今日的平均开盘溢价/折价。

    溢价率 > 0 → 赚钱效应;  < 0 → 亏钱效应(冰点信号)。
    """
    sorted_dates = sorted(klines["date"].unique())
    if date not in sorted_dates:
        return pd.DataFrame()
    idx = sorted_dates.index(date)
    if idx == 0:
        return pd.DataFrame()

    prev_date = sorted_dates[idx - 1]
    prev_day = klines[klines["date"] == prev_date].copy()
    if "change" in prev_day.columns:
        prev_day["pre_close"] = prev_day["close"] - prev_day["change"]
    elif "pre_close" not in prev_day.columns:
        return pd.DataFrame()

    prev_day["limit_ratio"] = prev_day["symbol"].apply(_limit_ratio)
    prev_day["limit_up_price"] = (prev_day["pre_close"] * (1 + prev_day["limit_ratio"])).round(2)

    limit_syms = prev_day[prev_day["close"] == prev_day["limit_up_price"]]["symbol"].tolist()
    if not limit_syms:
        return pd.DataFrame()

    today = klines[klines["date"] == date]
    merged = today[today["symbol"].isin(limit_syms)][["symbol", "open"]].merge(
        prev_day[prev_day["symbol"].isin(limit_syms)][["symbol", "close"]],
        on="symbol", suffixes=("_today", "_prev"),
    )
    merged["premium_rate"] = (merged["open_today"] / merged["close_prev"] - 1) * 100
    return merged


def calc_limit_up_premium_index(klines: pd.DataFrame, date: str) -> float:
    """涨停溢价指数(单一数值，%)。正=赚钱效应，负=亏钱效应。"""
    df = calc_limit_up_premium(klines, date)
    return round(float(df["premium_rate"].mean()), 2) if not df.empty else 0.0


# ==================================================================
#  封板率
# ==================================================================

def calc_seal_rate(klines: pd.DataFrame, date: str) -> dict:
    """封板率。> 80% 高潮(卖在一致)，< 60% 冰点(买在分歧)。"""
    day = klines[klines["date"] == date].copy()
    if day.empty:
        return {"date": date, "touched": 0, "sealed": 0, "seal_rate": 0.0, "blown": 0, "blown_rate": 0.0}

    if "change" in day.columns:
        day["pre_close"] = day["close"] - day["change"]
    elif "pre_close" not in day.columns:
        return {"date": date, "touched": 0, "sealed": 0, "seal_rate": 0.0, "blown": 0, "blown_rate": 0.0}

    day["limit_up_price"] = (day["pre_close"] * (1 + day["symbol"].apply(_limit_ratio))).round(2)
    touched = day[day["high"] >= day["limit_up_price"]]
    sealed = touched[touched["close"] == touched["limit_up_price"]]

    t, s = len(touched), len(sealed)
    blown = t - s
    return {
        "date": date,
        "touched": t,
        "sealed": s,
        "seal_rate": round(s / t * 100, 1) if t else 0.0,
        "blown": blown,
        "blown_rate": round(blown / t * 100, 1) if t else 0.0,
    }


# ==================================================================
#  量价潮汐
# ==================================================================

def calc_volume_tide(klines: pd.DataFrame, n_days: int = 5) -> pd.DataFrame:
    """全市场量价潮汐。放量→趋势可做; 缩量→高低切/低吸轮动。"""
    daily = klines.groupby("date")["amount"].sum().reset_index()
    daily.columns = ["date", "total_amount"]
    daily = daily.sort_values("date")
    daily[f"avg_{n_days}d"] = daily["total_amount"].rolling(n_days).mean()

    def _cls(row):
        avg = row[f"avg_{n_days}d"]
        if pd.isna(avg):
            return "数据不足"
        if row["total_amount"] > avg * 1.1:
            return "放量"
        if row["total_amount"] < avg * 0.9:
            return "缩量"
        return "平量"

    daily["tide"] = daily.apply(_cls, axis=1)
    return daily


# ==================================================================
#  连续冰点天数 & Day-N 上升段计数
# ==================================================================

def calc_consecutive_ice_days(klines: pd.DataFrame, date: str, max_lookback: int = 10) -> int:
    """从 date 往前数连续溢价为负的天数。"""
    sorted_dates = sorted(klines["date"].unique())
    if date not in sorted_dates:
        return 0
    idx = sorted_dates.index(date)
    count = 0
    for i in range(idx, max(idx - max_lookback, -1), -1):
        if calc_limit_up_premium_index(klines, sorted_dates[i]) < 0:
            count += 1
        else:
            break
    return count


def calc_day_n(klines: pd.DataFrame, date: str, max_lookback: int = 10) -> int:
    """Day-N 计数：从 date 往前数连续溢价为正的天数。"""
    sorted_dates = sorted(klines["date"].unique())
    if date not in sorted_dates:
        return 0
    idx = sorted_dates.index(date)
    count = 0
    for i in range(idx, max(idx - max_lookback, -1), -1):
        if calc_limit_up_premium_index(klines, sorted_dates[i]) > 0:
            count += 1
        else:
            break
    return count


# ==================================================================
#  新高检测 & 均线回踩检测
# ==================================================================

def is_near_new_high(series_close: pd.Series, lookback: int = 20) -> bool:
    """收盘价是否在 lookback 日新高附近(±2%)。"""
    if len(series_close) < lookback:
        return False
    high = series_close.iloc[-lookback - 1:-1].max()
    cur = series_close.iloc[-1]
    return cur >= high * 0.98


def detect_ma_pullback(closes: pd.Series, ma_period: int = 5) -> dict:
    """检测当前价格是否回踩 N 日均线(±2%以内)。"""
    if len(closes) < ma_period + 1:
        return {"signal": False}
    ma = closes.rolling(ma_period).mean().iloc[-1]
    cur = closes.iloc[-1]
    if pd.isna(ma) or ma <= 0:
        return {"signal": False}
    dev = (cur / ma - 1) * 100
    return {"signal": abs(dev) < 2.0, "ma": round(ma, 2), "deviation_pct": round(dev, 2)}


# ==================================================================
#  弱转强信号
# ==================================================================

def detect_weak_to_strong(klines: pd.DataFrame, symbol: str, date: str) -> dict:
    """前日暴量(>1.5x均量) + 今日高开 = 弱转强信号。"""
    sorted_dates = sorted(klines["date"].unique())
    if date not in sorted_dates:
        return {"signal": False}
    idx = sorted_dates.index(date)
    if idx == 0:
        return {"signal": False}
    prev_date = sorted_dates[idx - 1]

    today_row = klines[(klines["symbol"] == symbol) & (klines["date"] == date)]
    prev_row = klines[(klines["symbol"] == symbol) & (klines["date"] == prev_date)]
    if today_row.empty or prev_row.empty:
        return {"signal": False}

    t, p = today_row.iloc[0], prev_row.iloc[0]
    hist = klines[klines["symbol"] == symbol].sort_values("date")
    recent = hist[hist["date"] <= prev_date].tail(10)
    avg_vol = recent["volume"].mean()
    prev_heavy = (p["volume"] / avg_vol > 1.5) if avg_vol > 0 else False
    gap_up = t["open"] > p["close"]
    gap_pct = round((t["open"] / p["close"] - 1) * 100, 2) if p["close"] > 0 else 0

    return {
        "signal": prev_heavy and gap_up,
        "prev_vol_ratio": round(p["volume"] / avg_vol, 2) if avg_vol > 0 else 0,
        "gap_pct": gap_pct,
    }


# ==================================================================
#  低位首板扫描
# ==================================================================

def scan_low_first_boards(klines: pd.DataFrame, date: str, max_20d_pct: float = 30.0) -> list[str]:
    """扫描当日涨停且近20日涨幅 < max_20d_pct 的低位首板（近5日无涨停）。"""
    sorted_dates = sorted(klines["date"].unique())
    if date not in sorted_dates:
        return []
    today = klines[klines["date"] == date].copy()
    if today.empty:
        return []
    if "change" in today.columns:
        today["pre_close"] = today["close"] - today["change"]
    today["limit_up_price"] = (today["pre_close"] * (1 + today["symbol"].apply(_limit_ratio))).round(2)
    limit_up = today[today["close"] == today["limit_up_price"]]

    results = []
    for _, row in limit_up.iterrows():
        sym = row["symbol"]
        hist = klines[klines["symbol"] == sym].sort_values("date")
        before = hist[hist["date"] < date]
        if len(before) < 5:
            continue
        # 近5日无涨停
        r5 = before.tail(5).copy()
        if "change" in r5.columns:
            r5["pre_close"] = r5["close"] - r5["change"]
            r5["lp"] = (r5["pre_close"] * (1 + _limit_ratio(sym))).round(2)
            if (r5["close"] == r5["lp"]).any():
                continue
        # 近20日涨幅
        r20 = before.tail(20)
        min_c = r20["close"].min()
        if min_c > 0 and (row["close"] / min_c - 1) * 100 > max_20d_pct:
            continue
        results.append(sym)
    return results


# ==================================================================
#  20cm/30cm 弹性标扫描
# ==================================================================

def scan_elasticity_20cm(klines: pd.DataFrame, date: str, min_pct: float = 8.0) -> pd.DataFrame:
    """扫描当日涨幅 >= min_pct 的创业板/科创板(20cm/30cm)弹性标。"""
    day = klines[klines["date"] == date].copy()
    if day.empty:
        return pd.DataFrame()
    mask = day["symbol"].apply(lambda s: str(s).zfill(6)[:3] in ("300", "301", "688"))
    strong = day[mask & (day["pct_chg"] >= min_pct)].copy()
    if strong.empty:
        return pd.DataFrame()
    strong = strong.sort_values("pct_chg", ascending=False)
    return strong[["symbol", "close", "pct_chg", "amount"]].head(20)


# ==================================================================
#  抗跌 Alpha 扫描
# ==================================================================

def scan_anti_fragile(klines: pd.DataFrame, date: str, lookback: int = 5) -> pd.DataFrame:
    """大盘下跌中逆势抗跌的个股(RS > 市场均值+5%)。"""
    sorted_dates = sorted(klines["date"].unique())
    if date not in sorted_dates:
        return pd.DataFrame()
    idx = sorted_dates.index(date)
    if idx < lookback:
        return pd.DataFrame()
    recent_dates = sorted_dates[idx - lookback + 1: idx + 1]
    recent = klines[klines["date"].isin(recent_dates)]
    mkt_ret = recent.groupby("date")["pct_chg"].mean().sum()
    if mkt_ret >= 0:
        return pd.DataFrame()  # 大盘没跌
    stk_ret = recent.groupby("symbol")["pct_chg"].sum().reset_index()
    stk_ret.columns = ["symbol", "cum_pct"]
    af = stk_ret[stk_ret["cum_pct"] > mkt_ret + 5].copy()
    af["rs_alpha"] = af["cum_pct"] - mkt_ret
    return af.sort_values("rs_alpha", ascending=False).head(30)


# ==================================================================
#  辅助
# ==================================================================

def _limit_ratio(sym) -> float:
    s = str(sym).zfill(6)
    if s[:3] in ("300", "301", "688"):
        return 0.20
    return 0.10
