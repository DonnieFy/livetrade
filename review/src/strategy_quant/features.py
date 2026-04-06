"""Feature engineering for market state, stock strength, and themes."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP

EPSILON = 0.011
MAIN_LIMIT = 0.10
GEM_STAR_LIMIT = 0.20
BSE_LIMIT = 0.30
ST_LIMIT = 0.05


def _round_price(values) -> np.ndarray:
    """A股交易所标准四舍五入（非Python银行家舍入），精确到分。"""
    def _rh(v):
        return float(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    if isinstance(values, (pd.Series, np.ndarray)):
        return np.array([_rh(v) for v in values])
    return _rh(values)


def _limit_ratio(symbol: str, name: str = "") -> float:
    symbol = str(symbol).zfill(6)
    name = str(name or "").upper()
    if "ST" in name:
        return ST_LIMIT
    if symbol.startswith(("30", "68")):
        return GEM_STAR_LIMIT
    if symbol.startswith(("4", "8", "920")):
        return BSE_LIMIT
    return MAIN_LIMIT


def _safe_close_to(value: pd.Series, target: pd.Series) -> pd.Series:
    return (value - target).abs() <= EPSILON


def _calc_streak(flag: pd.Series) -> pd.Series:
    streak: list[int] = []
    current = 0
    for item in flag.fillna(False).astype(bool):
        current = current + 1 if item else 0
        streak.append(current)
    return pd.Series(streak, index=flag.index, dtype="int64")


def prepare_daily_features(
    klines: pd.DataFrame,
    basic: pd.DataFrame | None = None,
    stock_meta: pd.DataFrame | None = None,
    keep_st: bool = False,
) -> pd.DataFrame:
    """Build stock-level daily features from repo daily bars."""
    if klines.empty:
        return klines.copy()

    df = klines.copy()
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    if basic is not None and not basic.empty:
        base = basic.copy()
        base["symbol"] = base["symbol"].astype(str).str.zfill(6)
        if "name" not in base.columns:
            base["name"] = ""
        df = df.merge(base[["symbol", "name"]].drop_duplicates("symbol"), on="symbol", how="left")
    elif "name" not in df.columns:
        df["name"] = ""

    if stock_meta is not None and not stock_meta.empty:
        meta_cols = ["symbol", "industry", "market_cap_total_e8", "market_cap_float_e8"]
        stock_meta = stock_meta[[c for c in meta_cols if c in stock_meta.columns]].copy()
        stock_meta["symbol"] = stock_meta["symbol"].astype(str).str.zfill(6)
        df = df.merge(stock_meta.drop_duplicates("symbol"), on="symbol", how="left")

    df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    df["is_st"] = df["name"].astype(str).str.upper().str.contains("ST", na=False)
    if not keep_st:
        df = df[~df["is_st"]].copy()

    if "change" in df.columns:
        df["pre_close"] = df["close"] - df["change"]
    else:
        df["pre_close"] = df.groupby("symbol")["close"].shift(1)
    df["pre_close"] = df["pre_close"].where(df["pre_close"] > 0)

    df["limit_ratio"] = [
        _limit_ratio(symbol, name)
        for symbol, name in zip(df["symbol"], df["name"], strict=False)
    ]
    df["limit_up_price"] = _round_price(df["pre_close"] * (1 + df["limit_ratio"]))
    df["limit_down_price"] = _round_price(df["pre_close"] * (1 - df["limit_ratio"]))
    valid_pre_close = df["pre_close"].notna()
    df["is_limit_up"] = valid_pre_close & _safe_close_to(df["close"], df["limit_up_price"])
    df["is_limit_down"] = valid_pre_close & _safe_close_to(df["close"], df["limit_down_price"])
    df["hit_limit_up"] = valid_pre_close & (df["high"] >= df["limit_up_price"] - EPSILON)
    df["broken_limit_up"] = df["hit_limit_up"] & ~df["is_limit_up"]

    # 新股上市前5个交易日无涨跌停限制，排除这些股票的涨跌停标记
    _trade_day_count = df.groupby("symbol")["date"].transform("cumcount") + 1
    _is_ipo = _trade_day_count <= 5
    df.loc[_is_ipo, ["is_limit_up", "is_limit_down", "hit_limit_up", "broken_limit_up"]] = False

    df["gap_up"] = np.where(valid_pre_close, df["open"] / df["pre_close"] - 1.0, np.nan)
    df["close_from_open"] = np.where(df["open"] > 0, df["close"] / df["open"] - 1.0, np.nan)
    df["intraday_range"] = np.where(df["low"] > 0, df["high"] / df["low"] - 1.0, np.nan)
    df["amplitude_ratio"] = df["amplitude"] / 100.0
    if "turnover" in df.columns:
        df["turnover_rate"] = df["turnover"] / 100.0
    else:
        # Fallback: turnover column not in source CSV — set to NaN
        df["turnover_rate"] = np.nan

    for window in (3, 5, 10, 20, 30):
        df[f"ma{window}"] = df.groupby("symbol")["close"].transform(lambda s: s.rolling(window).mean())
        df[f"high_{window}"] = (
            df.groupby("symbol")["high"].transform(lambda s: s.rolling(window).max().shift(1))
        )
        df[f"low_{window}"] = (
            df.groupby("symbol")["low"].transform(lambda s: s.rolling(window).min().shift(1))
        )
        df[f"ret_{window}d"] = df.groupby("symbol")["close"].pct_change(window)

    df["distance_to_20d_high"] = np.where(df["high_20"] > 0, df["close"] / df["high_20"] - 1.0, np.nan)
    df["distance_to_30d_high"] = np.where(df["high_30"] > 0, df["close"] / df["high_30"] - 1.0, np.nan)
    df["is_new_high_20"] = df["close"] > df["high_20"]
    df["is_new_high_30"] = df["close"] > df["high_30"]
    df["close_vs_ma10"] = np.where(df["ma10"] > 0, df["close"] / df["ma10"] - 1.0, np.nan)
    df["close_vs_ma20"] = np.where(df["ma20"] > 0, df["close"] / df["ma20"] - 1.0, np.nan)

    df["amount_ma5"] = df.groupby("symbol")["amount"].transform(lambda s: s.rolling(5).mean())
    df["volume_ma5"] = df.groupby("symbol")["volume"].transform(lambda s: s.rolling(5).mean())
    df["amount_vs_ma5"] = np.where(df["amount_ma5"] > 0, df["amount"] / df["amount_ma5"], np.nan)
    df["volume_vs_ma5"] = np.where(df["volume_ma5"] > 0, df["volume"] / df["volume_ma5"], np.nan)
    df["limit_up_streak"] = (
        df.groupby("symbol", group_keys=False)["is_limit_up"].apply(_calc_streak).astype("int64")
    )
    return df.reset_index(drop=True)


def build_market_environment(features: pd.DataFrame) -> pd.DataFrame:
    """Build daily market environment labels used by all strategies."""
    grouped = features.groupby("date", sort=True)
    env = grouped.agg(
        listed_count=("symbol", "nunique"),
        advance_count=("pct_chg", lambda s: int((s > 0).sum())),
        decline_count=("pct_chg", lambda s: int((s < 0).sum())),
        limit_up_count=("is_limit_up", "sum"),
        limit_down_count=("is_limit_down", "sum"),
        hit_limit_up_count=("hit_limit_up", "sum"),
        broken_limit_up_count=("broken_limit_up", "sum"),
        streak2_count=("limit_up_streak", lambda s: int((s >= 2).sum())),
        streak3_count=("limit_up_streak", lambda s: int((s >= 3).sum())),
        market_height=("limit_up_streak", "max"),
        total_amount=("amount", "sum"),
        eq_return=("pct_chg", lambda s: float(s.mean()) / 100.0),
        avg_turnover_rate=("turnover_rate", "mean"),
    ).reset_index()

    env["seal_rate"] = np.where(
        env["hit_limit_up_count"] > 0,
        env["limit_up_count"] / env["hit_limit_up_count"],
        0.0,
    )
    env["advance_ratio"] = env["advance_count"] / env["listed_count"]
    env["amount_ma5"] = env["total_amount"].rolling(5).mean()
    env["amount_vs_ma5"] = np.where(env["amount_ma5"] > 0, env["total_amount"] / env["amount_ma5"], np.nan)
    env["eq_return_10d_std"] = env["eq_return"].rolling(10).std()
    env["market_height_20d_max"] = env["market_height"].rolling(20).max().shift(1)
    env["height_breakout"] = env["market_height"] > env["market_height_20d_max"].fillna(0)

    env["emotion_score"] = (
        35 * ((env["advance_ratio"] - 0.30) / 0.45).clip(0, 1)
        + 20 * env["seal_rate"].clip(0, 1)
        + 15 * (env["limit_up_count"] / 80.0).clip(0, 1)
        + 10 * (env["market_height"] / 8.0).clip(0, 1)
        + 10 * (env["streak2_count"] / 25.0).clip(0, 1)
        + 10 * (env["amount_vs_ma5"] - 0.9).clip(0, 0.3) / 0.3
        - 20 * (env["limit_down_count"] / 20.0).clip(0, 1)
    ).clip(0, 100)

    env["is_ice_point"] = (
        (
            ((env["advance_count"] < 1000) | (env["advance_ratio"] < 0.25))
            & (env["seal_rate"] < 0.60)
        )
        | (env["emotion_score"] < 15)
    )
    env["is_repair"] = (
        env["is_ice_point"].shift(1, fill_value=False)
        & (env["emotion_score"] - env["emotion_score"].shift(1) >= 12)
    )
    env["is_main_rise"] = (
        ((env["height_breakout"]) | (env["market_height"] >= 5))
        & (env["emotion_score"] >= 60)
        & (env["limit_down_count"] <= 10)
    )
    env["is_climax"] = (
        (env["advance_count"] > 3800)
        & (env["seal_rate"] > 0.75)
        & (env["limit_up_count"] >= 60)
        & (env["limit_down_count"] <= 5)
    )
    env["is_rotation"] = (
        (env["market_height"] <= 4)
        & (env["limit_up_count"] < 50)
        & ~env["is_ice_point"]
        & ~env["is_main_rise"]
    )
    env["climax_exit_risk"] = (
        (env["emotion_score"] >= 70)
        & (env["limit_up_count"] < env["limit_up_count"].shift(1).fillna(env["limit_up_count"]))
        & (env["limit_down_count"] > env["limit_down_count"].shift(1).fillna(0))
    )

    env["emotion_phase"] = "neutral"
    env.loc[env["is_ice_point"], "emotion_phase"] = "ice"
    env.loc[env["is_repair"], "emotion_phase"] = "repair"
    env.loc[env["is_rotation"], "emotion_phase"] = "rotation"
    env.loc[env["is_main_rise"], "emotion_phase"] = "main_rise"
    env.loc[env["is_climax"], "emotion_phase"] = "climax"

    vol_rank = env["eq_return_10d_std"].rank(pct=True)
    env["volatility_regime"] = "mid"
    env.loc[vol_rank <= 0.33, "volatility_regime"] = "low"
    env.loc[vol_rank >= 0.67, "volatility_regime"] = "high"

    env["liquidity_regime"] = "normal"
    env.loc[env["amount_vs_ma5"] < 0.92, "liquidity_regime"] = "shrinking"
    env.loc[env["amount_vs_ma5"] > 1.05, "liquidity_regime"] = "expanding"
    return env


def build_benchmark(features: pd.DataFrame) -> pd.DataFrame:
    """Create an equal-weight market benchmark from daily stock features."""
    env = build_market_environment(features)[["date", "eq_return"]].copy()
    env["benchmark_close"] = (1.0 + env["eq_return"].fillna(0.0)).cumprod()
    for window in (5, 10, 20):
        env[f"benchmark_ret_{window}"] = env["benchmark_close"].pct_change(window)
    return env


def build_stock_signals(
    features: pd.DataFrame,
    benchmark: pd.DataFrame | None = None,
    lookbacks: Sequence[int] = (5, 10, 20),
) -> pd.DataFrame:
    """Build reusable stock-level strategy signals."""
    out = features.copy()
    if benchmark is None:
        benchmark = build_benchmark(features)

    out = out.merge(benchmark, on="date", how="left")
    for window in lookbacks:
        out[f"rs_{window}"] = out[f"ret_{window}d"] - out[f"benchmark_ret_{window}"]

    out["trend_core_signal"] = (
        (out["rs_20"] > 0.12)
        & (out["distance_to_20d_high"] > -0.03)
        & (out["close_vs_ma20"] > 0.05)
        & (out["ret_20d"] > 0.15)
    )
    out["version_answer_signal"] = (
        out["is_new_high_20"]
        & (out["rs_20"] > 0.12)
        & (out["pct_chg"] > 4.0)
        & (out["benchmark_ret_20"] < 0.15)
    )
    out["low_absorption_signal"] = (
        (out["rs_5"] > 0)
        & (out["rs_20"] > 0.05)
        & (out["distance_to_20d_high"] > -0.08)
        & (out["pct_chg"].between(-3.0, 1.5))
        & (out["volume_vs_ma5"] < 0.9)
        & (out["close_vs_ma10"] > -0.02)
    )
    out["weak_to_strong_signal"] = (
        out.groupby("symbol")["broken_limit_up"].shift(1, fill_value=False)
        & (out["gap_up"] > 0.02)
        & (out["close"] > out["open"])
        & (out["amount_vs_ma5"] > 1.0)
    )
    out["climax_risk_signal"] = (
        (out["ret_3d"] > 0.25)
        | (out["ret_10d"] > 0.60)
        | (out["close_vs_ma10"] > 0.18)
        | ((out["gap_up"] > 0.06) & (out["close_from_open"] < 0))
    )
    out["breakout_pioneer_signal"] = (
        (out["limit_up_streak"] >= 3)
        & (out["distance_to_30d_high"] >= -0.01)
        & (out["amount_vs_ma5"] > 1.2)
    )
    return out


def build_intraday_snapshot(ticks: pd.DataFrame) -> pd.DataFrame:
    """Summarize intraday behavior for a single date."""
    if ticks.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    first = ticks.groupby("symbol", as_index=False).first()
    first = first.rename(columns={"now": "first_now", "buy": "first_buy", "sell": "first_sell"})
    frames.append(first[["symbol", "name", "open", "close", "first_now", "first_buy", "first_sell"]])

    within_5m = ticks[ticks["datetime"] <= ticks["datetime"].dt.normalize() + pd.Timedelta(hours=9, minutes=35)]
    if not within_5m.empty:
        last_5m = within_5m.groupby("symbol", as_index=False).last()
        last_5m = last_5m.rename(columns={"now": "now_5m"})
        frames.append(last_5m[["symbol", "now_5m"]])

    morning = ticks[ticks["datetime"] <= ticks["datetime"].dt.normalize() + pd.Timedelta(hours=10, minutes=0)]
    if not morning.empty:
        morning_low = morning.groupby("symbol", as_index=False)["now"].min().rename(columns={"now": "morning_low"})
        morning_last = morning.groupby("symbol", as_index=False).last().rename(columns={"now": "morning_last"})
        frames.extend(
            [
                morning_low[["symbol", "morning_low"]],
                morning_last[["symbol", "morning_last"]],
            ]
        )

    last_row = ticks.groupby("symbol", as_index=False).last()
    last_row = last_row.rename(columns={"now": "last_now", "buy": "last_buy", "sell": "last_sell"})
    frames.append(last_row[["symbol", "last_now", "last_buy", "last_sell"]])

    out = frames[0]
    for frame in frames[1:]:
        out = out.merge(frame, on="symbol", how="left")

    bid_cols = [f"bid{i}_volume" for i in range(1, 6)]
    ask_cols = [f"ask{i}_volume" for i in range(1, 6)]
    order_book = ticks.groupby("symbol", as_index=False).first()[["symbol"] + bid_cols + ask_cols]
    order_book["top5_bid_volume"] = order_book[bid_cols].sum(axis=1)
    order_book["top5_ask_volume"] = order_book[ask_cols].sum(axis=1)
    order_book["bid_ask_imbalance"] = np.where(
        order_book["top5_ask_volume"] > 0,
        order_book["top5_bid_volume"] / order_book["top5_ask_volume"],
        np.nan,
    )
    out = out.merge(order_book[["symbol", "bid_ask_imbalance"]], on="symbol", how="left")

    out["open_strength"] = np.where(out["close"] > 0, out["first_now"] / out["close"] - 1.0, np.nan)
    out["first_5m_return"] = np.where(out["first_now"] > 0, out["now_5m"] / out["first_now"] - 1.0, np.nan)
    out["morning_drawdown"] = np.where(out["first_now"] > 0, out["morning_low"] / out["first_now"] - 1.0, np.nan)
    out["morning_rebound"] = np.where(out["morning_low"] > 0, out["morning_last"] / out["morning_low"] - 1.0, np.nan)
    out["close_from_open_tick"] = np.where(out["first_now"] > 0, out["last_now"] / out["first_now"] - 1.0, np.nan)
    out["intraday_support_signal"] = (
        (out["morning_drawdown"] > -0.04)
        & (out["morning_rebound"] > 0.015)
        & (out["bid_ask_imbalance"] > 1.1)
    )
    return out


def build_theme_snapshot(day_stock: pd.DataFrame, theme_map: pd.DataFrame) -> pd.DataFrame:
    """Aggregate theme strength for a single trade date."""
    if day_stock.empty or theme_map.empty:
        return pd.DataFrame()

    merged = day_stock.merge(theme_map, on=["symbol"], how="inner", suffixes=("", "_theme"))
    grouped = merged.groupby(["theme", "theme_source"], sort=True)
    out = grouped.agg(
        member_count=("symbol", "nunique"),
        limit_up_count=("is_limit_up", "sum"),
        hit_limit_up_count=("hit_limit_up", "sum"),
        streak2_count=("limit_up_streak", lambda s: int((s >= 2).sum())),
        highest_streak=("limit_up_streak", "max"),
        breakout_count=("is_new_high_20", "sum"),
        avg_rs_5=("rs_5", "mean"),
        avg_rs_20=("rs_20", "mean"),
        avg_amount_ratio=("amount_vs_ma5", "mean"),
        avg_close_vs_ma20=("close_vs_ma20", "mean"),
    ).reset_index()

    out["seal_rate"] = np.where(
        out["hit_limit_up_count"] > 0,
        out["limit_up_count"] / out["hit_limit_up_count"],
        0.0,
    )
    out["theme_support_score"] = (
        25 * out["seal_rate"].fillna(0).clip(0, 1)
        + 20 * (out["limit_up_count"] / 8.0).fillna(0).clip(0, 1)
        + 15 * (out["streak2_count"] / 4.0).fillna(0).clip(0, 1)
        + 15 * (out["highest_streak"] / 6.0).fillna(0).clip(0, 1)
        + 15 * ((out["avg_rs_5"].fillna(0) + 0.05) / 0.15).clip(0, 1)
        + 10 * ((out["avg_close_vs_ma20"].fillna(0) + 0.05) / 0.15).clip(0, 1)
    ).clip(0, 100)
    return out.sort_values(["theme_support_score", "limit_up_count"], ascending=[False, False]).reset_index(drop=True)


def attach_best_theme(day_stock: pd.DataFrame, theme_map: pd.DataFrame, theme_snapshot: pd.DataFrame) -> pd.DataFrame:
    """Attach the strongest theme context to each stock for a date."""
    if day_stock.empty:
        return day_stock.copy()
    if theme_map.empty or theme_snapshot.empty:
        out = day_stock.copy()
        out["primary_theme"] = ""
        out["theme_support_score"] = np.nan
        return out

    merged = day_stock[["symbol", "name"]].merge(theme_map, on=["symbol"], how="left", suffixes=("", "_theme"))
    merged = merged.merge(theme_snapshot, on=["theme", "theme_source"], how="left")
    merged = merged.sort_values(
        ["symbol", "theme_support_score", "highest_streak", "limit_up_count"],
        ascending=[True, False, False, False],
    )
    best = merged.drop_duplicates("symbol").rename(columns={"theme": "primary_theme"})
    cols = [
        "symbol",
        "primary_theme",
        "theme_source",
        "theme_support_score",
        "member_count",
        "limit_up_count",
        "streak2_count",
        "highest_streak",
        "avg_rs_20",
    ]
    out = day_stock.merge(best[cols], on="symbol", how="left")
    return out


def build_mispriced_recovery_signals(
    ticks: pd.DataFrame,
    candidate_symbols: list[str],
    daily_features: pd.DataFrame,
    date: str,
) -> pd.DataFrame:
    """Analyze intraday tick data to detect bullish recovery actions.

    For stocks that were recently limit-up but got dragged down on *date*,
    compute signals that indicate the drop was a mispricing (active buying,
    recovery from lows, limit-down board being pried open, etc.).

    Parameters
    ----------
    ticks : pd.DataFrame
        Full-day trading ticks loaded via ``load_ticks_for_date``.
    candidate_symbols : list[str]
        6-digit symbols of candidate stocks to analyze.
    daily_features : pd.DataFrame
        Output of ``prepare_daily_features`` (needs pre_close, limit_down_price).
    date : str
        Target date ``"YYYY-MM-DD"``.

    Returns
    -------
    pd.DataFrame with columns:
        symbol, recovery_from_low, limit_down_open_count,
        volume_surge_at_low, close_above_low_pct, has_recovery_signal
    """
    if ticks.empty or not candidate_symbols:
        return pd.DataFrame(columns=[
            "symbol", "recovery_from_low", "limit_down_open_count",
            "volume_surge_at_low", "close_above_low_pct", "has_recovery_signal",
        ])

    day_info = daily_features[
        (daily_features["date"] == date)
        & (daily_features["symbol"].isin(candidate_symbols))
    ][["symbol", "pre_close", "limit_down_price", "close", "low", "high", "open"]].copy()

    if day_info.empty:
        return pd.DataFrame(columns=[
            "symbol", "recovery_from_low", "limit_down_open_count",
            "volume_surge_at_low", "close_above_low_pct", "has_recovery_signal",
        ])

    # Filter ticks to candidate symbols only (performance)
    ct = ticks[ticks["symbol"].isin(candidate_symbols)].copy()
    if ct.empty:
        return pd.DataFrame(columns=[
            "symbol", "recovery_from_low", "limit_down_open_count",
            "volume_surge_at_low", "close_above_low_pct", "has_recovery_signal",
        ])

    records: list[dict] = []

    for sym in candidate_symbols:
        stk = ct[ct["symbol"] == sym].sort_values("datetime")
        info = day_info[day_info["symbol"] == sym]
        if stk.empty or info.empty:
            continue
        info = info.iloc[0]
        pre_close = float(info["pre_close"])
        ld_price = float(info["limit_down_price"])
        day_close = float(info["close"])
        day_low = float(info["low"])
        day_high = float(info["high"])

        if pre_close <= 0:
            continue

        # --- 1. recovery_from_low: 从最低点到最高点的反弹幅度 ---
        recovery_from_low = (day_high - day_low) / pre_close if pre_close > 0 else 0

        # --- 2. limit_down_open_count: 跌停被撬开次数 ---
        # 定义: 价格触及跌停价后离开，再触及再离开的次数
        prices = stk["now"].values
        at_ld = np.abs(prices - ld_price) < EPSILON
        # Count transitions from at_limit_down=True to at_limit_down=False
        ld_opens = 0
        was_at_ld = False
        for p_at_ld in at_ld:
            if p_at_ld:
                was_at_ld = True
            elif was_at_ld:
                ld_opens += 1
                was_at_ld = False

        # --- 3. volume_surge_at_low: 低点附近成交量是否放大 ---
        # 将tick分成上下半段(按时间均分),比较低点区间和后续的成交量
        n_ticks = len(stk)
        mid = n_ticks // 2
        if mid > 0 and n_ticks > 10:
            vol_col = "volume"
            first_half_vol = stk.iloc[:mid][vol_col].diff().clip(lower=0).sum()
            second_half_vol = stk.iloc[mid:][vol_col].diff().clip(lower=0).sum()
            # 如果后半段成交量 > 前半段 * 1.5，说明低点有资金介入
            volume_surge = 1.0 if (second_half_vol > first_half_vol * 1.3 and first_half_vol > 0) else 0.0
        else:
            volume_surge = 0.0

        # --- 4. close_above_low_pct: 收盘价距最低价幅度 ---
        close_above_low = (day_close - day_low) / pre_close if pre_close > 0 else 0

        # --- 5. 综合信号判定 ---
        # 至少满足: 反弹>3% 或 撬板>=1次 或 (收盘远离最低点>2% 且有放量)
        has_signal = (
            recovery_from_low > 0.03
            or ld_opens >= 1
            or (close_above_low > 0.02 and volume_surge > 0)
        )

        records.append({
            "symbol": sym,
            "recovery_from_low": round(recovery_from_low, 4),
            "limit_down_open_count": ld_opens,
            "volume_surge_at_low": volume_surge,
            "close_above_low_pct": round(close_above_low, 4),
            "has_recovery_signal": has_signal,
        })

    return pd.DataFrame(records)


def build_elasticity_preference(day_stock: pd.DataFrame) -> dict[str, float | str]:
    """Judge whether 10cm or higher-elasticity boards dominate on a given date."""
    if day_stock.empty:
        return {
            "limit_up_ratio_20cm": 0.0,
            "amount_ratio_20cm": 0.0,
            "elasticity_preference": "unknown",
        }

    bucket = pd.Series("10cm", index=day_stock.index)
    bucket.loc[day_stock["symbol"].str.startswith(("30", "68"))] = "20cm"
    bucket.loc[day_stock["symbol"].str.startswith(("4", "8", "920"))] = "30cm"
    temp = day_stock.assign(elasticity_bucket=bucket)
    agg = temp.groupby("elasticity_bucket").agg(limit_up_count=("is_limit_up", "sum"), amount_sum=("amount", "sum"))

    total_limit_up = float(agg["limit_up_count"].sum())
    total_amount = float(agg["amount_sum"].sum())
    ratio_20_lu = float(agg.loc["20cm", "limit_up_count"] / total_limit_up) if "20cm" in agg.index and total_limit_up else 0.0
    ratio_20_amount = float(agg.loc["20cm", "amount_sum"] / total_amount) if "20cm" in agg.index and total_amount else 0.0
    ratio_30_lu = float(agg.loc["30cm", "limit_up_count"] / total_limit_up) if "30cm" in agg.index and total_limit_up else 0.0

    preference = "10cm"
    if ratio_20_lu >= 0.30 or ratio_20_amount >= 0.25:
        preference = "20cm"
    if ratio_30_lu >= 0.15:
        preference = "30cm"

    return {
        "limit_up_ratio_20cm": ratio_20_lu,
        "amount_ratio_20cm": ratio_20_amount,
        "limit_up_ratio_30cm": ratio_30_lu,
        "elasticity_preference": preference,
    }



