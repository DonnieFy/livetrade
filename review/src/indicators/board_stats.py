"""
板数与炸板统计。

统一输出盘面里与涨停、炸板、断板相关的机器事实，
供复盘报告与次日实盘策略共用。
"""

from __future__ import annotations

import pandas as pd

from .emotion_cycle import (
    _calc_consecutive_board,
    _calc_emotion_5d_matrix,
    _calc_first_board_premium,
    _calc_yesterday_limit_up_perf,
    _count_consecutive_limit_up,
    _filter_new_stocks,
    _filter_st,
    _get_limit_price,
    _get_limit_stocks,
)


def compute_board_stats(dc) -> dict:
    day_data = dc.get_day_klines()
    if day_data.empty:
        return _empty_result(dc.date or "")

    day_data = _filter_st(day_data.copy(), dc)
    day_data = _filter_new_stocks(day_data, dc.date, dc.klines)
    if day_data.empty:
        return _empty_result(dc.date or "")

    limit_up_df = _get_limit_stocks(day_data, "up")
    limit_down_df = _get_limit_stocks(day_data, "down")

    hit_limit_up = 0
    broken_limit_up = 0
    broken_board_stocks: list[dict] = []
    if "pre_close" in day_data.columns:
        limit_price = _get_limit_price(day_data["symbol"], day_data["pre_close"], "up")
        hit_mask = day_data["high"] >= limit_price - 0.011
        broken_mask = hit_mask & (day_data["close"] < limit_price)
        hit_limit_up = int(hit_mask.sum())
        broken_limit_up = int(broken_mask.sum())
        broken_board_stocks = _to_stock_records(day_data[broken_mask], dc)

    consecutive_board = _calc_consecutive_board(dc)
    consecutive_board_count = sum(
        len(stocks) for stocks in consecutive_board.get("ladder", {}).values()
    )
    board_breakers = _compute_board_breakers(dc)

    return {
        "date": dc.date,
        "limit_up_count": len(limit_up_df),
        "limit_down_count": len(limit_down_df),
        "hit_limit_up_count": hit_limit_up,
        "broken_board_count": broken_limit_up,
        "seal_rate": round(len(limit_up_df) / hit_limit_up * 100, 1) if hit_limit_up else 0.0,
        "broken_board_rate": round(broken_limit_up / hit_limit_up * 100, 1) if hit_limit_up else 0.0,
        "consecutive_board_count": consecutive_board_count,
        "max_board_height": consecutive_board.get("max_height", 0),
        "first_board_premium": _calc_first_board_premium(dc),
        "yesterday_limit_up_performance": _calc_yesterday_limit_up_perf(dc),
        "emotion_5d_matrix": _calc_emotion_5d_matrix(dc, days=5),
        "broken_board_stocks": broken_board_stocks,
        "board_breakers": board_breakers,
    }


def _compute_board_breakers(dc) -> list[dict]:
    prev_day = dc.get_prev_day_klines()
    today_data = dc.get_day_klines()
    if prev_day.empty or today_data.empty:
        return []

    prev_day = _filter_st(prev_day.copy(), dc)
    today_data = _filter_st(today_data.copy(), dc)
    prev_limit_up = _get_limit_stocks(prev_day, "up")
    if prev_limit_up.empty:
        return []

    all_dates = dc.get_trading_dates(20)
    recent_klines = dc.get_stock_klines(days=20)
    today_limit_symbols = set(_get_limit_stocks(today_data, "up")["symbol"].tolist())
    today_lookup = today_data.set_index("symbol")

    records: list[dict] = []
    for _, row in prev_limit_up.iterrows():
        symbol = str(row["symbol"]).zfill(6)
        board_count = _count_consecutive_limit_up(symbol, all_dates[1:], recent_klines)
        if board_count < 2 or symbol in today_limit_symbols:
            continue

        today_row = today_lookup.loc[symbol] if symbol in today_lookup.index else None
        if isinstance(today_row, pd.DataFrame):
            today_row = today_row.iloc[0]
        if today_row is None:
            continue

        records.append(
            {
                "symbol": symbol,
                "name": dc.get_stock_name(symbol),
                "prev_board_count": board_count,
                "pct_chg": round(float(today_row.get("pct_chg", 0)), 2),
                "close": round(float(today_row.get("close", 0)), 2),
            }
        )

    return sorted(records, key=lambda item: item["prev_board_count"], reverse=True)


def _to_stock_records(df: pd.DataFrame, dc) -> list[dict]:
    records: list[dict] = []
    for _, row in df.iterrows():
        symbol = str(row["symbol"]).zfill(6)
        amount_yi = round(float(row.get("amount", 0)) / 100000.0, 2)
        records.append(
            {
                "symbol": symbol,
                "name": dc.get_stock_name(symbol),
                "pct_chg": round(float(row.get("pct_chg", 0)), 2),
                "close": round(float(row.get("close", 0)), 2),
                "amount_yi": amount_yi,
            }
        )
    return sorted(records, key=lambda item: item["amount_yi"], reverse=True)


def _empty_result(date: str) -> dict:
    return {
        "date": date,
        "limit_up_count": 0,
        "limit_down_count": 0,
        "hit_limit_up_count": 0,
        "broken_board_count": 0,
        "seal_rate": 0.0,
        "broken_board_rate": 0.0,
        "consecutive_board_count": 0,
        "max_board_height": 0,
        "first_board_premium": None,
        "yesterday_limit_up_performance": {},
        "emotion_5d_matrix": [],
        "broken_board_stocks": [],
        "board_breakers": [],
    }
