"""
明星股指标计算（Phase 2 完善）

MVP 阶段：最高连板股 + 高波动高成交 Top 排名
"""
import pandas as pd
import numpy as np

from config import GEM_PREFIX, STAR_PREFIX


def compute_star_stocks(dc) -> dict:
    """
    计算明星股指标。

    Returns:
        {
            "highest_board": [{symbol, name, board_count, pct_chg}],
            "high_volatility_top": [{symbol, name, pct_chg, amount_yi, turnover_score}],
        }
    """
    result = {
        "highest_board": [],
        "high_volatility_top": [],
    }

    day_data = dc.get_day_klines()
    if day_data.empty:
        return result

    # 过滤 ST
    basic = dc.stock_basic
    if not basic.empty and "name" in basic.columns:
        st_symbols = basic[
            basic["name"].fillna("").str.upper().str.contains("ST")
        ]["symbol"].astype(str).str.zfill(6).tolist()
        day_data = day_data[~day_data["symbol"].isin(st_symbols)]

    # ── 最高连板股（复用 emotion_cycle 的连板计算逻辑）──
    # 这里简单从今日涨幅 > 9% 的股票中找连板
    from src.indicators.emotion_cycle import _get_limit_stocks, _count_consecutive_limit_up
    limit_up = _get_limit_stocks(day_data, "up")
    if not limit_up.empty:
        dates = dc.get_trading_dates(15)
        klines = dc.get_stock_klines(days=15)
        board_records = []
        for _, row in limit_up.iterrows():
            symbol = str(row["symbol"]).zfill(6)
            board_count = _count_consecutive_limit_up(symbol, dates, klines)
            board_records.append({
                "symbol": symbol,
                "name": dc.get_stock_name(symbol),
                "board_count": board_count,
                "pct_chg": round(float(row.get("pct_chg", 0)), 2),
                "close": round(float(row.get("close", 0)), 2),
            })
        # 按连板数降序，取 Top 10
        board_records.sort(key=lambda x: x["board_count"], reverse=True)
        result["highest_board"] = board_records[:10]

    # ── 高波动高成交 Top（非涨停的高涨幅股也值得关注）──
    if "pct_chg" in day_data.columns:
        high_pct = day_data[day_data["pct_chg"] > 3.0].copy()
        if not high_pct.empty and "amount" in high_pct.columns:
            # 综合评分 = pct_chg × log(amount+1)
            high_pct["score"] = high_pct["pct_chg"] * np.log1p(high_pct["amount"])
            top = high_pct.nlargest(15, "score")
            for _, row in top.iterrows():
                symbol = str(row["symbol"]).zfill(6)
                amount_yi = round(float(row["amount"]) / 100000.0, 2)
                result["high_volatility_top"].append({
                    "symbol": symbol,
                    "name": dc.get_stock_name(symbol),
                    "pct_chg": round(float(row["pct_chg"]), 2),
                    "amount_yi": amount_yi,
                    "close": round(float(row.get("close", 0)), 2),
                })

    return result
