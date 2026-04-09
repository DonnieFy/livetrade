# -*- coding: utf-8 -*-
"""
强更强策略 — 日线级别回测

验证策略筛选效果：
1. 每个交易日筛选出满足涨跌停频繁+反包+放量涨停的候选股
2. 次日检查开盘强度和涨停情况

用法:
    cd /home/fy/myown/livetrade
    python backtest/backtest_strong_gets_stronger_daily.py --start-date 2026-02-01 --end-date 2026-04-02
"""

import argparse
import logging

import numpy as np
import pandas as pd

import config
from strategies.strategy_utils import (
    calc_limit_ratio, calc_limit_up_price, calc_limit_down_price,
    ensure_pre_close, load_klines, get_sorted_dates, get_prev_dates,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def find_candidates(
    klines: pd.DataFrame,
    date: str,
    prev_dates: list[str],
    extreme_days_min: int = 5,
    lookback_days: int = 10,
    volume_expand_ratio: float = 1.5,
) -> list[dict]:
    """对某一日执行筛选逻辑。"""
    recent_dates = prev_dates[-(lookback_days + 5):]
    recent = klines[klines["date"].isin(recent_dates)].copy()
    ensure_pre_close(recent)

    prev_date = prev_dates[-1]
    candidates = []

    for sym, grp in recent.groupby("symbol"):
        grp = grp.sort_values("date")
        if len(grp) < lookback_days:
            continue

        last_n = grp.tail(lookback_days).copy()
        lr = calc_limit_ratio(sym)
        last_n["limit_up_price"] = (last_n["pre_close"] * (1 + lr)).round(2)
        last_n["limit_down_price"] = (last_n["pre_close"] * (1 - lr)).round(2)
        last_n["is_lu"] = last_n["close"] >= last_n["limit_up_price"]
        last_n["is_ld"] = last_n["close"] <= last_n["limit_down_price"]

        # 条件1: 涨跌停天数
        extreme_count = int(last_n["is_lu"].sum() + last_n["is_ld"].sum())
        if extreme_count < extreme_days_min:
            continue

        # 条件2: 反包模式
        has_fanbao = False
        for i in range(len(last_n) - 1):
            if last_n.iloc[i]["is_ld"]:
                next_close = last_n.iloc[i + 1]["close"]
                ld_open = last_n.iloc[i]["open"]
                if next_close > ld_open:
                    has_fanbao = True
                    break
        if not has_fanbao and last_n["is_ld"].sum() > 0:
            continue

        # 条件3: 昨日涨停且放量
        yesterday = last_n[last_n["date"] == prev_date]
        if yesterday.empty:
            continue
        y = yesterday.iloc[0]
        if not y["is_lu"]:
            continue

        avg_amount_5d = float(last_n.tail(5)["amount"].mean())
        yesterday_amount = float(y["amount"])
        if avg_amount_5d > 0 and yesterday_amount < avg_amount_5d * volume_expand_ratio:
            continue

        candidates.append({
            "symbol": sym,
            "yesterday_close": float(y["close"]),
            "limit_up_price": float(y["limit_up_price"]),
            "extreme_count": extreme_count,
            "has_fanbao": has_fanbao,
            "avg_amount_5d": avg_amount_5d,
            "yesterday_amount": yesterday_amount,
        })

    return candidates


def check_next_day(
    symbol: str, yesterday_close: float, limit_up_price: float,
    next_date: str, klines: pd.DataFrame,
) -> dict | None:
    """检查次日开盘强度和涨停情况。"""
    mask = (klines["symbol"] == symbol) & (klines["date"] == next_date)
    next_day = klines[mask]
    if next_day.empty:
        return None

    row = next_day.iloc[0]
    open_ = float(row["open"])
    close = float(row["close"])
    pct_chg = float(row.get("pct_chg", 0))

    open_strength = (open_ - yesterday_close) / yesterday_close * 100
    reached_limit = close >= limit_up_price * 0.99

    return {
        "open": open_,
        "close": close,
        "pct_chg": pct_chg,
        "open_strength": round(open_strength, 2),
        "reached_limit": reached_limit,
    }


def backtest(start_date: str, end_date: str, **params) -> dict:
    logger.info(f"开始日线回测: {start_date} ~ {end_date}")

    klines = load_klines()
    if klines is None:
        return {}

    sorted_dates = get_sorted_dates(klines)
    date_range = [d for d in sorted_dates if start_date <= d <= end_date]

    details = []
    total_candidates = 0
    total_limit_up = 0
    open_strengths = []
    returns = []

    for i, date in enumerate(date_range):
        if i >= len(date_range) - 1:
            break

        prev_dates = get_prev_dates(sorted_dates, date)
        if len(prev_dates) < 10:
            continue

        candidates = find_candidates(klines, date, prev_dates, **params)
        if not candidates:
            continue

        next_date = date_range[i + 1]
        logger.info(f"  {date}: {len(candidates)} 只候选")

        for cand in candidates:
            sym = cand["symbol"]
            next_perf = check_next_day(
                sym, cand["yesterday_close"], cand["limit_up_price"], next_date, klines,
            )
            if next_perf is None:
                continue

            total_candidates += 1
            if next_perf["reached_limit"]:
                total_limit_up += 1
            open_strengths.append(next_perf["open_strength"])
            returns.append(next_perf["pct_chg"])

            details.append({
                "date": date,
                "symbol": sym,
                **cand,
                **next_perf,
            })

            logger.info(
                f"    {sym} 涨跌停{cand['extreme_count']}次 "
                f"→ 次日开{next_perf['open_strength']:+.1f}% "
                f"收{next_perf['pct_chg']:+.2f}% "
                f"{'涨停' if next_perf['reached_limit'] else '未涨停'}"
            )

    limit_rate = (total_limit_up / total_candidates * 100) if total_candidates else 0
    avg_open = np.mean(open_strengths) if open_strengths else 0
    avg_return = np.mean(returns) if returns else 0
    win_rate = (sum(1 for r in returns if r > 0) / len(returns) * 100) if returns else 0

    result = {
        "start_date": start_date,
        "end_date": end_date,
        "total_candidates": total_candidates,
        "total_limit_up": total_limit_up,
        "limit_up_rate": round(limit_rate, 2),
        "avg_open_strength": round(avg_open, 2),
        "avg_return": round(avg_return, 2),
        "win_rate": round(win_rate, 2),
        "details": details,
    }

    logger.info("\n" + "=" * 60)
    logger.info("强更强策略 — 日线回测汇总")
    logger.info("=" * 60)
    logger.info(f"回测日期: {start_date} ~ {end_date}")
    logger.info(f"候选股总数: {total_candidates}")
    logger.info(f"次日涨停数: {total_limit_up} ({limit_rate:.1f}%)")
    logger.info(f"平均开盘强度: {avg_open:+.2f}%")
    logger.info(f"次日平均收益: {avg_return:+.2f}%")
    logger.info(f"胜率: {win_rate:.1f}%")
    logger.info("=" * 60)

    return result


def main():
    parser = argparse.ArgumentParser(description="强更强策略 — 日线回测")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--extreme-days-min", type=int, default=5)
    parser.add_argument("--volume-expand-ratio", type=float, default=1.5)
    args = parser.parse_args()

    backtest(
        start_date=args.start_date,
        end_date=args.end_date,
        extreme_days_min=args.extreme_days_min,
        volume_expand_ratio=args.volume_expand_ratio,
    )


if __name__ == "__main__":
    main()
