# -*- coding: utf-8 -*-
"""
波动率收敛突破策略 — 日线级别回测

验证策略筛选效果：
1. 每个交易日筛选出满足条件的候选股（大幅拉升+MA20附近+波动率收敛）
2. 次日检查是否突破前高，统计收益

用法:
    cd /home/fy/myown/livetrade
    python backtest/backtest_volatility_contraction_daily.py --start-date 2026-02-01 --end-date 2026-04-02
"""

import argparse
import logging

import numpy as np
import pandas as pd

import config
from strategies.strategy_utils import (
    detect_rally, load_klines, get_sorted_dates, get_prev_dates, ensure_pre_close,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def find_candidates(
    klines: pd.DataFrame,
    date: str,
    prev_dates: list[str],
    rally_gain_pct: float = 80.0,
    rally_max_days: int = 15,
    rally_lookback: int = 60,
    ma_proximity: float = 0.05,
) -> list[dict]:
    """对某一日执行 prepare 阶段的筛选逻辑，返回候选股列表。"""
    recent_dates = prev_dates[-rally_lookback:]
    recent = klines[klines["date"].isin(recent_dates)]

    candidates = []
    for sym, grp in recent.groupby("symbol"):
        grp = grp.sort_values("date")
        if len(grp) < 20:
            continue

        closes = grp["close"].values

        # 条件1: 大幅拉升
        rally = detect_rally(closes, min_gain_pct=rally_gain_pct,
                             max_days=rally_max_days, lookback=rally_lookback)
        if rally is None:
            continue

        # 条件2: MA20附近
        ma20 = float(closes[-20:].mean())
        last_close = float(closes[-1])
        if abs(last_close - ma20) / ma20 > ma_proximity:
            continue

        # 条件3: 近3天振幅递减
        last_3 = grp.tail(3)
        amplitudes = last_3["amplitude"].values
        if len(amplitudes) < 3:
            continue
        if not all(amplitudes[i] > amplitudes[i + 1] for i in range(len(amplitudes) - 1)):
            continue

        avg_amplitude_3d = float(amplitudes.mean())
        avg_amount_5d = float(grp.tail(5)["amount"].mean())
        prev_high = float(grp.iloc[-1]["high"])

        candidates.append({
            "symbol": sym,
            "close": last_close,
            "ma20": ma20,
            "prev_high": prev_high,
            "avg_amplitude_3d": avg_amplitude_3d,
            "avg_amount_5d": avg_amount_5d,
            "rally_gain": rally["gain_pct"],
            "rally_span": rally["span"],
        })

    return candidates


def check_next_day(
    symbol: str, prev_high: float, date: str, next_date: str, klines: pd.DataFrame,
) -> dict | None:
    """检查次日是否突破前高。"""
    mask = (klines["symbol"] == symbol) & (klines["date"] == next_date)
    next_day = klines[mask]
    if next_day.empty:
        return None

    row = next_day.iloc[0]
    high = float(row["high"])
    close = float(row["close"])
    open_ = float(row["open"])
    pct_chg = float(row.get("pct_chg", 0))
    amount = float(row.get("amount", 0))

    return {
        "high": high,
        "close": close,
        "open": open_,
        "pct_chg": pct_chg,
        "amount": amount,
        "broke_prev_high": high > prev_high,
        "close_return": pct_chg,
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
    total_breakout = 0
    returns = []

    for i, date in enumerate(date_range):
        if i >= len(date_range) - 1:
            break  # 最后一天没有次日数据

        prev_dates = get_prev_dates(sorted_dates, date)
        if len(prev_dates) < 20:
            continue

        candidates = find_candidates(klines, date, prev_dates, **params)
        if not candidates:
            continue

        next_date = date_range[i + 1]
        logger.info(f"  {date}: {len(candidates)} 只候选")

        for cand in candidates:
            sym = cand["symbol"]
            next_perf = check_next_day(sym, cand["prev_high"], date, next_date, klines)
            if next_perf is None:
                continue

            total_candidates += 1
            if next_perf["broke_prev_high"]:
                total_breakout += 1
            returns.append(next_perf["close_return"])

            details.append({
                "date": date,
                "symbol": sym,
                "close": cand["close"],
                "prev_high": cand["prev_high"],
                "rally_gain": cand["rally_gain"],
                "rally_span": cand["rally_span"],
                **next_perf,
            })

            logger.info(
                f"    {sym} 收{cand['close']:.2f} 前高{cand['prev_high']:.2f} "
                f"拉升{cand['rally_gain']:.0f}%/{cand['rally_span']}天 "
                f"→ 次日{next_perf['pct_chg']:+.2f}% "
                f"{'突破' if next_perf['broke_prev_high'] else '未突破'}"
            )

    breakout_rate = (total_breakout / total_candidates * 100) if total_candidates else 0
    avg_return = np.mean(returns) if returns else 0
    win_rate = (sum(1 for r in returns if r > 0) / len(returns) * 100) if returns else 0

    result = {
        "start_date": start_date,
        "end_date": end_date,
        "total_candidates": total_candidates,
        "total_breakout": total_breakout,
        "breakout_rate": round(breakout_rate, 2),
        "avg_return": round(avg_return, 2),
        "win_rate": round(win_rate, 2),
        "returns": returns,
        "details": details,
    }

    # 打印汇总
    logger.info("\n" + "=" * 60)
    logger.info("波动率收敛突破 — 日线回测汇总")
    logger.info("=" * 60)
    logger.info(f"回测日期: {start_date} ~ {end_date}")
    logger.info(f"候选股总数: {total_candidates}")
    logger.info(f"突破前高数: {total_breakout} ({breakout_rate:.1f}%)")
    logger.info(f"次日平均收益: {avg_return:+.2f}%")
    logger.info(f"胜率: {win_rate:.1f}%")
    if returns:
        logger.info(f"收益范围: {min(returns):.2f}% ~ {max(returns):.2f}%")
        logger.info(f"中位数: {np.median(returns):.2f}%")
    logger.info("=" * 60)

    return result


def main():
    parser = argparse.ArgumentParser(description="波动率收敛突破策略 — 日线回测")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--rally-gain-pct", type=float, default=80.0)
    parser.add_argument("--rally-max-days", type=int, default=15)
    parser.add_argument("--ma-proximity", type=float, default=0.05)
    args = parser.parse_args()

    backtest(
        start_date=args.start_date,
        end_date=args.end_date,
        rally_gain_pct=args.rally_gain_pct,
        rally_max_days=args.rally_max_days,
        ma_proximity=args.ma_proximity,
    )


if __name__ == "__main__":
    main()
