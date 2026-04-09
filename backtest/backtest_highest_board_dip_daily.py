# -*- coding: utf-8 -*-
"""
最高板板块共振低吸策略 — 日线级别回测

验证策略筛选效果：
1. 从 review 数据获取每个交易日的最高板股票
2. 检查一字板历史、次日分歧程度、低吸收益

用法:
    cd /home/fy/myown/livetrade
    python backtest/backtest_highest_board_dip_daily.py --start-date 2026-03-01 --end-date 2026-04-02
"""

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

import config
from strategies.strategy_utils import (
    calc_limit_up_price, ensure_pre_close, is_one_word_limit_up,
    load_klines, get_sorted_dates, get_prev_dates,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def load_review_machine(review_date: str) -> dict:
    """加载 review machine.json。"""
    path = config.REVIEW_DAILY_DIR / review_date / config.REVIEW_MACHINE_FILENAME
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_highest_board(machine: dict) -> list[dict]:
    """从 review 数据获取最高板股票。"""
    ladder = machine.get("board_stats", {}).get("consecutive_board_ladder", {})
    if not ladder:
        return []
    max_height = max(int(h) for h in ladder.keys())
    return ladder.get(str(max_height), [])


def check_one_word_history(
    klines: pd.DataFrame, symbol: str, prev_dates: list[str], min_days: int = 2,
) -> int:
    """检查最近N天中一字板的天数。"""
    recent = prev_dates[-10:]
    sym_data = klines[
        (klines["symbol"] == symbol) & (klines["date"].isin(recent))
    ].sort_values("date").copy()

    if len(sym_data) < min_days + 1:
        return 0

    ensure_pre_close(sym_data)
    lr = 0.20 if symbol.startswith(("300", "301", "688")) else 0.10
    sym_data["limit_up_price"] = (sym_data["pre_close"] * (1 + lr)).round(2)

    # 检查昨日之前（不含昨日本身）的一字板天数
    count = 0
    for _, row in sym_data.iloc[:-1].iterrows():
        if is_one_word_limit_up(row["open"], row["high"], row["low"],
                                row["close"], row["limit_up_price"]):
            count += 1
    return count


def find_candidates(
    klines: pd.DataFrame, date: str, prev_dates: list[str],
    one_word_days_min: int = 2,
) -> list[dict]:
    """对某一日执行筛选逻辑。"""
    # 需要 review 数据获取最高板
    # review_date 是 date 前一个交易日的复盘
    review_date = prev_dates[-1] if prev_dates else ""
    machine = load_review_machine(review_date)

    high_boards = get_highest_board(machine)
    if not high_boards:
        return []

    candidates = []
    for stock in high_boards:
        sym = str(stock.get("symbol", "")).zfill(6)
        name = str(stock.get("name", ""))
        board_count = int(stock.get("board_count", 0))

        one_word_count = check_one_word_history(klines, sym, prev_dates, one_word_days_min)

        # 获取前一日数据
        prev_date = prev_dates[-1]
        prev_data = klines[(klines["symbol"] == sym) & (klines["date"] == prev_date)]
        if prev_data.empty:
            continue
        y = prev_data.iloc[0]

        candidates.append({
            "symbol": sym,
            "name": name,
            "board_count": board_count,
            "one_word_count": one_word_count,
            "prev_close": float(y["close"]),
            "prev_amount": float(y.get("amount", 0)),
        })

    return candidates


def check_next_day(
    symbol: str, prev_close: float, next_date: str, klines: pd.DataFrame,
) -> dict | None:
    """检查次日表现（分歧程度+收益）。"""
    mask = (klines["symbol"] == symbol) & (klines["date"] == next_date)
    next_day = klines[mask]
    if next_day.empty:
        return None

    row = next_day.iloc[0]
    open_ = float(row["open"])
    high = float(row["high"])
    low = float(row["low"])
    close = float(row["close"])
    pct_chg = float(row.get("pct_chg", 0))
    amount = float(row.get("amount", 0))

    # 分歧指标
    amplitude = float(row.get("amplitude", (high - low) / prev_close * 100 if prev_close else 0))
    max_dip = (low - prev_close) / prev_close * 100 if prev_close else 0

    return {
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "pct_chg": pct_chg,
        "amount": amount,
        "amplitude": round(amplitude, 2),
        "max_dip": round(max_dip, 2),
        "is_limit_up": close >= calc_limit_up_price(prev_close, symbol) * 0.99,
        "buy_at_dip_return": round((close - low) / low * 100, 2) if low > 0 else 0,
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
    dip_returns = []
    close_returns = []

    for i, date in enumerate(date_range):
        if i >= len(date_range) - 1:
            break

        prev_dates = get_prev_dates(sorted_dates, date)
        if len(prev_dates) < 5:
            continue

        candidates = find_candidates(klines, date, prev_dates, **params)
        if not candidates:
            continue

        next_date = date_range[i + 1]

        for cand in candidates:
            sym = cand["symbol"]
            next_perf = check_next_day(sym, cand["prev_close"], next_date, klines)
            if next_perf is None:
                continue

            total_candidates += 1
            if next_perf["is_limit_up"]:
                total_limit_up += 1
            if next_perf["max_dip"] < -3:
                dip_returns.append(next_perf["buy_at_dip_return"])
            close_returns.append(next_perf["pct_chg"])

            details.append({
                "date": date,
                **cand,
                **next_perf,
            })

            logger.info(
                f"  {date} {sym} {cand['name']} {cand['board_count']}板 "
                f"(一字{cand['one_word_count']}天) "
                f"→ 次日收{next_perf['pct_chg']:+.2f}% "
                f"最大回撤{next_perf['max_dip']:.1f}% "
                f"振幅{next_perf['amplitude']:.1f}% "
                f"{'回封' if next_perf['is_limit_up'] else '未回封'}"
            )

    limit_rate = (total_limit_up / total_candidates * 100) if total_candidates else 0
    avg_dip_return = np.mean(dip_returns) if dip_returns else 0
    avg_close_return = np.mean(close_returns) if close_returns else 0

    result = {
        "start_date": start_date,
        "end_date": end_date,
        "total_candidates": total_candidates,
        "total_limit_up": total_limit_up,
        "limit_up_rate": round(limit_rate, 2),
        "avg_dip_return": round(avg_dip_return, 2),
        "avg_close_return": round(avg_close_return, 2),
        "dip_return_count": len(dip_returns),
        "details": details,
    }

    logger.info("\n" + "=" * 60)
    logger.info("最高板板块共振低吸 — 日线回测汇总")
    logger.info("=" * 60)
    logger.info(f"回测日期: {start_date} ~ {end_date}")
    logger.info(f"最高板候选总数: {total_candidates}")
    logger.info(f"次日回封数: {total_limit_up} ({limit_rate:.1f}%)")
    logger.info(f"低吸平均收益(回撤>3%时): {avg_dip_return:+.2f}% ({len(dip_returns)}次)")
    logger.info(f"次日平均收益: {avg_close_return:+.2f}%")
    logger.info("=" * 60)

    return result


def main():
    parser = argparse.ArgumentParser(description="最高板板块共振低吸策略 — 日线回测")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--one-word-days-min", type=int, default=2)
    args = parser.parse_args()

    backtest(
        start_date=args.start_date,
        end_date=args.end_date,
        one_word_days_min=args.one_word_days_min,
    )


if __name__ == "__main__":
    main()
