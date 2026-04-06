# -*- coding: utf-8 -*-
"""
首板1进2策略 - 真实tick数据回测

使用真实的竞价tick数据验证策略效果
"""

import argparse
import gzip
import logging
import os
from typing import Dict, List, Tuple

import pandas as pd

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def load_klines() -> pd.DataFrame:
    """加载日线数据"""
    klines_path = config.KLINES_DAILY_FILE
    if not os.path.exists(klines_path):
        logger.error(f"日线数据不存在: {klines_path}")
        raise FileNotFoundError(f"日线数据不存在: {klines_path}")

    klines = pd.read_csv(klines_path, compression="gzip", dtype={"symbol": str})
    klines["date"] = klines["date"].astype(str)
    klines["symbol"] = klines["symbol"].astype(str).str.zfill(6)
    return klines


def calc_limit_ratio(symbol: str) -> float:
    """计算涨停比例"""
    s = str(symbol).zfill(6)
    if s.startswith(("300", "301", "688")):
        return 0.20
    return 0.10


def count_consecutive_limit_up(symbol: str, dates: List[str], klines: pd.DataFrame) -> int:
    """计算连续涨停天数"""
    stock_data = klines[klines["symbol"] == symbol].set_index("date")
    count = 0

    for date in dates:
        if date not in stock_data.index:
            break
        row = stock_data.loc[date]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]

        if "pre_close" not in row.index or "close" not in row.index:
            break

        pre_close = float(row["pre_close"])
        close = float(row["close"])
        if pre_close <= 0:
            break

        ratio = calc_limit_ratio(symbol)
        limit_price = round(pre_close * (1 + ratio), 2)

        if close >= limit_price:
            count += 1
        else:
            break

    return count


def check_low_volatility(symbol: str, date: str, klines: pd.DataFrame,
                        lookback_days: int = 10, volatility_threshold: float = 4.0) -> Tuple[bool, float]:
    """检查首板前是否低波动"""
    sym_data = klines[klines["symbol"] == symbol].copy().sort_values("date").reset_index(drop=True)
    target_pos = sym_data[sym_data["date"] == date].index
    if len(target_pos) == 0:
        return False, 0
    target_pos = target_pos[0]

    if target_pos < lookback_days:
        return False, 0

    before_board = sym_data.iloc[target_pos - lookback_days:target_pos]
    avg_abs_pct = before_board["pct_chg"].abs().mean()

    return avg_abs_pct <= volatility_threshold, avg_abs_pct


def find_first_board_stocks(klines: pd.DataFrame, date: str,
                           lookback_days: int = 10,
                           volatility_threshold: float = 4.0,
                           max_amount_yi: float = 15.0) -> pd.DataFrame:
    """筛选某日的首板股票"""
    sorted_dates = sorted(klines["date"].unique())

    date_idx = sorted_dates.index(date) if date in sorted_dates else -1
    if date_idx < lookback_days + 5:
        logger.warning(f"{date} 历史数据不足，跳过")
        return pd.DataFrame()

    lookback_dates = sorted_dates[max(0, date_idx - 20):date_idx + 1]
    lookback_data = klines[klines["date"].isin(lookback_dates)].copy()

    # 计算涨停价
    if "change" in lookback_data.columns:
        lookback_data["pre_close"] = lookback_data["close"] - lookback_data["change"]
    else:
        lookback_data["pre_close"] = lookback_data["close"] / (1 + lookback_data["pct_chg"] / 100)

    lookback_data["limit_ratio"] = lookback_data["symbol"].apply(calc_limit_ratio)
    lookback_data["limit_up_price"] = (lookback_data["pre_close"] * (1 + lookback_data["limit_ratio"])).round(2)
    lookback_data["is_limit_up"] = lookback_data["close"] == lookback_data["limit_up_price"]

    # 计算连板天数
    consecutive_board = {}
    for symbol in lookback_data["symbol"].unique():
        dates = sorted([d for d in lookback_dates if d < date], reverse=True)
        consecutive_board[symbol] = count_consecutive_limit_up(symbol, dates, lookback_data)

    # 筛选首板
    day_data = lookback_data[lookback_data["date"] == date].copy()
    day_data["consecutive_board"] = day_data["symbol"].map(consecutive_board)
    first_board = day_data[day_data["consecutive_board"] == 1].copy()

    if first_board.empty:
        return pd.DataFrame()

    # 筛选低波动
    vol_results = []
    for _, row in first_board.iterrows():
        is_low_vol, avg_pct = check_low_volatility(
            row["symbol"], date, klines, lookback_days, volatility_threshold
        )
        vol_results.append((is_low_vol, avg_pct))

    first_board["low_volatility"] = [r[0] for r in vol_results]
    first_board["avg_abs_pct"] = [r[1] for r in vol_results]
    first_board = first_board[first_board["low_volatility"]].copy()

    # 筛选成交额
    first_board["amount_yi"] = first_board["amount"] / 100000000.0
    first_board = first_board[first_board["amount_yi"] <= max_amount_yi].copy()

    return first_board


def load_auction_tick(date: str) -> pd.DataFrame:
    """加载竞价tick数据"""
    tick_dir = config.TICKS_DATA_DIR / date
    auction_file = tick_dir / f"{date}_auction_open.csv.gz"

    if not auction_file.exists():
        logger.warning(f"竞价数据不存在: {auction_file}")
        return pd.DataFrame()

    # 读取竞价tick数据（无列头）
    columns = [
        "code", "name", "open", "close", "now", "high", "low",
        "buy", "sell", "turnover", "volume",
        "bid1_volume", "bid1", "bid2_volume", "bid2",
        "bid3_volume", "bid3", "bid4_volume", "bid4",
        "bid5_volume", "bid5",
        "ask1_volume", "ask1", "ask2_volume", "ask2",
        "ask3_volume", "ask3", "ask4_volume", "ask4",
        "ask5_volume", "ask5",
        "date", "time",
    ]

    df = pd.read_csv(auction_file, compression="gzip", header=None, names=columns, dtype={"code": str})
    df["date"] = df["date"].astype(str)
    df["symbol"] = df["code"].str[2:].str.zfill(6)

    return df


def calculate_score(fb_data: dict, amount_ratio: float, open_strength: float,
                    yesterday_amount: float) -> float:
    """计算综合评分"""
    score = 0.0

    # 1. 竞价强度（30分）
    ratio_min, ratio_max = 0.10, 0.125
    ratio_optimal = (ratio_min + ratio_max) / 2

    if amount_ratio < ratio_min:
        ratio_score = 0
    elif amount_ratio > ratio_max * 1.5:
        ratio_score = 5
    else:
        dist = abs(amount_ratio - ratio_optimal)
        ratio_score = max(0, 15 - dist * 100)

    if open_strength < 0.02:
        open_score = 0
    elif open_strength > 0.08:
        open_score = 5
    else:
        open_score = min(15, (open_strength - 0.02) / 0.03 * 15)

    auction_score = ratio_score + open_score

    # 2. 前期走势质量（40分）
    avg_volatility = fb_data.get("volatility", {}).get("avg_abs_pct", 10.0)
    volatility_score = max(0, 25 - avg_volatility * 5)

    yesterday_amount_yi = yesterday_amount / 100000000.0
    if yesterday_amount_yi < 2.0:
        volume_score = 15
    elif yesterday_amount_yi < 10.0:
        volume_score = 10
    elif yesterday_amount_yi < 15.0:
        volume_score = 5
    else:
        volume_score = 0

    quality_score = volatility_score + volume_score

    # 3. 首板质量（30分）
    first_board_pct = fb_data["pct_chg"]
    if first_board_pct >= 9.9:
        board_score = 30
    elif first_board_pct >= 9.0:
        board_score = 20
    elif first_board_pct >= 7.0:
        board_score = 10
    else:
        board_score = 5

    score = auction_score + quality_score + board_score
    return score


def check_next_day_performance(symbol: str, date: str, klines: pd.DataFrame,
                              sorted_dates: List[str]) -> Tuple[bool, float, bool]:
    """检查次日表现"""
    date_idx = sorted_dates.index(date) if date in sorted_dates else -1
    if date_idx < 0 or date_idx >= len(sorted_dates) - 1:
        return False, 0.0, False

    next_date = sorted_dates[date_idx + 1]

    day_data = klines[klines["date"] == next_date]
    if day_data.empty:
        return False, 0.0, False

    sym_data = day_data[day_data["symbol"] == symbol]
    if sym_data.empty:
        return False, 0.0, False

    row = sym_data.iloc[0]

    if "pre_close" not in row.index or "open" not in row.index:
        return False, 0.0, False

    pre_close = float(row["pre_close"])
    open_price = float(row["open"])
    close = float(row["close"])

    ratio = calc_limit_ratio(symbol)
    limit_price = round(pre_close * (1 + ratio), 2)

    is_limit_open = (open_price >= limit_price)
    is_second_board = close >= limit_price
    premium = (open_price - pre_close) / pre_close * 100

    return is_second_board, premium, is_limit_open


def backtest_with_real_tick(start_date: str, end_date: str,
                            lookback_days: int = 10,
                            volatility_threshold: float = 4.0,
                            max_amount_yi: float = 15.0,
                            top_n: int = 3) -> Dict:
    """使用真实tick数据回测"""
    logger.info(f"开始回测: {start_date} ~ {end_date} (使用真实tick数据)")
    logger.info(f"参数: lookback_days={lookback_days}, volatility_threshold={volatility_threshold}%, max_amount={max_amount_yi}亿, TOP{top_n}")

    klines = load_klines()
    sorted_dates = sorted(klines["date"].unique())
    date_range = [d for d in sorted_dates if start_date <= d <= end_date]

    total_first_board = 0
    total_after_auction = 0
    total_second_board = 0
    total_premium = 0.0
    premium_list = []
    details = []

    ratio_min, ratio_max = 0.10, 0.125
    min_open = 0.02

    for date in date_range:
        logger.info(f"\n=== {date} ===")

        # 找出首板
        first_board = find_first_board_stocks(
            klines, date, lookback_days, volatility_threshold, max_amount_yi
        )

        if first_board.empty:
            logger.info("  无符合条件的首板")
            continue

        logger.info(f"  符合基础条件的首板: {len(first_board)} 只")

        # 加载竞价tick数据
        auction_df = load_auction_tick(date)
        if auction_df.empty:
            logger.info("  竞价数据不存在，跳过")
            continue

        # 收集候选股票
        candidates = []
        for _, row in first_board.iterrows():
            symbol = row["symbol"]

            # 在竞价数据中查找该股票
            sym_auction = auction_df[auction_df["symbol"] == symbol]
            if sym_auction.empty:
                continue

            # 取竞价最后时刻的数据
            last_tick = sym_auction.iloc[-1]

            now_price = last_tick["now"]
            close_price = last_tick["close"]
            if now_price <= 0 or close_price <= 0:
                continue

            # 高开判断
            open_strength = (now_price - close_price) / close_price
            if open_strength < min_open:
                continue

            # 竞价金额比例
            yesterday_amount = row["amount"]
            today_volume = last_tick["volume"]
            amount_ratio = today_volume / yesterday_amount if yesterday_amount > 0 else 0

            # 竞价金额比例判断
            if amount_ratio < ratio_min or amount_ratio > ratio_max:
                continue

            # 计算评分
            fb_data = {
                "pct_chg": float(row["pct_chg"]),
                "amount": float(row["amount"]),
                "volatility": {"avg_abs_pct": row["avg_abs_pct"]},
            }

            score = calculate_score(fb_data, amount_ratio, open_strength, yesterday_amount)

            candidates.append({
                "symbol": symbol,
                "amount_yi": row["amount_yi"],
                "avg_volatility": row["avg_abs_pct"],
                "score": score,
                "amount_ratio": amount_ratio,
                "open_strength": open_strength,
            })

        total_first_board += len(first_board)

        if not candidates:
            logger.info("  无通过竞价筛选的股票")
            continue

        # 按评分排序，取TOP N
        candidates.sort(key=lambda x: x["score"], reverse=True)
        top_candidates = candidates[:top_n]
        total_after_auction += len(top_candidates)

        logger.info(f"  通过竞价筛选: {len(candidates)} 只, TOP{top_n}:")

        # 检查次日表现
        for cand in top_candidates:
            is_second, premium, is_limit_open = check_next_day_performance(cand["symbol"], date, klines, sorted_dates)

            if is_limit_open:
                logger.info(f"    {cand['symbol']} 评分{cand['score']:.0f} -> 次日一字板(买不进) 跳过")
                total_premium += premium
                premium_list.append(premium)
                details.append({
                    "date": date,
                    "symbol": cand["symbol"],
                    "score": round(cand["score"], 1),
                    "is_second_board": False,
                    "premium": round(premium, 2),
                    "is_limit_open": True,
                })
                continue

            total_second_board += 1 if is_second else 0
            total_premium += premium
            premium_list.append(premium)

            details.append({
                "date": date,
                "symbol": cand["symbol"],
                "score": round(cand["score"], 1),
                "is_second_board": is_second,
                "premium": round(premium, 2),
                "is_limit_open": False,
            })

            logger.info(f"    {cand['symbol']} 评分{cand['score']:.0f} -> 二板:{is_second} 溢价:{premium:.2f}%")

    # 统计结果
    base_count = total_after_auction
    second_board_rate = (total_second_board / base_count * 100) if base_count > 0 else 0
    avg_premium = (total_premium / base_count) if base_count > 0 else 0

    result = {
        "start_date": start_date,
        "end_date": end_date,
        "total_first_board": total_first_board,
        "total_after_auction": total_after_auction,
        "total_second_board": total_second_board,
        "second_board_rate": round(second_board_rate, 2),
        "avg_premium": round(avg_premium, 2),
        "premium_list": premium_list,
        "details": details,
    }

    return result


def print_summary(result: Dict):
    """打印汇总结果"""
    logger.info("\n" + "=" * 60)
    logger.info("回测汇总（真实tick数据）")
    logger.info("=" * 60)
    logger.info(f"回测日期: {result['start_date']} ~ {result['end_date']}")
    logger.info(f"符合基础条件首板总数: {result['total_first_board']} 只")
    logger.info(f"通过竞价筛选: {result['total_after_auction']} 只")
    logger.info(f"封住二板的数量: {result['total_second_board']} 只")
    logger.info(f"二板成功率: {result['second_board_rate']}%")
    logger.info(f"次日平均溢价率: {result['avg_premium']}%")

    if result['premium_list']:
        logger.info(f"溢价率范围: {min(result['premium_list']):.2f}% ~ {max(result['premium_list']):.2f}%")

    # 按日期统计
    date_stats = {}
    for d in result['details']:
        date = d['date']
        if date not in date_stats:
            date_stats[date] = {'total': 0, 'second': 0}
        date_stats[date]['total'] += 1
        if d['is_second_board']:
            date_stats[date]['second'] += 1

    logger.info("\n按日期统计:")
    for date, stats in sorted(date_stats.items()):
        rate = (stats['second'] / stats['total'] * 100) if stats['total'] > 0 else 0
        logger.info(f"  {date}: {stats['second']}/{stats['total']} ({rate:.1f}%)")

    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="首板1进2策略 - 真实tick数据回测")
    parser.add_argument("--start-date", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=10, help="首板前回看天数")
    parser.add_argument("--volatility-threshold", type=float, default=4.0, help="波动阈值（%）")
    parser.add_argument("--max-amount-yi", type=float, default=15.0, help="最大成交额（亿元）")
    parser.add_argument("--top-n", type=int, default=3, help="输出TOP N")

    args = parser.parse_args()

    result = backtest_with_real_tick(
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=args.lookback_days,
        volatility_threshold=args.volatility_threshold,
        max_amount_yi=args.max_amount_yi,
        top_n=args.top_n,
    )

    print_summary(result)


if __name__ == "__main__":
    main()
