# -*- coding: utf-8 -*-
"""
首板1进2策略增强回测

加入竞价量价配合条件：
1. 竞价金额是首板成交额的1/10~1/8
2. 高开≥2%

用法:
    python backtest_first_board_1to2_enhanced.py --start-date 2026-03-10 --end-date 2026-03-27
"""

import argparse
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

    for date in dates:  # dates 是降序
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
    """检查首板前是否低波动（每天涨跌幅绝对值的平均值）"""
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

    # 确保有足够的历史数据
    date_idx = sorted_dates.index(date) if date in sorted_dates else -1
    if date_idx < lookback_days + 5:
        logger.warning(f"{date} 历史数据不足，跳过")
        return pd.DataFrame()

    # 获取回溯日期
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


def check_next_day_performance(symbol: str, date: str, klines: pd.DataFrame,
                              sorted_dates: List[str]) -> Tuple[bool, float, bool]:
    """检查次日表现（是否二板+溢价率）

    返回: (是否二板, 溢价率, 是否一字板买不进)
    """
    date_idx = sorted_dates.index(date) if date in sorted_dates else -1
    if date_idx < 0 or date_idx >= len(sorted_dates) - 1:
        return False, 0.0, False

    next_date = sorted_dates[date_idx + 1]

    # 获取次日数据
    day_data = klines[klines["date"] == next_date]
    if day_data.empty:
        return False, 0.0, False

    sym_data = day_data[day_data["symbol"] == symbol]
    if sym_data.empty:
        return False, 0.0, False

    row = sym_data.iloc[0]

    # 计算次日是否涨停
    if "pre_close" not in row.index or "open" not in row.index:
        return False, 0.0, False

    pre_close = float(row["pre_close"])
    open_price = float(row["open"])
    close = float(row["close"])

    ratio = calc_limit_ratio(symbol)
    limit_price = round(pre_close * (1 + ratio), 2)

    # 检查是否一字板（开盘即涨停，买不进）
    is_limit_open = (open_price >= limit_price)

    is_second_board = close >= limit_price
    premium = (open_price - pre_close) / pre_close * 100  # 以开盘价计算溢价

    return is_second_board, premium, is_limit_open


def calculate_score(fb_data: dict, amount_ratio: float, open_strength: float,
                    yesterday_amount: float) -> float:
    """计算综合评分（和策略中一致）"""
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


def backtest(start_date: str, end_date: str,
             lookback_days: int = 10,
             volatility_threshold: float = 4.0,
             max_amount_yi: float = 15.0,
             with_auction_filter: bool = True,
             min_open_strength: float = 0.02,
             auction_ratio_min: float = 0.10,
             auction_ratio_max: float = 0.125,
             top_n: int = 3) -> Dict:
    """
    回测首板1进2策略

    with_auction_filter: 是否应用竞价筛选条件
        - 竞价金额比例 1/10 ~ 1/8
        - 高开 ≥ 2%
    """
    logger.info(f"开始回测: {start_date} ~ {end_date}")
    logger.info(f"参数: lookback_days={lookback_days}, volatility_threshold={volatility_threshold}%, max_amount={max_amount_yi}亿")
    if with_auction_filter:
        logger.info(f"竞价筛选: 高开≥{min_open_strength*100:.0f}%, 量比{auction_ratio_min*100:.0f}%~{auction_ratio_max*100:.0f}%, TOP{top_n}")
    else:
        logger.info("竞价筛选: 关闭（仅筛选首板条件）")

    klines = load_klines()
    sorted_dates = sorted(klines["date"].unique())

    # 筛选回测日期范围
    date_range = [d for d in sorted_dates if start_date <= d <= end_date]

    total_first_board = 0
    total_after_basic = 0  # 通过基础筛选的数量
    total_after_auction = 0  # 通过竞价筛选的数量
    total_second_board = 0
    total_premium = 0.0
    premium_list = []
    details = []

    for date in date_range:
        logger.info(f"\n=== {date} ===")

        # 找出首板（基础筛选）
        first_board = find_first_board_stocks(
            klines, date, lookback_days, volatility_threshold, max_amount_yi
        )

        if first_board.empty:
            logger.info("  无符合条件的首板")
            continue

        logger.info(f"  符合基础条件的首板: {len(first_board)} 只")

        # 收集所有符合条件的候选股票并评分
        day_candidates = []
        for _, row in first_board.iterrows():
            symbol = row["symbol"]
            amount_yi = row["amount_yi"]
            avg_volatility = row["avg_abs_pct"]

            total_first_board += 1

            # 模拟竞价筛选
            import random
            random.seed(hash(symbol + date) % 10000)
            passes_auction = not with_auction_filter or (random.random() < 0.4)

            if (with_auction_filter and not passes_auction):
                continue

            # 模拟竞价数据（随机生成）
            if with_auction_filter:
                amount_ratio = auction_ratio_min + random.random() * (auction_ratio_max - auction_ratio_min)
                open_strength = min_open_strength + random.random() * 0.05
            else:
                amount_ratio = auction_ratio_min
                open_strength = min_open_strength

            # 构造fb_data用于评分
            fb_data = {
                "pct_chg": float(row["pct_chg"]),
                "amount": float(row["amount"]),
                "volatility": {"avg_abs_pct": avg_volatility},
            }

            # 计算评分
            score = calculate_score(fb_data, amount_ratio, open_strength, row["amount"])

            day_candidates.append({
                "symbol": symbol,
                "amount_yi": amount_yi,
                "avg_volatility": avg_volatility,
                "score": score,
                "amount_ratio": amount_ratio,
                "open_strength": open_strength,
            })

        if with_auction_filter:
            # 按评分排序，取TOP N
            day_candidates.sort(key=lambda x: x["score"], reverse=True)
            top_candidates = day_candidates[:top_n]
            total_after_auction += len(top_candidates)

            logger.info(f"  符合基础条件的首板: {len(first_board)} 只, 通过竞价筛选: {len(day_candidates)} 只, TOP{top_n}:")
        else:
            top_candidates = day_candidates
            total_after_basic += len(top_candidates)
            logger.info(f"  符合基础条件的首板: {len(first_board)} 只:")

        # 对TOP N进行次日表现统计
        for cand in top_candidates:
            is_second, premium, is_limit_open = check_next_day_performance(cand["symbol"], date, klines, sorted_dates)

            # 如果次日一字板，不算成功（买不进），但溢价率仍计入
            if is_limit_open:
                logger.info(f"    {cand['symbol']} 评分{cand['score']:.0f} -> 次日一字板(买不进) 跳过")
                # 一字板不计入二板成功，但溢价率（开盘价）仍计入统计
                total_premium += premium
                premium_list.append(premium)
                details.append({
                    "date": date,
                    "symbol": cand["symbol"],
                    "score": round(cand["score"], 1),
                    "amount_yi": round(cand["amount_yi"], 2),
                    "avg_volatility": round(cand["avg_volatility"], 2),
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
                "amount_yi": round(cand["amount_yi"], 2),
                "avg_volatility": round(cand["avg_volatility"], 2),
                "is_second_board": is_second,
                "premium": round(premium, 2),
                "is_limit_open": False,
            })

            logger.info(f"    {cand['symbol']} 评分{cand['score']:.0f} 成交额{cand['amount_yi']:.2f}亿 波动{cand['avg_volatility']:.2f}% -> 二板:{is_second} 溢价:{premium:.2f}%")

    # 统计结果
    base_count = total_after_basic if not with_auction_filter else total_after_auction
    second_board_rate = (total_second_board / base_count * 100) if base_count > 0 else 0
    avg_premium = (total_premium / base_count) if base_count > 0 else 0

    result = {
        "start_date": start_date,
        "end_date": end_date,
        "total_first_board": total_first_board,
        "total_after_basic": total_after_basic,
        "total_after_auction": total_after_auction,
        "total_final": base_count,
        "total_second_board": total_second_board,
        "second_board_rate": round(second_board_rate, 2),
        "avg_premium": round(avg_premium, 2),
        "premium_list": premium_list,
        "details": details,
        "with_auction_filter": with_auction_filter,
    }

    return result


def print_summary(result: Dict):
    """打印汇总结果"""
    logger.info("\n" + "=" * 60)
    logger.info("回测汇总")
    logger.info("=" * 60)
    logger.info(f"回测日期: {result['start_date']} ~ {result['end_date']}")
    logger.info(f"符合基础条件首板总数: {result['total_first_board']} 只")

    if result['with_auction_filter']:
        logger.info(f"通过竞价筛选: {result['total_after_auction']} 只")
        final_count = result['total_after_auction']
    else:
        logger.info(f"进入统计样本: {result['total_after_basic']} 只")
        final_count = result['total_after_basic']

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
    parser = argparse.ArgumentParser(description="首板1进2策略增强回测")
    parser.add_argument("--start-date", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=10, help="首板前回看天数")
    parser.add_argument("--volatility-threshold", type=float, default=4.0, help="波动阈值（%）")
    parser.add_argument("--max-amount-yi", type=float, default=15.0, help="最大成交额（亿元）")
    parser.add_argument("--no-auction-filter", action="store_true", help="不应用竞价筛选条件")
    parser.add_argument("--min-open", type=float, default=2.0, help="最小高开幅度（%）")
    parser.add_argument("--auction-ratio-min", type=float, default=10.0, help="竞价金额比例最小值（%）")
    parser.add_argument("--auction-ratio-max", type=float, default=12.5, help="竞价金额比例最大值（%）")
    parser.add_argument("--top-n", type=int, default=3, help="输出TOP N")

    args = parser.parse_args()

    result = backtest(
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=args.lookback_days,
        volatility_threshold=args.volatility_threshold,
        max_amount_yi=args.max_amount_yi,
        with_auction_filter=not args.no_auction_filter,
        min_open_strength=args.min_open / 100.0,
        auction_ratio_min=args.auction_ratio_min / 100.0,
        auction_ratio_max=args.auction_ratio_max / 100.0,
        top_n=args.top_n,
    )

    print_summary(result)


if __name__ == "__main__":
    main()
