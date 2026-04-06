# -*- coding: utf-8 -*-
"""
首板1进2策略回测

验证策略效果：
1. 符合条件的首板有多少只
2. 其中封住二板的有多少只
3. 次日平均溢价率

用法:
    python backtest_first_board_1to2.py --start-date 2026-03-10 --end-date 2026-03-27
"""

import argparse
import logging
import os
from datetime import datetime
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
                        lookback_days: int = 10, volatility_threshold: float = 4.0) -> bool:
    """检查首板前是否低波动（每天涨跌幅绝对值的平均值小于阈值）"""
    sym_data = klines[klines["symbol"] == symbol].copy()
    sym_data = sym_data.sort_values("date")

    # 找到目标日期的位置
    target_idx = sym_data[sym_data["date"] == date].index
    if len(target_idx) == 0:
        return False
    target_idx = target_idx[0]

    # 获取该日期在排序后的位置
    sym_data = sym_data.reset_index(drop=True)
    target_pos = sym_data[sym_data["date"] == date].index[0]

    # 检查前面N天的涨跌幅
    if target_pos < lookback_days:
        return False

    before_board = sym_data.iloc[target_pos - lookback_days:target_pos]
    # 计算每天涨跌幅绝对值的平均值
    avg_abs_pct = before_board["pct_chg"].abs().mean()

    return avg_abs_pct <= volatility_threshold


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
    first_board["low_volatility"] = first_board.apply(
        lambda row: check_low_volatility(
            row["symbol"], date, klines, lookback_days, volatility_threshold
        ),
        axis=1
    )
    first_board = first_board[first_board["low_volatility"]].copy()

    # 筛选成交额
    first_board["amount_yi"] = first_board["amount"] / 100000000.0
    first_board = first_board[first_board["amount_yi"] <= max_amount_yi].copy()

    return first_board


def check_next_day_performance(symbol: str, date: str, klines: pd.DataFrame,
                              sorted_dates: List[str]) -> Tuple[bool, float]:
    """检查次日表现（是否二板+溢价率）"""
    date_idx = sorted_dates.index(date) if date in sorted_dates else -1
    if date_idx < 0 or date_idx >= len(sorted_dates) - 1:
        return False, 0.0

    next_date = sorted_dates[date_idx + 1]

    # 获取次日数据
    day_data = klines[klines["date"] == next_date]
    if day_data.empty:
        return False, 0.0

    sym_data = day_data[day_data["symbol"] == symbol]
    if sym_data.empty:
        return False, 0.0

    row = sym_data.iloc[0]

    # 计算次日是否涨停
    if "pre_close" not in row.index:
        return False, 0.0

    pre_close = float(row["pre_close"])
    close = float(row["close"])

    ratio = calc_limit_ratio(symbol)
    limit_price = round(pre_close * (1 + ratio), 2)

    is_second_board = close >= limit_price
    premium = (close - pre_close) / pre_close * 100

    return is_second_board, premium


def backtest(start_date: str, end_date: str,
             lookback_days: int = 10,
             volatility_threshold: float = 4.0,
             max_amount_yi: float = 15.0) -> Dict:
    """回测首板1进2策略"""
    logger.info(f"开始回测: {start_date} ~ {end_date}")
    logger.info(f"参数: lookback_days={lookback_days}, volatility_threshold={volatility_threshold}%, max_amount={max_amount_yi}亿")

    klines = load_klines()
    sorted_dates = sorted(klines["date"].unique())

    # 筛选回测日期范围
    date_range = [d for d in sorted_dates if start_date <= d <= end_date]

    total_first_board = 0
    total_second_board = 0
    total_premium = 0.0
    premium_list = []
    details = []

    for date in date_range:
        logger.info(f"\n=== {date} ===")

        # 找出首板
        first_board = find_first_board_stocks(
            klines, date, lookback_days, volatility_threshold, max_amount_yi
        )

        if first_board.empty:
            logger.info("  无符合条件的首板")
            continue

        logger.info(f"  符合条件的首板: {len(first_board)} 只")

        # 检查次日表现
        for _, row in first_board.iterrows():
            symbol = row["symbol"]
            name = row.get("name", "")
            amount_yi = row["amount_yi"]

            is_second, premium = check_next_day_performance(symbol, date, klines, sorted_dates)

            total_first_board += 1
            total_premium += premium
            premium_list.append(premium)

            if is_second:
                total_second_board += 1

            details.append({
                "date": date,
                "symbol": symbol,
                "name": name,
                "amount_yi": round(amount_yi, 2),
                "is_second_board": is_second,
                "premium": round(premium, 2),
            })

            logger.info(f"  {symbol} {name} 成交额{amount_yi:.2f}亿 -> 二板:{is_second} 溢价:{premium:.2f}%")

    # 统计结果
    second_board_rate = (total_second_board / total_first_board * 100) if total_first_board > 0 else 0
    avg_premium = (total_premium / total_first_board) if total_first_board > 0 else 0

    result = {
        "start_date": start_date,
        "end_date": end_date,
        "total_first_board": total_first_board,
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
    logger.info("回测汇总")
    logger.info("=" * 60)
    logger.info(f"回测日期: {result['start_date']} ~ {result['end_date']}")
    logger.info(f"符合条件的首板总数: {result['total_first_board']} 只")
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
    parser = argparse.ArgumentParser(description="首板1进2策略回测")
    parser.add_argument("--start-date", required=True, help="开始日期 YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="结束日期 YYYY-MM-DD")
    parser.add_argument("--lookback-days", type=int, default=10, help="首板前回看天数")
    parser.add_argument("--volatility-threshold", type=float, default=4.0, help="波动阈值（%）")
    parser.add_argument("--max-amount-yi", type=float, default=15.0, help="最大成交额（亿元）")

    args = parser.parse_args()

    result = backtest(
        start_date=args.start_date,
        end_date=args.end_date,
        lookback_days=args.lookback_days,
        volatility_threshold=args.volatility_threshold,
        max_amount_yi=args.max_amount_yi,
    )

    print_summary(result)


if __name__ == "__main__":
    main()
