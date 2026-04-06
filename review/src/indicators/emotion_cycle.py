"""
情绪周期指标计算

输出：涨停/跌停统计、连板梯队、首板溢价率、昨涨停表现
"""
import pandas as pd
import numpy as np
from decimal import Decimal, ROUND_HALF_UP

from config import GEM_PREFIX, STAR_PREFIX


def _round_price(values) -> np.ndarray:
    """A股交易所标准四舍五入（非Python银行家舍入），精确到分。"""
    def _rh(v):
        return float(Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    if isinstance(values, (pd.Series, np.ndarray)):
        return np.array([_rh(v) for v in values])
    return _rh(values)


def compute_emotion_cycle(dc) -> dict:
    """
    计算情绪周期指标。

    Args:
        dc: DataCollector 实例

    Returns:
        {
            "date": str,
            "limit_up_count": int,
            "limit_down_count": int,
            "limit_ratio": float,
            "limit_up_stocks": [{symbol, name, pct_chg, close}],
            "limit_down_stocks": [{symbol, name, pct_chg, close}],
            "consecutive_board": {
                "max_height": int,
                "ladder": {"N": [{symbol, name, board_count}]}
            },
            "first_board_premium": float | null,
            "yesterday_limit_up_performance": {
                "avg_pct_chg": float,
                "win_rate": float,
                "count": int
            },
            "daily_limit_up_trend": [{date, limit_up, limit_down}]
        }
    """
    result = {
        "date": dc.date,
        "limit_up_count": 0,
        "limit_down_count": 0,
        "limit_ratio": 0,
        "limit_up_stocks": [],
        "limit_down_stocks": [],
        "consecutive_board": {"max_height": 0, "ladder": {}},
        "first_board_premium": None,
        "yesterday_limit_up_performance": {},
        "daily_limit_up_trend": [],
    }

    day_data = dc.get_day_klines()
    if day_data.empty:
        return result

    # 过滤 ST 和上市前5日新股（无涨跌停限制）
    day_data = _filter_st(day_data, dc)
    day_data = _filter_new_stocks(day_data, dc.date, dc.klines)

    # ── 涨停 / 跌停 ──
    limit_up_df = _get_limit_stocks(day_data, direction="up")
    limit_down_df = _get_limit_stocks(day_data, direction="down")

    result["limit_up_count"] = len(limit_up_df)
    result["limit_down_count"] = len(limit_down_df)
    result["limit_ratio"] = (
        round(len(limit_up_df) / len(limit_down_df), 2)
        if len(limit_down_df) > 0
        else float("inf") if len(limit_up_df) > 0 else 0
    )

    # 涨停股列表
    result["limit_up_stocks"] = _to_stock_list(limit_up_df, dc)
    result["limit_down_stocks"] = _to_stock_list(limit_down_df, dc)

    # ── 连板梯队 ──
    result["consecutive_board"] = _calc_consecutive_board(dc)

    # ── 首板溢价率 ──
    result["first_board_premium"] = _calc_first_board_premium(dc)

    # ── 昨日涨停今日表现 ──
    result["yesterday_limit_up_performance"] = _calc_yesterday_limit_up_perf(dc)

    # ── 每日涨停趋势（最近 10 天）──
    result["daily_limit_up_trend"] = _calc_daily_limit_trend(dc, days=10)

    # ── 5日情绪节奏矩阵（完整per-day指标）──
    result["emotion_5d_matrix"] = _calc_emotion_5d_matrix(dc, days=5)

    return result


def _get_limit_price(symbols: pd.Series, pre_close: pd.Series, direction: str) -> pd.Series:
    """计算涨停/跌停价（支持主板10%、创业板/科创板20%、北交所30%）"""
    sym_str = symbols.astype(str).str.zfill(6)
    is_gem_star = sym_str.str.startswith((GEM_PREFIX,)) | sym_str.str.startswith((STAR_PREFIX,))
    is_bse = sym_str.str.startswith(("4", "8", "920"))

    if direction == "up":
        limit_price = np.where(is_bse, pre_close * 1.3,
                      np.where(is_gem_star, pre_close * 1.2, pre_close * 1.1))
    else:
        limit_price = np.where(is_bse, pre_close * 0.7,
                      np.where(is_gem_star, pre_close * 0.8, pre_close * 0.9))

    return pd.Series(_round_price(limit_price), index=symbols.index)


def _get_limit_stocks(day_data: pd.DataFrame, direction: str) -> pd.DataFrame:
    """获取涨停或跌停股票"""
    if day_data.empty or "pre_close" not in day_data.columns:
        return pd.DataFrame()

    limit_price = _get_limit_price(day_data["symbol"], day_data["pre_close"], direction)

    if direction == "up":
        return day_data[day_data["close"] >= limit_price].copy()
    else:
        return day_data[day_data["close"] <= limit_price].copy()


def _filter_st(df: pd.DataFrame, dc) -> pd.DataFrame:
    """过滤 ST 股票"""
    if dc.stock_basic.empty or "name" not in dc.stock_basic.columns:
        return df
    st_symbols = dc.stock_basic[
        dc.stock_basic["name"].fillna("").str.upper().str.contains("ST")
    ]["symbol"].astype(str).str.zfill(6).tolist()
    return df[~df["symbol"].isin(st_symbols)]


def _filter_new_stocks(df: pd.DataFrame, target_date: str, klines: pd.DataFrame, days: int = 5) -> pd.DataFrame:
    """
    过滤上市不足 N 个交易日的新股（上市前5日无涨跌停限制）。

    通过统计该股票在 klines 中截至 target_date 的交易天数来判断。
    """
    if df.empty or klines.empty:
        return df
    # 计算每只股票截至 target_date 的交易天数
    hist = klines[klines["date"] <= target_date].copy()
    trade_days = hist.groupby("symbol")["date"].nunique()
    new_stock_symbols = trade_days[trade_days <= days].index.tolist()
    if not new_stock_symbols:
        return df
    return df[~df["symbol"].isin(new_stock_symbols)]


def _to_stock_list(df: pd.DataFrame, dc) -> list[dict]:
    """将 DataFrame 转为简单 stock list"""
    if df.empty:
        return []
    records = []
    for _, row in df.iterrows():
        symbol = str(row["symbol"]).zfill(6)
        records.append({
            "symbol": symbol,
            "name": dc.get_stock_name(symbol),
            "close": round(float(row.get("close", 0)), 2),
            "pct_chg": round(float(row.get("pct_chg", 0)), 2),
        })
    return sorted(records, key=lambda x: x["pct_chg"], reverse=True)


def _calc_consecutive_board(dc) -> dict:
    """
    计算连板梯队。
    
    算法：回溯最近 N 个交易日，对今日涨停股计算连续涨停天数。
    """
    result = {"max_height": 0, "ladder": {}}

    day_data = dc.get_day_klines()
    if day_data.empty:
        return result

    day_data = _filter_st(day_data, dc)
    today_limit_up = _get_limit_stocks(day_data, "up")
    if today_limit_up.empty:
        return result

    # 获取最近 15 个交易日的数据用于回溯
    dates = dc.get_trading_dates(15)
    klines = dc.get_stock_klines(days=15)
    if klines.empty:
        return result

    # 对每只今日涨停股，检查连续涨停天数
    ladder = {}
    for _, row in today_limit_up.iterrows():
        symbol = str(row["symbol"]).zfill(6)
        board_count = _count_consecutive_limit_up(symbol, dates, klines)
        if board_count >= 2:  # 只统计 2 连板及以上
            key = str(board_count)
            if key not in ladder:
                ladder[key] = []
            ladder[key].append({
                "symbol": symbol,
                "name": dc.get_stock_name(symbol),
                "board_count": board_count,
            })

    max_height = max([int(k) for k in ladder.keys()], default=1)

    return {
        "max_height": max_height,
        "ladder": dict(sorted(ladder.items(), key=lambda x: int(x[0]), reverse=True)),
    }


def _count_consecutive_limit_up(symbol: str, dates: list[str], klines: pd.DataFrame) -> int:
    """回溯计算某股票的连续涨停天数"""
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

        is_gem_star = symbol.startswith(GEM_PREFIX) or symbol.startswith(STAR_PREFIX)
        ratio = 1.2 if is_gem_star else 1.1
        limit_price = round(pre_close * ratio, 2)

        if close >= limit_price:
            count += 1
        else:
            break

    return count


def _calc_first_board_premium(dc) -> float | None:
    """
    计算首板溢价率：昨日首次涨停（非连板）的股票，今日的平均涨跌幅。
    """
    dates = dc.get_trading_dates(3)
    if len(dates) < 2:
        return None

    today = dates[0]
    yesterday = dates[1]
    day_before = dates[2] if len(dates) >= 3 else None

    klines = dc.get_stock_klines(days=5)
    if klines.empty:
        return None

    # 昨日涨停股
    yesterday_data = _filter_st(klines[klines["date"] == yesterday].copy(), dc)
    yesterday_limit = _get_limit_stocks(yesterday_data, "up")
    if yesterday_limit.empty:
        return None

    # 排除前天也涨停的（留下首板）
    first_board_symbols = set(yesterday_limit["symbol"].tolist())
    if day_before:
        day_before_data = _filter_st(klines[klines["date"] == day_before].copy(), dc)
        day_before_limit = _get_limit_stocks(day_before_data, "up")
        if not day_before_limit.empty:
            first_board_symbols -= set(day_before_limit["symbol"].tolist())

    if not first_board_symbols:
        return None

    # 这些首板股今日的涨跌幅
    today_data = klines[klines["date"] == today]
    today_first_board = today_data[today_data["symbol"].isin(first_board_symbols)]
    if today_first_board.empty or "pct_chg" not in today_first_board.columns:
        return None

    return round(float(today_first_board["pct_chg"].mean()), 2)


def _calc_yesterday_limit_up_perf(dc) -> dict:
    """昨日所有涨停今日的表现（平均涨跌幅 + 胜率）"""
    dates = dc.get_trading_dates(2)
    if len(dates) < 2:
        return {}

    today = dates[0]
    yesterday = dates[1]

    klines = dc.get_stock_klines(days=5)
    if klines.empty:
        return {}

    yesterday_data = _filter_st(klines[klines["date"] == yesterday].copy(), dc)
    yesterday_limit = _get_limit_stocks(yesterday_data, "up")
    if yesterday_limit.empty:
        return {}

    symbols = yesterday_limit["symbol"].tolist()
    today_data = klines[(klines["date"] == today) & (klines["symbol"].isin(symbols))]
    if today_data.empty or "pct_chg" not in today_data.columns:
        return {}

    avg_chg = round(float(today_data["pct_chg"].mean()), 2)
    win_count = int((today_data["pct_chg"] > 0).sum())
    total = len(today_data)

    return {
        "avg_pct_chg": avg_chg,
        "win_rate": round(win_count / total * 100, 1) if total > 0 else 0,
        "count": total,
    }


def _calc_daily_limit_trend(dc, days: int = 10) -> list[dict]:
    """最近 N 天的每日涨停/跌停数量趋势"""
    dates = dc.get_trading_dates(days)
    klines = dc.get_stock_klines(days=days)
    if klines.empty:
        return []

    records = []
    for d in dates:
        day_data = _filter_st(klines[klines["date"] == d].copy(), dc)
        if day_data.empty:
            continue
        limit_up = _get_limit_stocks(day_data, "up")
        limit_down = _get_limit_stocks(day_data, "down")
        records.append({
            "date": d,
            "limit_up": len(limit_up),
            "limit_down": len(limit_down),
        })

    return sorted(records, key=lambda x: x["date"])


def _calc_emotion_5d_matrix(dc, days: int = 5) -> list[dict]:
    """
    最近 N 个交易日的完整情绪节奏矩阵。

    每天包含：成交额、涨跌家数、涨停跌停、封板率、连板高度、首板溢价、情绪定性。
    """
    dates = dc.get_trading_dates(days + 3)  # 多取几天，用于首板溢价回溯
    if len(dates) < 2:
        return []

    target_dates = dates[:days]  # 降序，最近 N 天
    klines = dc.get_stock_klines(days=days + 5)  # 多取几天用于连板回溯
    if klines.empty:
        return []

    # 使用完整 klines 判断新股，避免停牌股被误判
    full_klines = dc.klines

    records = []
    for i, d in enumerate(target_dates):
        day_data = _filter_st(klines[klines["date"] == d].copy(), dc)
        day_data = _filter_new_stocks(day_data, d, full_klines)
        if day_data.empty:
            continue

        # 涨停/跌停
        limit_up_df = _get_limit_stocks(day_data, "up")
        limit_down_df = _get_limit_stocks(day_data, "down")
        limit_up = len(limit_up_df)
        limit_down = len(limit_down_df)

        # 触板/炸板/封板率
        hit_limit_up = 0
        broken_limit_up = 0
        if "pre_close" in day_data.columns:
            limit_price = _get_limit_price(day_data["symbol"], day_data["pre_close"], "up")
            hit_mask = day_data["high"] >= limit_price - 0.011
            hit_limit_up = int(hit_mask.sum())
            broken_limit_up = hit_limit_up - limit_up
        seal_rate = round(limit_up / hit_limit_up, 4) if hit_limit_up > 0 else 0.0

        # 涨跌家数
        advance = int((day_data["pct_chg"] > 0).sum()) if "pct_chg" in day_data.columns else 0
        decline = int((day_data["pct_chg"] < 0).sum()) if "pct_chg" in day_data.columns else 0
        total = len(day_data)
        advance_pct = round(advance / total * 100, 1) if total > 0 else 0

        # 成交额（千元→亿元）
        total_amount_yi = round(float(day_data["amount"].sum()) / 100000.0, 2) if "amount" in day_data.columns else 0

        # 连板高度 & 2连板/3连板数
        market_height = 0
        streak2_count = 0
        streak3_count = 0
        if not limit_up_df.empty:
            all_dates = sorted(klines["date"].unique(), reverse=True)
            for _, row in limit_up_df.iterrows():
                symbol = str(row["symbol"]).zfill(6)
                bc = _count_consecutive_limit_up(symbol, all_dates, klines)
                market_height = max(market_height, bc)
                if bc >= 2:
                    streak2_count += 1
                if bc >= 3:
                    streak3_count += 1

        # 首板溢价率
        first_board_premium = _calc_first_board_premium_for_date(d, dates, klines, dc)

        # 情绪定性
        emotion_label = _classify_emotion_phase(
            advance=advance, total=total, seal_rate=seal_rate,
            limit_up=limit_up, limit_down=limit_down,
            market_height=market_height, streak2_count=streak2_count,
        )

        records.append({
            "date": d,
            "total_amount_yi": total_amount_yi,
            "advance": advance,
            "decline": decline,
            "advance_pct": advance_pct,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "hit_limit_up": hit_limit_up,
            "broken_limit_up": broken_limit_up,
            "seal_rate": round(seal_rate * 100, 1),  # 百分比形式
            "streak2_count": streak2_count,
            "streak3_count": streak3_count,
            "market_height": market_height,
            "first_board_premium": first_board_premium,
            "emotion_label": emotion_label,
        })

    return sorted(records, key=lambda x: x["date"])


def _calc_first_board_premium_for_date(
    target_date: str, all_dates: list[str], klines: pd.DataFrame, dc
) -> float | None:
    """
    计算指定日期的首板溢价率。

    即 target_date 的前一个交易日中首次涨停的股票，在 target_date 当日的平均涨跌幅。
    """
    # 找到 target_date 在 all_dates 中的位置
    try:
        idx = all_dates.index(target_date)
    except ValueError:
        return None

    if idx + 1 >= len(all_dates):
        return None
    yesterday = all_dates[idx + 1]  # all_dates 降序

    day_before = all_dates[idx + 2] if idx + 2 < len(all_dates) else None

    # 昨日涨停股
    yesterday_data = _filter_st(klines[klines["date"] == yesterday].copy(), dc)
    yesterday_limit = _get_limit_stocks(yesterday_data, "up")
    if yesterday_limit.empty:
        return None

    # 排除前天也涨停的（留下首板）
    first_board_symbols = set(yesterday_limit["symbol"].tolist())
    if day_before:
        day_before_data = _filter_st(klines[klines["date"] == day_before].copy(), dc)
        day_before_limit = _get_limit_stocks(day_before_data, "up")
        if not day_before_limit.empty:
            first_board_symbols -= set(day_before_limit["symbol"].tolist())

    if not first_board_symbols:
        return None

    # 这些首板股在 target_date 的涨跌幅
    today_data = klines[klines["date"] == target_date]
    today_first_board = today_data[today_data["symbol"].isin(first_board_symbols)]
    if today_first_board.empty or "pct_chg" not in today_first_board.columns:
        return None

    return round(float(today_first_board["pct_chg"].mean()), 2)


def _classify_emotion_phase(
    *, advance: int, total: int, seal_rate: float,
    limit_up: int, limit_down: int,
    market_height: int, streak2_count: int,
) -> str:
    """
    简化版情绪阶段分类（与 strategy_quant/features.py 的 build_market_environment 逻辑对齐）。
    """
    advance_ratio = advance / total if total > 0 else 0

    # 情绪评分（简化版，与 features.py 公式一致）
    emotion_score = (
        35 * max(0, min(1, (advance_ratio - 0.30) / 0.45))
        + 20 * max(0, min(1, seal_rate))
        + 15 * max(0, min(1, limit_up / 80.0))
        + 10 * max(0, min(1, market_height / 8.0))
        + 10 * max(0, min(1, streak2_count / 25.0))
        - 20 * max(0, min(1, limit_down / 20.0))
    )
    emotion_score = max(0, min(100, emotion_score))

    # 高潮
    if advance > 3800 and seal_rate > 0.75 and limit_up >= 60 and limit_down <= 5:
        return "高潮"
    # 主升
    if market_height >= 5 and emotion_score >= 60 and limit_down <= 10:
        return "主升"
    # 冰点
    if ((advance < 1000 or advance_ratio < 0.25) and seal_rate < 0.60) or emotion_score < 15:
        return "冰点"
    # 轮动
    if market_height <= 4 and limit_up < 50:
        return "轮动"
    # 默认
    return "中性"

