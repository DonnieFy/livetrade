"""
异动数据加载与分析 — 读取九阳公社 action 数据，结合 k-lines 计算板块成交额与趋势

数据来源:
  - stock-vector-knowledge/data/jiuyangongshe/action/YYYY-MM-DD.json
  - ashares-k-lines/data/klines_daily.csv.gz (通过 DataCollector)

输出:
  - 板块级异动汇总 (涨停数、连板数、成交金额 from klines)
  - 整体情绪指标 (涨停总量、连板总量、最高连板高度)
  - 多日趋势 (最近 N 天的板块强度变化, top3 板块)
  - 板块 3/5/10/20 日涨幅趋势
"""
import json
from pathlib import Path

import pandas as pd

from config import ACTION_DATA_DIR


def _normalize_symbol(code: str) -> str:
    """sh603538 -> 603538"""
    code = str(code)
    if code.startswith(("sh", "sz", "bj")) and len(code) >= 8:
        return code[2:].zfill(6)
    return code.zfill(6)


def load_action_data(date: str, action_dir: Path = ACTION_DATA_DIR) -> dict | None:
    """Load a single day's action JSON file."""
    path = action_dir / f"{date}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _extract_sector_symbols(raw: dict) -> dict[str, list[dict]]:
    """Parse action JSON into {sector_name: [stock_info_dict, ...]}."""
    result: dict[str, list[dict]] = {}
    for field in raw.get("fields", []):
        sector_name = field.get("name", "未知")
        if sector_name in ("ST板块", "新股"):
            continue
        stocks = field.get("stocks", [])
        if not stocks:
            continue
        parsed = []
        for stk in stocks:
            parsed.append({
                "code": _normalize_symbol(stk.get("code", "")),
                "name": stk.get("name", ""),
                # day: 观察天数 — 该股被九阳公社异动追踪关注的总天数（含非涨停日）
                "day": int(stk.get("day", 1) or 1),
                # edition: 期间涨停次数 — 在 day 天内实际涨停的次数（不要求连续！）
                #   例: day=5, edition=3 表示 "5天3板"，即5天内涨停了3次，中间可能断开
                #   注意: 这与 emotion_cycle 中的"连板"(consecutive board)是不同概念
                "edition": int(stk.get("edition", 0) or 0),
                "shares_range": float(stk.get("shares_range", 0) or 0),
            })
        result[sector_name] = parsed
    return result


def compute_action_indicators(
    date: str,
    klines: pd.DataFrame | None = None,
    action_dir: Path = ACTION_DATA_DIR,
) -> dict:
    """Compute action-based indicators for a single date.

    If *klines* is provided, sector total_amount (亿) and avg_pct_chg are
    computed from the actual daily bar data instead of the action JSON's
    ``shares_range`` field.

    Returns:
        {
            "date": str,
            "available": bool,
            "total_limit_up_count": int,
            "total_streak_count": int,
            "max_streak_height": int,
            "total_limit_up_amount_yi": float,
            "sectors": [{
                "name", "stock_count", "limit_up_count", "streak_count",
                "max_height", "total_amount_yi", "avg_pct_chg",
                "top_stocks": [{code, name, day, edition, amount_yi, pct_chg}]
            }],
        }
    """
    raw = load_action_data(date, action_dir)
    if raw is None:
        return {"date": date, "available": False}

    sector_map = _extract_sector_symbols(raw)

    # Build a quick klines lookup for the target date
    day_klines: pd.DataFrame | None = None
    if klines is not None and not klines.empty:
        mask = klines["date"] == date
        if mask.any():
            day_klines = klines[mask].copy()
            day_klines["symbol"] = day_klines["symbol"].astype(str).str.zfill(6)
            day_klines = day_klines.set_index("symbol")

    sectors = []
    total_limit_up = 0
    total_streak = 0
    max_height = 0
    total_amount_yi = 0.0

    for sector_name, stocks in sector_map.items():
        s_limit_up = 0
        s_streak = 0
        s_max_height = 0
        s_total_amount_yi = 0.0
        s_pct_chg_sum = 0.0
        s_pct_chg_count = 0
        top_stocks = []

        for stk in stocks:
            code = stk["code"]
            day = stk["day"]
            edition = stk["edition"]

            # Get amount and pct_chg from klines if available
            amount_yi = 0.0
            pct_chg = 0.0
            if day_klines is not None and code in day_klines.index:
                row = day_klines.loc[code]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                # Tushare amount in 千元 -> 亿
                amount_yi = round(float(row.get("amount", 0)) / 100000.0, 4)
                pct_chg = round(float(row.get("pct_chg", 0)), 2)
            else:
                # Fallback to action's shares_range
                amount_yi = round(stk["shares_range"] / 10000.0, 4)  # 万 -> 亿

            if edition >= 1:
                s_limit_up += 1
            if day >= 2:
                s_streak += 1
            s_max_height = max(s_max_height, day)
            s_total_amount_yi += amount_yi
            s_pct_chg_sum += pct_chg
            s_pct_chg_count += 1

            top_stocks.append({
                "code": code,
                "name": stk["name"],
                "day": day,
                "edition": edition,
                "amount_yi": round(amount_yi, 2),
                "pct_chg": pct_chg,
            })

        top_stocks.sort(key=lambda x: (x["day"], x["amount_yi"]), reverse=True)
        avg_pct_chg = round(s_pct_chg_sum / s_pct_chg_count, 2) if s_pct_chg_count > 0 else 0.0

        sectors.append({
            "name": sector_name,
            "stock_count": len(stocks),
            "limit_up_count": s_limit_up,
            "streak_count": s_streak,
            "max_height": s_max_height,
            "total_amount_yi": round(s_total_amount_yi, 2),
            "avg_pct_chg": avg_pct_chg,
            "top_stocks": top_stocks[:5],
        })

        total_limit_up += s_limit_up
        total_streak += s_streak
        max_height = max(max_height, s_max_height)
        total_amount_yi += s_total_amount_yi

    sectors.sort(key=lambda x: (x["limit_up_count"], x["streak_count"]), reverse=True)

    return {
        "date": date,
        "available": True,
        "total_limit_up_count": total_limit_up,
        "total_streak_count": total_streak,
        "max_streak_height": max_height,
        "total_limit_up_amount_yi": round(total_amount_yi, 2),
        "sectors": sectors,
    }


def compute_action_trend(
    dates: list[str],
    klines: pd.DataFrame | None = None,
    action_dir: Path = ACTION_DATA_DIR,
) -> dict:
    """Compute multi-day action trend with top 3 sectors per day.

    For ``sector_trends``, each stock's 3/5/10/20-day cumulative return is
    looked up from *klines*, then averaged across all stocks in the sector.
    """
    records = []
    # Collect all symbols per sector across all dates (for trend calc)
    sector_all_symbols: dict[str, set[str]] = {}
    sector_daily_meta: dict[str, list[dict]] = {}

    for d in dates:
        ind = compute_action_indicators(d, klines=klines, action_dir=action_dir)
        if not ind.get("available"):
            continue

        top3 = [s["name"] for s in ind["sectors"][:3]]
        records.append({
            "date": d,
            "limit_up_count": ind["total_limit_up_count"],
            "streak_count": ind["total_streak_count"],
            "max_height": ind["max_streak_height"],
            "amount_yi": ind["total_limit_up_amount_yi"],
            "top_sectors": top3,
        })

        for sec in ind["sectors"]:
            name = sec["name"]
            if name not in sector_all_symbols:
                sector_all_symbols[name] = set()
                sector_daily_meta[name] = []
            for stk in sec.get("top_stocks", []):
                sector_all_symbols[name].add(stk["code"])
            # Also add all stock codes from the full list
            raw = load_action_data(d, action_dir)
            if raw:
                for field in raw.get("fields", []):
                    if field.get("name") == name:
                        for stk in field.get("stocks", []):
                            sector_all_symbols[name].add(
                                _normalize_symbol(stk.get("code", ""))
                            )
            sector_daily_meta[name].append({
                "date": d,
                "amount_yi": sec["total_amount_yi"],
                "limit_up_count": sec["limit_up_count"],
            })

    sorted_records = sorted(records, key=lambda x: x["date"])
    target_date = sorted_records[-1]["date"] if sorted_records else ""

    sector_trends = _compute_sector_trends(
        sector_all_symbols, sector_daily_meta, klines, target_date,
    )

    return {
        "daily": sorted_records,
        "sector_trends": sector_trends,
    }


def _compute_stock_nday_returns(
    symbols: set[str],
    klines: pd.DataFrame,
    target_date: str,
) -> dict[str, dict[str, float]]:
    """For each symbol, compute N-day cumulative return ending at target_date.

    Returns {symbol: {"ret_3": float, "ret_5": float, ...}}
    """
    if klines is None or klines.empty or not symbols:
        return {}

    sub = klines[klines["symbol"].isin(symbols)].copy()
    if sub.empty:
        return {}

    # Get sorted unique dates up to target_date
    all_dates = sorted(sub["date"].unique())
    if target_date not in all_dates:
        # Use closest prior date
        prior = [d for d in all_dates if d <= target_date]
        if not prior:
            return {}
        target_date = prior[-1]

    target_idx = all_dates.index(target_date)
    results: dict[str, dict[str, float]] = {}

    for sym in symbols:
        stk_data = sub[sub["symbol"] == sym].set_index("date")
        if target_date not in stk_data.index:
            continue
        target_close = float(stk_data.loc[target_date]["close"])
        if target_close <= 0:
            continue

        ret_dict: dict[str, float] = {}
        for n in (3, 5, 10, 20):
            lookback_idx = target_idx - n
            if lookback_idx < 0:
                continue
            lookback_date = all_dates[lookback_idx]
            if lookback_date in stk_data.index:
                base_close = float(stk_data.loc[lookback_date]["close"])
                if base_close > 0:
                    ret_dict[f"ret_{n}"] = round(
                        (target_close - base_close) / base_close * 100, 2
                    )
        if ret_dict:
            results[sym] = ret_dict

    return results


def _compute_sector_trends(
    sector_all_symbols: dict[str, set[str]],
    sector_daily_meta: dict[str, list[dict]],
    klines: pd.DataFrame | None,
    target_date: str,
) -> list[dict]:
    """Compute per-sector trends using actual multi-day stock returns from klines.

    For each sector, looks up every stock's 3/5/10/20 day return from klines,
    then averages across all stocks. Also includes amount trends from daily meta.
    """
    if klines is None or klines.empty:
        return []

    # Compute returns for all unique symbols at once
    all_symbols: set[str] = set()
    for syms in sector_all_symbols.values():
        all_symbols |= syms
    stock_returns = _compute_stock_nday_returns(all_symbols, klines, target_date)

    results = []
    for name, symbols in sector_all_symbols.items():
        meta_entries = sector_daily_meta.get(name, [])
        if len(meta_entries) < 2:
            continue

        meta_entries.sort(key=lambda x: x["date"])
        amt_list = [e["amount_yi"] for e in meta_entries]

        # Average each N-day return across sector stocks
        ret_agg: dict[str, list[float]] = {
            "ret_3": [], "ret_5": [], "ret_10": [], "ret_20": [],
        }
        for sym in symbols:
            if sym in stock_returns:
                for key in ret_agg:
                    if key in stock_returns[sym]:
                        ret_agg[key].append(stock_returns[sym][key])

        def _avg(lst: list[float]) -> float | None:
            return round(sum(lst) / len(lst), 2) if lst else None

        results.append({
            "name": name,
            "active_days": len(meta_entries),
            "stock_count": len(symbols),
            "avg_ret_3d": _avg(ret_agg["ret_3"]),
            "avg_ret_5d": _avg(ret_agg["ret_5"]),
            "avg_ret_10d": _avg(ret_agg["ret_10"]),
            "avg_ret_20d": _avg(ret_agg["ret_20"]),
            "amount_yi_latest": amt_list[-1] if amt_list else 0,
            "amount_yi_trend": amt_list[-3:],
            "total_limit_ups": sum(e["limit_up_count"] for e in meta_entries),
        })

    results.sort(key=lambda x: (x["active_days"], x.get("avg_ret_3d") or 0), reverse=True)
    return results

