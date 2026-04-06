"""
指数状态指标计算

输出：核心指数涨跌幅、成交额变化率、MA 位置、均线排列、市场宽度
"""
import pandas as pd
import numpy as np

from config import CORE_INDEXES


def compute_index_status(dc) -> dict:
    """
    计算指数状态指标。

    Args:
        dc: DataCollector 实例

    Returns:
        {
            "indexes": {
                "000001.SH": {
                    "name", "close", "pct_chg",
                    "volume_change_pct", "ma5_pos", "ma20_pos",
                    "recent_5d_pct": [最近5天涨幅]
                },
                ...
            },
            "breadth": {
                "date", "advance", "decline", "flat", "total",
                "advance_pct", "decline_pct", "ratio"
            },
            "volume": {
                "total_amount_yi", "prev_amount_yi", "change_pct"
            }
        }
    """
    result = {"indexes": {}, "breadth": {}, "volume": {}}

    # ── 指数数据 ──
    idx_klines = dc.get_index_klines(days=30)
    if not idx_klines.empty:
        for ts_code, name in CORE_INDEXES.items():
            idx_data = idx_klines[idx_klines["ts_code"] == ts_code].sort_values("date")
            if idx_data.empty:
                continue

            latest = idx_data.iloc[-1]
            close = float(latest.get("close", 0))

            # 涨跌幅 — 用 pct_chg 或手动算
            pct_chg = float(latest.get("pct_chg", 0)) if "pct_chg" in idx_data.columns else 0
            if pct_chg == 0 and len(idx_data) >= 2:
                prev_close = float(idx_data.iloc[-2]["close"])
                if prev_close > 0:
                    pct_chg = round((close - prev_close) / prev_close * 100, 2)

            # 成交额变化率
            vol_change = _calc_volume_change(idx_data)

            # MA 位置
            ma5_pos = _calc_ma_position(idx_data, 5)
            ma20_pos = _calc_ma_position(idx_data, 20)

            # 最近 5 天涨幅
            recent_5d = _calc_recent_pct(idx_data, 5)

            result["indexes"][ts_code] = {
                "name": name,
                "close": round(close, 2),
                "pct_chg": round(pct_chg, 2),
                "volume_change_pct": vol_change,
                "ma5_pos": ma5_pos,
                "ma20_pos": ma20_pos,
                "recent_5d_pct": recent_5d,
            }

    # ── 市场宽度（涨跌比）──
    day_data = dc.get_day_klines()
    if not day_data.empty and "pct_chg" in day_data.columns:
        advance = int((day_data["pct_chg"] > 0).sum())
        decline = int((day_data["pct_chg"] < 0).sum())
        flat = int((day_data["pct_chg"] == 0).sum())
        total = len(day_data)

        result["breadth"] = {
            "date": dc.date,
            "advance": advance,
            "decline": decline,
            "flat": flat,
            "total": total,
            "advance_pct": round(advance / total * 100, 1) if total > 0 else 0,
            "decline_pct": round(decline / total * 100, 1) if total > 0 else 0,
            "ratio": round(advance / decline, 2) if decline > 0 else float("inf"),
        }

    # ── 全市场成交额 ──
    if not day_data.empty and "amount" in day_data.columns:
        total_amount = day_data["amount"].sum()
        # Tushare amount 单位千元 → 亿元
        total_yi = round(float(total_amount) / 100000.0, 2)

        prev_data = dc.get_prev_day_klines()
        prev_yi = 0
        if not prev_data.empty and "amount" in prev_data.columns:
            prev_yi = round(float(prev_data["amount"].sum()) / 100000.0, 2)

        change_pct = 0
        if prev_yi > 0:
            change_pct = round((total_yi - prev_yi) / prev_yi * 100, 1)

        result["volume"] = {
            "total_amount_yi": total_yi,
            "prev_amount_yi": prev_yi,
            "change_pct": change_pct,
        }

    return result


def _calc_volume_change(df: pd.DataFrame) -> float:
    """计算最新日成交额相对前日的变化率（%）"""
    if len(df) < 2:
        return 0
    vol_col = "amount" if "amount" in df.columns else "volume" if "volume" in df.columns else "vol"
    if vol_col not in df.columns:
        return 0
    latest_vol = float(df.iloc[-1][vol_col])
    prev_vol = float(df.iloc[-2][vol_col])
    if prev_vol == 0:
        return 0
    return round((latest_vol - prev_vol) / prev_vol * 100, 1)


def _calc_ma_position(df: pd.DataFrame, period: int) -> str:
    """
    判断最新收盘价相对于 MA 的位置。
    返回 "above" / "below" / "unknown"
    """
    if len(df) < period or "close" not in df.columns:
        return "unknown"
    ma = df["close"].tail(period).mean()
    close = float(df.iloc[-1]["close"])
    return "above" if close >= ma else "below"


def _calc_recent_pct(df: pd.DataFrame, n: int) -> list[float]:
    """获取最近 N 天的涨跌幅列表"""
    if df.empty:
        return []
    tail = df.tail(n)
    if "pct_chg" in tail.columns:
        return [round(float(x), 2) for x in tail["pct_chg"].tolist()]
    # 手动计算
    closes = tail["close"].tolist()
    pcts = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            pcts.append(round((closes[i] - closes[i - 1]) / closes[i - 1] * 100, 2))
    return pcts
