"""
热门板块指标计算（Phase 2 完善）

MVP 阶段：提取涨停股 + 高波动股代码列表，供 SKILL 中调用 svk 分析
"""
import pandas as pd
import numpy as np

from config import GEM_PREFIX, STAR_PREFIX, HIGH_VOLATILITY_PCT, HIGH_VOLATILITY_TOP_N


def compute_hot_sectors(dc) -> dict:
    """
    计算热门板块相关指标。

    MVP 阶段：输出涨停股和高波动股的代码列表，
    供 SKILL.md 指导 AI 调用 svk cluster/analyze。

    Returns:
        {
            "limit_up_symbols": [涨停股代码列表],
            "high_volatility_symbols": [高波动股代码列表],
            "combined_symbols": [去重合并列表],
            "svk_command_hint": "svk analyze xxx xxx ... -k 5 --top 5"
        }
    """
    day_data = dc.get_day_klines()
    if day_data.empty:
        return {
            "limit_up_symbols": [],
            "high_volatility_symbols": [],
            "combined_symbols": [],
            "svk_command_hint": "",
        }

    # 过滤 ST
    basic = dc.stock_basic
    if not basic.empty and "name" in basic.columns:
        st_symbols = basic[
            basic["name"].fillna("").str.upper().str.contains("ST")
        ]["symbol"].astype(str).str.zfill(6).tolist()
        day_data = day_data[~day_data["symbol"].isin(st_symbols)]

    # 涨停股
    limit_up_symbols = _get_limit_up_symbols(day_data)

    # 高波动股（涨幅 > 阈值，成交量加权排序）
    high_vol_symbols = _get_high_volatility_symbols(day_data)

    # 合并去重
    combined = list(dict.fromkeys(limit_up_symbols + high_vol_symbols))

    # 生成 svk 命令提示
    svk_hint = ""
    if combined:
        codes_str = " ".join(combined[:80])  # 限制数量避免命令过长
        k = min(max(3, len(combined) // 10), 8)
        svk_hint = f"svk analyze {codes_str} -k {k} --top 5"

    return {
        "limit_up_symbols": limit_up_symbols,
        "high_volatility_symbols": high_vol_symbols,
        "combined_symbols": combined,
        "svk_command_hint": svk_hint,
    }


def _get_limit_up_symbols(day_data: pd.DataFrame) -> list[str]:
    """获取涨停股代码列表"""
    if day_data.empty or "pre_close" not in day_data.columns:
        return []

    is_gem_star = day_data["symbol"].astype(str).str.startswith((GEM_PREFIX,)) | \
                  day_data["symbol"].astype(str).str.startswith((STAR_PREFIX,))
    limit_price = np.where(is_gem_star, day_data["pre_close"] * 1.2, day_data["pre_close"] * 1.1)
    limit_price = np.round(limit_price, 2)

    mask = day_data["close"] >= limit_price
    return day_data[mask]["symbol"].tolist()


def _get_high_volatility_symbols(day_data: pd.DataFrame) -> list[str]:
    """
    获取高波动股：涨幅 > 阈值，按（涨幅 × 成交量权重）排序取 Top N
    """
    if day_data.empty or "pct_chg" not in day_data.columns:
        return []

    high_vol = day_data[day_data["pct_chg"] > HIGH_VOLATILITY_PCT].copy()
    if high_vol.empty:
        return []

    # 成交量加权评分：涨幅 × log(成交额 + 1)
    if "amount" in high_vol.columns:
        high_vol["score"] = high_vol["pct_chg"] * np.log1p(high_vol["amount"])
    else:
        high_vol["score"] = high_vol["pct_chg"]

    high_vol = high_vol.nlargest(HIGH_VOLATILITY_TOP_N, "score")
    return high_vol["symbol"].tolist()
