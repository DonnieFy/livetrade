# -*- coding: utf-8 -*-
"""
策略：波动率收敛突破

核心逻辑:
    大幅拉升后回踩均线、波动率收敛，某日突然放量突破前高的买点。

案例: 301373 — 2/25~3/10 近乎翻倍后回踩20日线，波动率逐日收敛，
      某日分时快速拉升突破前日高点。

筛选条件:
    1. 近60天内有过大幅拉升（>=80% in <=15天）
    2. 当前股价在MA20附近（±5%以内）
    3. 近3天高点递减（高点逐渐走低，今天突然突破）
    4. 分时突破前日高点 + 波动率放大 + 放量

prepare() 阶段:
    - 读取日线数据，逐股检测条件1~3，筛选候选池

on_tick() 阶段:
    - 检测分时突破前日高点
    - 检测波动率放大
    - 检测放量确认
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy
from strategies.strategy_utils import (
    detect_rally,
    load_klines,
    get_sorted_dates,
    get_prev_dates,
)

logger = logging.getLogger(__name__)


@register_strategy
class VolatilityContractionStrategy(BaseStrategy):
    slug = "volatility_contraction"
    name = "波动率收敛突破"
    description = "大幅拉升后回踩均线、波动率收敛后放量突破前高"

    def prepare(self, ctx: StrategyContext) -> None:
        klines = load_klines()
        if klines is None:
            ctx.state["ready"] = False
            return

        date = ctx.market.date
        sorted_dates = get_sorted_dates(klines)
        prev_dates = get_prev_dates(sorted_dates, date)
        if len(prev_dates) < 20:
            ctx.state["ready"] = False
            return

        # 参数
        rally_gain_pct = ctx.params.get("rally_gain_pct", 80.0)
        rally_max_days = ctx.params.get("rally_max_days", 15)
        rally_lookback = ctx.params.get("rally_lookback", 60)
        ma_proximity = ctx.params.get("ma_proximity", 0.05)
        vol_expand_multiple = ctx.params.get("vol_expand_multiple", 1.5)
        volume_ratio = ctx.params.get("volume_ratio", 1.2)
        daily_amount_unit = float(ctx.params.get("daily_amount_unit", 1000.0))

        # 取最近60天数据用于计算
        recent_dates = prev_dates[-rally_lookback:]
        recent = klines[klines["date"].isin(recent_dates)].copy()

        # 候选股过滤（如果有手动指定 candidates）
        candidate_symbols = None
        if ctx.candidates:
            candidate_symbols = {
                code[2:] if len(code) > 2 else code for code in ctx.candidates
            }
            recent = recent[recent["symbol"].isin(candidate_symbols)]

        candidates = {}

        for sym, grp in recent.groupby("symbol"):
            grp = grp.sort_values("date")
            if len(grp) < 20:
                continue

            closes = grp["close"].values

            # 条件1: 近期大幅拉升
            rally = detect_rally(closes, min_gain_pct=rally_gain_pct,
                                 max_days=rally_max_days, lookback=rally_lookback)
            if rally is None:
                continue

            # 条件2: 当前股价在MA20附近
            ma20 = float(closes[-20:].mean())
            last_close = float(closes[-1])
            if abs(last_close - ma20) / ma20 > ma_proximity:
                continue

            # 条件3: 近3天高点递减（高点逐渐走低）
            last_3 = grp.tail(3)
            if len(last_3) < 3:
                continue
            highs_3d = last_3["high"].values
            is_highs_declining = all(highs_3d[i] > highs_3d[i + 1] for i in range(len(highs_3d) - 1))
            if not is_highs_declining:
                continue

            # 记录候选股特征
            amplitudes = last_3["amplitude"].values
            avg_amplitude_3d = float(amplitudes.mean())
            avg_amount_5d = float(grp.tail(5)["amount"].mean()) * daily_amount_unit
            prev_high = float(grp.iloc[-1]["high"])  # 前一日最高价
            pre_close = float(grp.iloc[-1]["close"])  # 前一日收盘价（当日昨收）

            candidates[sym] = {
                "ma20": ma20,
                "prev_high": prev_high,
                "pre_close": pre_close,
                "avg_amplitude_3d": avg_amplitude_3d,
                "avg_amount_5d": avg_amount_5d,
                "rally_gain": rally["gain_pct"],
                "rally_span": rally["span"],
            }

        ctx.state["candidates"] = candidates
        ctx.state["ready"] = True
        ctx.state["vol_expand_multiple"] = vol_expand_multiple
        ctx.state["volume_ratio"] = volume_ratio
        ctx.state["daily_amount_unit"] = daily_amount_unit
        ctx.state["alerted_codes"] = set()

        logger.info(
            f"[{self.slug}] prepare 完成，"
            f"筛选出 {len(candidates)} 只候选股"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        candidates = ctx.state.get("candidates", {})
        if not candidates:
            return []

        alerted = ctx.state.get("alerted_codes", set())
        vol_expand_multiple = ctx.state.get("vol_expand_multiple", 1.5)
        volume_ratio_threshold = ctx.state.get("volume_ratio", 1.2)

        alerts = []

        codes = frame["code"].values
        nows = frame["now"].values
        closes = frame["close"].values
        lows = frame["low"].values if "low" in frame.columns else None
        highs = frame["high"].values if "high" in frame.columns else None
        names = frame["name"].values if "name" in frame.columns else None
        volumes = frame["volume"].values if "volume" in frame.columns else None
        pct_chgs = frame["pct_chg"].values if "pct_chg" in frame.columns else None

        snapshots = ctx.stock_snapshots

        for i in range(len(codes)):
            code = codes[i]
            if code in alerted:
                continue

            pure_code = code[2:] if len(code) > 2 else code
            feat = candidates.get(pure_code)
            if feat is None:
                continue

            now_price = nows[i]
            pre_close = closes[i]
            if now_price <= 0 or pre_close <= 0:
                continue

            # 条件4a: 分时突破前日高点
            if now_price <= feat["prev_high"]:
                continue

            # 条件4b: 波动率放大
            snap = snapshots.get(code)
            if snap and snap.high > 0 and snap.low < 999999:
                intraday_amplitude = (snap.high - snap.low) / pre_close * 100
                if intraday_amplitude < feat["avg_amplitude_3d"] * vol_expand_multiple:
                    continue
            else:
                if highs is not None and lows is not None:
                    h_val = highs[i]
                    l_val = lows[i]
                    if h_val > 0 and l_val < 999999:
                        intraday_amplitude = (h_val - l_val) / pre_close * 100
                        if intraday_amplitude < feat["avg_amplitude_3d"] * vol_expand_multiple:
                            continue
                    else:
                        continue
                else:
                    continue

            # 条件4c: 放量确认
            today_amount = volumes[i] if volumes is not None else 0
            if feat["avg_amount_5d"] > 0:
                if today_amount < feat["avg_amount_5d"] * volume_ratio_threshold:
                    continue

            pct_chg = pct_chgs[i] if pct_chgs is not None else 0
            name = names[i] if names is not None else ""

            alerts.append(Alert(
                code=code,
                name=name,
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=(
                    f"突破前高 {feat['prev_high']:.2f}→{now_price:.2f}, "
                    f"涨幅{pct_chg:.2f}%, "
                    f"振幅放大{intraday_amplitude:.1f}%vs3日均{feat['avg_amplitude_3d']:.1f}%, "
                    f"此前拉升{feat['rally_gain']:.0f}%/{feat['rally_span']}天"
                ),
                level="important",
            ))
            alerted.add(code)

        return alerts
