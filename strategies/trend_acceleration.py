# -*- coding: utf-8 -*-
"""
策略：趋势加速突破

核心逻辑:
    长期趋势向好、短期从底部走强的股票，在波动率确认放大的时刻买入。

案例: 天通股份(600330) — 长期均线多头排列，4/8放量涨7.3%，
      4/9从-1.6%快速拉升到涨停，买点在波动率扩大的分时确认时刻。

筛选条件:
    1. 长期趋势：MA5 > MA20 > MA60（均线多头排列）
    2. 短期走强：近5日低点抬高（底部走强）
    3. 近期有过放量上涨（近5日最大涨幅 > 3%）
    4. 分时波动率放大确认（日内振幅突破近5日平均振幅的N倍）

prepare() 阶段:
    - 读取日线，筛选满足条件1~3的候选股

on_tick() 阶段:
    - 检测分时波动率放大（日内振幅 > 近5日平均振幅 × 倍数）
    - 检测当前涨幅为正（确认是加速而非崩盘）
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy
from strategies.strategy_utils import (
    is_bullish_ma_alignment,
    is_bottom_strengthening,
    load_klines,
    get_sorted_dates,
    get_prev_dates,
    calc_moving_averages,
    calc_tick_rv,
)

logger = logging.getLogger(__name__)


@register_strategy
class TrendAccelerationStrategy(BaseStrategy):
    slug = "trend_acceleration"
    name = "趋势加速突破"
    description = "长期趋势向好+短期底部走强，波动率放大确认时刻买入"

    def prepare(self, ctx: StrategyContext) -> None:
        klines = load_klines()
        if klines is None:
            ctx.state["ready"] = False
            return

        date = ctx.market.date
        sorted_dates = get_sorted_dates(klines)
        prev_dates = get_prev_dates(sorted_dates, date)
        if len(prev_dates) < 60:
            ctx.state["ready"] = False
            return

        # 参数
        vol_expand_multiple = ctx.params.get("vol_expand_multiple", 1.5)
        volume_ratio = ctx.params.get("volume_ratio", 1.0)
        daily_amount_unit = ctx.params.get("daily_amount_unit", 1000.0)
        recent_rally_pct = ctx.params.get("recent_rally_pct", 3.0)
        recent_days = ctx.params.get("recent_days", 5)
        rv_min = ctx.params.get("rv_min", 0.0)

        prev_date = prev_dates[-1]
        recent_dates = prev_dates[-60:]
        recent = klines[klines["date"].isin(recent_dates)].copy()

        # 候选股过滤
        candidate_symbols = None
        if ctx.candidates:
            candidate_symbols = {
                code[2:] if len(code) > 2 else code for code in ctx.candidates
            }
            recent = recent[recent["symbol"].isin(candidate_symbols)]

        candidates = {}

        for sym, grp in recent.groupby("symbol"):
            grp = grp.sort_values("date")
            if len(grp) < 60:
                continue

            closes = grp["close"].values
            lows = grp["low"].values
            highs = grp["high"].values

            # 条件1: 中长期趋势向好 MA20 > MA60（不要求严格多头排列）
            mas = calc_moving_averages(closes, [20, 60])
            ma20 = mas.get(20, 0)
            ma60 = mas.get(60, 0)
            if ma20 <= 0 or ma60 <= 0 or ma20 <= ma60:
                continue

            # 条件2: 近5日低点抬高
            if not is_bottom_strengthening(lows, days=recent_days):
                continue

            # 条件3: 近期有过放量上涨（近N日最大单日涨幅 > 阈值）
            recent_grp = grp.tail(recent_days)
            max_pct = recent_grp["pct_chg"].max() if "pct_chg" in recent_grp.columns else 0
            if max_pct < recent_rally_pct:
                continue

            # 记录特征
            amplitudes = grp.tail(5)["amplitude"].values if "amplitude" in grp.columns else np.array([5.0])
            avg_amplitude_5d = float(amplitudes.mean())
            # klines_daily.amount 常见口径为"千元"，转换到 tick 的"元"口径再比较
            avg_amount_5d = float(grp.tail(5)["amount"].mean()) * float(daily_amount_unit)
            prev_high = float(grp.iloc[-1]["high"])
            pre_close = float(grp.iloc[-1]["close"])

            candidates[sym] = {
                "avg_amplitude_5d": avg_amplitude_5d,
                "avg_amount_5d": avg_amount_5d,
                "prev_high": prev_high,
                "pre_close": pre_close,
                "max_recent_pct": float(max_pct),
            }

        ctx.state["candidates"] = candidates
        ctx.state["ready"] = True
        ctx.state["vol_expand_multiple"] = vol_expand_multiple
        ctx.state["volume_ratio"] = volume_ratio
        ctx.state["daily_amount_unit"] = daily_amount_unit
        ctx.state["rv_min"] = rv_min
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
        volume_ratio_threshold = ctx.state.get("volume_ratio", 1.0)
        rv_min = ctx.state.get("rv_min", 0.0)
        # 增量维护每只候选股的价格序列
        price_bufs: dict[str, list[float]] = ctx.state.get("_price_bufs", {})
        buf_window = 20

        alerts = []

        codes = frame["code"].values
        nows = frame["now"].values
        closes = frame["close"].values
        highs = frame["high"].values if "high" in frame.columns else None
        lows = frame["low"].values if "low" in frame.columns else None
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

            # 增量维护价格buffer
            buf = price_bufs.get(code, [])
            buf.append(float(now_price))
            if len(buf) > buf_window:
                buf = buf[-buf_window:]
            price_bufs[code] = buf

            pct_chg = pct_chgs[i] if pct_chgs is not None else 0

            # 涨幅必须为正（确认是加速而非崩盘）
            if pct_chg <= 0:
                continue

            # 波动率放大：日内振幅 > 近5日平均振幅 × 倍数
            snap = snapshots.get(code)
            if snap and snap.high > 0 and snap.low < 999999:
                intraday_amplitude = (snap.high - snap.low) / pre_close * 100
            elif highs is not None and lows is not None:
                intraday_amplitude = (highs[i] - lows[i]) / pre_close * 100
            else:
                continue

            if intraday_amplitude < feat["avg_amplitude_5d"] * vol_expand_multiple:
                continue

            # 放量确认（可选）
            today_amount = volumes[i] if volumes is not None else 0
            if volume_ratio_threshold > 0 and feat["avg_amount_5d"] > 0:
                if today_amount < feat["avg_amount_5d"] * volume_ratio_threshold:
                    continue

            # Tick级RV过滤：波动率过低说明不是真正的加速
            rv = 0.0
            if rv_min > 0:
                rv = calc_tick_rv(np.array(buf))
                if 0 < rv < rv_min:
                    continue

            name = names[i] if names is not None else ""

            rv_info = f", RV={rv:.4f}" if rv_min > 0 else ""
            alerts.append(Alert(
                code=code,
                name=name,
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=(
                    f"趋势加速: 涨幅{pct_chg:.2f}%, "
                    f"振幅{intraday_amplitude:.1f}%vs5日均{feat['avg_amplitude_5d']:.1f}%, "
                    f"近5日最强{feat['max_recent_pct']:.1f}%{rv_info}"
                ),
                level="important",
            ))
            alerted.add(code)

        return alerts
