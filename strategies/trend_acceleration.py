# -*- coding: utf-8 -*-
"""
策略：趋势加速突破

核心逻辑:
    长期趋势向好、短期从底部走强的股票，在波动率确认放大的时刻买入。

补充分支:
    对于尚未走成中长期均线完全多头、但已经具备大容量和短期主升特征的票，
    若早盘先承压再快速回流并收复关键价位，也属于可交易的主线容量套利。

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
    signal_role = "support"

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
        breakout_pct_min = ctx.params.get("breakout_pct_min", 0.01)
        min_amount_yi = ctx.params.get("min_amount_yi", 8.0)
        rotation_min_amount_yi = float(ctx.params.get("rotation_min_amount_yi", 30.0))
        rotation_min_close_vs_ma20 = float(ctx.params.get("rotation_min_close_vs_ma20", 0.15))
        rotation_min_recent_rally_pct = float(
            ctx.params.get("rotation_min_recent_rally_pct", 4.0)
        )

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
            # klines_daily.amount 常见口径为“千元”，转换到 tick 的“元”口径再比较
            avg_amount_5d = float(grp.tail(5)["amount"].mean()) * float(daily_amount_unit)
            prev_amount_yi = float(grp.iloc[-1]["amount"]) / 100000.0
            prev_high = float(grp.iloc[-1]["high"])
            pre_close = float(grp.iloc[-1]["close"])

            close_vs_ma20 = pre_close / ma20 - 1 if ma20 > 0 else 0.0
            candidate_type = None
            if ma20 > 0 and ma60 > 0 and ma20 > ma60 and prev_amount_yi >= min_amount_yi:
                candidate_type = "trend_accel"
            elif (
                ma20 > 0
                and prev_amount_yi >= rotation_min_amount_yi
                and close_vs_ma20 >= rotation_min_close_vs_ma20
                and max_pct >= rotation_min_recent_rally_pct
            ):
                candidate_type = "capacity_rotation"

            if candidate_type is None:
                continue

            candidates[sym] = {
                "avg_amplitude_5d": avg_amplitude_5d,
                "avg_amount_5d": avg_amount_5d,
                "prev_high": prev_high,
                "pre_close": pre_close,
                "max_recent_pct": float(max_pct),
                "prev_amount_yi": prev_amount_yi,
                "close_vs_ma20": float(close_vs_ma20),
                "candidate_type": candidate_type,
            }

        ctx.state["candidates"] = candidates
        ctx.state["ready"] = True
        ctx.state["vol_expand_multiple"] = vol_expand_multiple
        ctx.state["volume_ratio"] = volume_ratio
        ctx.state["daily_amount_unit"] = daily_amount_unit
        ctx.state["rv_min"] = rv_min
        ctx.state["breakout_pct_min"] = breakout_pct_min
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
        breakout_pct_min = ctx.state.get("breakout_pct_min", 0.01)
        rotation_reclaim_pct = float(ctx.params.get("rotation_reclaim_pct", 0.001))
        rotation_min_rebound_pct = float(ctx.params.get("rotation_min_rebound_pct", 0.015))
        rotation_confirm_by = ctx.params.get("rotation_confirm_by", "09:36:00")
        rotation_volume_ratio = float(ctx.params.get("rotation_volume_ratio", 0.08))
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
            today_amount = volumes[i] if volumes is not None else 0
            candidate_type = feat.get("candidate_type", "trend_accel")

            # 涨幅必须为正（确认是加速而非崩盘）
            if pct_chg <= 0:
                continue

            snap = snapshots.get(code)
            if snap and snap.high > 0 and 0 < snap.low < 999999 and snap.low <= snap.high:
                intraday_amplitude = (snap.high - snap.low) / pre_close * 100
            elif highs is not None and lows is not None and highs[i] > 0 and lows[i] > 0:
                intraday_amplitude = (highs[i] - lows[i]) / pre_close * 100
            else:
                continue

            if candidate_type == "trend_accel":
                if now_price < feat["prev_high"] * (1 + breakout_pct_min):
                    continue

                if intraday_amplitude < feat["avg_amplitude_5d"] * vol_expand_multiple:
                    continue

                # 放量确认（可选）
                if volume_ratio_threshold > 0 and feat["avg_amount_5d"] > 0:
                    if today_amount < feat["avg_amount_5d"] * volume_ratio_threshold:
                        continue

                # Tick级RV过滤：波动率过低说明不是真正的加速
                rv = 0.0
                if rv_min > 0:
                    rv = calc_tick_rv(np.array(buf))
                    if rv < rv_min:
                        continue

                name = names[i] if names is not None else ""

                rv_info = f", RV={rv:.4f}" if rv_min > 0 else ""
                alerts.append(Alert(
                    code=code,
                    name=name,
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=(
                        f"趋势加速: 涨幅(较昨收){pct_chg:.2f}%, "
                        f"突破昨高{feat['prev_high']:.2f}, "
                        f"振幅{intraday_amplitude:.1f}%vs5日均{feat['avg_amplitude_5d']:.1f}%, "
                        f"近5日最大单日涨幅{feat['max_recent_pct']:.1f}%, "
                        f"昨成交额{feat['prev_amount_yi']:.1f}亿{rv_info}"
                    ),
                    level="important",
                ))
                alerted.add(code)
                continue

            if ctx.market.current_time > rotation_confirm_by:
                continue

            if now_price < pre_close * (1 + rotation_reclaim_pct):
                continue

            open_price = snapshots.get(code).open if snapshots.get(code) else pre_close
            if open_price <= 0:
                open_price = pre_close
            if now_price < open_price:
                continue

            rebound_from_low = (now_price - snap.low) / pre_close if snap and pre_close > 0 else 0.0
            if rebound_from_low < rotation_min_rebound_pct:
                continue

            if feat["avg_amount_5d"] > 0 and today_amount < feat["avg_amount_5d"] * rotation_volume_ratio:
                continue

            name = names[i] if names is not None else ""
            alerts.append(Alert(
                code=code,
                name=name,
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=(
                    f"主线容量回流: 涨幅(较昨收){pct_chg:.2f}%, "
                    f"日内低点{snap.low:.2f}回拉至{now_price:.2f}, "
                    f"收复昨收/开盘, 近5日最大单日涨幅{feat['max_recent_pct']:.1f}%, "
                    f"昨成交额{feat['prev_amount_yi']:.1f}亿"
                ),
                level="important",
            ))
            alerted.add(code)

        # 持久化增量状态，避免每帧重置导致 RV 恒为 0
        ctx.state["_price_bufs"] = price_bufs
        ctx.state["alerted_codes"] = alerted

        return alerts
