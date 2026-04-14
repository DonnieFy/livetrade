# -*- coding: utf-8 -*-
"""
策略：首板1+1

核心逻辑:
    前天首板、昨日断板的股票，今日快速突破昨日最高价，构成1+1涨停买点。

案例: 柏诚股份(601133) — 4/7首板(+10%), 4/8断板(收+4.7%, 高23.16),
      4/9平开后快速突破23.16, 最终涨停(+10%)。

筛选条件:
    1. 前天(倒数第2个交易日)首板涨停
    2. 昨天未涨停（断板），但形态尚好（收盘 > MA5）
    3. 今天分时突破昨日最高价，且突破幅度 >= breakout_pct_min（过滤假突破）
    4. 放量确认（成交量放大）

prepare() 阶段:
    - 读取日线，检测条件1~2，记录昨日最高价

on_tick() 阶段:
    - 检测分时价格突破昨日最高价
    - 检测放量确认
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy
from strategies.strategy_utils import (
    calc_limit_ratio,
    calc_moving_averages,
    ensure_pre_close,
    load_klines,
    get_sorted_dates,
    get_prev_dates,
    calc_tick_rv,
)

logger = logging.getLogger(__name__)


@register_strategy
class FirstBoard1Plus1Strategy(BaseStrategy):
    slug = "first_board_1plus1"
    name = "首板1+1"
    description = "前天首板、昨天断板，今日突破昨高构成1+1买点"

    def prepare(self, ctx: StrategyContext) -> None:
        klines = load_klines()
        if klines is None:
            ctx.state["ready"] = False
            return

        date = ctx.market.date
        sorted_dates = get_sorted_dates(klines)
        prev_dates = get_prev_dates(sorted_dates, date)
        if len(prev_dates) < 10:
            ctx.state["ready"] = False
            return

        # 参数
        volume_ratio = ctx.params.get("volume_ratio", 1.0)
        rv_min = ctx.params.get("rv_min", 0.0)
        breakout_pct_min = ctx.params.get("breakout_pct_min", 0.0)
        pct_chg_min = ctx.params.get("pct_chg_min", 0.0)
        daily_amount_unit = float(ctx.params.get("daily_amount_unit", 1000.0))

        # 需要3天: 前天(首板日)、昨天(断板日)、今天(交易日)
        if len(prev_dates) < 2:
            ctx.state["ready"] = False
            return

        yesterday = prev_dates[-1]
        day_before = prev_dates[-2]

        # 取近10天数据
        recent_dates = prev_dates[-10:]
        recent = klines[klines["date"].isin(recent_dates)].copy()
        ensure_pre_close(recent)

        # 候选股过滤
        if ctx.candidates:
            candidate_symbols = {
                code[2:] if len(code) > 2 else code for code in ctx.candidates
            }
            recent = recent[recent["symbol"].isin(candidate_symbols)]

        candidates = {}

        for sym, grp in recent.groupby("symbol"):
            grp = grp.sort_values("date")
            if len(grp) < 5:
                continue

            sym_str = str(sym).zfill(6)
            limit_ratio = calc_limit_ratio(sym_str)

            # 前天数据
            db_data = grp[grp["date"] == day_before]
            if db_data.empty:
                continue
            db = db_data.iloc[0]
            db_pre_close = float(db.get("pre_close", db["close"] / (1 + db.get("pct_chg", 0) / 100)))
            db_limit_up = round(db_pre_close * (1 + limit_ratio), 2)

            # 条件1: 前天首板（收盘 >= 涨停价，且前天之前一天不是涨停）
            if db["close"] < db_limit_up:
                continue

            # 检查是否首板：大前天不是涨停
            db_idx = grp[grp["date"] == day_before].index[0]
            db_pos = grp.index.get_loc(db_idx)
            if db_pos >= 1:
                prev_row = grp.iloc[db_pos - 1]
                prev_pre = float(prev_row.get("pre_close", prev_row["close"] / (1 + prev_row.get("pct_chg", 0) / 100)))
                prev_limit = round(prev_pre * (1 + limit_ratio), 2)
                if prev_row["close"] >= prev_limit:
                    continue  # 前天之前也是涨停，不是首板

            # 昨天数据
            yd_data = grp[grp["date"] == yesterday]
            if yd_data.empty:
                continue
            yd = yd_data.iloc[0]
            yd_pre_close = float(yd.get("pre_close", yd["close"] / (1 + yd.get("pct_chg", 0) / 100)))
            yd_limit_up = round(yd_pre_close * (1 + limit_ratio), 2)

            # 条件2: 昨天未涨停（断板）
            if yd["close"] >= yd_limit_up:
                continue

            # 昨天形态尚好：收盘 > MA5
            closes = grp["close"].values
            mas = calc_moving_averages(closes, [5])
            ma5 = mas.get(5, float(yd["close"]))
            if float(yd["close"]) < ma5 * 0.95:  # 允许5%偏离
                continue

            # 记录
            candidates[sym_str] = {
                "yesterday_high": float(yd["high"]),
                "yesterday_close": float(yd["close"]),
                "yesterday_amount": float(yd.get("amount", 0)) * daily_amount_unit,
                "yesterday_pre_close": yd_pre_close,
                "day_before_close": float(db["close"]),
                "ma5": ma5,
            }

        ctx.state["candidates"] = candidates
        ctx.state["ready"] = True
        ctx.state["volume_ratio"] = volume_ratio
        ctx.state["rv_min"] = rv_min
        ctx.state["breakout_pct_min"] = breakout_pct_min
        ctx.state["pct_chg_min"] = pct_chg_min
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
        volume_ratio_threshold = ctx.state.get("volume_ratio", 1.0)
        rv_min = ctx.state.get("rv_min", 0.0)
        breakout_pct_min = ctx.state.get("breakout_pct_min", 0.0)
        pct_chg_min = ctx.state.get("pct_chg_min", 0.0)
        # 增量维护每只候选股的价格序列，避免每帧扫描全量tick_history
        price_bufs: dict[str, list[float]] = ctx.state.get("_price_bufs", {})
        buf_window = 10

        alerts = []

        codes = frame["code"].values
        nows = frame["now"].values
        closes = frame["close"].values
        names = frame["name"].values if "name" in frame.columns else None
        volumes = frame["volume"].values if "volume" in frame.columns else None
        pct_chgs = frame["pct_chg"].values if "pct_chg" in frame.columns else None

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

            # 增量维护价格buffer（在突破检查前收集，确保突破时有足够数据）
            buf = price_bufs.get(pure_code, [])
            buf.append(float(now_price))
            if len(buf) > buf_window:
                buf = buf[-buf_window:]
            price_bufs[pure_code] = buf

            # 核心条件: 突破昨日最高价
            if now_price <= feat["yesterday_high"]:
                continue

            # 突破幅度过滤：过滤"刚好突破1分钱"的假突破
            breakout_pct = (now_price - feat["yesterday_high"]) / feat["yesterday_high"]
            if breakout_pct_min > 0 and breakout_pct < breakout_pct_min:
                continue

            # 涨幅过滤：涨幅不足说明动能不够
            pct_chg = pct_chgs[i] if pct_chgs is not None else 0
            if pct_chg_min > 0 and pct_chg < pct_chg_min * 100:
                continue

            # Tick级RV过滤（buffer数据不足时跳过）
            rv = 0.0
            if rv_min > 0 and len(buf) >= 5:
                rv = calc_tick_rv(np.array(buf))
                if rv < rv_min:
                    continue

            # 放量确认（可选）
            if volume_ratio_threshold > 0 and feat["yesterday_amount"] > 0:
                today_amount = volumes[i] if volumes is not None else 0
                if today_amount < feat["yesterday_amount"] * volume_ratio_threshold:
                    continue

            name = names[i] if names is not None else ""

            rv_info = f", RV={rv:.4f}" if rv_min > 0 else ""
            alerts.append(Alert(
                code=code,
                name=name,
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=(
                    f"1+1突破: 突破昨高{feat['yesterday_high']:.2f}→{now_price:.2f}(+{breakout_pct*100:.1f}%), "
                    f"涨幅{pct_chg:.2f}%, "
                    f"昨收{feat['yesterday_close']:.2f}(断板), 前收{feat['day_before_close']:.2f}(首板)"
                    f"{rv_info}"
                ),
                level="important",
            ))
            alerted.add(code)

        return alerts
