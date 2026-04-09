# -*- coding: utf-8 -*-
"""
策略：强更强

核心逻辑:
    近期频繁涨跌停、反包模式下的极端强势股，次日继续加速涨停。
    需要观察其他涨停股的竞价后表现来确认板块/情绪支撑。

案例: 新能泰山 — 近10个交易日不是涨停就是跌停，跌停后次日反包，
      昨日放量涨停，今日高开9%+快速涨停，叠加冰点回暖情绪。

筛选条件:
    1. 近10天涨跌停天数 >= 5
    2. 存在反包模式（跌停日次日收盘 > 跌停日开盘）
    3. 昨日涨停且放量（amount > 5日均额 × 1.5）
    4. 今日开盘 > 昨收 × 1.09
    5. 开盘10分钟内接近涨停价
    6. 其他涨停股竞价后表现强（板块/情绪支撑）

prepare() 阶段:
    - 读取日线，筛选满足条件1~3的候选股
    - 获取昨日涨停股集合（用于on_tick观察）

on_tick() 阶段:
    - 竞价阶段检查开盘强度
    - 通过 ctx.stock_snapshots 观察其他涨停股表现
    - 开盘10分钟内检测快速涨停
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy
from strategies.strategy_utils import (
    calc_limit_ratio,
    calc_limit_up_price,
    calc_limit_down_price,
    ensure_pre_close,
    load_klines,
    get_sorted_dates,
    get_prev_dates,
)

logger = logging.getLogger(__name__)


@register_strategy
class StrongGetsStrongerStrategy(BaseStrategy):
    slug = "strong_gets_stronger"
    name = "强更强"
    description = "近期频繁涨跌停反包后的极端强势股加速信号"

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
        extreme_days_min = ctx.params.get("extreme_days_min", 5)
        lookback_days = ctx.params.get("lookback_days", 10)
        volume_expand_ratio = ctx.params.get("volume_expand_ratio", 1.2)

        # 取最近 lookback+5 天数据
        recent_dates = prev_dates[-(lookback_days + 5):]
        recent = klines[klines["date"].isin(recent_dates)].copy()
        ensure_pre_close(recent)

        # 候选股过滤
        candidate_symbols = None
        if ctx.candidates:
            candidate_symbols = {
                code[2:] if len(code) > 2 else code for code in ctx.candidates
            }
            recent = recent[recent["symbol"].isin(candidate_symbols)]

        # 昨日涨停股集合（用于 on_tick 观察板块支撑）
        prev_date = prev_dates[-1]
        prev_day = recent[recent["date"] == prev_date].copy()
        prev_day["limit_ratio"] = prev_day["symbol"].apply(calc_limit_ratio)
        prev_day["limit_up_price"] = (prev_day["pre_close"] * (1 + prev_day["limit_ratio"])).round(2)
        prev_day["is_limit_up"] = prev_day["close"] >= prev_day["limit_up_price"]

        yesterday_limit_up_symbols = set(
            prev_day[prev_day["is_limit_up"]]["symbol"].tolist()
        )

        # 也从 review 数据获取昨日涨停股
        review_limit_up = ctx.review.machine.get("stocks", {}).get("limit_up", [])
        if review_limit_up:
            review_symbols = {
                str(item.get("symbol", "")).zfill(6)
                for item in review_limit_up
                if item.get("symbol")
            }
            yesterday_limit_up_symbols |= review_symbols

        # 筛选候选股
        candidates = {}

        for sym, grp in recent.groupby("symbol"):
            grp = grp.sort_values("date")
            if len(grp) < lookback_days:
                continue

            last_n = grp.tail(lookback_days).copy()
            last_n["limit_ratio"] = last_n["symbol"].apply(calc_limit_ratio)
            last_n["limit_up_price"] = (
                last_n["pre_close"] * (1 + last_n["limit_ratio"])
            ).round(2)
            last_n["limit_down_price"] = (
                last_n["pre_close"] * (1 - last_n["limit_ratio"])
            ).round(2)
            last_n["is_lu"] = last_n["close"] >= last_n["limit_up_price"]
            last_n["is_ld"] = last_n["close"] <= last_n["limit_down_price"]

            # 条件1: 近N天涨跌停天数 >= min
            extreme_count = int(last_n["is_lu"].sum() + last_n["is_ld"].sum())
            if extreme_count < extreme_days_min:
                continue

            # 条件2: 反包模式 — 跌停日次日收盘 > 跌停日开盘
            has_fanbao = False
            for i in range(len(last_n) - 1):
                if last_n.iloc[i]["is_ld"]:
                    next_close = last_n.iloc[i + 1]["close"]
                    ld_open = last_n.iloc[i]["open"]
                    if next_close > ld_open:
                        has_fanbao = True
                        break
            if not has_fanbao and (last_n["is_ld"].sum() > 0):
                continue  # 有跌停但没有反包，跳过

            # 条件3: 昨日涨停且放量
            yesterday = last_n[last_n["date"] == prev_date]
            if yesterday.empty:
                continue
            y = yesterday.iloc[0]
            if not y["is_lu"]:
                continue

            avg_amount_5d = float(last_n.tail(5)["amount"].mean())
            yesterday_amount = float(y["amount"])
            if avg_amount_5d > 0 and yesterday_amount < avg_amount_5d * volume_expand_ratio:
                continue

            candidates[sym] = {
                "yesterday_close": float(y["close"]),
                "yesterday_open": float(y["open"]),
                "yesterday_amount": yesterday_amount,
                "avg_amount_5d": avg_amount_5d,
                "limit_up_price": float(y["limit_up_price"]),
                "limit_ratio": float(y["limit_ratio"]),
                "extreme_count": extreme_count,
                "has_fanbao": has_fanbao,
            }

        ctx.state["candidates"] = candidates
        ctx.state["yesterday_limit_up_symbols"] = yesterday_limit_up_symbols
        ctx.state["ready"] = True
        ctx.state["open_checked"] = set()
        ctx.state["alerted_codes"] = set()

        logger.info(
            f"[{self.slug}] prepare 完成，"
            f"筛选出 {len(candidates)} 只候选股，"
            f"昨日涨停 {len(yesterday_limit_up_symbols)} 只"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        candidates = ctx.state.get("candidates", {})
        if not candidates:
            return []

        alerted = ctx.state.get("alerted_codes", set())
        open_checked = ctx.state.get("open_checked", set())
        yesterday_limit_up = ctx.state.get("yesterday_limit_up_symbols", set())

        open_strength_min = ctx.params.get("open_strength_min", 0.09)
        limit_approach_pct = ctx.params.get("limit_approach_pct", 0.99)
        limit_approach_by_time = ctx.params.get("limit_approach_by_time", "09:40:00")

        current_time = ctx.market.current_time
        alerts = []

        # 观察其他昨日涨停股的表现（板块/情绪支撑）
        strong_peers = 0
        total_peers = 0
        peer_info = []
        snapshots = ctx.stock_snapshots
        for peer_sym in yesterday_limit_up:
            peer_code_prefix = (
                f"sz{peer_sym}" if peer_sym.startswith(("0", "1", "2", "3"))
                else f"sh{peer_sym}" if peer_sym.startswith("6")
                else f"bj{peer_sym}"
            )
            snap = snapshots.get(peer_code_prefix)
            if snap and snap.close > 0:
                total_peers += 1
                if snap.pct_chg > 3:
                    strong_peers += 1
                    if len(peer_info) < 3:
                        peer_info.append(f"{snap.name}+{snap.pct_chg:.1f}%")

        # 用 numpy 数组替代 iterrows
        codes = frame["code"].values
        nows = frame["now"].values
        closes = frame["close"].values
        names = frame["name"].values if "name" in frame.columns else None
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

            pct_chg = pct_chgs[i] if pct_chgs is not None else 0

            # 条件4: 开盘强度 > 9%
            if code not in open_checked:
                open_strength = (now_price - pre_close) / pre_close
                if open_strength < open_strength_min:
                    continue
                open_checked.add(code)

            # 条件5: 快速接近涨停
            if current_time > limit_approach_by_time:
                continue

            if now_price < feat["limit_up_price"] * limit_approach_pct:
                continue

            name = names[i] if names is not None else ""
            peer_desc = ""
            if strong_peers > 0:
                peer_desc = f", 涨停同袍{strong_peers}/{total_peers}强势({', '.join(peer_info)})"

            alerts.append(Alert(
                code=code,
                name=name,
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=(
                    f"强更强: 涨幅{pct_chg:.2f}%, "
                    f"近10天{feat['extreme_count']}次涨跌停"
                    f"{'含反包' if feat['has_fanbao'] else ''}, "
                    f"昨放{feat['yesterday_amount'] / feat['avg_amount_5d']:.1f}倍量涨停"
                    f"{peer_desc}"
                ),
                level="important",
            ))
            alerted.add(code)

        return alerts
