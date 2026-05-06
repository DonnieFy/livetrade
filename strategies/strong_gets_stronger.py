# -*- coding: utf-8 -*-
"""
策略：强更强

核心逻辑:
    近期频繁涨跌停、反包模式下的极端强势股，次日继续加速涨停。
    需要观察其他涨停股的竞价后表现来确认板块/情绪支撑。

补充分支:
    对于并非连续涨跌停、但近几日大开大合且昨日已修复涨停的票，
    如果今天平高开后在早盘快速完成涨停，也属于可交易的反包上板确认。

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
    signal_role = "support"

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
        rebound_volume_expand_ratio = float(
            ctx.params.get("rebound_volume_expand_ratio", 1.0)
        )
        swing_vol_pct = float(ctx.params.get("swing_vol_pct", 6.0))
        swing_days_min = int(ctx.params.get("swing_days_min", 4))
        swing_up_pct = float(ctx.params.get("swing_up_pct", 8.0))
        swing_down_pct = float(ctx.params.get("swing_down_pct", -5.0))

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

            extreme_count = int(last_n["is_lu"].sum() + last_n["is_ld"].sum())

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
            extreme_volume_ok = (
                avg_amount_5d <= 0 or yesterday_amount >= avg_amount_5d * volume_expand_ratio
            )
            rebound_volume_ok = (
                avg_amount_5d <= 0 or yesterday_amount >= avg_amount_5d * rebound_volume_expand_ratio
            )

            swing_count = int((last_n["pct_chg"].abs() >= swing_vol_pct).sum())
            has_big_up = bool((last_n["pct_chg"] >= swing_up_pct).any())
            has_big_down = bool((last_n["pct_chg"] <= swing_down_pct).any())
            is_swing_rebound = (
                swing_count >= swing_days_min
                and has_big_up
                and has_big_down
            )

            candidate_type = "extreme_accel" if (
                extreme_count >= extreme_days_min and extreme_volume_ok
            ) else None
            if candidate_type is None and is_swing_rebound and rebound_volume_ok:
                candidate_type = "swing_rebound"
            if candidate_type is None:
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
                "swing_count": swing_count,
                "candidate_type": candidate_type,
            }

        ctx.state["candidates"] = candidates
        ctx.state["yesterday_limit_up_symbols"] = yesterday_limit_up_symbols
        ctx.state["ready"] = True
        ctx.state["open_checked"] = set()
        ctx.state["alerted_signals"] = set()

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

        alerted_signals = ctx.state.get("alerted_signals", set())
        open_checked = ctx.state.get("open_checked", set())
        yesterday_limit_up = ctx.state.get("yesterday_limit_up_symbols", set())

        open_strength_min = ctx.params.get("open_strength_min", 0.09)
        limit_approach_pct = ctx.params.get("limit_approach_pct", 0.99)
        limit_approach_by_time = ctx.params.get("limit_approach_by_time", "09:40:00")
        rebound_open_strength_min = ctx.params.get("rebound_open_strength_min", 0.03)
        rebound_limit_by_time = ctx.params.get("rebound_limit_by_time", "09:32:00")
        rebound_reconfirm_by_time = ctx.params.get("rebound_reconfirm_by_time", "09:32:30")
        rebound_reconfirm_min_lift = float(ctx.params.get("rebound_reconfirm_min_lift", 0.015))
        min_strong_peers = ctx.params.get("min_strong_peers", 2)
        min_peer_strength_ratio = ctx.params.get("min_peer_strength_ratio", 0.12)

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
        limit_up_prices = (
            frame["limit_up_price"].values if "limit_up_price" in frame.columns else None
        )

        for i in range(len(codes)):
            code = codes[i]

            pure_code = code[2:] if len(code) > 2 else code
            feat = candidates.get(pure_code)
            if feat is None:
                continue

            now_price = nows[i]
            pre_close = closes[i]
            if now_price <= 0 or pre_close <= 0:
                continue

            current_limit_up_price = (
                float(limit_up_prices[i])
                if limit_up_prices is not None and pd.notna(limit_up_prices[i])
                else float(feat["yesterday_close"] * (1 + feat["limit_ratio"]))
            )
            pct_chg = pct_chgs[i] if pct_chgs is not None else 0

            open_strength = (now_price - pre_close) / pre_close
            candidate_type = feat.get("candidate_type", "extreme_accel")

            if candidate_type == "extreme_accel":
                # 条件4: 开盘强度 > 9%
                if code not in open_checked:
                    if open_strength < open_strength_min:
                        continue
                    open_checked.add(code)

                # 条件5: 快速接近涨停
                if current_time > limit_approach_by_time:
                    continue

                if now_price < current_limit_up_price * limit_approach_pct:
                    continue
            else:
                if code not in open_checked:
                    if open_strength < rebound_open_strength_min:
                        continue
                    open_checked.add(code)

                if current_time > rebound_limit_by_time:
                    continue

                if now_price < current_limit_up_price:
                    continue

            peer_strength_ratio = (
                strong_peers / total_peers if total_peers > 0 else 0.0
            )
            if strong_peers < min_strong_peers or peer_strength_ratio < min_peer_strength_ratio:
                continue

            name = names[i] if names is not None else ""
            peer_desc = ""
            if strong_peers > 0:
                peer_desc = (
                    f", 涨停同袍{strong_peers}/{total_peers}强势"
                    f"({', '.join(peer_info)})"
                )

            primary_key = f"{code}:primary"
            if primary_key not in alerted_signals:
                alerts.append(Alert(
                    code=code,
                    name=name,
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=(
                        (
                            f"强更强: 涨幅{pct_chg:.2f}%, "
                            f"近10天{feat['extreme_count']}次涨跌停"
                            f"{'含反包' if feat['has_fanbao'] else ''}, "
                            f"昨放{feat['yesterday_amount'] / feat['avg_amount_5d']:.1f}倍量涨停, "
                            f"同袍强度{peer_strength_ratio:.0%}"
                            f"{peer_desc}"
                        )
                        if candidate_type == "extreme_accel" else
                        (
                            f"高波动反包板: 涨幅{pct_chg:.2f}%, "
                            f"近10天{feat['swing_count']}天振幅级波动, "
                            f"昨放{feat['yesterday_amount'] / feat['avg_amount_5d']:.1f}倍量涨停, "
                            f"早盘快速封板, 同袍强度{peer_strength_ratio:.0%}"
                            f"{peer_desc}"
                        )
                    ),
                    level="important",
                ))
                alerted_signals.add(primary_key)

            if (
                candidate_type == "swing_rebound"
                and "09:30:00" <= current_time <= rebound_reconfirm_by_time
                and now_price >= current_limit_up_price
            ):
                reconfirm_key = f"{code}:reconfirm"
                snap = snapshots.get(code)
                session_open = snap.open if snap and snap.open > 0 else pre_close
                session_low = snap.low if snap and 0 < snap.low < 999999 else now_price
                lift_from_open = (
                    (now_price - session_open) / pre_close
                    if pre_close > 0 and session_open > 0 else 0.0
                )
                rebound_from_low = (
                    (now_price - session_low) / pre_close
                    if pre_close > 0 and session_low > 0 else 0.0
                )
                if (
                    reconfirm_key not in alerted_signals
                    and session_open < current_limit_up_price
                    and max(lift_from_open, rebound_from_low) >= rebound_reconfirm_min_lift
                ):
                    alerts.append(Alert(
                        code=code,
                        name=name,
                        strategy_slug=self.slug,
                        strategy_name=self.name,
                        message=(
                            f"开盘拉升确认: 开盘后从{session_open:.2f}拉至涨停{current_limit_up_price:.2f}, "
                            f"较开盘再拉{lift_from_open*100:.1f}%, "
                            f"自低点回升{rebound_from_low*100:.1f}%, "
                            f"同袍强度{peer_strength_ratio:.0%}{peer_desc}"
                        ),
                        level="important",
                    ))
                    alerted_signals.add(reconfirm_key)

        return alerts
