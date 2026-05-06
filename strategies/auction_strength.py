# -*- coding: utf-8 -*-
"""
策略：竞价强度异动

对应逻辑文档策略二 — 情绪连板接力 / 弱转强

核心逻辑:
    竞价阶段追踪竞价量比、开盘强度、价格抬升与撮合放大，
    筛选竞价超预期的弱转强品种。

prepare() 阶段:
    - 读取 k-lines 昨日数据，筛选昨日涨停股、连板股
    - 计算昨日成交量/成交额基线

on_tick() 阶段:
    - 竞价阶段检测量比（相对昨日成交量的放大倍数）
    - 检测开盘强度（now vs close）
    - 检测撮合量/竞价额持续放大
"""

from __future__ import annotations

import logging
import os

import numpy as np
import pandas as pd

import config
from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy

logger = logging.getLogger(__name__)


@register_strategy
class AuctionStrengthStrategy(BaseStrategy):
    slug = "auction_strength"
    name = "竞价强度异动"
    description = "竞价阶段检测量价超预期、接力确认与爆量抬价信号"
    signal_role = "primary"
    review_candidates_mode = "ignore"

    def prepare(self, ctx: StrategyContext) -> None:
        """加载昨日 K 线数据，计算涨停股和成交量基线。"""
        klines_path = config.KLINES_DAILY_FILE
        if not os.path.exists(klines_path):
            logger.warning(f"日线数据不存在: {klines_path}")
            ctx.state["ready"] = False
            return

        try:
            klines = pd.read_csv(klines_path, compression="gzip", dtype={"symbol": str})
        except Exception as e:
            logger.error(f"加载日线数据失败: {e}")
            ctx.state["ready"] = False
            return

        klines["date"] = klines["date"].astype(str)
        klines["symbol"] = klines["symbol"].astype(str).str.zfill(6)

        date = ctx.market.date
        sorted_dates = sorted(klines["date"].unique())
        prev_dates = [d for d in sorted_dates if d < date]
        if not prev_dates:
            ctx.state["ready"] = False
            return

        prev_date = prev_dates[-1]
        prev_day = klines[klines["date"] == prev_date].copy()

        # 计算昨日涨停
        def _limit_ratio(sym):
            s = str(sym).zfill(6)
            if s.startswith(("300", "301", "688")):
                return 0.20
            return 0.10

        if "change" in prev_day.columns:
            prev_day["pre_close"] = prev_day["close"] - prev_day["change"]
        else:
            prev_day["pre_close"] = prev_day["close"] / (1 + prev_day["pct_chg"] / 100)

        prev_day["limit_ratio"] = prev_day["symbol"].apply(_limit_ratio)
        prev_day["limit_up_price"] = (prev_day["pre_close"] * (1 + prev_day["limit_ratio"])).round(2)
        prev_day["is_limit_up"] = prev_day["close"] == prev_day["limit_up_price"]

        # 昨日涨停股（弱转强核心观察池）
        limit_up_symbols = set(prev_day[prev_day["is_limit_up"]]["symbol"].tolist())
        review_limit_up = ctx.review.machine.get("stocks", {}).get("limit_up", [])
        if review_limit_up:
            limit_up_symbols = {
                str(item.get("symbol", "")).zfill(6)
                for item in review_limit_up
                if item.get("symbol")
            }

        # 日线 amount 常见口径为“千元”，统一换算到 tick 的“元”口径
        daily_amount_unit = float(ctx.params.get("daily_amount_unit", 1000.0))

        # 昨日成交量基线 (symbol -> amount in 元)
        vol_baseline = {}
        for _, row in prev_day.iterrows():
            sym = str(row["symbol"]).zfill(6)
            base = float(row.get("amount", row.get("volume", 0)))
            vol_baseline[sym] = base * daily_amount_unit

        ctx.state["limit_up_symbols"] = limit_up_symbols
        ctx.state["vol_baseline"] = vol_baseline
        ctx.state["ready"] = True

        # 策略参数
        ctx.state["vol_multiple_threshold"] = ctx.params.get("vol_multiple_threshold", 2.0)
        ctx.state["min_open_strength"] = ctx.params.get("min_open_strength", 0.02)
        ctx.state["top_amount_rank_max"] = ctx.params.get("top_amount_rank_max", 10)
        ctx.state["top_strength_rank_max"] = ctx.params.get("top_strength_rank_max", 8)
        ctx.state["min_auction_amount"] = float(ctx.params.get("min_auction_amount", 30_000_000))
        ctx.state["min_strong_peers"] = int(ctx.params.get("min_strong_peers", 2))
        ctx.state["min_peer_ratio"] = float(ctx.params.get("min_peer_ratio", 0.10))
        ctx.state["lift_confirm_time"] = ctx.params.get("lift_confirm_time", "09:24:30")
        ctx.state["min_lift_open_strength"] = float(ctx.params.get("min_lift_open_strength", 0.08))
        ctx.state["min_lift_growth_ratio"] = float(ctx.params.get("min_lift_growth_ratio", 0.25))
        ctx.state["lift_vol_multiple_threshold"] = float(
            ctx.params.get("lift_vol_multiple_threshold", 1.0)
        )
        ctx.state["near_limit_ratio"] = float(ctx.params.get("near_limit_ratio", 0.998))
        ctx.state["daily_amount_unit"] = daily_amount_unit

        # 已触发过的股票（防重复报警）
        ctx.state["alerted_codes"] = set()
        ctx.state["post920_ref"] = {}
        ctx.state["stats"] = {
            "relay_watch_count": 0,
            "capacity_watch_count": 0,
            "lift_watch_count": 0,
            "relay_alert_count": 0,
            "capacity_alert_count": 0,
            "lift_alert_count": 0,
        }

        logger.info(
            f"[{self.slug}] prepare 完成，昨日涨停 {len(limit_up_symbols)} 只，"
            f"成交量基线 {len(vol_baseline)} 只"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        alerts = []
        limit_up_symbols = ctx.state.get("limit_up_symbols", set())
        vol_baseline = ctx.state.get("vol_baseline", {})
        alerted = ctx.state.get("alerted_codes", set())
        vol_threshold = ctx.state.get("vol_multiple_threshold", 2.0)
        min_open_strength = ctx.state.get("min_open_strength", 0.02)
        top_amount_rank_max = ctx.state.get("top_amount_rank_max", 10)
        top_strength_rank_max = ctx.state.get("top_strength_rank_max", 8)
        min_auction_amount = ctx.state.get("min_auction_amount", 30_000_000.0)
        min_strong_peers = ctx.state.get("min_strong_peers", 2)
        min_peer_ratio = ctx.state.get("min_peer_ratio", 0.10)
        lift_confirm_time = ctx.state.get("lift_confirm_time", "09:24:30")
        min_lift_open_strength = ctx.state.get("min_lift_open_strength", 0.08)
        min_lift_growth_ratio = ctx.state.get("min_lift_growth_ratio", 0.25)
        lift_vol_multiple_threshold = ctx.state.get("lift_vol_multiple_threshold", 1.0)
        near_limit_ratio = ctx.state.get("near_limit_ratio", 0.998)
        min_lift_price_ratio = float(ctx.params.get("min_lift_price_ratio", 0.003))
        post920_ref = ctx.state.get("post920_ref", {})
        stats = ctx.state.get("stats", {})
        current_time = ctx.market.current_time or ""

        ranked_rows = []
        for _, row in frame.iterrows():
            code = row["code"]
            pure_code = code[2:] if len(code) > 2 else code
            now_price = row["now"]
            close_price = row["close"]
            if now_price <= 0 or close_price <= 0:
                continue

            open_strength = (now_price - close_price) / close_price
            matched_volume = float(row.get("bid1_volume", 0) or 0)
            today_amount = row.get("volume", 0)
            baseline = vol_baseline.get(pure_code, 0)
            vol_ratio = today_amount / (baseline * 0.05) if baseline > 0 else 0
            limit_ratio = 0.2 if pure_code.startswith(("300", "301", "688")) else 0.1
            limit_up_price = round(close_price * (1 + limit_ratio), 2)
            near_limit = now_price >= limit_up_price * near_limit_ratio
            if pure_code not in post920_ref:
                post920_ref[pure_code] = {
                    "first_amount": float(today_amount),
                    "first_matched_volume": matched_volume,
                    "first_price": float(now_price),
                }
            ref = post920_ref[pure_code]
            amount_growth_ratio = (
                (today_amount - ref["first_amount"]) / ref["first_amount"]
                if ref["first_amount"] > 0 else 0.0
            )
            matched_growth_ratio = (
                (matched_volume - ref["first_matched_volume"]) / ref["first_matched_volume"]
                if ref["first_matched_volume"] > 0 else 0.0
            )
            price_lift_pct = (
                (now_price - ref["first_price"]) / ref["first_price"]
                if ref["first_price"] > 0 else 0.0
            )
            strength_score = (
                open_strength * 100
                + min(vol_ratio, 6.0)
                + min(amount_growth_ratio * 10, 6.0)
                + (3.0 if near_limit else 0.0)
            )
            ranked_rows.append({
                "code": code,
                "pure_code": pure_code,
                "row": row,
                "open_strength": open_strength,
                "matched_volume": matched_volume,
                "today_amount": today_amount,
                "vol_ratio": vol_ratio,
                "limit_up_price": limit_up_price,
                "near_limit": near_limit,
                "amount_growth_ratio": amount_growth_ratio,
                "matched_growth_ratio": matched_growth_ratio,
                "price_lift_pct": price_lift_pct,
                "strength_score": strength_score,
            })

        amount_rank = {
            item["code"]: idx + 1
            for idx, item in enumerate(
                sorted(ranked_rows, key=lambda item: item["today_amount"], reverse=True)
            )
        }
        strength_rank = {
            item["code"]: idx + 1
            for idx, item in enumerate(
                sorted(ranked_rows, key=lambda item: item["strength_score"], reverse=True)
            )
        }

        relay_pool = []
        for item in ranked_rows:
            if item["pure_code"] not in limit_up_symbols:
                continue
            if item["open_strength"] < min_open_strength:
                continue
            if item["vol_ratio"] < vol_threshold:
                continue
            relay_pool.append(item)

        strong_peer_count = len(relay_pool)
        peer_ratio = (
            strong_peer_count / len(limit_up_symbols)
            if limit_up_symbols else 0.0
        )

        for item in ranked_rows:
            row = item["row"]
            code = item["code"]
            if code in alerted:
                continue

            pure_code = item["pure_code"]
            open_strength = item["open_strength"]
            today_amount = item["today_amount"]
            vol_ratio = item["vol_ratio"]
            a_rank = amount_rank.get(code, 9999)
            s_rank = strength_rank.get(code, 9999)

            if pure_code in limit_up_symbols:
                stats["relay_watch_count"] = stats.get("relay_watch_count", 0) + 1
                if (
                    open_strength >= min_open_strength
                    and vol_ratio >= vol_threshold
                    and strong_peer_count >= min_strong_peers
                    and peer_ratio >= min_peer_ratio
                    and s_rank <= top_strength_rank_max
                ):
                    alerts.append(Alert(
                        code=code,
                        name=row["name"],
                        strategy_slug=self.slug,
                        strategy_name=self.name,
                        message=(
                            f"接力确认: 高开{open_strength*100:.1f}%, "
                            f"量比{vol_ratio:.1f}x, "
                            f"同袍{strong_peer_count}/{len(limit_up_symbols)}强势, "
                            f"强度排位{s_rank}"
                        ),
                        level="important",
                    ))
                    stats["relay_alert_count"] = stats.get("relay_alert_count", 0) + 1
                    alerted.add(code)
                    continue

                stats["lift_watch_count"] = stats.get("lift_watch_count", 0) + 1
                if (
                    current_time >= lift_confirm_time
                    and open_strength >= min_lift_open_strength
                    and vol_ratio >= lift_vol_multiple_threshold
                    and today_amount >= min_auction_amount
                    and a_rank <= top_amount_rank_max
                    and s_rank <= top_strength_rank_max
                    and (
                        item["near_limit"]
                        or item["price_lift_pct"] >= min_lift_price_ratio
                    )
                    and (
                        item["amount_growth_ratio"] >= min_lift_growth_ratio
                        or item["matched_growth_ratio"] >= min_lift_growth_ratio
                    )
                ):
                    price_desc = (
                        f"价格贴近涨停{item['limit_up_price']:.2f}"
                        if item["near_limit"] else
                        f"价格抬升{item['price_lift_pct']:.1%}"
                    )
                    alerts.append(Alert(
                        code=code,
                        name=row["name"],
                        strategy_slug=self.slug,
                        strategy_name=self.name,
                        message=(
                            f"爆量抬价确认: 高开{open_strength*100:.1f}%, "
                            f"竞价额{today_amount/1e8:.2f}亿, 量比{vol_ratio:.1f}x, "
                            f"撮合增幅{item['matched_growth_ratio']:.0%}, "
                            f"{price_desc}, 强度排位{s_rank}"
                        ),
                        level="important",
                    ))
                    stats["lift_alert_count"] = stats.get("lift_alert_count", 0) + 1
                    alerted.add(code)
                    continue

            stats["capacity_watch_count"] = stats.get("capacity_watch_count", 0) + 1
            if (
                today_amount >= min_auction_amount
                and open_strength >= min_open_strength
                and vol_ratio >= vol_threshold
                and a_rank <= top_amount_rank_max
                and s_rank <= top_strength_rank_max
                and (not item["near_limit"])
            ):
                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=(
                        f"容量确认: 高开{open_strength*100:.1f}%, "
                        f"竞价额{today_amount/1e8:.2f}亿, 量比{vol_ratio:.1f}x, "
                        f"成交额排位{a_rank}, 强度排位{s_rank}"
                    ),
                    level="warn",
                ))
                stats["capacity_alert_count"] = stats.get("capacity_alert_count", 0) + 1
                alerted.add(code)
                continue

        ctx.state["post920_ref"] = post920_ref
        return alerts

    def on_phase_end(self, phase: str, ctx: StrategyContext) -> None:
        if phase != config.PHASE_AUCTION_OPEN:
            return
        stats = ctx.state.get("stats", {})
        logger.info(
            "[%s] auction_open总结: 接力观察=%s, 抬价观察=%s, 容量观察=%s, 接力信号=%s, 抬价信号=%s, 容量信号=%s",
            self.slug,
            stats.get("relay_watch_count", 0),
            stats.get("lift_watch_count", 0),
            stats.get("capacity_watch_count", 0),
            stats.get("relay_alert_count", 0),
            stats.get("lift_alert_count", 0),
            stats.get("capacity_alert_count", 0),
        )
