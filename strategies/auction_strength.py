# -*- coding: utf-8 -*-
"""
策略：竞价强度异动

对应逻辑文档策略二 — 情绪连板接力 / 弱转强

核心逻辑:
    竞价阶段追踪竞价量比、开盘强度、买卖盘力量，
    筛选竞价超预期的弱转强品种。

prepare() 阶段:
    - 读取 k-lines 昨日数据，筛选昨日涨停股、连板股
    - 计算昨日成交量/成交额基线

on_tick() 阶段:
    - 竞价阶段检测量比（相对昨日成交量的放大倍数）
    - 检测开盘强度（now vs close）
    - 检测买一卖一力量对比
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
    description = "竞价阶段检测量比放大、开盘强度、弱转强等信号"

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

        # 昨日成交量基线 (symbol -> volume)
        vol_baseline = {}
        for _, row in prev_day.iterrows():
            sym = str(row["symbol"]).zfill(6)
            vol_baseline[sym] = row.get("amount", row.get("volume", 0))

        ctx.state["limit_up_symbols"] = limit_up_symbols
        ctx.state["vol_baseline"] = vol_baseline
        ctx.state["ready"] = True

        # 策略参数
        ctx.state["vol_multiple_threshold"] = ctx.params.get("vol_multiple_threshold", 2.0)
        ctx.state["min_bid_ask_ratio"] = ctx.params.get("min_bid_ask_ratio", 1.5)
        ctx.state["min_open_strength"] = ctx.params.get("min_open_strength", 0.02)

        # 已触发过的股票（防重复报警）
        ctx.state["alerted_codes"] = set()

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
        min_bid_ask = ctx.state.get("min_bid_ask_ratio", 1.5)
        min_open_strength = ctx.state.get("min_open_strength", 0.02)

        for _, row in frame.iterrows():
            code = row["code"]
            if code in alerted:
                continue

            pure_code = code[2:] if len(code) > 2 else code
            now_price = row["now"]
            close_price = row["close"]  # 昨收
            if now_price <= 0 or close_price <= 0:
                continue

            # 开盘强度
            open_strength = (now_price - close_price) / close_price

            # 买一卖一力量对比
            bid1_vol = row.get("bid1_volume", 0)
            ask1_vol = row.get("ask1_volume", 0)
            bid_ask_ratio = bid1_vol / ask1_vol if ask1_vol > 0 else 0

            # 量比（竞价成交量 vs 昨日全天成交额，仅作为相对判断）
            today_vol = row.get("volume", 0)
            baseline = vol_baseline.get(pure_code, 0)
            # 竞价阶段量比判断：竞价阶段成交额应该是昨全天的一小部分
            # 如果竞价阶段已超过昨日的 5%，说明明显放量
            vol_ratio = today_vol / (baseline * 0.05) if baseline > 0 else 0

            # 信号 1: 昨日涨停 + 今日高开 = 弱转强
            if pure_code in limit_up_symbols and open_strength >= min_open_strength:
                msg_parts = [f"昨日涨停今日高开{open_strength*100:.1f}%"]
                if bid_ask_ratio >= min_bid_ask:
                    msg_parts.append(f"买盘积极(比{bid_ask_ratio:.1f})")
                if vol_ratio >= vol_threshold:
                    msg_parts.append(f"竞价放量{vol_ratio:.1f}x")

                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=", ".join(msg_parts),
                    level="important",
                ))
                alerted.add(code)
                continue

            # 信号 2: 非涨停股但竞价强势放量 + 高开
            if (
                open_strength >= min_open_strength * 1.5
                and vol_ratio >= vol_threshold
                and bid_ask_ratio >= min_bid_ask
            ):
                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=(
                        f"竞价强势: 高开{open_strength*100:.1f}%, "
                        f"量比{vol_ratio:.1f}x, "
                        f"买卖比{bid_ask_ratio:.1f}"
                    ),
                    level="warn",
                ))
                alerted.add(code)

        return alerts
