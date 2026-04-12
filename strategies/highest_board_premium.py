# -*- coding: utf-8 -*-
"""
策略：最高板套利

核心逻辑:
    昨日大量连板断板后，连板个数稀少，此时最高板股票在竞价/开盘阶段
    获得情绪溢价。叠加同日一字涨停股的情绪支持和热门板块，构成套利买点。

案例: 汇源通信(000586) — 4/8连板断板潮后仅剩汇源通信为最高板(5板)，
      4/9竞价一字开(25.0 vs 昨收22.84, +9.5%)，中安科/奥瑞德等一字提供情绪支撑，
      叠加光通信最热板块，构成最高板套利。

筛选条件:
    1. 当前唯一或少数最高板股票（连板高度全场最高）
    2. 叠加热门板块（review数据中主线条材之一，或板块内涨停股>=3只）
    3. 竞价高开 > 5% 或一字开
    4. 其他昨日涨停股有一字开盘提供情绪支撑

prepare() 阶段:
    - 从 review 数据获取最高板
    - 检查板块热度

on_tick() 阶段:
    - 竞价阶段检测高开/一字
    - 观察其他昨日涨停股的开盘表现（情绪支撑）
    - 热门板块确认
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy
from strategies.strategy_utils import (
    calc_moving_averages,
    ensure_pre_close,
    load_klines,
    get_sorted_dates,
    get_prev_dates,
)

logger = logging.getLogger(__name__)


@register_strategy
class HighestBoardPremiumStrategy(BaseStrategy):
    slug = "highest_board_premium"
    name = "最高板套利"
    description = "连板稀少时最高板获得情绪溢价，叠加板块热度和一字支撑"

    def prepare(self, ctx: StrategyContext) -> None:
        # 从 review 数据获取最高板
        board_ladder = (
            ctx.review.machine
            .get("board_stats", {})
            .get("consecutive_board_ladder", {})
        )
        if not board_ladder:
            logger.info(f"[{self.slug}] 无连板梯队数据，跳过")
            ctx.state["ready"] = False
            return

        # 找到最高板
        max_height = max(int(h) for h in board_ladder.keys())
        high_board_stocks = board_ladder[str(max_height)]
        if not high_board_stocks:
            ctx.state["ready"] = False
            return

        # 参数
        open_strength_min = ctx.params.get("open_strength_min", 0.05)
        peer_limit_pct = ctx.params.get("peer_limit_pct", 0.08)
        max_board_count = ctx.params.get("max_board_count", 3)  # 最高板<=N只时触发

        # 获取昨日涨停股集合（用于情绪支撑判断）
        review_limit_up = ctx.review.machine.get("stocks", {}).get("limit_up", [])
        yesterday_limit_up_symbols = set()
        for item in review_limit_up:
            sym = str(item.get("symbol", "")).zfill(6)
            if sym:
                yesterday_limit_up_symbols.add(sym)

        # 最高板数量判断（稀少时才有溢价）
        if len(high_board_stocks) > max_board_count:
            logger.info(
                f"[{self.slug}] 最高板{max_height}板有{len(high_board_stocks)}只，"
                f"超过上限{max_board_count}，不触发"
            )
            ctx.state["ready"] = False
            return

        # 加载 ThemeResolver 判断板块热度
        from market_pulse.theme_resolver import ThemeResolver
        import config
        theme_resolver = ThemeResolver()
        try:
            theme_resolver.load(
                knowledge_root=config.VECTOR_PROJECT,
                industry_file=config.INDUSTRY_FILE,
                stock_basic_file=config.STOCK_BASIC_FILE,
            )
        except Exception as e:
            logger.warning(f"[{self.slug}] ThemeResolver 加载失败: {e}")

        # 获取热门题材
        main_themes = set()
        for t in ctx.review.analyst.get("main_themes", []):
            main_themes.add(t.get("name", ""))

        # 筛选最高板候选
        candidates = {}
        for stock in high_board_stocks:
            sym = str(stock.get("symbol", "")).zfill(6)
            name = str(stock.get("name", ""))
            board_count = int(stock.get("board_count", max_height))

            # 获取题材
            themes = []
            is_hot_theme = False
            if theme_resolver.loaded:
                themes = theme_resolver.resolve_themes(sym)
                for t in themes:
                    if t in main_themes:
                        is_hot_theme = True
                        break

            candidates[sym] = {
                "name": name,
                "board_count": board_count,
                "themes": themes,
                "is_hot_theme": is_hot_theme,
            }

        ctx.state["candidates"] = candidates
        ctx.state["theme_resolver"] = theme_resolver
        ctx.state["yesterday_limit_up_symbols"] = yesterday_limit_up_symbols
        ctx.state["ready"] = True
        ctx.state["open_strength_min"] = open_strength_min
        ctx.state["peer_limit_pct"] = peer_limit_pct
        ctx.state["alerted_codes"] = set()

        logger.info(
            f"[{self.slug}] prepare 完成，最高板 {max_height} 板，"
            f"筛选出 {len(candidates)} 只候选股: "
            f"{[f'{c['name']}({sym})' for sym, c in candidates.items()]}"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        candidates = ctx.state.get("candidates", {})
        if not candidates:
            return []

        alerted = ctx.state.get("alerted_codes", set())
        yesterday_limit_up = ctx.state.get("yesterday_limit_up_symbols", set())
        open_strength_min = ctx.state.get("open_strength_min", 0.05)
        peer_limit_pct = ctx.state.get("peer_limit_pct", 0.08)

        current_time = ctx.market.current_time
        alerts = []

        # 观察昨日涨停股的情绪支撑
        strong_peers = 0
        total_peers = 0
        one_word_peers = 0
        peer_info = []
        snapshots = ctx.stock_snapshots
        for peer_sym in yesterday_limit_up:
            if peer_sym.startswith(("0", "1", "2", "3")):
                peer_code = f"sz{peer_sym}"
            elif peer_sym.startswith("6"):
                peer_code = f"sh{peer_sym}"
            else:
                peer_code = f"bj{peer_sym}"
            snap = snapshots.get(peer_code)
            if snap and snap.close > 0:
                total_peers += 1
                if snap.pct_chg > peer_limit_pct * 100:
                    strong_peers += 1
                    if len(peer_info) < 5:
                        peer_info.append(f"{snap.name}+{snap.pct_chg:.1f}%")
                # 一字涨停检测：开盘≈收盘≈最高≈最低 且 涨停
                if snap.is_limit_up and snap.open > 0:
                    if abs(snap.high - snap.low) / snap.open < 0.01:
                        one_word_peers += 1

        # 检查候选股
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

            # 条件: 开盘强度 > 阈值
            open_strength = (now_price - pre_close) / pre_close
            if open_strength < open_strength_min:
                continue

            # 时间窗口：只在开盘阶段触发（09:25~09:40）
            if current_time > "09:45:00":
                continue

            # 至少要有情绪支撑（一字股或强势股）
            if one_word_peers == 0 and strong_peers == 0:
                continue

            name = names[i] if names is not None else ""
            hot_tag = ",热门口" if feat["is_hot_theme"] else ""
            peer_desc = f", 昨涨停{strong_peers}/{total_peers}强(一字{one_word_peers}只: {', '.join(peer_info[:3])})"

            alerts.append(Alert(
                code=code,
                name=name,
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=(
                    f"最高板套利: {feat['board_count']}板{feat['name']}, "
                    f"开{pct_chg:.1f}%{hot_tag}{peer_desc}"
                ),
                level="important",
            ))
            alerted.add(code)

        return alerts
