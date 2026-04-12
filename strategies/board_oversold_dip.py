# -*- coding: utf-8 -*-
"""
策略：连板错杀低吸

核心逻辑:
    近期连续涨停的股票，次日小高开后快速下跌，但所属板块走势不弱，
    存在错杀可能，构成低吸买点。

案例: 通鼎互联(002491) — 3/30~4/8期间多次涨停（含2连板），
      4/9高开+2.6%后快速跌至-5.3%，同期光通信板块不弱，构成错杀低吸。

筛选条件:
    1. 近N日有连续涨停（2板以上）
    2. 趋势形态良好（MA5 > MA20）
    3. 今日开盘 > 昨收（小高开）
    4. 今日快速下跌至 -4%~-5%以下
    5. 所属板块/概念平均涨幅 > 0（板块不弱，存在错杀）

prepare() 阶段:
    - 读取日线，筛选满足条件1~2的候选股
    - 加载 ThemeResolver 用于板块强度判断

on_tick() 阶段:
    - 检测快速下跌达到阈值
    - 通过 ThemeResolver 计算所属板块平均涨幅
    - 板块不弱时触发低吸信号
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy
from strategies.strategy_utils import (
    calc_limit_ratio,
    calc_sector_strength,
    calc_moving_averages,
    count_consecutive_boards,
    ensure_pre_close,
    is_bullish_ma_alignment,
    load_klines,
    get_sorted_dates,
    get_prev_dates,
)

logger = logging.getLogger(__name__)


@register_strategy
class BoardOversoldDipStrategy(BaseStrategy):
    slug = "board_oversold_dip"
    name = "连板错杀低吸"
    description = "近期连板股高开后快速下跌，板块不弱构成错杀低吸"

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
        min_boards = ctx.params.get("min_boards", 2)
        lookback_days = ctx.params.get("lookback_days", 10)
        dip_threshold = ctx.params.get("dip_threshold", -4.0)
        sector_min_avg = ctx.params.get("sector_min_avg", 0.0)

        prev_date = prev_dates[-1]
        recent_dates = prev_dates[-max(lookback_days, 25):]  # 至少25天确保MA20可算
        recent = klines[klines["date"].isin(recent_dates)].copy()
        ensure_pre_close(recent)

        # 候选股过滤
        if ctx.candidates:
            candidate_symbols = {
                code[2:] if len(code) > 2 else code for code in ctx.candidates
            }
            recent = recent[recent["symbol"].isin(candidate_symbols)]

        # 加载 ThemeResolver
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

        # 筛选候选股
        candidates = {}
        seen_symbols = set()

        for sym, grp in recent.groupby("symbol"):
            grp = grp.sort_values("date")
            if len(grp) < 5:
                continue

            sym_str = str(sym).zfill(6)
            if sym_str in seen_symbols:
                continue

            closes = grp["close"].values

            # 条件1: 近N日有连板 >= min_boards
            board_count = count_consecutive_boards(recent[recent["symbol"] == sym_str], sym_str)
            if board_count < min_boards:
                continue

            # 条件2: 趋势良好 MA5 > MA20
            mas = calc_moving_averages(closes, [5, 20])
            ma5 = mas.get(5, 0)
            ma20 = mas.get(20, 0)
            if ma5 <= 0 or ma20 <= 0 or ma5 <= ma20:
                continue

            # 昨日数据
            yesterday = grp.iloc[-1]
            pre_close = float(yesterday["close"])

            # 获取题材
            themes = []
            if theme_resolver.loaded:
                themes = theme_resolver.resolve_themes(sym_str)

            candidates[sym_str] = {
                "board_count": board_count,
                "ma5": ma5,
                "ma20": ma20,
                "pre_close": pre_close,
                "themes": themes,
            }
            seen_symbols.add(sym_str)

        ctx.state["candidates"] = candidates
        ctx.state["theme_resolver"] = theme_resolver
        ctx.state["ready"] = True
        ctx.state["dip_threshold"] = dip_threshold
        ctx.state["sector_min_avg"] = sector_min_avg
        ctx.state["alerted_codes"] = set()
        ctx.state["lowest_pct"] = {}  # {sym: lowest_pct_seen}

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
        lowest_pct = ctx.state.get("lowest_pct", {})
        dip_threshold = ctx.state.get("dip_threshold", -4.0)
        sector_min_avg = ctx.state.get("sector_min_avg", 0.0)
        theme_resolver = ctx.state.get("theme_resolver")

        alerts = []

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

            pct_chg = pct_chgs[i] if pct_chgs is not None else 0
            now_price = nows[i]
            pre_close = closes[i]
            if now_price <= 0 or pre_close <= 0:
                continue

            # 追踪最低点
            prev_low = lowest_pct.get(pure_code, pct_chg)
            lowest_pct[pure_code] = min(prev_low, pct_chg)

            # 检测快速下跌达到阈值
            if pct_chg > dip_threshold:
                continue

            # 板块强度判断（相对大盘）
            sector_ok = False
            sector_info = ""
            if theme_resolver and theme_resolver.loaded:
                stock_themes = set(feat.get("themes", []))
                sector_strengths = calc_sector_strength(
                    ctx.stock_snapshots, theme_resolver, threshold=-99,
                )
                # 计算全市场平均涨幅作为大盘基准
                all_pcts = [
                    snap.pct_chg for snap in ctx.stock_snapshots.values()
                    if snap.close > 0
                ]
                market_avg = sum(all_pcts) / len(all_pcts) if all_pcts else 0

                # 板块均涨 > 大盘均涨 即认为板块不弱
                for theme in stock_themes:
                    if theme in sector_strengths:
                        avg_pct = sector_strengths[theme]
                        if avg_pct > market_avg:
                            sector_ok = True
                            sector_info = f", 板块[{theme}]均涨{avg_pct:.1f}%>大盘{market_avg:.1f}%"
                            break

            if not sector_ok:
                continue

            name = names[i] if names is not None else ""

            alerts.append(Alert(
                code=code,
                name=name,
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=(
                    f"错杀低吸: {feat['board_count']}连板股跌至{pct_chg:.1f}%, "
                    f"MA5={feat['ma5']:.2f}>MA20={feat['ma20']:.2f}"
                    f"{sector_info}"
                ),
                level="important",
            ))
            alerted.add(code)

        ctx.state["lowest_pct"] = lowest_pct
        return alerts
