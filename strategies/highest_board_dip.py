# -*- coding: utf-8 -*-
"""
策略：最高板板块共振低吸

核心逻辑:
    全场最高板开板放量分歧时，通过实时跌幅聚类发现板块共振杀跌，
    在共振低吸博弈回封。

案例: 津药药业(600488) — 5板最高标，前几日一字板，今日放量分歧，
      与创新药板块共振杀跌后回升，两个买点：
      1. 板块共振杀跌时的低吸（博弈分歧后继续向上）
      2. 板块企稳后的回封前买入（情绪明朗后套利）

关键创新:
    不依赖预映射板块，而是监控到个股快速杀跌时，
    实时获取全市场同时间段跌幅最深的100只股，
    通过 ThemeResolver 聚类分析出共振的题材概念，
    判断是否为板块级别的共振杀跌。

prepare() 阶段:
    - 从 review 数据获取最高板股票
    - 加载日线检查一字板历史
    - 用 ThemeResolver 获取主题材

on_tick() 阶段:
    - 监控候选股快速杀跌
    - 触发时扫描全市场 pct_chg，取跌幅最深的100只
    - 用 ThemeResolver 聚类发现共振题材
    - 判定板块共振后输出买点信号
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

import config
from context import StrategyContext
from market_pulse.theme_resolver import ThemeResolver
from strategy_base import Alert, BaseStrategy, register_strategy
from strategies.strategy_utils import (
    calc_limit_up_price,
    calc_moving_averages,
    ensure_pre_close,
    get_prev_dates,
    get_sorted_dates,
    is_one_word_limit_up,
    load_klines,
)

logger = logging.getLogger(__name__)


def _code_from_symbol(symbol: str) -> str:
    """6位纯数字 symbol 转为带前缀 code。"""
    pure = symbol.zfill(6)
    if pure.startswith(("4", "8", "92")):
        return f"bj{pure}"
    if pure.startswith(("0", "1", "2", "3")):
        return f"sz{pure}"
    return f"sh{pure}"


@register_strategy
class HighestBoardDipStrategy(BaseStrategy):
    slug = "highest_board_dip"
    name = "最高板板块共振低吸"
    description = "最高板放量分歧时，通过实时跌幅聚类发现板块共振低吸博弈回封"

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
        one_word_days_min = ctx.params.get("one_word_days_min", 2)
        dip_pct_threshold = ctx.params.get("dip_pct_threshold", -3.0)
        dip_from_high_pct = ctx.params.get("dip_from_high_pct", 6.0)
        sector_decline_pct = ctx.params.get("sector_decline_pct", -1.0)
        cluster_top_n = ctx.params.get("cluster_top_n", 100)
        pullback_ratio = ctx.params.get("pullback_ratio", 0.98)

        # 加载日线
        klines = load_klines()
        if klines is None:
            ctx.state["ready"] = False
            return

        date = ctx.market.date
        sorted_dates = get_sorted_dates(klines)
        prev_dates = get_prev_dates(sorted_dates, date)
        if len(prev_dates) < 5:
            ctx.state["ready"] = False
            return

        prev_date = prev_dates[-1]

        # 加载 ThemeResolver
        theme_resolver = ThemeResolver()
        try:
            theme_resolver.load(
                knowledge_root=config.VECTOR_PROJECT,
                industry_file=config.INDUSTRY_FILE,
                stock_basic_file=config.STOCK_BASIC_FILE,
                hot_theme_names=[],  # 后面可从 review 获取
            )
        except Exception as e:
            logger.warning(f"[{self.slug}] ThemeResolver 加载失败: {e}，将不使用题材聚类")

        # 筛选最高板中的候选股
        candidates = {}

        for stock in high_board_stocks:
            sym = str(stock.get("symbol", "")).zfill(6)
            name = str(stock.get("name", ""))
            board_count = int(stock.get("board_count", max_height))

            # 加载最近10天日线
            recent_dates = prev_dates[-10:]
            sym_klines = klines[
                (klines["symbol"] == sym) & (klines["date"].isin(recent_dates))
            ].sort_values("date").copy()

            if len(sym_klines) < 3:
                continue

            ensure_pre_close(sym_klines)
            sym_klines["limit_ratio"] = sym_klines["symbol"].apply(
                lambda s: 0.20 if s.startswith(("300", "301", "688")) else 0.10
            )
            sym_klines["limit_up_price"] = (
                sym_klines["pre_close"] * (1 + sym_klines["limit_ratio"])
            ).round(2)

            # 检查前N天是否一字板（不含最近一天即昨日，因为昨日也是涨停）
            prev_klines = sym_klines.iloc[:-1] if len(sym_klines) > 1 else sym_klines
            one_word_count = 0
            for _, kr in prev_klines.iterrows():
                if is_one_word_limit_up(
                    kr["open"], kr["high"], kr["low"],
                    kr["close"], kr["limit_up_price"],
                ):
                    one_word_count += 1

            # 获取主题材
            theme = None
            if theme_resolver.loaded:
                theme = theme_resolver.resolve_primary_theme(sym)

            # 计算MA5
            closes = sym_klines["close"].values
            ma_dict = calc_moving_averages(closes, [5])

            # 前一日成交额
            yesterday = sym_klines.iloc[-1]
            prev_amount = float(yesterday.get("amount", 0))
            pre_close = float(yesterday["close"])

            candidates[sym] = {
                "name": name,
                "board_count": board_count,
                "limit_up_price": float(yesterday["limit_up_price"]),
                "pre_close": pre_close,
                "prev_amount": prev_amount,
                "one_word_count": one_word_count,
                "theme": theme,
                "ma5": ma_dict.get(5, pre_close),
                "code": _code_from_symbol(sym),
            }

        ctx.state["candidates"] = candidates
        ctx.state["theme_resolver"] = theme_resolver
        ctx.state["ready"] = True

        # 买点追踪状态
        ctx.state["bp1_triggered"] = set()  # 买点1已触发
        ctx.state["bp2_triggered"] = set()  # 买点2已触发
        ctx.state["intraday_high_pct"] = {}  # {sym: max_pct}
        ctx.state["prev_pct_chg"] = {}  # {sym: pct_chg} — 上一帧
        ctx.state["cluster_top_n"] = cluster_top_n
        ctx.state["dip_pct_threshold"] = dip_pct_threshold
        ctx.state["dip_from_high_pct"] = dip_from_high_pct
        ctx.state["sector_decline_pct"] = sector_decline_pct
        ctx.state["pullback_ratio"] = pullback_ratio

        logger.info(
            f"[{self.slug}] prepare 完成，"
            f"最高板 {max_height} 板，筛选出 {len(candidates)} 只候选股: "
            f"{[f'{c['name']}({sym})' for sym, c in candidates.items()]}"
        )

    def _cluster_declining_stocks(
        self, ctx: StrategyContext, threshold_pct: float,
    ) -> list[dict]:
        """扫描全市场，取跌幅最深的 top N 只股，用 ThemeResolver 聚类。

        返回 [{"theme": str, "count": int, "avg_pct": float, "symbols": list}, ...]
        """
        theme_resolver: ThemeResolver = ctx.state.get("theme_resolver")
        if not theme_resolver or not theme_resolver.loaded:
            return []

        cluster_top_n = ctx.state.get("cluster_top_n", 100)

        # 从全市场 snapshot 中取跌幅最深的100只
        declining = []
        for code, snap in ctx.stock_snapshots.items():
            if snap.close <= 0:
                continue
            if snap.pct_chg < threshold_pct:
                pure = code[2:] if len(code) > 2 else code
                declining.append((pure, snap.pct_chg))

        # 按涨幅排序取跌幅最深的
        declining.sort(key=lambda x: x[1])
        top_declining = declining[:cluster_top_n]

        if len(top_declining) < 5:
            return []

        # 用 ThemeResolver 聚类
        buckets = theme_resolver.aggregate_top_themes(
            symbol_pct_pairs=top_declining,
            top_n=5,
        )

        result = []
        for b in buckets:
            result.append({
                "theme": b.theme,
                "count": b.count,
                "avg_pct": b.avg_pct_chg,
                "symbols": b.symbols,
                "names": b.names,
            })

        return result

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        candidates = ctx.state.get("candidates", {})
        if not candidates:
            return []

        bp1_triggered = ctx.state.get("bp1_triggered", set())
        bp2_triggered = ctx.state.get("bp2_triggered", set())
        intraday_high_pct = ctx.state.get("intraday_high_pct", {})
        prev_pct_chg = ctx.state.get("prev_pct_chg", {})
        dip_from_high_pct = ctx.state.get("dip_from_high_pct", 6.0)
        sector_decline_pct = ctx.state.get("sector_decline_pct", -1.0)
        pullback_ratio = ctx.state.get("pullback_ratio", 0.98)

        alerts = []

        codes = frame["code"].values
        nows = frame["now"].values
        names = frame["name"].values if "name" in frame.columns else None
        pct_chgs = frame["pct_chg"].values if "pct_chg" in frame.columns else None

        for i in range(len(codes)):
            code = codes[i]
            pure_code = code[2:] if len(code) > 2 else code
            feat = candidates.get(pure_code)
            if feat is None:
                continue

            pct_chg = pct_chgs[i] if pct_chgs is not None else 0
            now_price = nows[i]

            # 更新日内最高涨幅
            prev_high = intraday_high_pct.get(pure_code, pct_chg)
            intraday_high_pct[pure_code] = max(prev_high, pct_chg)

            # ---- 买点1: 炸板回撤 + 板块共振低吸 ----
            if pure_code not in bp1_triggered:
                high_pct = intraday_high_pct.get(pure_code, 0)
                drop_from_high = high_pct - pct_chg

                if drop_from_high >= dip_from_high_pct:
                    clusters = self._cluster_declining_stocks(ctx, sector_decline_pct)

                    # 获取候选股的所有题材（不仅是 primary）
                    theme_resolver: ThemeResolver = ctx.state.get("theme_resolver")
                    stock_themes = set()
                    if theme_resolver and theme_resolver.loaded:
                        stock_themes = set(theme_resolver.resolve_themes(pure_code))
                    if feat.get("theme"):
                        stock_themes.add(feat["theme"])

                    resonance_found = False
                    resonance_info = ""

                    if stock_themes and clusters:
                        for cl in clusters:
                            cl_theme = cl["theme"]
                            # 题材交集匹配
                            if cl_theme in stock_themes or pure_code in cl.get("symbols", []):
                                resonance_found = True
                                resonance_info = (
                                    f", 板块[{cl_theme}]共振{cl['count']}只 "
                                    f"平均{cl['avg_pct']:.1f}%"
                                )
                                break

                    name = names[i] if names is not None else ""

                    if resonance_found:
                        bp1_triggered.add(pure_code)
                        alerts.append(Alert(
                            code=code,
                            name=name,
                            strategy_slug=self.slug,
                            strategy_name=self.name,
                            message=(
                                f"买点1[炸板回撤低吸]: {feat['board_count']}板{feat['name']}, "
                                f"从高点{high_pct:.1f}%回撤至{pct_chg:.1f}%({drop_from_high:.1f}%){resonance_info}"
                            ),
                            level="important",
                        ))
                    else:
                        # 无共振匹配或聚类不可用时，仅基于价格回撤触发
                        bp1_triggered.add(pure_code)
                        cluster_info = f"(聚类{len(clusters)}个无匹配)" if clusters else "(聚类数据不可用)"
                        alerts.append(Alert(
                            code=code,
                            name=name,
                            strategy_slug=self.slug,
                            strategy_name=self.name,
                            message=(
                                f"买点1[炸板回撤低吸]: {feat['board_count']}板{feat['name']}, "
                                f"从高点{high_pct:.1f}%回撤至{pct_chg:.1f}%({drop_from_high:.1f}%), "
                                f"{cluster_info}"
                            ),
                            level="important",
                        ))

            # ---- 买点2: 回升企稳后买入 ----
            if pure_code not in bp2_triggered and pure_code in bp1_triggered:
                sector_stable = False
                stability_info = ""

                if pct_chg > 5.0:
                    sector_stable = True
                    stability_info = f", 从低点回升至{pct_chg:.1f}%"

                if sector_stable:
                    high_pct = intraday_high_pct.get(pure_code, 0)
                    if high_pct > 0 and pct_chg < high_pct * pullback_ratio:
                        ma5 = feat.get("ma5", 0)
                        if ma5 > 0 and now_price > ma5 * 0.98:
                            name = names[i] if names is not None else ""
                            bp2_triggered.add(pure_code)
                            alerts.append(Alert(
                                code=code,
                                name=name,
                                strategy_slug=self.slug,
                                strategy_name=self.name,
                                message=(
                                    f"买点2[板块企稳买入]: {feat['board_count']}板{feat['name']}, "
                                    f"涨幅{pct_chg:.1f}%(高点{high_pct:.1f}%), "
                                    f"MA5={feat['ma5']:.2f}{stability_info}"
                                ),
                                level="warn",
                            ))

            prev_pct_chg[pure_code] = pct_chg

        ctx.state["intraday_high_pct"] = intraday_high_pct
        ctx.state["prev_pct_chg"] = prev_pct_chg

        return alerts
