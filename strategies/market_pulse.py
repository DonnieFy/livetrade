# -*- coding: utf-8 -*-
"""
策略：盘面脉搏

在关键时点（T1:09:25, T2:09:30, T3:09:33）输出高密度盘面事实卡片。

prepare() 阶段:
  - 加载昨日复盘快照（ReviewSnapshot）
  - 构建题材解析器（ThemeResolver）
  - 初始化时间点状态机

on_tick() 阶段:
  - 检查是否命中 T1/T2/T3 时点
  - 调用计算器生成数据
  - 组装报告并通过 Alert 输出
"""

from __future__ import annotations

import logging

import pandas as pd

import config
from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy

from market_pulse.timepoints import (
    TimepointResult,
    check_timepoint,
    get_saved_snapshot,
    init_state,
    save_snapshot,
)
from market_pulse.review_snapshot import ReviewSnapshot, load_review_snapshot
from market_pulse.theme_resolver import ThemeResolver
from market_pulse.calculators import (
    calc_big_face_risk,
    calc_board_ladder_premium,
    calc_group_premium,
    calc_high_board_reception,
    calc_mainline_continuation,
    calc_market_breadth,
    calc_open_slippage,
    calc_theme_performance,
    top_n_by_pct_chg,
    top_n_by_volume,
)
from market_pulse.report_builder import (
    build_t1_market_report,
    build_t1_premium_report,
    build_t2_report,
    build_t3_rise_fall_report,
    build_t3_risk_report,
    split_report_lines,
)

logger = logging.getLogger(__name__)


@register_strategy
class MarketPulseStrategy(BaseStrategy):
    slug = "market_pulse"
    name = "盘面脉搏"
    description = "关键时点输出高密度盘面事实卡片"

    # 依赖组件（在 prepare 中初始化）
    _review_snap: ReviewSnapshot
    _theme_resolver: ThemeResolver

    def prepare(self, ctx: StrategyContext) -> None:
        """加载依赖并初始化状态。"""
        # 初始化时间点状态机
        init_state(ctx.state)

        # 加载昨日复盘快照
        machine = ctx.review.machine if ctx.review.available else {}
        analyst = ctx.review.analyst if ctx.review.available else {}
        review_date = ctx.review.review_date if ctx.review.available else ""

        self._review_snap = load_review_snapshot(
            machine_data=machine,
            analyst_data=analyst,
            review_date=review_date,
        )

        # 构建题材解析器
        self._theme_resolver = ThemeResolver()

        # 收集昨日热门题材名称
        hot_theme_names = []
        for sector in self._review_snap.hot_sectors:
            if sector.name:
                hot_theme_names.append(sector.name)
        for name in self._review_snap.main_themes:
            if name not in hot_theme_names:
                hot_theme_names.append(name)
        for name in self._review_snap.secondary_themes:
            if name not in hot_theme_names:
                hot_theme_names.append(name)

        self._theme_resolver.load(
            knowledge_root=config.VECTOR_PROJECT,
            industry_file=config.INDUSTRY_FILE,
            stock_basic_file=config.STOCK_BASIC_FILE,
            hot_theme_names=hot_theme_names,
        )

        # 将组件引用存入 state，方便访问
        ctx.state["review_snap"] = self._review_snap
        ctx.state["theme_resolver"] = self._theme_resolver

        logger.info(
            f"[{self.slug}] prepare 完成 — "
            f"复盘日期: {review_date or '无'}, "
            f"题材解析器就绪: {self._theme_resolver.loaded}"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        """每个 tick 检查时点并生成报告。"""
        # 获取当前 tick 时间
        tick_time = ""
        if not frame.empty and "time" in frame.columns:
            tick_time = str(frame.iloc[0].get("time", "")).strip()

        if not tick_time:
            return []

        # 检查时点触发
        tp = check_timepoint(tick_time, ctx.state)
        if tp is None:
            return []

        # 保存当前快照
        save_snapshot(tp.timepoint_id, ctx.state, ctx.stock_snapshots)

        # 获取当前全市场快照（dict 形式）
        current_snapshot = ctx.state.get(
            f"snapshot_{tp.timepoint_id.lower()}", {}
        )
        # 直接从 ctx.stock_snapshots 构建快照字典
        snapshots = self._snapshots_to_dict(ctx.stock_snapshots)

        # 根据时点分发
        try:
            if tp.timepoint_id == "T1":
                return self._handle_t1(tp, snapshots, ctx)
            elif tp.timepoint_id == "T2":
                return self._handle_t2(tp, snapshots, ctx)
            elif tp.timepoint_id == "T3":
                return self._handle_t3(tp, snapshots, ctx)
        except Exception as e:
            logger.error(f"[{self.slug}] {tp.timepoint_id} 报告生成失败: {e}", exc_info=True)
            return [Alert(
                code="market",
                name="盘面",
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=f"{tp.timepoint_id}({tp.label})报告生成异常: {e}",
                level="warn",
                time=tp.tick_time,
            )]

        return []

    def _snapshots_to_dict(self, stock_snapshots: dict) -> dict:
        """将 StockSnapshot 对象转为扁平字典。"""
        result = {}
        for code, snap in stock_snapshots.items():
            result[code] = {
                "code": snap.code,
                "name": snap.name,
                "close": snap.close,
                "open": snap.open,
                "high": snap.high,
                "low": snap.low,
                "volume": getattr(snap, "volume", 0),
                "turnover": snap.turnover,
                "pct_chg": snap.pct_chg,
                "is_limit_up": snap.is_limit_up,
                "is_limit_down": snap.is_limit_down,
                "limit_up_price": snap.limit_up_price,
                "limit_down_price": snap.limit_down_price,
            }
        return result

    def _make_alerts(self, tp: TimepointResult, text: str) -> list[Alert]:
        """将报告文本拆分为多条 Alert。"""
        chunks = split_report_lines(text)
        alerts = []
        for i, chunk in enumerate(chunks):
            alerts.append(Alert(
                code="market",
                name="盘面",
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=chunk,
                level="important" if i == 0 else "info",
                time=tp.tick_time,
            ))
        return alerts

    def _handle_t1(
        self, tp: TimepointResult, snapshots: dict, ctx: StrategyContext
    ) -> list[Alert]:
        """T1: 竞价结束（09:25）— 昨日溢价 + 竞价聚合 + 市场温度。"""
        snap = self._review_snap
        resolver = self._theme_resolver
        alerts = []

        # --- Part A: 昨日分组溢价 ---
        premiums = []
        if snap.has_limit_ups():
            premiums.append(calc_group_premium(
                [s.symbol for s in snap.limit_ups], snapshots, "昨日首板"
            ))
        if snap.has_broken_boards():
            premiums.append(calc_group_premium(
                [s.symbol for s in snap.broken_boards], snapshots, "昨日炸板"
            ))
        if snap.has_board_breakers():
            premiums.append(calc_group_premium(
                [s.symbol for s in snap.board_breakers], snapshots, "昨日断板"
            ))
        board_ladder_premiums = calc_board_ladder_premium(snap.board_ladder, snapshots)

        report_a = build_t1_premium_report(premiums, board_ladder_premiums)
        alerts.extend(self._make_alerts(tp, report_a))

        # --- Part B: 市场温度 + 竞价聚合 ---
        breadth = calc_market_breadth(snapshots)

        # 竞价量能 TOP50 题材聚合
        vol_top = top_n_by_volume(snapshots, n=50)
        vol_theme_pairs = [(code, 0) for code, _ in vol_top]
        volume_themes = resolver.aggregate_top_themes(
            [(code, snapshots[code]["pct_chg"]) for code, _ in vol_top if code in snapshots],
            top_n=3,
        )

        # 竞价涨幅 TOP50 题材聚合
        pct_top = top_n_by_pct_chg(snapshots, n=50, ascending=False)
        pct_themes = resolver.aggregate_top_themes(
            [(code, pct) for code, pct in pct_top],
            top_n=3,
        )

        # 昨日热门题材今日竞价表现
        hot_theme_names = [s.name for s in snap.hot_sectors]
        theme_perf = calc_theme_performance(hot_theme_names, resolver, snapshots)

        report_b = build_t1_market_report(breadth, volume_themes, pct_themes, theme_perf)
        alerts.extend(self._make_alerts(tp, report_b))

        return alerts

    def _handle_t2(
        self, tp: TimepointResult, snapshots: dict, ctx: StrategyContext
    ) -> list[Alert]:
        """T2: 开盘（09:30）— 竞价兑现偏离。"""
        prev_snapshot = get_saved_snapshot("T1", ctx.state)
        if prev_snapshot is None:
            logger.warning("[market_pulse] T2 未找到 T1 快照，跳过假高开检测")
            prev_snapshot = {}

        slippage = calc_open_slippage(prev_snapshot, snapshots)
        breadth = calc_market_breadth(snapshots)

        report = build_t2_report(slippage, breadth)
        return self._make_alerts(tp, report)

    def _handle_t3(
        self, tp: TimepointResult, snapshots: dict, ctx: StrategyContext
    ) -> list[Alert]:
        """T3: 开盘3分钟（09:33）— 涨跌幅聚合 + 风险 + 高标 + 主线。"""
        snap = self._review_snap
        resolver = self._theme_resolver
        t1_snapshot = get_saved_snapshot("T1", ctx.state) or {}
        alerts = []

        # --- Part A: 涨跌幅 TOP50 题材聚合 ---
        rise_top = top_n_by_pct_chg(snapshots, n=50, ascending=False)
        rise_themes = resolver.aggregate_top_themes(
            [(code, pct) for code, pct in rise_top], top_n=3,
        )

        fall_top = top_n_by_pct_chg(snapshots, n=50, ascending=True)
        fall_themes = resolver.aggregate_top_themes(
            [(code, pct) for code, pct in fall_top], top_n=3,
        )

        report_a = build_t3_rise_fall_report(rise_themes, fall_themes)
        alerts.extend(self._make_alerts(tp, report_a))

        # --- Part B: 高标 + 大面股 + 主线延续 ---
        # 高标承接
        high_board_results = calc_high_board_reception(
            snap.high_boards, snapshots, t1_snapshot,
        )

        # 大面股（风险池 = 昨日涨停 + 炸板 + 高标）
        risk_symbols = snap.limit_up_symbols | snap.broken_board_symbols
        for ref in snap.high_boards:
            risk_symbols.add(ref.symbol)
        big_face = calc_big_face_risk(snapshots, risk_symbols)

        # 主线延续
        mainline_names = snap.main_themes + snap.secondary_themes
        mainline = calc_mainline_continuation(mainline_names, resolver, snapshots)

        report_b = build_t3_risk_report(high_board_results, big_face, mainline)
        alerts.extend(self._make_alerts(tp, report_b))

        return alerts
