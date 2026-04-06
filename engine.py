# -*- coding: utf-8 -*-
"""
Livetrade — 核心调度引擎

粘合所有模块：加载策略配置 → 实例化策略 → 启动文件监听 →
每帧数据驱动策略执行 → 输出信号。
"""

from __future__ import annotations

import importlib
import logging
import os
import pkgutil
from datetime import time as dt_time

import pandas as pd
import yaml

import config
from alert_writer import AlertWriter
from context import (
    MarketContext,
    StockSnapshot,
    StrategyContext,
    TICK_TS_COLUMNS,
    TICK_TS_AUCTION_EXTRA,
    TICK_TS_NUMERIC,
)
from strategy_base import (
    Alert,
    BaseStrategy,
    get_registered_strategies,
)
from review.runtime import load_review_for_trade
from tick_parser import calc_limit_up_price, calc_pct_change, extract_tick_time
from tick_watcher import OnNewRowsCallback, ReplayWatcher, TickWatcher

logger = logging.getLogger(__name__)

# 涨跌停系数
GEM_STAR_PREFIXES = ("300", "301", "688")


def _auto_discover_strategies() -> None:
    """自动导入 strategies/ 包下所有模块以触发 @register_strategy。"""
    try:
        import strategies
        pkg_path = os.path.dirname(strategies.__file__)
        for importer, modname, ispkg in pkgutil.iter_modules([pkg_path]):
            full_name = f"strategies.{modname}"
            try:
                importlib.import_module(full_name)
                logger.debug(f"自动加载策略模块: {full_name}")
            except Exception as e:
                logger.error(f"加载策略模块 {full_name} 失败: {e}", exc_info=True)
    except ImportError:
        logger.warning("未找到 strategies/ 包，跳过自动发现")


def _parse_time(s: str) -> dt_time:
    """将 'HH:MM' 或 'HH:MM:SS' 字符串解析为 time 对象。"""
    parts = s.strip().split(":")
    if len(parts) == 2:
        return dt_time(int(parts[0]), int(parts[1]))
    return dt_time(int(parts[0]), int(parts[1]), int(parts[2]))


def _limit_ratio(code: str) -> float:
    """计算涨跌停系数。"""
    pure = code[2:] if len(code) > 2 else code
    if pure.startswith(GEM_STAR_PREFIXES):
        return config.LIMIT_RATIO_GEM_STAR
    return config.LIMIT_RATIO_MAIN


def _normalize_candidate_code(code: str) -> str:
    code = str(code).strip()
    if not code:
        return code
    if code.startswith(("sh", "sz", "bj")):
        return code
    pure = code.zfill(6)
    if pure.startswith(("4", "8", "92")):
        return f"bj{pure}"
    if pure.startswith(("0", "1", "2", "3")):
        return f"sz{pure}"
    return f"sh{pure}"


class Engine:
    """实盘/回测主引擎。"""

    def __init__(self, date_string: str, config_path: str | None = None,
                 backtest: bool = False, data_dir: str | None = None):
        self.date_string = date_string
        self.backtest = backtest
        self.data_dir = data_dir

        # 加载策略配置
        config_file = config_path or str(config.STRATEGY_CONFIG_FILE)
        self.strategy_configs = self._load_config(config_file)
        self.review_data = load_review_for_trade(date_string)

        # 构建全局上下文
        self.market_ctx = MarketContext(date=date_string, review=self.review_data)

        # 每股静态/缓变快照
        self._stock_snapshots: dict[str, StockSnapshot] = {}

        # 瘦时序历史
        self._tick_history = pd.DataFrame(columns=TICK_TS_COLUMNS)

        # 自动发现并加载策略
        _auto_discover_strategies()
        registry = get_registered_strategies()

        # 实例化已启用的策略
        self._active_strategies: list[tuple[BaseStrategy, StrategyContext, dict]] = []
        self._setup_strategies(registry)

        # 信号输出器
        self.alert_writer = AlertWriter(date_string, overwrite=backtest)

        # 阶段跟踪
        self._current_phase: str | None = None

    def _load_config(self, config_path: str) -> dict:
        if not os.path.exists(config_path):
            logger.warning(f"策略配置文件不存在: {config_path}，使用空配置")
            return {}
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return data.get("strategies", {})

    def _setup_strategies(self, registry: dict[str, type[BaseStrategy]]) -> None:
        active_from_review = self.review_data.get_active_strategy_slugs()
        for slug, strat_config in self.strategy_configs.items():
            enabled_in_config = strat_config.get("enabled", True)
            if active_from_review:
                if slug not in active_from_review:
                    logger.info(f"策略 '{slug}' 不在 analyst.active_strategies 中，跳过")
                    continue
            elif not enabled_in_config:
                logger.info(f"策略 '{slug}' 已禁用，跳过")
                continue

            if self.review_data.is_strategy_excluded(slug):
                logger.info(f"策略 '{slug}' 被 analyst.manual_overrides 排除，跳过")
                continue

            if slug not in registry:
                logger.warning(f"策略 '{slug}' 未注册，跳过")
                continue

            strat_cls = registry[slug]
            strat = strat_cls()

            candidates_list = self.review_data.get_strategy_candidates(slug)
            if not candidates_list:
                candidates_list = strat_config.get("candidates", [])
            normalized_candidates = [
                _normalize_candidate_code(code) for code in candidates_list
            ]
            candidates = set(normalized_candidates) if normalized_candidates else None

            ctx = StrategyContext(
                market=self.market_ctx,
                review=self.review_data,
                params=strat_config.get("params", {}),
                candidates=candidates,
                stock_snapshots=self._stock_snapshots,
                tick_history=self._tick_history,
            )

            logger.info(f"初始化策略: {strat}")

            try:
                strat.prepare(ctx)
            except Exception as e:
                logger.error(f"策略 {slug} prepare() 失败: {e}", exc_info=True)

            self._active_strategies.append((strat, ctx, strat_config))

    def run(self) -> None:
        logger.info(f"Engine 启动 — 日期: {self.date_string}, 回测: {self.backtest}")
        logger.info(f"活跃策略: {len(self._active_strategies)} 个")
        if self.review_data.available:
            logger.info(
                "复盘上下文已加载 — trade_date=%s, review_date=%s",
                self.review_data.trade_date,
                self.review_data.review_date,
            )
        else:
            logger.info("未找到可用复盘上下文，按默认配置运行")

        if self.backtest:
            watcher = ReplayWatcher(
                self.date_string, self._on_new_rows, data_dir=self.data_dir
            )
        else:
            watcher = TickWatcher(self.date_string, self._on_new_rows)

        try:
            watcher.start()
        except KeyboardInterrupt:
            logger.info("收到中断信号，停止引擎")
        finally:
            watcher.stop()

        logger.info(self.alert_writer.summary())
        logger.info("Engine 已停止")

    # ================================================================
    # 核心回调
    # ================================================================

    def _on_new_rows(self, phase: str, raw_df: pd.DataFrame, tick_time: str) -> None:
        """文件增量数据回调 — 核心调度逻辑。"""
        if raw_df.empty:
            return

        # 阶段切换处理
        if phase != self._current_phase:
            old_phase = self._current_phase
            self._current_phase = phase
            self.market_ctx.reset_phase(phase)

            if old_phase is not None:
                for strat, ctx, _ in self._active_strategies:
                    try:
                        strat.on_phase_end(old_phase, ctx)
                    except Exception as e:
                        logger.error(f"[{strat.slug}] on_phase_end 异常: {e}")

            for strat, ctx, _ in self._active_strategies:
                try:
                    strat.on_phase_start(phase, ctx)
                except Exception as e:
                    logger.error(f"[{strat.slug}] on_phase_start 异常: {e}")

        # ---- 衍生列计算（在原始 frame 上）----
        raw_df = calc_pct_change(raw_df)
        raw_df = calc_limit_up_price(raw_df)

        # ---- 更新每股快照 ----
        self._update_snapshots(raw_df)

        # ---- 构建瘦时序行并追加到 tick_history ----
        is_auction = phase in (config.PHASE_AUCTION_OPEN, config.PHASE_AUCTION_CLOSE)
        ts_cols = TICK_TS_COLUMNS[:]
        if is_auction:
            ts_cols = ts_cols + [c for c in TICK_TS_AUCTION_EXTRA if c in raw_df.columns]
        available_cols = [c for c in ts_cols if c in raw_df.columns]
        slim_df = raw_df[available_cols].copy()

        MAX_HISTORY_ROWS = 500_000  # 瘦列后同样内存可存更多行
        self._tick_history = pd.concat(
            [self._tick_history, slim_df], ignore_index=True
        )
        if len(self._tick_history) > MAX_HISTORY_ROWS:
            self._tick_history = self._tick_history.iloc[-MAX_HISTORY_ROWS:].reset_index(drop=True)

        # ---- 更新 MarketContext ----
        self.market_ctx.update_from_snapshots(self._stock_snapshots, phase, tick_time)

        # ---- 调度策略 ----
        all_alerts: list[Alert] = []

        for strat, ctx, strat_config in self._active_strategies:
            # 阶段过滤
            allowed_phases = strat_config.get("phases", config.ALL_PHASES)
            if phase not in allowed_phases:
                continue

            # 时间范围过滤
            time_range = strat_config.get("time_range")
            if time_range and tick_time:
                try:
                    t_start = _parse_time(time_range[0])
                    t_end = _parse_time(time_range[1])
                    t_now = _parse_time(tick_time)
                    if not (t_start <= t_now <= t_end):
                        continue
                except (ValueError, IndexError):
                    pass

            # 更新上下文引用
            ctx.tick_history = self._tick_history
            ctx.stock_snapshots = self._stock_snapshots

            # 候选股过滤
            frame = ctx.filter_frame(raw_df)
            if frame.empty:
                continue

            # 执行策略
            try:
                alerts = strat.on_tick(frame, ctx)
                if alerts:
                    for alert in alerts:
                        if not alert.time:
                            alert.time = tick_time
                    all_alerts.extend(alerts)
            except Exception as e:
                logger.error(f"[{strat.slug}] on_tick 异常: {e}", exc_info=True)

        if all_alerts:
            self.alert_writer.write(all_alerts, tick_time)

    def _update_snapshots(self, frame: pd.DataFrame) -> None:
        """从原始 frame 更新每股快照。"""
        for _, row in frame.iterrows():
            code = row["code"]
            now_price = row["now"]
            close_price = row["close"]  # 昨收

            snap = self._stock_snapshots.get(code)
            if snap is None:
                lr = _limit_ratio(code)
                snap = StockSnapshot(
                    code=code,
                    name=row["name"],
                    close=close_price,
                    open=now_price,
                    high=now_price,
                    low=now_price,
                    limit_up_price=round(close_price * (1 + lr), 2),
                    limit_down_price=round(close_price * (1 - lr), 2),
                )
                self._stock_snapshots[code] = snap

            # 更新缓变字段
            if now_price > snap.high:
                snap.high = now_price
            if now_price < snap.low:
                snap.low = now_price
            snap.volume = row.get("volume", 0)  # 成交额（累计值）
            snap.turnover = row.get("turnover", 0)
            snap.pct_chg = row.get("pct_chg", 0)
            snap.is_limit_up = row.get("is_limit_up", False)
            snap.is_limit_down = row.get("is_limit_down", False)
