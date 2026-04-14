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


def _normalize_auction_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """竞价阶段数据标准化。

    新浪在竞价阶段经常返回 now/turnover/volume=0。
    为了让策略层有可用口径：
    - 价格：优先用 (buy+sell)/2；若单边有效则退化到 buy 或 sell
    - 量：用 max(bid1_volume, ask1_volume) 作为成交量代理（股）
    - 额：用 成交量代理 × 价格 作为成交额代理（元）
    """
    out = frame.copy()

    def _num(col: str) -> pd.Series:
        if col not in out.columns:
            return pd.Series([0.0] * len(out), index=out.index, dtype=float)
        return pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    now = _num("now")
    buy = _num("buy")
    sell = _num("sell")
    bid1_volume = _num("bid1_volume")
    ask1_volume = _num("ask1_volume")
    turnover = _num("turnover")
    volume = _num("volume")

    # 竞价价格代理：双边优先，其次单边
    quote_price = ((buy + sell) / 2.0).where((buy > 0) & (sell > 0), 0.0)
    quote_price = quote_price.where(quote_price > 0, buy.where(buy > 0, 0.0))
    quote_price = quote_price.where(quote_price > 0, sell.where(sell > 0, 0.0))

    need_proxy_price = (now <= 0) & (quote_price > 0)
    now = now.where(~need_proxy_price, quote_price)
    out["now"] = now

    # 竞价量/额代理（仅在原值无效时填充）
    proxy_turnover = pd.concat([bid1_volume, ask1_volume], axis=1).max(axis=1)
    turnover = turnover.where(turnover > 0, proxy_turnover)
    out["turnover"] = turnover

    proxy_volume = proxy_turnover * now
    volume = volume.where(volume > 0, proxy_volume)
    out["volume"] = volume

    # 高频策略依赖 high/low，竞价阶段缺失时回填到代理价格
    if "high" in out.columns:
        high = _num("high").where(_num("high") > 0, now)
        out["high"] = high
    if "low" in out.columns:
        low = _num("low").where(_num("low") > 0, now)
        out["low"] = low
    if "open" in out.columns:
        open_price = _num("open").where(_num("open") > 0, now)
        out["open"] = open_price

    return out


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

        # 瘦时序历史（用 list 缓存，避免逐帧 pd.concat）
        self._tick_history_chunks: list[pd.DataFrame] = []
        self._tick_history: pd.DataFrame = pd.DataFrame(columns=TICK_TS_COLUMNS)
        self._tick_history_dirty = False
        self._tick_history_total_rows = 0

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
                    # enabled: true 可覆盖 analyst 的 active_strategies 限制
                    if not enabled_in_config:
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
            # 从策略配置中聚合最小时间范围
            bt_time_range = self._compute_backtest_time_range()

            watcher = ReplayWatcher(
                self.date_string, self._on_new_rows,
                data_dir=self.data_dir,
                time_range=bt_time_range,
            )
        else:
            watcher = TickWatcher(self.date_string, self._on_new_rows)

        try:
            watcher.start()
        except KeyboardInterrupt:
            logger.info("收到中断信号，停止引擎")
        finally:
            watcher.stop()

    def _compute_backtest_time_range(self) -> tuple[str, str] | None:
        """从所有活跃策略的 time_range 配置中聚合出最小覆盖范围。

        例如策略A要求 09:30~10:00，策略B要求 09:15~09:45，
        则返回 (09:15, 10:00)。

        如果没有策略配置 time_range，返回 None（全量回放）。
        """
        if not self._active_strategies:
            return None

        starts = []
        ends = []
        for _, _, strat_config in self._active_strategies:
            tr = strat_config.get("time_range")
            if tr and len(tr) == 2:
                starts.append(tr[0])
                ends.append(tr[1])

        if not starts:
            return None

        return (min(starts), max(ends))

        logger.info(self.alert_writer.summary())
        logger.info("Engine 已停止")

    # ================================================================
    # 核心回调
    # ================================================================

    def _on_new_rows(self, phase: str, raw_df: pd.DataFrame, tick_time: str) -> None:
        """文件增量数据回调 — 核心调度逻辑。"""
        if raw_df.empty:
            return

        is_auction = phase in (config.PHASE_AUCTION_OPEN, config.PHASE_AUCTION_CLOSE)

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

        # ---- 衍生列计算 ----
        # 竞价阶段优先用买一/卖一口径修正 now/volume，再统一重算 pct/涨跌停
        if is_auction:
            raw_df = _normalize_auction_frame(raw_df)
            raw_df = calc_pct_change(raw_df)
            raw_df = calc_limit_up_price(raw_df)
        elif not self.backtest:
            raw_df = calc_pct_change(raw_df)
            raw_df = calc_limit_up_price(raw_df)

        # ---- 更新每股快照 ----
        self._update_snapshots(raw_df)

        # ---- 构建瘦时序行并追加到 tick_history（list 缓存，避免逐帧 concat）----
        ts_cols = TICK_TS_COLUMNS[:]
        if is_auction:
            ts_cols = ts_cols + [c for c in TICK_TS_AUCTION_EXTRA if c in raw_df.columns]
        available_cols = [c for c in ts_cols if c in raw_df.columns]
        slim_df = raw_df[available_cols].copy()

        MAX_HISTORY_ROWS = 500_000
        self._tick_history_chunks.append(slim_df)
        self._tick_history_total_rows += len(slim_df)
        self._tick_history_dirty = True

        # 超限时截断旧 chunk
        if self._tick_history_total_rows > MAX_HISTORY_ROWS:
            kept = []
            remaining = self._tick_history_total_rows
            for chunk in self._tick_history_chunks:
                if remaining - len(chunk) > MAX_HISTORY_ROWS:
                    remaining -= len(chunk)
                    continue
                if remaining > MAX_HISTORY_ROWS:
                    excess = remaining - MAX_HISTORY_ROWS
                    kept.append(chunk.iloc[excess:].reset_index(drop=True))
                    remaining = MAX_HISTORY_ROWS
                else:
                    kept.append(chunk)
            self._tick_history_chunks = kept
            self._tick_history_total_rows = min(self._tick_history_total_rows, MAX_HISTORY_ROWS)
            self._tick_history_dirty = True

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

            # 更新上下文引用（lazy merge tick_history）
            if self._tick_history_dirty:
                self._tick_history = pd.concat(
                    self._tick_history_chunks, ignore_index=True
                )
                self._tick_history_dirty = False
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
        """从原始 frame 更新每股快照（向量化优化版）。"""
        codes = frame["code"].values
        nows = frame["now"].values
        closes = frame["close"].values
        names = frame["name"].values if "name" in frame.columns else None
        volumes = frame["volume"].values if "volume" in frame.columns else None
        turnovers = frame["turnover"].values if "turnover" in frame.columns else None
        pct_chgs = frame["pct_chg"].values if "pct_chg" in frame.columns else None
        is_lus = frame["is_limit_up"].values if "is_limit_up" in frame.columns else None
        is_lds = frame["is_limit_down"].values if "is_limit_down" in frame.columns else None

        snapshots = self._stock_snapshots

        for i in range(len(codes)):
            code = codes[i]
            now_price = nows[i]
            close_price = closes[i]

            # 过滤无效行情，避免竞价阶段 now=0 污染日内 high/low 快照
            if now_price <= 0 or close_price <= 0:
                continue

            snap = snapshots.get(code)
            if snap is None:
                lr = _limit_ratio(code)
                snap = StockSnapshot(
                    code=code,
                    name=names[i] if names is not None else "",
                    close=close_price,
                    open=now_price,
                    high=now_price,
                    low=now_price,
                    limit_up_price=round(close_price * (1 + lr), 2),
                    limit_down_price=round(close_price * (1 - lr), 2),
                )
                snapshots[code] = snap

            # 修复历史脏值（例如 low=0），防止后续策略误判超大振幅
            if snap.high <= 0:
                snap.high = now_price
            if snap.low <= 0 or snap.low >= 999999:
                snap.low = now_price

            # 更新缓变字段
            if now_price > snap.high:
                snap.high = now_price
            if now_price < snap.low:
                snap.low = now_price
            if volumes is not None:
                snap.volume = volumes[i]
            if turnovers is not None:
                snap.turnover = turnovers[i]
            if pct_chgs is not None:
                snap.pct_chg = pct_chgs[i]
            if is_lus is not None:
                snap.is_limit_up = bool(is_lus[i])
            if is_lds is not None:
                snap.is_limit_down = bool(is_lds[i])
