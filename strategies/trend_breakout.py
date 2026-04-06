# -*- coding: utf-8 -*-
"""
策略：产业趋势突破

对应逻辑文档策略一 — 产业趋势与基本面波段

核心逻辑:
    依托产业趋势或基本面边际变化，跟随机构/趋势资金审美。
    盘中检测个股沿均线走强、突破新高、回踩均线后企稳等信号。

prepare() 阶段:
    - 读取 k-lines 日线数据计算 MA5/MA10/MA20、RS_20、近 20 日新高等基线
    - 可选读取 vector-knowledge 筛选特定板块关联股

on_tick() 阶段:
    - 检测价格突破近 N tick 高点 + 成交放量
    - 检测回踩均线后企稳反弹
    - 领涨抗跌：大盘跌但个股不破位
"""

from __future__ import annotations

import logging
import os

import pandas as pd

import config
from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy

logger = logging.getLogger(__name__)


def _load_daily_klines() -> pd.DataFrame | None:
    """加载日线数据。"""
    path = config.KLINES_DAILY_FILE
    if not os.path.exists(path):
        logger.warning(f"日线数据文件不存在: {path}")
        return None
    try:
        df = pd.read_csv(path, compression="gzip", dtype={"symbol": str})
        return df
    except Exception as e:
        logger.error(f"加载日线数据失败: {e}")
        return None


@register_strategy
class TrendBreakoutStrategy(BaseStrategy):
    slug = "trend_breakout"
    name = "产业趋势突破"
    description = "依托产业趋势，检测盘中突破新高、回踩均线企稳、领涨抗跌等信号"

    def prepare(self, ctx: StrategyContext) -> None:
        """预计算日线级别基线指标。"""
        klines = _load_daily_klines()
        if klines is None:
            ctx.state["ready"] = False
            return

        date = ctx.market.date

        # 确保日期列为字符串
        klines["date"] = klines["date"].astype(str)
        klines["symbol"] = klines["symbol"].astype(str).str.zfill(6)

        # 取最新 N 天数据构建个股特征
        sorted_dates = sorted(klines["date"].unique())
        # 找到 <= date 的最新日期作为"昨日"
        prev_dates = [d for d in sorted_dates if d <= date]
        if len(prev_dates) < 5:
            ctx.state["ready"] = False
            return

        recent_dates = prev_dates[-25:]  # 取最近 25 天用于计算
        recent = klines[klines["date"].isin(recent_dates)].copy()
        candidate_symbols = None
        if ctx.candidates:
            candidate_symbols = {
                code[2:] if len(code) > 2 else code for code in ctx.candidates
            }
            recent = recent[recent["symbol"].isin(candidate_symbols)]

        # 按个股计算特征
        features = {}
        for sym, grp in recent.groupby("symbol"):
            grp = grp.sort_values("date")
            if len(grp) < 5:
                continue
            closes = grp["close"].values
            # MA
            ma5 = closes[-5:].mean() if len(closes) >= 5 else closes.mean()
            ma10 = closes[-10:].mean() if len(closes) >= 10 else closes.mean()
            ma20 = closes[-20:].mean() if len(closes) >= 20 else closes.mean()
            # 近 20 日最高收盘价
            high_20d = closes[-20:].max() if len(closes) >= 20 else closes.max()
            # 最新收盘价
            last_close = closes[-1]

            features[str(sym).zfill(6)] = {
                "ma5": ma5,
                "ma10": ma10,
                "ma20": ma20,
                "high_20d": high_20d,
                "last_close": last_close,
                "close_vs_ma5": last_close / ma5 - 1 if ma5 > 0 else 0,
                "close_vs_ma20": last_close / ma20 - 1 if ma20 > 0 else 0,
                "is_near_high": last_close >= high_20d * 0.97,
            }

        ctx.state["features"] = features
        ctx.state["ready"] = True

        # 策略参数（从 YAML params 读取，带默认值）
        ctx.state["breakout_pct"] = ctx.params.get("breakout_pct", 0.02)
        ctx.state["lookback_ticks"] = ctx.params.get("lookback_ticks", 10)
        ctx.state["min_amount"] = ctx.params.get("min_amount", 0)

        # 已触发的股票（防重复）
        ctx.state["alerted_codes"] = set()

        logger.info(f"[{self.slug}] prepare 完成，预计算 {len(features)} 只股票特征")

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        alerts = []
        features = ctx.state.get("features", {})
        alerted = ctx.state.get("alerted_codes", set())
        breakout_pct = ctx.state.get("breakout_pct", 0.02)
        min_amount = ctx.state.get("min_amount", 0)
        lookback = ctx.state.get("lookback_ticks", 10)

        # 成交额过滤
        if min_amount > 0:
            frame = frame[frame["volume"] >= min_amount]

        for _, row in frame.iterrows():
            code = row["code"]
            if code in alerted:
                continue
            # code 格式 "sh600000" → 纯数字 "600000"
            pure_code = code[2:] if len(code) > 2 else code
            feat = features.get(pure_code)
            if feat is None:
                continue

            now_price = row["now"]
            pct_chg = row.get("pct_chg", 0)
            if now_price <= 0:
                continue

            # 信号 1: 突破 20 日新高
            if feat["is_near_high"] and now_price > feat["high_20d"] * (1 + breakout_pct):
                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=f"突破20日新高 {feat['high_20d']:.2f}→{now_price:.2f}, 涨幅{pct_chg:.2f}%",
                    level="important",
                ))
                alerted.add(code)
                continue  # 一只股票只触发一种信号

            # 信号 2: 回踩 MA5/MA10 企稳（昨日接近均线，今日企稳反弹）
            if (
                abs(feat["close_vs_ma5"]) < 0.02  # 昨日收盘接近 MA5
                and pct_chg > 0.5                   # 今日微涨
                and now_price > feat["ma5"]          # 站上 MA5
            ):
                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=f"回踩MA5企稳反弹, MA5={feat['ma5']:.2f}, 涨幅{pct_chg:.2f}%",
                    level="info",
                ))
                alerted.add(code)
                continue

            # 信号 3: 领涨抗跌 — 大盘跌但个股不破位
            market_pct = ctx.market.market_avg_pct_chg
            if (
                market_pct < -0.5         # 大盘下跌
                and pct_chg > 0            # 个股逆势上涨
                and now_price > feat["ma10"]  # 不破 MA10
                and feat.get("close_vs_ma20", 0) > 0  # 整体趋势向上
            ):
                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=f"领涨抗跌: 大盘{market_pct:.2f}%, 个股+{pct_chg:.2f}%, MA10上方",
                    level="warn",
                ))
                alerted.add(code)

        return alerts
