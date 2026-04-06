# -*- coding: utf-8 -*-
"""
策略：冰点修复

对应逻辑文档策略四 — 极值冰点反核与超跌反弹

核心逻辑:
    当市场情绪连续退潮（跌停激增、连板压缩、涨停溢价持续为负），
    做空动能衰竭，博弈情绪拐点修复的反弹溢价。

prepare() 阶段:
    - 读取 k-lines 近 5 日数据
    - 计算涨停溢价指数、封板率、上涨家数等情绪指标
    - 判断是否处于冰点环境

on_tick() 阶段:
    - 冰点环境下检测盘中弱转强（从水下拉起翻红）
    - 检测跌停板撬板（大资金承接）
    - 检测成交量突增的反核先锋
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
class IcePointRepairStrategy(BaseStrategy):
    slug = "ice_point_repair"
    name = "冰点修复"
    description = "冰点环境下检测弱转强、跌停撬板、成交突增等反弹信号"

    def prepare(self, ctx: StrategyContext) -> None:
        """评估当前情绪环境是否为冰点。"""
        analyst_ice = ctx.review.analyst.get("is_ice_point")
        klines_path = config.KLINES_DAILY_FILE
        if not os.path.exists(klines_path):
            ctx.state["ready"] = False
            ctx.state["is_ice_point"] = False
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

        if len(prev_dates) < 3:
            ctx.state["ready"] = False
            return

        def _limit_ratio(sym):
            s = str(sym).zfill(6)
            if s.startswith(("300", "301", "688")):
                return 0.20
            return 0.10

        # 分析近 5 日情绪指标
        check_dates = prev_dates[-5:]
        ice_signals = 0
        daily_stats = []

        for d in check_dates:
            day_data = klines[klines["date"] == d].copy()
            if day_data.empty:
                continue

            # 涨跌家数
            up_count = int((day_data["pct_chg"] > 0).sum())
            down_count = int((day_data["pct_chg"] < 0).sum())

            # 涨停/跌停家数
            if "change" in day_data.columns:
                day_data["pre_close"] = day_data["close"] - day_data["change"]
            else:
                day_data["pre_close"] = day_data["close"] / (1 + day_data["pct_chg"] / 100)

            day_data["lr"] = day_data["symbol"].apply(_limit_ratio)
            day_data["lu_price"] = (day_data["pre_close"] * (1 + day_data["lr"])).round(2)
            day_data["ld_price"] = (day_data["pre_close"] * (1 - day_data["lr"])).round(2)

            limit_up_count = int((day_data["close"] == day_data["lu_price"]).sum())
            limit_down_count = int((day_data["close"] == day_data["ld_price"]).sum())

            daily_stats.append({
                "date": d,
                "up_count": up_count,
                "down_count": down_count,
                "limit_up": limit_up_count,
                "limit_down": limit_down_count,
            })

        # 冰点判定：近 3 日跌停家数较多 + 下跌家数 > 3000
        recent_3 = daily_stats[-3:] if len(daily_stats) >= 3 else daily_stats
        for stat in recent_3:
            if stat["limit_down"] >= 15:
                ice_signals += 1
            if stat["down_count"] >= 3000:
                ice_signals += 1
            if stat["limit_up"] < 20:
                ice_signals += 1

        is_ice = ice_signals >= 3  # 多个冰点信号
        if analyst_ice is not None:
            is_ice = bool(analyst_ice)

        ctx.state["is_ice_point"] = is_ice
        ctx.state["daily_stats"] = daily_stats
        ctx.state["ready"] = True

        # 策略参数
        ctx.state["min_reversal_pct"] = ctx.params.get("min_reversal_pct", 3.0)
        ctx.state["min_volume_surge"] = ctx.params.get("min_volume_surge", 2.0)

        # 已触发的股票
        ctx.state["alerted_codes"] = set()

        # 上一帧各股票价格（用于检测分时弱转强）
        ctx.state["prev_prices"] = {}

        env_desc = "🧊 冰点环境" if is_ice else "非冰点"
        logger.info(
            f"[{self.slug}] prepare 完成, {env_desc}, "
            f"冰点信号得分 {ice_signals}"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        # 即使非冰点环境也要更新价格，为可能的环境转换做准备
        # 但只在冰点环境下产生信号
        alerts = []
        prev_prices = ctx.state.get("prev_prices", {})
        alerted = ctx.state.get("alerted_codes", set())
        is_ice = ctx.state.get("is_ice_point", False)
        min_reversal = ctx.state.get("min_reversal_pct", 3.0)
        min_vol_surge = ctx.state.get("min_volume_surge", 2.0)

        for _, row in frame.iterrows():
            code = row["code"]
            now_price = row["now"]
            close_price = row["close"]  # 昨收
            pct_chg = row.get("pct_chg", 0)

            if now_price <= 0 or close_price <= 0:
                # 更新价格并跳过
                prev_prices[code] = now_price
                continue

            # 记录上一帧价格
            prev_price = prev_prices.get(code, now_price)
            prev_prices[code] = now_price

            if not is_ice:
                continue

            if code in alerted:
                continue

            # 信号 1: 分时弱转强 — 从水下拉起翻红
            # 上一帧下跌，现在翻红
            prev_pct = (prev_price - close_price) / close_price * 100 if close_price > 0 else 0
            if prev_pct < -1.0 and pct_chg > 0.5:
                reversal_amplitude = pct_chg - prev_pct
                if reversal_amplitude >= min_reversal:
                    alerts.append(Alert(
                        code=code,
                        name=row["name"],
                        strategy_slug=self.slug,
                        strategy_name=self.name,
                        message=(
                            f"分时弱转强: {prev_pct:.1f}%→{pct_chg:.1f}%, "
                            f"反转幅度{reversal_amplitude:.1f}%"
                        ),
                        level="important",
                    ))
                    alerted.add(code)
                    continue

            # 信号 2: 跌停撬板 — 接近跌停但大幅拉起
            is_near_limit_down = row.get("is_limit_down", False)
            if is_near_limit_down and pct_chg > row.get("limit_ratio", 0.10) * -100 + 2:
                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=f"跌停撬板信号: 当前{pct_chg:.1f}%, 从跌停附近拉起",
                    level="important",
                ))
                alerted.add(code)
                continue

            # 信号 3: 冰点日逆势抗跌翻红
            market_pct = ctx.market.market_avg_pct_chg
            if (
                market_pct < -1.0       # 大盘下跌
                and pct_chg > 2.0        # 个股明显上涨
                and ctx.market.market_down_count > 3000  # 普跌
            ):
                alerts.append(Alert(
                    code=code,
                    name=row["name"],
                    strategy_slug=self.slug,
                    strategy_name=self.name,
                    message=(
                        f"冰点逆势翻红: 大盘{market_pct:.1f}%, "
                        f"个股+{pct_chg:.1f}%, "
                        f"下跌{ctx.market.market_down_count}家"
                    ),
                    level="warn",
                ))
                alerted.add(code)

        ctx.state["prev_prices"] = prev_prices
        return alerts
