# -*- coding: utf-8 -*-
"""
策略：首板1进2

核心逻辑:
    筛选昨日首板股票（连板天数=1），且首板前N天波动较小，
    次日竞价阶段量能合适且高开的股票。

筛选条件:
    1. 昨日首次涨停（连板天数=1）
    2. 首板前N天涨跌幅小（±4%以内），无大起大落
    3. 首板成交额适中（几亿级别，而非几十亿）
    4. 今日竞价金额是昨日成交额的1/10~1/8
    5. 今日高开

prepare() 阶段:
    - 读取 k-lines 日线数据，计算每只股票的连板天数
    - 筛选昨日首板股
    - 计算首板前N天的波动情况
    - 记录首板成交额作为竞价量比基线

on_tick() 阶段:
    - 竞价阶段检测竞价金额 vs 首板成交额的比例
    - 检测开盘强度（高开）
    - 产生信号
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
class FirstBoard1to2Strategy(BaseStrategy):
    slug = "first_board_1to2"
    name = "首板1进2"
    description = "筛选昨日首板且竞价阶段量价配合的股票"

    def prepare(self, ctx: StrategyContext) -> None:
        """加载日线数据，筛选首板股并计算波动基线。"""
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
        if len(prev_dates) < 10:  # 至少需要10天历史数据
            ctx.state["ready"] = False
            return

        prev_date = prev_dates[-1]

        # 计算每只股票的连板天数
        # 需要往前回溯多日数据
        lookback_dates = sorted_dates[max(0, len(sorted_dates) - 20):]  # 取最近20天
        lookback_data = klines[klines["date"].isin(lookback_dates)].copy()

        # 计算涨停价
        def _limit_ratio(sym):
            s = str(sym).zfill(6)
            if s.startswith(("300", "301", "688")):
                return 0.20
            return 0.10

        if "change" in lookback_data.columns:
            lookback_data["pre_close"] = lookback_data["close"] - lookback_data["change"]
        else:
            lookback_data["pre_close"] = lookback_data["close"] / (1 + lookback_data["pct_chg"] / 100)

        lookback_data["limit_ratio"] = lookback_data["symbol"].apply(_limit_ratio)
        lookback_data["limit_up_price"] = (lookback_data["pre_close"] * (1 + lookback_data["limit_ratio"])).round(2)
        lookback_data["is_limit_up"] = lookback_data["close"] == lookback_data["limit_up_price"]

        # 计算每只股票的连板天数（截至昨日）
        consecutive_board = {}
        for symbol in lookback_data["symbol"].unique():
            sym_data = lookback_data[lookback_data["symbol"] == symbol].sort_values("date", ascending=False)
            count = 0
            for _, row in sym_data.iterrows():
                if row["is_limit_up"]:
                    count += 1
                else:
                    break
            consecutive_board[symbol] = count

        # 昨日数据
        prev_day = lookback_data[lookback_data["date"] == prev_date].copy()

        # 筛选昨日首板（连板天数=1）
        prev_day["consecutive_board"] = prev_day["symbol"].map(consecutive_board)
        first_board_stocks = prev_day[prev_day["consecutive_board"] == 1].copy()

        # 计算首板前N天的波动情况
        stable_symbols = set()
        volatility_info = {}  # symbol -> 波动信息
        lookback_days = ctx.params.get("lookback_days", 10)  # 首板前回看天数

        for _, row in first_board_stocks.iterrows():
            symbol = row["symbol"]
            # 获取该股票的最近数据
            sym_data = lookback_data[lookback_data["symbol"] == symbol].sort_values("date").reset_index(drop=True)

            # 找到昨日首板的位置
            target_pos = sym_data[sym_data["date"] == prev_date].index
            if len(target_pos) == 0:
                continue
            target_pos = target_pos[0]

            # 检查前面N天的涨跌幅
            if target_pos < lookback_days:
                continue

            before_board = sym_data.iloc[target_pos - lookback_days:target_pos]

            # 波动判断：每天涨跌幅绝对值的平均值
            avg_abs_pct = before_board["pct_chg"].abs().mean()
            volatility_threshold = ctx.params.get("volatility_threshold", 4.0)

            if avg_abs_pct <= volatility_threshold:
                stable_symbols.add(symbol)
                volatility_info[symbol] = {
                    "avg_abs_pct": float(avg_abs_pct),
                    "lookback_days": lookback_days,
                }

        # 记录首板成交额基线（用于竞价量比计算）
        amount_baseline = {}
        for _, row in first_board_stocks.iterrows():
            symbol = row["symbol"]
            if symbol in stable_symbols:
                amount_baseline[symbol] = row.get("amount", 0)

        # 过滤成交额过大的首板
        max_amount_yi = ctx.params.get("max_amount_yi", 15.0)  # 最大成交额（亿元）
        filtered_symbols = set()
        for symbol in stable_symbols:
            amount_yi = amount_baseline.get(symbol, 0) / 100000000.0  # 转为亿元
            if amount_yi <= max_amount_yi:
                filtered_symbols.add(symbol)
            else:
                logger.debug(f"[{self.slug}] {symbol} 成交额过大 {amount_yi:.2f}亿，跳过")

        # 记录首板股票的昨日数据
        first_board_data = {}
        for _, row in first_board_stocks.iterrows():
            symbol = row["symbol"]
            if symbol in filtered_symbols:
                # 重新计算平均波动用于显示
                vol_data = volatility_info.get(symbol, {})
                avg_abs_pct = vol_data.get("avg_abs_pct", 0)

                first_board_data[symbol] = {
                    "close": float(row["close"]),
                    "amount": float(row.get("amount", 0)),
                    "pct_chg": float(row.get("pct_chg", 0)),
                    "volatility": {
                        "avg_abs_pct": avg_abs_pct,
                    },
                }

        ctx.state["first_board_symbols"] = filtered_symbols
        ctx.state["first_board_data"] = first_board_data
        ctx.state["ready"] = True

        # 策略参数
        ctx.state["auction_amount_ratio_min"] = ctx.params.get("auction_amount_ratio_min", 0.10)  # 竞价金额比例最小值
        ctx.state["auction_amount_ratio_max"] = ctx.params.get("auction_amount_ratio_max", 0.125)  # 竞价金额比例最大值
        ctx.state["min_open_strength"] = ctx.params.get("min_open_strength", 0.02)  # 最小高开幅度

        # 已触发过的股票（防重复报警）
        ctx.state["alerted_codes"] = set()

        logger.info(
            f"[{self.slug}] prepare 完成，昨日首板 {len(first_board_stocks)} 只，"
            f"低波动筛选后 {len(filtered_symbols)} 只"
        )

    def _calculate_score(self, row: pd.Series, fb_data: dict, amount_ratio: float, open_strength: float, ctx: StrategyContext) -> float:
        """计算综合评分

        评分维度：
        1. 竞价强度（30分） - 量比+高开
        2. 前期走势质量（40分） - 波动率低+量能适中
        3. 首板质量（30分） - 成交额合适
        """
        score = 0.0

        # 1. 竞价强度（30分）
        # 量比得分：0.10~0.12得15分，偏离递减
        ratio_min = ctx.state.get("auction_amount_ratio_min", 0.10)
        ratio_max = ctx.state.get("auction_amount_ratio_max", 0.125)
        ratio_optimal = (ratio_min + ratio_max) / 2

        if amount_ratio < ratio_min:
            ratio_score = 0
        elif amount_ratio > ratio_max * 1.5:
            ratio_score = 5
        else:
            # 在范围内，越接近optimal得分越高
            dist = abs(amount_ratio - ratio_optimal)
            ratio_score = max(0, 15 - dist * 100)  # 0~15分

        # 高开得分：2%~5%得15分，超过5%递减
        min_open = ctx.state.get("min_open_strength", 0.02)
        if open_strength < min_open:
            open_score = 0
        elif open_strength > 0.08:
            open_score = 5
        else:
            open_score = min(15, (open_strength - min_open) / 0.03 * 15)

        auction_score = ratio_score + open_score  # 0~30分

        # 2. 前期走势质量（40分）
        # 波动率得分：平均波动越低越好
        avg_volatility = fb_data.get("volatility", {}).get("avg_abs_pct", 10.0)
        volatility_score = max(0, 25 - avg_volatility * 5)  # 0~25分

        # 量能得分：前期量能不能太大（成交额适中）
        yesterday_amount_yi = fb_data["amount"] / 100000000.0
        if yesterday_amount_yi < 2.0:
            volume_score = 15  # 小市值，加分
        elif yesterday_amount_yi < 10.0:
            volume_score = 10
        elif yesterday_amount_yi < 15.0:
            volume_score = 5
        else:
            volume_score = 0  # 太大，不参与

        quality_score = volatility_score + volume_score  # 0~40分

        # 3. 首板质量（30分）
        # 涨停质量：最好是一字板或快速封板（这里用涨幅简单判断）
        first_board_pct = fb_data["pct_chg"]
        if first_board_pct >= 9.9:  # 接近涨停
            board_score = 30
        elif first_board_pct >= 9.0:
            board_score = 20
        elif first_board_pct >= 7.0:
            board_score = 10
        else:
            board_score = 5

        score = auction_score + quality_score + board_score

        return score

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        # 只在竞价阶段检测
        if ctx.market.current_phase != "auction":
            return []

        alerts = []
        first_board_symbols = ctx.state.get("first_board_symbols", set())
        first_board_data = ctx.state.get("first_board_data", {})
        alerted = ctx.state.get("alerted_codes", set())

        ratio_min = ctx.state.get("auction_amount_ratio_min", 0.10)
        ratio_max = ctx.state.get("auction_amount_ratio_max", 0.125)
        min_open = ctx.state.get("min_open_strength", 0.02)

        # 收集所有符合条件的候选股票并评分
        candidates = []

        for _, row in frame.iterrows():
            code = row["code"]
            if code in alerted:
                continue

            pure_code = code[2:] if len(code) > 2 else code

            # 只关注昨日首板股
            if pure_code not in first_board_symbols:
                continue

            fb_data = first_board_data[pure_code]
            yesterday_amount = fb_data["amount"]

            now_price = row["now"]
            close_price = row["close"]  # 昨收
            if now_price <= 0 or close_price <= 0:
                continue

            # 开盘强度（高开）
            open_strength = (now_price - close_price) / close_price
            if open_strength < min_open:
                continue

            # 竞价金额
            today_volume = row.get("volume", 0)  # 这是成交额
            amount_ratio = today_volume / yesterday_amount if yesterday_amount > 0 else 0

            # 竞价金额比例判断（1/10 ~ 1/8）
            if amount_ratio < ratio_min or amount_ratio > ratio_max:
                continue

            # 计算综合评分
            score = self._calculate_score(row, fb_data, amount_ratio, open_strength, ctx)

            candidates.append({
                "code": code,
                "name": row["name"],
                "fb_data": fb_data,
                "amount_ratio": amount_ratio,
                "open_strength": open_strength,
                "today_volume": today_volume,
                "score": score,
            })

        # 按评分排序，取TOP 3
        candidates.sort(key=lambda x: x["score"], reverse=True)
        top_candidates = candidates[:ctx.params.get("top_n", 3)]

        # 生成警报
        for i, cand in enumerate(top_candidates, 1):
            fb_data = cand["fb_data"]
            yesterday_amount_yi = fb_data["amount"] / 100000000.0
            today_volume_wan = cand["today_volume"] / 10000.0
            avg_volatility = fb_data.get("volatility", {}).get("avg_abs_pct", 0)

            msg_parts = [
                f"TOP{i} 评分{cand['score']:.0f}",
                f"昨日首板{fb_data['pct_chg']:.1f}%",
                f"成交额{yesterday_amount_yi:.2f}亿",
                f"前10天平均波动{avg_volatility:.2f}%",
                f"今日竞价{today_volume_wan:.1f}万",
                f"量比{cand['amount_ratio']*100:.1f}%",
                f"高开{cand['open_strength']*100:.1f}%",
            ]

            alerts.append(Alert(
                code=cand["code"],
                name=cand["name"],
                strategy_slug=self.slug,
                strategy_name=self.name,
                message=", ".join(msg_parts),
                level="important",
            ))
            alerted.add(cand["code"])

        return alerts
