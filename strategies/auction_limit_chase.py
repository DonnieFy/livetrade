# -*- coding: utf-8 -*-
"""
策略：竞价涨停加单

9:20后竞价封涨停且封单持续加单的跟风买入信号。

核心逻辑:
    A股竞价规则：9:15-9:20 可撤单（假动作多），9:20-9:25 不可撤单（真金白银）。
    监控 9:20 后封涨停的股票，当封单满足以下任一条件时触发信号：
    1. 瞬间巨量：单帧封单金额骤增超过阈值
    2. 加速加单：连续多帧封单增量递增（每帧增加的越来越多）
    3. 稳态大封：封单金额 > 8000 万且持续增加

    排除 9:20 前已有巨量封单的股票（从未真正撤单，不是"加回来"的模式）。

竞价数据结构:
    bid1 = ask1 = buy = sell = 当前撮合价
    bid1_volume = ask1_volume = 已撮合数量
    bid2_volume = 多余的买单数量（股）
    当 buy == limit_up_price 时，bid2_volume 即为封单量
    封单金额 = bid2_volume × 涨停价
"""

from __future__ import annotations

import logging

import pandas as pd

import config
from context import StrategyContext
from strategy_base import Alert, BaseStrategy, register_strategy

logger = logging.getLogger(__name__)

# 9:20 分界线
TIME_920 = "09:20:00"


@register_strategy
class AuctionLimitChaseStrategy(BaseStrategy):
    slug = "auction_limit_chase"
    name = "竞价涨停加单"
    description = "9:20后竞价封涨停且封单持续加单的跟风买入信号"

    def prepare(self, ctx: StrategyContext) -> None:
        ctx.state["ready"] = True
        ctx.state["alerted_codes"] = set()

        # 9:20 后每股封单历史: {code: [(tick_time, seal_amount), ...]}
        ctx.state["seal_history"] = {}

        # 9:20 前最后一帧封单金额 {code: seal_amount}
        ctx.state["pre920_last_seal"] = {}
        # 9:20 后第一帧封单金额 {code: seal_amount}
        ctx.state["post920_first_seal"] = {}

        # ---- 可配置参数 ----
        # 条件3: 稳态大封阈值 (元)
        ctx.state["seal_threshold"] = ctx.params.get(
            "seal_threshold", 30_000_000
        )  # 3000 万

        # 条件1: 单帧封单增量阈值 (元)，超过立即触发
        ctx.state["sudden_increase_threshold"] = ctx.params.get(
            "sudden_increase_threshold", 20_000_000
        )  # 2000 万

        # 条件2: 加速判断需要的最少连续递增帧数
        ctx.state["accel_min_ticks"] = ctx.params.get("accel_min_ticks", 3)

        # 排除: 封单基准值大于此阈值的股票
        ctx.state["exclude_threshold"] = ctx.params.get(
            "exclude_threshold", 50_000_000
        )  # 5000 万

        logger.info(
            f"[{self.slug}] prepare 完成, "
            f"seal_threshold={ctx.state['seal_threshold']/1e4:.0f}万, "
            f"sudden={ctx.state['sudden_increase_threshold']/1e4:.0f}万, "
            f"accel_ticks={ctx.state['accel_min_ticks']}, "
            f"exclude={ctx.state['exclude_threshold']/1e4:.0f}万"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        tick_time = ctx.market.current_time
        if not tick_time:
            return []

        alerts: list[Alert] = []
        alerted = ctx.state["alerted_codes"]
        seal_history = ctx.state["seal_history"]
        pre920_last_seal = ctx.state["pre920_last_seal"]
        post920_first_seal = ctx.state["post920_first_seal"]

        threshold = ctx.state["seal_threshold"]
        sudden_threshold = ctx.state["sudden_increase_threshold"]
        accel_min = ctx.state["accel_min_ticks"]
        exclude_threshold = ctx.state["exclude_threshold"]

        is_post_920 = tick_time >= TIME_920

        for _, row in frame.iterrows():
            code = row["code"]
            close = row["close"]  # 昨收
            if close <= 0:
                continue

            buy_price = row["buy"]
            bid2_vol = row.get("bid2_volume", 0) or 0

            # limit_up_price 由 engine 的 calc_limit_up_price 已计算
            limit_up = row.get("limit_up_price", 0)
            if limit_up <= 0:
                # 回退: 自行计算
                pure = code[2:] if len(code) > 2 else code
                if pure.startswith(("300", "301", "688")):
                    ratio = config.LIMIT_RATIO_GEM_STAR
                else:
                    ratio = config.LIMIT_RATIO_MAIN
                limit_up = round(close * (1 + ratio), 2)

            # 判断是否在涨停价封单
            # buy == limit_up 表示撮合价在涨停, bid2_volume > 0 表示有多余买单
            is_at_limit = (abs(buy_price - limit_up) < 0.005) and bid2_vol > 0
            seal_amount = bid2_vol * limit_up if is_at_limit else 0

            if not is_post_920:
                # ---- 9:20 前: 记录封单状态 ----
                if is_at_limit and seal_amount > 0:
                    pre920_last_seal[code] = seal_amount
                else:
                    # 不在涨停了 (撤了), 清除
                    pre920_last_seal.pop(code, None)
                continue

            # ==== 9:20 后: 信号检测 ====

            if code in alerted:
                continue

            if not is_at_limit:
                # 不在涨停, 清理历史 (可能后续再涨停再重新追踪)
                seal_history.pop(code, None)
                continue

            # 记录 9:20 后第一帧封单 (仅记录一次)
            if code not in post920_first_seal:
                post920_first_seal[code] = seal_amount

            # 排除逻辑: 取 min(pre920_last, post920_first)
            # 如果封单减少(撤了)用减少后的值(后者), 如果增加用增加前的值(前者)
            # 即取两者中较小值作为排除基准
            pre_seal = pre920_last_seal.get(code, 0)
            first_seal = post920_first_seal.get(code, 0)
            exclude_base = min(pre_seal, first_seal)
            if exclude_base > exclude_threshold:
                continue

            # 记录封单历史
            history = seal_history.setdefault(code, [])
            history.append((tick_time, seal_amount))

            # ---- 条件 1: 瞬间巨量 ----
            # 单帧封单增量超过阈值
            if len(history) >= 2:
                delta = seal_amount - history[-2][1]
                if delta >= sudden_threshold:
                    alerts.append(self._make_alert(
                        code, row["name"], tick_time,
                        f"瞬间巨量加单: "
                        f"封单{self._fmt_amount(seal_amount)}, "
                        f"单帧增加{self._fmt_amount(delta)}",
                        "important",
                    ))
                    alerted.add(code)
                    continue

            # ---- 条件 2: 加速加单 ----
            # 连续 N 帧增量为正且递增
            if len(history) >= accel_min + 1:
                recent = history[-(accel_min + 1):]
                deltas = [
                    recent[i + 1][1] - recent[i][1]
                    for i in range(len(recent) - 1)
                ]
                if (
                    all(d > 0 for d in deltas)
                    and all(deltas[i] > deltas[i - 1]
                            for i in range(1, len(deltas)))
                ):
                    alerts.append(self._make_alert(
                        code, row["name"], tick_time,
                        f"加速加单: "
                        f"封单{self._fmt_amount(seal_amount)}, "
                        f"连续{len(deltas)}帧加速, "
                        f"最新增量{self._fmt_amount(deltas[-1])}",
                        "important",
                    ))
                    alerted.add(code)
                    continue

            # ---- 条件 3: 稳态大封 + 持续增加 ----
            if seal_amount >= threshold and len(history) >= 3:
                recent_3 = [h[1] for h in history[-3:]]
                if all(
                    recent_3[i] > recent_3[i - 1]
                    for i in range(1, len(recent_3))
                ):
                    alerts.append(self._make_alert(
                        code, row["name"], tick_time,
                        f"大封加单: "
                        f"封单{self._fmt_amount(seal_amount)}"
                        f"(>{self._fmt_amount(threshold)}), "
                        f"持续增加中",
                        "warn",
                    ))
                    alerted.add(code)
                    continue

        return alerts

    # ----------------------------------------------------------
    # helpers
    # ----------------------------------------------------------

    @staticmethod
    def _fmt_amount(value: float) -> str:
        """格式化金额: 亿 / 万。"""
        if abs(value) >= 1e8:
            return f"{value / 1e8:.2f}亿"
        return f"{value / 1e4:.0f}万"

    @staticmethod
    def _make_alert(
        code: str, name: str, tick_time: str, message: str, level: str
    ) -> Alert:
        return Alert(
            code=code,
            name=name,
            strategy_slug="auction_limit_chase",
            strategy_name="竞价涨停加单",
            message=message,
            level=level,
            time=tick_time,
        )
