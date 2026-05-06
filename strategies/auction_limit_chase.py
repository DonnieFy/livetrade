# -*- coding: utf-8 -*-
"""
策略：竞价涨停加单

9:20后竞价封涨停且盘口强化的领先竞价信号。

核心逻辑:
    A股竞价规则：9:15-9:20 可撤单（假动作多），9:20-9:25 不可撤单（真金白银）。
    监控 9:20 后站上涨停价的股票，拆成三个层面的信号：
    1. 一字指引：9:20 前就有大封单，9:20 后仍位于全场最强封单前列
    2. 转瞬封住：9:20 前并未封死，9:20 后突然封住并快速强化，具备排队买入窗口
    3. 末尾回封：中途明显漏单后，9:24 附近卖盘骤减并重新补出显性封单

竞价数据结构:
    bid1 = ask1 = buy = sell = 当前撮合价
    bid1_volume = ask1_volume = 已撮合数量
    bid2_volume = 涨停价上的显性剩余买单
    ask2_volume 的快速收缩，往往也代表上方卖单被快速承接
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
    description = "9:20后竞价封涨停且封单持续强化的领先竞价信号"
    signal_role = "primary"
    review_candidates_mode = "ignore"

    def prepare(self, ctx: StrategyContext) -> None:
        ctx.state["ready"] = True
        ctx.state["alerted_signals"] = set()

        # 9:20 后每股封单历史: {code: [(tick_time, seal_amount), ...]}
        ctx.state["seal_history"] = {}
        ctx.state["ask2_history"] = {}
        ctx.state["had_mid_leak_codes"] = set()
        ctx.state["stats"] = {
            "post920_limit_count": 0,
            "top_rank_seen_count": 0,
            "guide_watch_count": 0,
            "queue_watch_count": 0,
            "late_reseal_watch_count": 0,
            "guide_alert_count": 0,
            "queue_alert_count": 0,
            "late_reseal_alert_count": 0,
            "alert_count": 0,
        }

        # 9:20 前最后一帧封单金额 {code: seal_amount}
        ctx.state["pre920_last_seal"] = {}
        # 9:20 后第一帧封单金额 {code: seal_amount}
        ctx.state["post920_first_seal"] = {}

        # ---- 可配置参数 ----
        # 通用：只看足够大的封单
        ctx.state["seal_threshold"] = ctx.params.get(
            "seal_threshold", 30_000_000
        )

        # 转瞬封住：单帧封单增量阈值 (元)，超过立即触发
        ctx.state["sudden_increase_threshold"] = ctx.params.get(
            "sudden_increase_threshold", 20_000_000
        )

        # 转瞬封住：加速判断需要的最少连续递增帧数
        ctx.state["accel_min_ticks"] = ctx.params.get("accel_min_ticks", 3)

        # 只看当下竞价最强的少数封单
        ctx.state["top_rank_max"] = ctx.params.get("top_rank_max", 3)
        ctx.state["min_growth_ratio"] = ctx.params.get("min_growth_ratio", 0.08)
        ctx.state["min_reseal_delta"] = ctx.params.get("min_reseal_delta", 5_000_000)
        ctx.state["new_seal_cutoff"] = ctx.params.get("new_seal_cutoff", "09:22:30")
        ctx.state["new_seal_pre920_ceiling"] = ctx.params.get(
            "new_seal_pre920_ceiling", 5_000_000
        )
        ctx.state["late_reseal_cutoff"] = ctx.params.get("late_reseal_cutoff", "09:24:30")
        ctx.state["late_reseal_min_bid2"] = ctx.params.get("late_reseal_min_bid2", 100_000)
        ctx.state["late_reseal_min_ask2_drop"] = ctx.params.get(
            "late_reseal_min_ask2_drop", 1_000_000
        )
        ctx.state["late_reseal_min_ask2_drop_ratio"] = ctx.params.get(
            "late_reseal_min_ask2_drop_ratio", 0.7
        )

        # 一字指引：只看最强的一字封单
        ctx.state["guide_rank_max"] = ctx.params.get("guide_rank_max", 2)
        ctx.state["guide_seal_threshold"] = ctx.params.get(
            "guide_seal_threshold", 100_000_000
        )
        ctx.state["guide_min_ticks"] = ctx.params.get("guide_min_ticks", 2)

        logger.info(
            f"[{self.slug}] prepare 完成, "
            f"seal_threshold={ctx.state['seal_threshold']/1e4:.0f}万, "
            f"sudden={ctx.state['sudden_increase_threshold']/1e4:.0f}万, "
            f"accel_ticks={ctx.state['accel_min_ticks']}, "
            f"top_rank<={ctx.state['top_rank_max']}, "
            f"guide_rank<={ctx.state['guide_rank_max']}"
        )

    def on_tick(self, frame: pd.DataFrame, ctx: StrategyContext) -> list[Alert]:
        if not ctx.state.get("ready"):
            return []

        tick_time = ctx.market.current_time
        if not tick_time:
            return []

        alerts: list[Alert] = []
        alerted_signals = ctx.state["alerted_signals"]
        seal_history = ctx.state["seal_history"]
        ask2_history = ctx.state["ask2_history"]
        had_mid_leak_codes = ctx.state["had_mid_leak_codes"]
        pre920_last_seal = ctx.state["pre920_last_seal"]
        post920_first_seal = ctx.state["post920_first_seal"]
        stats = ctx.state["stats"]

        threshold = ctx.state["seal_threshold"]
        sudden_threshold = ctx.state["sudden_increase_threshold"]
        accel_min = ctx.state["accel_min_ticks"]
        top_rank_max = ctx.state["top_rank_max"]
        min_growth_ratio = ctx.state["min_growth_ratio"]
        min_reseal_delta = ctx.state["min_reseal_delta"]
        new_seal_cutoff = ctx.state["new_seal_cutoff"]
        new_seal_pre920_ceiling = ctx.state["new_seal_pre920_ceiling"]
        late_reseal_cutoff = ctx.state["late_reseal_cutoff"]
        late_reseal_min_bid2 = ctx.state["late_reseal_min_bid2"]
        late_reseal_min_ask2_drop = ctx.state["late_reseal_min_ask2_drop"]
        late_reseal_min_ask2_drop_ratio = ctx.state["late_reseal_min_ask2_drop_ratio"]
        guide_rank_max = ctx.state["guide_rank_max"]
        guide_seal_threshold = ctx.state["guide_seal_threshold"]
        guide_min_ticks = ctx.state["guide_min_ticks"]

        is_post_920 = tick_time >= TIME_920

        # 当前帧的全市场封单强度排名，用来约束只看最领先的一小撮票
        ranked_seals: dict[str, tuple[int, float]] = {}
        if is_post_920:
            current_seals = []
            for _, row in frame.iterrows():
                close = row["close"]
                if close <= 0:
                    continue
                buy_price = row["buy"]
                bid2_vol = row.get("bid2_volume", 0) or 0
                limit_up = row.get("limit_up_price", 0)
                if limit_up <= 0:
                    pure = row["code"][2:] if len(row["code"]) > 2 else row["code"]
                    ratio = config.LIMIT_RATIO_GEM_STAR if pure.startswith(("300", "301", "688")) else config.LIMIT_RATIO_MAIN
                    limit_up = round(close * (1 + ratio), 2)
                is_at_limit = abs(buy_price - limit_up) < 0.005
                if not is_at_limit:
                    continue
                seal_amount = bid2_vol * limit_up
                current_seals.append((row["code"], seal_amount))
            current_seals.sort(key=lambda item: item[1], reverse=True)
            ranked_seals = {
                code: (idx + 1, seal_amount)
                for idx, (code, seal_amount) in enumerate(current_seals)
            }

        for _, row in frame.iterrows():
            code = row["code"]
            close = row["close"]  # 昨收
            if close <= 0:
                continue

            buy_price = row["buy"]
            bid2_vol = row.get("bid2_volume", 0) or 0
            ask2_vol = row.get("ask2_volume", 0) or 0

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

            # 竞价阶段只要撮合价在涨停价，就认为“站上涨停价”
            is_at_limit = abs(buy_price - limit_up) < 0.005
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

            if not is_at_limit:
                history = seal_history.setdefault(code, [])
                ask2_hist = ask2_history.setdefault(code, [])
                if history:
                    history.append((tick_time, 0.0))
                    ask2_hist.append((tick_time, float(ask2_vol)))
                    had_mid_leak_codes.add(code)
                continue

            stats["post920_limit_count"] += 1

            # 记录 9:20 后第一帧封单 (仅记录一次)
            if code not in post920_first_seal:
                post920_first_seal[code] = seal_amount

            # 记录封单 / ask2 历史，供后续多分支共用
            history = seal_history.setdefault(code, [])
            history.append((tick_time, seal_amount))
            ask2_hist = ask2_history.setdefault(code, [])
            ask2_hist.append((tick_time, float(ask2_vol)))

            # 3) 末尾回封：中途漏单后，尾盘 ask2 快速收缩并重新补出显性封单
            late_key = f"{code}:late_reseal"
            if tick_time >= late_reseal_cutoff and late_key not in alerted_signals:
                stats["late_reseal_watch_count"] += 1
                prev_ask2 = ask2_hist[-2][1] if len(ask2_hist) >= 2 else 0.0
                ask2_drop = prev_ask2 - float(ask2_vol)
                ask2_drop_ratio = ask2_drop / prev_ask2 if prev_ask2 > 0 else 0.0
                had_mid_leak = (
                    code in had_mid_leak_codes
                    or any(item[1] <= 0 for item in history[:-1])
                )
                resealed_now = bid2_vol >= late_reseal_min_bid2

                if (
                    had_mid_leak
                    and resealed_now
                    and ask2_drop >= late_reseal_min_ask2_drop
                    and ask2_drop_ratio >= late_reseal_min_ask2_drop_ratio
                ):
                    alerts.append(self._make_alert(
                        code,
                        row["name"],
                        tick_time,
                        (
                            f"末尾回封: 先漏单后回封, ask2减少{int(ask2_drop):,}股"
                            f"({ask2_drop_ratio:.0%}), 显性封单回到{int(bid2_vol):,}股"
                        ),
                        "important",
                    ))
                    stats["late_reseal_alert_count"] += 1
                    stats["alert_count"] += 1
                    alerted_signals.add(late_key)
                    continue

            rank_info = ranked_seals.get(code)
            if not rank_info:
                continue
            rank_no, current_rank_seal = rank_info
            if rank_no > top_rank_max:
                continue
            if current_rank_seal < threshold:
                continue
            stats["top_rank_seen_count"] += 1

            # 基准封单：取 9:20 前最后一帧和 9:20 后第一帧的较小值
            pre_seal = pre920_last_seal.get(code, 0)
            first_seal = post920_first_seal.get(code, 0)
            baseline = min(pre_seal, first_seal) if pre_seal and first_seal else max(pre_seal, first_seal)
            net_increase = seal_amount - baseline if baseline > 0 else seal_amount
            growth_ratio = net_increase / baseline if baseline > 0 else 1.0

            # 1) 一字指引：9:20 前已有大封，9:20 后仍处于最强前排
            guide_key = f"{code}:guide"
            is_guide = (
                pre_seal >= guide_seal_threshold
                and rank_no <= guide_rank_max
                and seal_amount >= guide_seal_threshold
            )
            if is_guide:
                stats["guide_watch_count"] += 1
                if guide_key not in alerted_signals and len(history) >= guide_min_ticks:
                    alerts.append(self._make_alert(
                        code,
                        row["name"],
                        tick_time,
                        (
                            f"一字指引: 封单{self._fmt_amount(seal_amount)}(排位{rank_no}), "
                            f"9:20前已有大封{self._fmt_amount(pre_seal)}, "
                            f"继续维持最强指引"
                        ),
                        "warn",
                    ))
                    stats["guide_alert_count"] += 1
                    stats["alert_count"] += 1
                    alerted_signals.add(guide_key)
                    continue

            # 2) 转瞬封住：9:20 前没封住，9:20 后突然封板且快速强化
            queue_key = f"{code}:queue"
            is_newly_sealed = pre_seal <= new_seal_pre920_ceiling
            if not is_newly_sealed or tick_time > new_seal_cutoff:
                continue

            stats["queue_watch_count"] += 1

            if queue_key in alerted_signals:
                pass
            else:
                if len(history) == 1 and first_seal >= threshold:
                    alerts.append(self._make_alert(
                        code,
                        row["name"],
                        tick_time,
                        (
                            f"转瞬封住: 9:20前未封死, 首次显性封单{self._fmt_amount(first_seal)}"
                            f"(排位{rank_no}), 可排队观察"
                        ),
                        "important",
                    ))
                    stats["queue_alert_count"] += 1
                    stats["alert_count"] += 1
                    alerted_signals.add(queue_key)
                    continue

                if len(history) >= 2:
                    delta = seal_amount - history[-2][1]
                    if delta >= sudden_threshold:
                        alerts.append(self._make_alert(
                            code,
                            row["name"],
                            tick_time,
                            (
                                f"转瞬封住强化: 显性封单{self._fmt_amount(seal_amount)}(排位{rank_no}), "
                                f"单帧新增{self._fmt_amount(delta)}"
                            ),
                            "important",
                        ))
                        stats["queue_alert_count"] += 1
                        stats["alert_count"] += 1
                        alerted_signals.add(queue_key)
                        continue

                if len(history) >= accel_min + 1:
                    recent = history[-(accel_min + 1):]
                    deltas = [
                        recent[i + 1][1] - recent[i][1]
                        for i in range(len(recent) - 1)
                    ]
                    if (
                        all(d > 0 for d in deltas)
                        and all(deltas[i] > deltas[i - 1] for i in range(1, len(deltas)))
                    ):
                        alerts.append(self._make_alert(
                            code,
                            row["name"],
                            tick_time,
                            (
                                f"转瞬封住加速: 显性封单{self._fmt_amount(seal_amount)}(排位{rank_no}), "
                                f"连续{len(deltas)}帧递增强化"
                            ),
                            "important",
                        ))
                        stats["queue_alert_count"] += 1
                        stats["alert_count"] += 1
                        alerted_signals.add(queue_key)
                        continue

                if len(history) >= 3:
                    recent_3 = [h[1] for h in history[-3:]]
                    if all(
                        recent_3[i] > recent_3[i - 1]
                        for i in range(1, len(recent_3))
                    ) and (growth_ratio >= min_growth_ratio or net_increase >= min_reseal_delta):
                        alerts.append(self._make_alert(
                            code,
                            row["name"],
                            tick_time,
                            (
                                f"转瞬封住稳态: 显性封单{self._fmt_amount(seal_amount)}(排位{rank_no}), "
                                f"较基准净增{self._fmt_amount(net_increase)}, 持续强化"
                            ),
                            "warn",
                        ))
                        stats["queue_alert_count"] += 1
                        stats["alert_count"] += 1
                        alerted_signals.add(queue_key)
                        continue

        return alerts

    def on_phase_end(self, phase: str, ctx: StrategyContext) -> None:
        if phase != config.PHASE_AUCTION_OPEN:
            return

        stats = ctx.state.get("stats", {})
        logger.info(
            "[%s] auction_open总结: post920观察=%s, 前排观察=%s, 一字观察=%s, 转瞬观察=%s, 回封观察=%s, 一字信号=%s, 转瞬信号=%s, 回封信号=%s, 实际信号=%s",
            self.slug,
            stats.get("post920_limit_count", 0),
            stats.get("top_rank_seen_count", 0),
            stats.get("guide_watch_count", 0),
            stats.get("queue_watch_count", 0),
            stats.get("late_reseal_watch_count", 0),
            stats.get("guide_alert_count", 0),
            stats.get("queue_alert_count", 0),
            stats.get("late_reseal_alert_count", 0),
            stats.get("alert_count", 0),
        )

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
