# -*- coding: utf-8 -*-
"""
盘面脉搏 — 时间点状态机

管理 T1(09:25) / T2(09:30) / T3(09:33) 三个触发时点，
确保每个时点只触发一次，并缓存各时点的快照数据。

状态存储在 ctx.state 中：
    fired_timepoints: set[str]          — 已触发的时点集合
    auction_0925_snapshot: dict|None    — T1 时刻全市场快照
    open_0930_snapshot: dict|None       — T2 时刻全市场快照
    latest_0933_snapshot: dict|None     — T3 时刻全市场快照
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# 时点定义：(时点ID, 触发时间前缀, 描述)
TIMEPOINTS = [
    ("T1", "09:25", "竞价结束"),
    ("T2", "09:30", "开盘"),
    ("T3", "09:33", "开盘3分钟"),
]

# 状态 key 前缀
SNAPSHOT_KEY_MAP = {
    "T1": "auction_0925_snapshot",
    "T2": "open_0930_snapshot",
    "T3": "latest_0933_snapshot",
}


@dataclass
class TimepointResult:
    """时点触发结果。"""
    timepoint_id: str       # "T1" / "T2" / "T3"
    label: str              # 描述
    tick_time: str          # 实际触发时间 "HH:MM:SS"


def init_state(state: dict) -> None:
    """初始化状态机（在 prepare() 中调用）。"""
    state["fired_timepoints"] = set()
    for key in SNAPSHOT_KEY_MAP.values():
        state[key] = None


def check_timepoint(tick_time: str, state: dict) -> TimepointResult | None:
    """检查当前 tick 是否命中某个未触发的时点。

    判断逻辑：tick_time >= 触发时间 且 该时点尚未触发。
    每个时点只返回一次。

    参数:
        tick_time: 当前 tick 时间 "HH:MM:SS" 或 "HH:MM"
        state: ctx.state

    返回:
        命中时返回 TimepointResult，否则返回 None
    """
    fired: set = state.get("fired_timepoints", set())

    for tp_id, tp_time, label in TIMEPOINTS:
        if tp_id in fired:
            continue
        if tick_time >= tp_time:
            fired.add(tp_id)
            state["fired_timepoints"] = fired
            logger.info(f"[market_pulse] 触发时点 {tp_id} ({label}) @ {tick_time}")
            return TimepointResult(
                timepoint_id=tp_id,
                label=label,
                tick_time=tick_time,
            )

    return None


def save_snapshot(timepoint_id: str, state: dict, snapshots: dict) -> None:
    """保存某时点的全市场快照到状态中。

    参数:
        timepoint_id: "T1" / "T2" / "T3"
        state: ctx.state
        snapshots: 全市场 {code: StockSnapshot} 的浅拷贝字典
    """
    key = SNAPSHOT_KEY_MAP.get(timepoint_id)
    if key is None:
        return
    # 只保存计算需要的字段，避免持有全量引用
    snapshot_data = {}
    for code, snap in snapshots.items():
        snapshot_data[code] = {
            "code": snap.code,
            "name": snap.name,
            "close": snap.close,
            "open": snap.open,
            "high": snap.high,
            "low": snap.low,
            "volume": snap.volume,
            "pct_chg": snap.pct_chg,
            "is_limit_up": snap.is_limit_up,
            "is_limit_down": snap.is_limit_down,
            "limit_up_price": snap.limit_up_price,
            "limit_down_price": snap.limit_down_price,
        }
    state[key] = snapshot_data


def get_saved_snapshot(timepoint_id: str, state: dict) -> dict | None:
    """获取之前保存的时点快照。"""
    key = SNAPSHOT_KEY_MAP.get(timepoint_id)
    if key is None:
        return None
    return state.get(key)
