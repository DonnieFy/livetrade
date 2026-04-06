# -*- coding: utf-8 -*-
"""
Livetrade — 信号输出

将策略触发的 Alert 写入文件：
  {ALERT_OUTPUT_DIR}/{YYYY-MM-DD}/{HH_MM_SS}.txt

没有信号时不创建文件。
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import config
from strategy_base import Alert

logger = logging.getLogger(__name__)


class AlertWriter:
    """信号输出管理器。"""

    def __init__(self, date_string: str, overwrite: bool = False):
        """
        参数:
            date_string: 日期，如 "2026-03-26"
            overwrite: 回测模式下为 True，覆盖同名文件
        """
        self.date_string = date_string
        self.overwrite = overwrite
        self.output_dir = os.path.join(config.ALERT_OUTPUT_DIR, date_string)
        self.total_alerts = 0

        os.makedirs(self.output_dir, exist_ok=True)

    def write(self, alerts: list[Alert], tick_time: str | None = None) -> None:
        """将 Alert 列表写入文件。

        参数:
            alerts: Alert 列表（空则跳过）
            tick_time: tick 时间 "HH:MM:SS"，用于生成文件名
        """
        if not alerts:
            return

        # 确定文件名时间
        if tick_time:
            time_str = tick_time.replace(":", "_")
        else:
            time_str = datetime.now().strftime("%H_%M_%S")

        filename = f"{time_str}.txt"
        filepath = os.path.join(self.output_dir, filename)

        # 格式化内容
        lines = [alert.format_line() for alert in alerts]
        content = "\n".join(lines) + "\n"

        # 写入模式
        mode = "w" if self.overwrite else "a"

        # 如果是追加模式且文件已存在，添加分隔
        if mode == "a" and os.path.exists(filepath):
            content = "\n" + content

        with open(filepath, mode, encoding="utf-8") as f:
            f.write(content)

        self.total_alerts += len(alerts)
        logger.info(
            f"输出 {len(alerts)} 条信号 → {filepath}"
        )
        for alert in alerts:
            logger.info(f"  {alert.format_line()}")

    def summary(self) -> str:
        """返回输出统计摘要。"""
        return f"当日共输出 {self.total_alerts} 条信号 → {self.output_dir}"
