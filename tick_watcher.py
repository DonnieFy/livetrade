# -*- coding: utf-8 -*-
"""
Livetrade — 文件监听与增量读取

基于轮询的文件监听器，检查当日三个 CSV 文件的大小变化，
文件变大时 seek 到上次偏移量读取新增内容。

包含两种实现：
- TickWatcher: 实盘轮询监听
- ReplayWatcher: 回测模式，将历史 .csv.gz 文件按帧模拟回放
"""

from __future__ import annotations

import gzip
import logging
import os
import time
from typing import Callable

import pandas as pd

import config
from tick_parser import parse_csv_text, extract_tick_time

logger = logging.getLogger(__name__)

# 回调签名: on_new_rows(phase: str, new_df: pd.DataFrame, tick_time: str)
OnNewRowsCallback = Callable[[str, pd.DataFrame, str], None]


class TickWatcher:
    """轮询文件监听器 — 实盘模式。"""

    def __init__(self, date_string: str, callback: OnNewRowsCallback):
        self.date_string = date_string
        self.callback = callback
        self.data_dir = os.path.join(config.TICKS_DATA_DIR, date_string)
        self.running = False

        # 文件偏移量记录 {phase: byte_offset}
        self._offsets: dict[str, int] = {}

    def _get_csv_path(self, phase: str) -> str:
        """获取阶段对应的 CSV 文件路径。"""
        return os.path.join(
            self.data_dir, f"{self.date_string}_{phase}.csv"
        )

    def start(self) -> None:
        """启动轮询循环（阻塞当前线程）。"""
        self.running = True
        logger.info(f"TickWatcher 启动，监听目录: {self.data_dir}")

        while self.running:
            for phase in config.ALL_PHASES:
                csv_path = self._get_csv_path(phase)

                if not os.path.exists(csv_path):
                    # 文件尚未创建，跳过
                    continue

                try:
                    self._check_file(phase, csv_path)
                except Exception as e:
                    logger.error(f"检查文件 {csv_path} 失败: {e}", exc_info=True)

            time.sleep(config.WATCHER_POLL_INTERVAL)

    def stop(self) -> None:
        """停止轮询。"""
        self.running = False
        logger.info("TickWatcher 停止")

    def _check_file(self, phase: str, csv_path: str) -> None:
        """检查单个文件的增量变化。"""
        current_size = os.path.getsize(csv_path)
        last_offset = self._offsets.get(phase, 0)

        if current_size < last_offset:
            # 文件被截断或替换，重置偏移量
            logger.warning(f"[{phase}] 文件被截断/替换，重置偏移量")
            last_offset = 0

        if current_size <= last_offset:
            return  # 无新数据

        # 读取增量内容
        with open(csv_path, "r", encoding="utf-8") as f:
            f.seek(last_offset)
            new_content = f.read()

        self._offsets[phase] = current_size

        if not new_content.strip():
            return

        # 解析并回调
        df = parse_csv_text(new_content)
        if df.empty:
            return

        tick_time = extract_tick_time(df) or ""
        logger.debug(
            f"[{phase}] 增量读取: {len(df)} 行, "
            f"偏移 {last_offset} → {current_size}, "
            f"tick_time={tick_time}"
        )
        self.callback(phase, df, tick_time)


class ReplayWatcher:
    """回测/回放模式 — 将历史 .csv.gz 按帧模拟回调。"""

    def __init__(self, date_string: str, callback: OnNewRowsCallback,
                 data_dir: str | None = None):
        self.date_string = date_string
        self.callback = callback
        self.data_dir = data_dir or os.path.join(config.TICKS_DATA_DIR, date_string)

    def start(self) -> None:
        """按阶段顺序回放所有帧。"""
        logger.info(f"ReplayWatcher 启动，回放日期: {self.date_string}")

        for phase in config.ALL_PHASES:
            self._replay_phase(phase)

        logger.info("ReplayWatcher 回放完成")

    def _replay_phase(self, phase: str) -> None:
        """回放单个阶段的数据文件（支持大文件分块读取）。"""
        gz_path = os.path.join(
            self.data_dir, f"{self.date_string}_{phase}.csv.gz"
        )
        csv_path = os.path.join(
            self.data_dir, f"{self.date_string}_{phase}.csv"
        )

        if os.path.exists(gz_path):
            file_path = gz_path
            compression = "gzip"
            logger.info(f"[{phase}] 读取压缩文件: {gz_path}")
        elif os.path.exists(csv_path):
            file_path = csv_path
            compression = None
            logger.info(f"[{phase}] 读取 CSV 文件: {csv_path}")
        else:
            logger.warning(f"[{phase}] 无数据文件")
            return

        # 使用 pandas 分块读取，每次读 50000 行（约 10 帧 × 5000 股）
        CHUNK_SIZE = 50000
        frame_count = 0
        pending_rows = pd.DataFrame()  # 跨 chunk 边界的未完成帧

        try:
            reader = pd.read_csv(
                file_path,
                names=config.CSV_COLUMNS,
                header=None,
                compression=compression,
                chunksize=CHUNK_SIZE,
                dtype=str,  # 先全部读为字符串，parse 时再转
            )
        except Exception as e:
            logger.error(f"[{phase}] 打开文件失败: {e}")
            return

        for chunk in reader:
            # 数值列类型转换
            for col in config.NUMERIC_COLUMNS:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors="coerce")

            # 合并上一 chunk 遗留的不完整帧
            if not pending_rows.empty:
                chunk = pd.concat([pending_rows, chunk], ignore_index=True)
                pending_rows = pd.DataFrame()

            if "time" not in chunk.columns or chunk.empty:
                continue

            # 最后一个 time 值可能跨 chunk 边界，暂存到下一轮
            last_time = chunk.iloc[-1]["time"]
            tail_mask = chunk["time"] == last_time
            pending_rows = chunk[tail_mask].copy()
            chunk = chunk[~tail_mask]

            if chunk.empty:
                continue

            # 按 time 列分帧
            for tick_time, frame_df in chunk.groupby("time", sort=True):
                frame_df = frame_df.reset_index(drop=True)
                self.callback(phase, frame_df, str(tick_time))
                frame_count += 1

        # 处理最后一批 pending_rows
        if not pending_rows.empty:
            tick_time = str(pending_rows.iloc[0]["time"])
            pending_rows = pending_rows.reset_index(drop=True)
            self.callback(phase, pending_rows, tick_time)
            frame_count += 1

        logger.info(f"[{phase}] 回放完成，共 {frame_count} 帧")

    def stop(self) -> None:
        """回放模式无需停止。"""
        pass
