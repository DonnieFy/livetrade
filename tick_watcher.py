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
import numpy as np
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
    """回测/回放模式 — 将历史 .csv.gz 按帧模拟回调。

    优化策略：一次性加载整个文件并预计算衍生列，避免逐帧重复计算。
    支持按时间范围裁剪，只加载和回放指定时间段的数据。
    """

    def __init__(self, date_string: str, callback: OnNewRowsCallback,
                 data_dir: str | None = None,
                 time_range: tuple[str, str] | None = None):
        self.date_string = date_string
        self.callback = callback
        self.data_dir = data_dir or os.path.join(config.TICKS_DATA_DIR, date_string)
        self.time_range = time_range  # ("09:25", "10:00") 或 None（全量）

    def start(self) -> None:
        """按阶段顺序回放所有帧。"""
        logger.info(f"ReplayWatcher 启动，回放日期: {self.date_string}")

        for phase in config.ALL_PHASES:
            self._replay_phase(phase)

        logger.info("ReplayWatcher 回放完成")

    # 回测所需的最小列集（策略 + 预计算衍生列所需的源列）
    _ESSENTIAL_COLUMNS = [
        "code", "name", "close", "now", "high", "low",
        "open", "volume", "turnover", "time",
    ]

    def _replay_phase(self, phase: str) -> None:
        """回放单个阶段 — 分块加载 + 提前过滤 + 预计算 + 按帧回调。"""
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

        import time as _time
        t0 = _time.time()

        # 确定需要的列名（只读必要列，减少内存和解压开销）
        essential = set(self._ESSENTIAL_COLUMNS)
        usecols = [c for c in config.CSV_COLUMNS if c in essential]

        # 分块读取 + 提前过滤
        has_time_filter = self.time_range is not None
        chunks = []
        total_raw = 0
        try:
            for chunk in pd.read_csv(
                file_path,
                names=config.CSV_COLUMNS,
                header=None,
                compression=compression,
                dtype=str,
                usecols=usecols,
                chunksize=1_000_000,
            ):
                total_raw += len(chunk)
                if has_time_filter and "time" in chunk.columns:
                    t_start, t_end = self.time_range
                    chunk = chunk[
                        (chunk["time"] >= t_start) & (chunk["time"] <= t_end)
                    ]
                if not chunk.empty:
                    chunks.append(chunk)
        except Exception as e:
            logger.error(f"[{phase}] 打开文件失败: {e}")
            return

        if not chunks:
            if has_time_filter:
                logger.info(
                    f"[{phase}] 时间裁剪 {self.time_range[0]}~{self.time_range[1]}: "
                    f"{total_raw}→0 行 (裁剪 {total_raw} 行)"
                )
                logger.info(f"[{phase}] 裁剪后无数据，跳过")
            return

        t_load = _time.time()

        if len(chunks) == 1:
            df = chunks[0]
        else:
            df = pd.concat(chunks, ignore_index=True)

        if has_time_filter:
            logger.info(
                f"[{phase}] 时间裁剪 {self.time_range[0]}~{self.time_range[1]}: "
                f"{total_raw}→{len(df)} 行 (裁剪 {total_raw - len(df)} 行)"
            )

        # 释放 chunks 内存
        del chunks

        # 批量转换数值列
        numeric_in_df = [c for c in config.NUMERIC_COLUMNS if c in df.columns]
        for col in numeric_in_df:
            arr = df[col].values
            try:
                df[col] = arr.astype(np.float64)
            except (ValueError, TypeError):
                df[col] = pd.to_numeric(arr, errors="coerce")

        t_numeric = _time.time()

        # 预计算衍生列
        close_vals = df["close"].values
        now_vals = df["now"].values
        codes = df["code"].values

        mask = close_vals > 0
        pct_chg = np.zeros(len(df), dtype=np.float64)
        pct_chg[mask] = np.round((now_vals[mask] - close_vals[mask]) / close_vals[mask] * 100, 4)
        df["pct_chg"] = pct_chg

        codes_str = codes.astype(str)
        pure_codes = np.array([c[2:] if len(c) > 6 else c for c in codes_str])
        limit_ratios = np.where(
            np.char.startswith(pure_codes, "300")
            | np.char.startswith(pure_codes, "301")
            | np.char.startswith(pure_codes, "688"),
            config.LIMIT_RATIO_GEM_STAR,
            config.LIMIT_RATIO_MAIN,
        )
        df["limit_ratio"] = limit_ratios
        df["limit_up_price"] = np.round(close_vals * (1 + limit_ratios), 2)
        df["limit_down_price"] = np.round(close_vals * (1 - limit_ratios), 2)
        df["is_limit_up"] = now_vals >= df["limit_up_price"].values
        df["is_limit_down"] = now_vals <= df["limit_down_price"].values

        t_calc = _time.time()

        # 按 time 分帧回调
        frame_count = 0
        for tick_time, frame_df in df.groupby("time", sort=True):
            frame_df = frame_df.reset_index(drop=True)
            self.callback(phase, frame_df, str(tick_time))
            frame_count += 1

        t_end = _time.time()
        logger.info(
            f"[{phase}] 回放完成，共 {frame_count} 帧 "
            f"(加载 {t_load - t0:.1f}s, 数值转换 {t_numeric - t_load:.1f}s, "
            f"预计算 {t_calc - t_numeric:.1f}s, 回放 {t_end - t_calc:.1f}s)"
        )

    def stop(self) -> None:
        """回放模式无需停止。"""
        pass
