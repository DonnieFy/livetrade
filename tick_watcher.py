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

import csv
import gzip
import logging
import os
import time
from datetime import datetime
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
        # 末尾未写完的单行缓存 {phase: partial_line}
        self._partial_lines: dict[str, str] = {}
        # 跨轮询未完成的 tick_time 桶缓存 {phase: {tick_time: lines}}
        self._pending_frame_buckets: dict[str, dict[str, list[str]]] = {}
        self._pending_frame_updated_at: dict[str, float] = {}

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
            # 每天 15:05 自动安全退出，以便第二天 systemd 定时器能拉起新的进程
            now = datetime.now()
            if now.hour == 15 and now.minute >= 5:
                logger.info("已过 15:05 自动关机时间，安静退出等待明日唤醒...")
                self.running = False
                break

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
            self._partial_lines.pop(phase, None)
            self._pending_frame_buckets.pop(phase, None)
            self._pending_frame_updated_at.pop(phase, None)

        if current_size <= last_offset:
            self._maybe_flush_pending_frame(phase)
            return  # 无新数据

        # 按真实 tick_time 聚合帧，而不是按任意 10000 行整块回调。
        # 这样竞价策略才能正确识别 09:20 前后切换、末尾回封等时序信号。
        with open(csv_path, "r", encoding="utf-8") as f:
            f.seek(last_offset)

            raw_text = f.read()
            self._offsets[phase] = f.tell()

        if not raw_text:
            return

        raw_text = self._partial_lines.get(phase, "") + raw_text
        if raw_text.endswith("\n"):
            lines = raw_text.splitlines(keepends=True)
            self._partial_lines[phase] = ""
        else:
            lines = raw_text.splitlines(keepends=True)
            if lines:
                self._partial_lines[phase] = lines.pop()
            else:
                self._partial_lines[phase] = raw_text
                return

        buckets = {
            tick_time: lines.copy()
            for tick_time, lines in self._pending_frame_buckets.get(phase, {}).items()
        }

        for line in lines:
            tick_time = self._extract_time_from_line(line)
            if not tick_time:
                logger.debug(f"[{phase}] 跳过无法解析 time 的原始行")
                continue

            buckets.setdefault(tick_time, []).append(line)

        if buckets:
            tick_times = sorted(buckets)
            latest_tick_time = tick_times[-1]
            for tick_time in tick_times[:-1]:
                self._emit_frame(phase, buckets[tick_time], tick_time)

            self._pending_frame_buckets[phase] = {
                latest_tick_time: buckets[latest_tick_time]
            }
            self._pending_frame_updated_at[phase] = time.time()
        else:
            self._pending_frame_buckets.pop(phase, None)
            self._pending_frame_updated_at.pop(phase, None)

    def flush_pending(self) -> None:
        """将当前缓存中的尾帧强制输出。

        适用于文件已写完或回测结束前的收尾阶段，避免最后一个 tick_time
        因为等待下一帧边界而一直滞留在缓存里。
        """
        for phase in config.ALL_PHASES:
            buckets = self._pending_frame_buckets.pop(phase, None) or {}
            self._partial_lines.pop(phase, None)
            self._pending_frame_updated_at.pop(phase, None)
            for tick_time in sorted(buckets):
                self._emit_frame(phase, buckets[tick_time], tick_time)

    @staticmethod
    def _extract_time_from_line(line: str) -> str:
        """从单行原始 CSV 中提取 time 列。"""
        try:
            row = next(csv.reader([line]))
        except Exception:
            return ""
        if len(row) != len(config.CSV_COLUMNS):
            return ""
        return str(row[-1]).strip()

    def _emit_frame(self, phase: str, frame_lines: list[str], tick_time: str) -> None:
        """解析并回调单个真实 tick_time 帧。"""
        if not frame_lines or not tick_time:
            return

        df = parse_csv_text("".join(frame_lines))
        if df.empty:
            return

        parsed_tick_time = extract_tick_time(df) or tick_time
        logger.debug(
            f"[{phase}] 增量读取帧: {len(df)} 行, "
            f"tick_time={parsed_tick_time}"
        )
        self.callback(phase, df, parsed_tick_time)

    def _maybe_flush_pending_frame(self, phase: str) -> None:
        """若某帧在一轮轮询内没有再增长，则认为该帧已写完并输出。"""
        buckets = self._pending_frame_buckets.get(phase) or {}
        updated_at = self._pending_frame_updated_at.get(phase, 0.0)
        if not buckets or updated_at <= 0:
            return

        idle_seconds = time.time() - updated_at
        if idle_seconds < config.WATCHER_POLL_INTERVAL:
            return

        self._pending_frame_buckets.pop(phase, None)
        self._pending_frame_updated_at.pop(phase, None)
        for tick_time in sorted(buckets):
            self._emit_frame(phase, buckets[tick_time], tick_time)


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
    # 注意保留竞价关键盘口列，确保 _normalize_auction_frame 可用，
    # 且竞价策略（如 auction_limit_chase）在回测时不会因缺列失真。
    _ESSENTIAL_COLUMNS = [
        "code", "name", "close", "now", "high", "low",
        "open", "volume", "turnover", "time",
        "buy", "sell",
        "bid1_volume", "bid1",
        "bid2_volume", "bid2",
        "ask1_volume", "ask1",
        "ask2_volume", "ask2",
    ]

    _CHUNK_SIZE = 300_000

    def _replay_phase(self, phase: str) -> None:
        """回放单个阶段 — 流式分块加载 + 按帧回调。"""
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

        has_time_filter = self.time_range is not None
        total_raw = 0
        total_kept = 0
        frame_count = 0
        tail_df: pd.DataFrame | None = None
        try:
            for chunk in pd.read_csv(
                file_path,
                names=config.CSV_COLUMNS,
                header=None,
                compression=compression,
                dtype=str,
                usecols=usecols,
                chunksize=self._CHUNK_SIZE,
            ):
                total_raw += len(chunk)
                if has_time_filter and "time" in chunk.columns:
                    t_start, t_end = self.time_range
                    chunk = chunk[
                        (chunk["time"] >= t_start) & (chunk["time"] <= t_end)
                    ]
                if not chunk.empty:
                    total_kept += len(chunk)
                    prepared = self._prepare_chunk(chunk)
                    if tail_df is not None and not tail_df.empty:
                        prepared = pd.concat([tail_df, prepared], ignore_index=True)

                    last_tick_time = str(prepared.iloc[-1]["time"]).strip()
                    if not last_tick_time:
                        tail_df = None
                        continue

                    is_last_time = prepared["time"].astype(str).str.strip() == last_tick_time
                    emit_df = prepared.loc[~is_last_time]
                    tail_df = prepared.loc[is_last_time].reset_index(drop=True)

                    if not emit_df.empty:
                        frame_count += self._emit_grouped_frames(phase, emit_df)
        except Exception as e:
            logger.error(f"[{phase}] 打开文件失败: {e}")
            return

        if total_kept == 0:
            if has_time_filter:
                logger.info(
                    f"[{phase}] 时间裁剪 {self.time_range[0]}~{self.time_range[1]}: "
                    f"{total_raw}→0 行 (裁剪 {total_raw} 行)"
                )
                logger.info(f"[{phase}] 裁剪后无数据，跳过")
            return

        t_load = _time.time()

        if has_time_filter:
            logger.info(
                f"[{phase}] 时间裁剪 {self.time_range[0]}~{self.time_range[1]}: "
                f"{total_raw}→{total_kept} 行 (裁剪 {total_raw - total_kept} 行)"
            )

        t_numeric = _time.time()
        t_calc = _time.time()
        if tail_df is not None and not tail_df.empty:
            frame_count += self._emit_grouped_frames(phase, tail_df)

        t_end = _time.time()
        logger.info(
            f"[{phase}] 回放完成，共 {frame_count} 帧 "
            f"(加载 {t_load - t0:.1f}s, 数值转换 {t_numeric - t_load:.1f}s, "
            f"预计算 {t_calc - t_numeric:.1f}s, 回放 {t_end - t_calc:.1f}s)"
        )

    def _prepare_chunk(self, df: pd.DataFrame) -> pd.DataFrame:
        """对单个分块做数值转换和衍生列预计算。"""
        df = df.copy()

        numeric_in_df = [c for c in config.NUMERIC_COLUMNS if c in df.columns]
        for col in numeric_in_df:
            arr = df[col].values
            try:
                df[col] = arr.astype(np.float64)
            except (ValueError, TypeError):
                df[col] = pd.to_numeric(arr, errors="coerce")

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
        df["time"] = df["time"].astype(str).str.strip()

        return df

    def _emit_grouped_frames(self, phase: str, df: pd.DataFrame) -> int:
        """将已预处理分块按 tick_time 分帧回调。"""
        frame_count = 0
        for tick_time, frame_df in df.groupby("time", sort=False):
            frame_df = frame_df.reset_index(drop=True)
            self.callback(phase, frame_df, str(tick_time))
            frame_count += 1
        return frame_count

    def stop(self) -> None:
        """回放模式无需停止。"""
        pass
