# -*- coding: utf-8 -*-
"""
Livetrade — 回测/回放模块

使用历史 .csv.gz 数据按帧模拟回放，验证策略有效性。
复用 Engine 的策略加载和上下文管理，仅替换 TickWatcher 为 ReplayWatcher。

用法:
    python backtest.py --date 2026-03-26
    python backtest.py --date 2026-03-26 --config custom_config.yaml
    python backtest.py --date 2026-03-26 --data-dir /path/to/custom/data
"""

import argparse
import logging
import os
import sys
from datetime import datetime

import config
from engine import Engine


def setup_logging(date_string: str) -> None:
    """初始化回测日志。"""
    os.makedirs(config.LOG_DIR, exist_ok=True)

    log_file = os.path.join(config.LOG_DIR, f"backtest_{date_string}.log")

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # 文件 handler
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    )

    # 控制台 handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(
        logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    )

    root.addHandler(fh)
    root.addHandler(ch)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Livetrade 回测模块 — 使用历史 tick 数据验证策略"
    )
    parser.add_argument(
        "--date", required=True,
        help="回测日期，格式 YYYY-MM-DD"
    )
    parser.add_argument(
        "--config",
        help="策略配置 YAML 文件路径（默认使用项目 strategy_config.yaml）"
    )
    parser.add_argument(
        "--data-dir",
        help="数据目录路径（默认使用 ashares-ticks/data/{date}）"
    )
    args = parser.parse_args()

    date_string = args.date
    setup_logging(date_string)

    logger = logging.getLogger("backtest")
    logger.info("=" * 60)
    logger.info(f"Livetrade 回测 — {date_string}")
    logger.info("=" * 60)

    engine = Engine(
        date_string=date_string,
        config_path=args.config,
        backtest=True,
        data_dir=args.data_dir,
    )

    engine.run()

    logger.info("=" * 60)
    logger.info("回测完成")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
