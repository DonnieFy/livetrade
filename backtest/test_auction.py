# -*- coding: utf-8 -*-
"""快速测试竞价策略 — 仅回放 auction_open 阶段。"""

import logging
import os
import sys

import config
from engine import Engine

# Monkey-patch ALL_PHASES to only include auction_open for speed
config.ALL_PHASES = [config.PHASE_AUCTION_OPEN]


def setup_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(
        logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    )
    root.addHandler(ch)


def main():
    dates = sys.argv[1:] if len(sys.argv) > 1 else ["2026-03-27"]

    setup_logging()
    logger = logging.getLogger("test_auction")

    for date_string in dates:
        logger.info("=" * 60)
        logger.info(f"测试竞价策略 — {date_string}")
        logger.info("=" * 60)

        # Determine data dir
        data_dir = os.path.join(config.TICKS_DATA_DIR, date_string)
        if not os.path.exists(data_dir):
            logger.warning(f"数据目录不存在: {data_dir}, 跳过")
            continue

        engine = Engine(
            date_string=date_string,
            backtest=True,
            data_dir=data_dir,
        )
        engine.run()

        logger.info("")


if __name__ == "__main__":
    main()
