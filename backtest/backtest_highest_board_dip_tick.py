# -*- coding: utf-8 -*-
"""
最高板板块共振低吸策略 — Tick级别回测

通过引擎回放历史 tick 数据，验证实时跌幅聚类和两个买点。

用法:
    cd /home/fy/myown/livetrade
    python backtest/backtest_highest_board_dip_tick.py --date 2026-04-02
"""

import argparse
import logging
import os
import tempfile

import yaml

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_backtest(date: str) -> None:
    strategy_config = {
        "date": date,
        "strategies": {
            "highest_board_dip": {
                "enabled": True,
                "phases": ["trading"],
                "time_range": ["09:30", "14:57"],
                "candidates": [],
                "params": {
                    "one_word_days_min": 2,
                    "cluster_top_n": 100,
                    "dip_from_high_pct": 6.0,
                    "sector_decline_pct": -1.0,
                    "pullback_ratio": 0.98,
                },
            },
        },
    }

    tmp_dir = tempfile.mkdtemp()
    config_path = os.path.join(tmp_dir, "strategy_config.yaml")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(strategy_config, f, allow_unicode=True, sort_keys=False)

    logger.info(f"临时配置: {config_path}")
    logger.info(f"回测日期: {date}")
    logger.info("=" * 60)

    from engine import Engine

    engine = Engine(
        date_string=date,
        config_path=config_path,
        backtest=True,
    )
    engine.run()

    logger.info("=" * 60)
    logger.info("Tick回测完成")

    os.remove(config_path)
    os.rmdir(tmp_dir)


def main():
    parser = argparse.ArgumentParser(description="最高板板块共振低吸策略 — Tick回测")
    parser.add_argument("--date", required=True, help="回测日期 YYYY-MM-DD")
    args = parser.parse_args()

    run_backtest(args.date)


if __name__ == "__main__":
    main()
