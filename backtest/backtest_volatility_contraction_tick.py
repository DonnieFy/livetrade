# -*- coding: utf-8 -*-
"""
波动率收敛突破策略 — Tick级别回测

通过引擎回放历史 tick 数据，精确验证分时突破买点。

用法:
    cd /home/fy/myown/livetrade
    python backtest/backtest_volatility_contraction_tick.py --date 2026-04-02
"""

import argparse
import logging
import os
import sys
import tempfile

import yaml

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def run_backtest(date: str) -> None:
    """生成临时配置文件，仅启用波动率收敛突破策略，然后运行引擎回测。"""
    # 生成仅启用目标策略的配置
    strategy_config = {
        "date": date,
        "strategies": {
            "volatility_contraction": {
                "enabled": True,
                "phases": ["trading"],
                "time_range": ["09:30", "14:57"],
                "candidates": [],
                "params": {
                    "rally_gain_pct": 80.0,
                    "rally_max_days": 15,
                    "rally_lookback": 60,
                    "ma_proximity": 0.05,
                    "vol_expand_multiple": 1.5,
                    "volume_ratio": 1.2,
                },
            },
        },
    }

    # 写入临时配置文件
    tmp_dir = tempfile.mkdtemp()
    config_path = os.path.join(tmp_dir, "strategy_config.yaml")
    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(strategy_config, f, allow_unicode=True, sort_keys=False)

    logger.info(f"临时配置: {config_path}")
    logger.info(f"回测日期: {date}")
    logger.info("=" * 60)

    # 使用引擎回测
    from engine import Engine

    engine = Engine(
        date_string=date,
        config_path=config_path,
        backtest=True,
    )
    engine.run()

    logger.info("=" * 60)
    logger.info("Tick回测完成")

    # 清理
    os.remove(config_path)
    os.rmdir(tmp_dir)


def main():
    parser = argparse.ArgumentParser(description="波动率收敛突破策略 — Tick回测")
    parser.add_argument("--date", required=True, help="回测日期 YYYY-MM-DD")
    args = parser.parse_args()

    run_backtest(args.date)


if __name__ == "__main__":
    main()
