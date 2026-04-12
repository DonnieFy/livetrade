# -*- coding: utf-8 -*-
"""首板1+1策略 — Tick回测

用法: cd /home/fy/myown/livetrade && python backtest/backtest_first_board_1plus1_tick.py --date 2026-04-09
"""
import argparse, logging, os, tempfile, yaml, config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_backtest(date: str) -> None:
    strategy_config = {
        "date": date,
        "strategies": {
            "first_board_1plus1": {
                "enabled": True,
                "phases": ["trading"],
                "time_range": ["09:30", "14:57"],
                "candidates": [],
                "params": {
                    "volume_ratio": 1.0,
                    "rv_min": 0.0,
                    "breakout_pct_min": 0.01,
                    "pct_chg_min": 0.04,
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
    engine = Engine(date_string=date, config_path=config_path, backtest=True)
    engine.run()

    logger.info("=" * 60)
    logger.info("Tick回测完成")
    os.remove(config_path)
    os.rmdir(tmp_dir)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    run_backtest(parser.parse_args().date)
