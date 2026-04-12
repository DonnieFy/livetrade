# -*- coding: utf-8 -*-
"""连板错杀低吸策略 — Tick回测

用法: cd /home/fy/myown/livetrade && python backtest/backtest_board_oversold_dip_tick.py --date 2026-04-09
"""
import argparse, logging, os, tempfile, yaml, config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def run_backtest(date: str) -> None:
    strategy_config = {
        "date": date,
        "strategies": {
            "board_oversold_dip": {
                "enabled": True,
                "phases": ["trading"],
                "time_range": ["09:30", "09:45"],
                "candidates": [],
                "params": {
                    "min_boards": 2,
                    "lookback_days": 10,
                    "dip_threshold": -4.0,
                    "sector_min_avg": 0.0,
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
