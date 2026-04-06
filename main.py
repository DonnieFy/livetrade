# -*- coding: utf-8 -*-
"""
Livetrade — 实盘主入口

启动流程：
1. 初始化日志
2. 加载策略配置
3. 启动 Engine（文件监听 → 策略执行 → 信号输出）
4. 等待收盘后退出
"""

import logging
import os
import sys
from datetime import datetime

import config
from engine import Engine

logger = logging.getLogger("livetrade")


def setup_logging() -> None:
    """初始化日志系统。"""
    os.makedirs(config.LOG_DIR, exist_ok=True)

    date_string = datetime.now().strftime("%Y-%m-%d")
    log_file = os.path.join(config.LOG_DIR, f"{date_string}.log")

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
    """实盘主入口。"""
    setup_logging()

    date_string = datetime.now().strftime("%Y-%m-%d")

    logger.info("=" * 60)
    logger.info(f"Livetrade 实盘监控 — {date_string}")
    logger.info("=" * 60)

    engine = Engine(date_string=date_string)

    try:
        engine.run()
    except KeyboardInterrupt:
        logger.info("收到中断信号，正在停止...")
    except Exception as e:
        logger.error(f"引擎异常退出: {e}", exc_info=True)

    logger.info("=" * 60)
    logger.info("当日实盘监控结束")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
