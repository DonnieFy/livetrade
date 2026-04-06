"""Project constants and default paths.

All path configuration is centralized in config.py.
This module re-exports the paths needed by strategy_quant and defines
data-format constants (e.g. tick columns).
"""

import sys
from pathlib import Path

# Ensure project root is importable
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from config import (  # noqa: E402
    KLINES_DATA_DIR,
    KLINES_DAILY_FILE as KLINE_FILE,
    STOCK_BASIC_FILE as BASIC_FILE,
    TICKS_DATA_DIR as DEFAULT_TICKS_ROOT,
    VECTOR_PROJECT as DEFAULT_KNOWLEDGE_ROOT,
)

TICK_COLUMNS = [
    "code",
    "name",
    "open",
    "close",
    "now",
    "high",
    "low",
    "buy",
    "sell",
    "turnover",
    "volume",
    "bid1_volume",
    "bid1",
    "bid2_volume",
    "bid2",
    "bid3_volume",
    "bid3",
    "bid4_volume",
    "bid4",
    "bid5_volume",
    "bid5",
    "ask1_volume",
    "ask1",
    "ask2_volume",
    "ask2",
    "ask3_volume",
    "ask3",
    "ask4_volume",
    "ask4",
    "ask5_volume",
    "ask5",
    "date",
    "time",
]
