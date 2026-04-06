# -*- coding: utf-8 -*-
"""
Livetrade 实盘监控项目 — 全局配置

实盘与复盘共用这一份配置，避免再维护第二套路径常量。
"""

from pathlib import Path

# ============================================================
# 目录配置
# ============================================================

PROJECT_ROOT = Path(__file__).parent.resolve()

# 数据源 — 关联项目
DATABASE_ROOT = PROJECT_ROOT.parent / "database"
KNOWLEDGE_ROOT = PROJECT_ROOT.parent / "knowledge"

# ashares-ticks 数据路径（实时 tick 来源）
TICKS_PROJECT = DATABASE_ROOT / "ashares-ticks"
TICKS_DATA_DIR = TICKS_PROJECT / "data"

# ashares-k-lines 数据路径（日线数据，策略 prepare 阶段使用）
KLINES_PROJECT = DATABASE_ROOT / "ashares-k-lines"
KLINES_DATA_DIR = KLINES_PROJECT / "data"
STOCK_BASIC_FILE = KLINES_DATA_DIR / "stock_basic.csv.gz"
KLINES_DAILY_FILE = KLINES_DATA_DIR / "klines_daily.csv.gz"
INDEX_BASIC_FILE = KLINES_DATA_DIR / "index_basic.csv.gz"
INDEX_KLINES_FILE = KLINES_DATA_DIR / "index_klines_daily.csv.gz"

# stock-vector-knowledge 路径（板块/概念向量库）
VECTOR_PROJECT = KNOWLEDGE_ROOT / "stock-vector-knowledge"
JIUYANGONGSHE_DATA_DIR = VECTOR_PROJECT / "data" / "jiuyangongshe"
ACTION_DATA_DIR = JIUYANGONGSHE_DATA_DIR / "action"
INDUSTRY_FILE = JIUYANGONGSHE_DATA_DIR / "industry.json"
TIMELINE_FILE = JIUYANGONGSHE_DATA_DIR / "timeline.json"

# knowledge-a-shares 路径
KNOWLEDGE_ASHARES = KNOWLEDGE_ROOT / "knowledge-a-shares"

# 信号输出目录
ALERT_OUTPUT_DIR = PROJECT_ROOT / "output"

# 策略配置文件
STRATEGY_CONFIG_FILE = PROJECT_ROOT / "strategy_config.yaml"

# 复盘目录
REVIEW_ROOT = PROJECT_ROOT / "review"
REVIEW_DAILY_DIR = REVIEW_ROOT / "daily"
REVIEW_SCHEMA_DIR = REVIEW_ROOT / "schemas"
REVIEW_MACHINE_FILENAME = "machine.json"
REVIEW_ANALYST_FILENAME = "analyst.yaml"
REVIEW_REPORT_FILENAME = "review.md"

# ============================================================
# 交易阶段配置
# ============================================================

PHASE_AUCTION_OPEN = "auction_open"
PHASE_TRADING = "trading"
PHASE_AUCTION_CLOSE = "auction_close"

ALL_PHASES = [PHASE_AUCTION_OPEN, PHASE_TRADING, PHASE_AUCTION_CLOSE]

# ============================================================
# CSV 列定义（与 ashares-ticks 完全一致，33 列无列头）
# ============================================================

CSV_COLUMNS = [
    "code",
    "name",
    "open",
    "close",       # 昨收
    "now",         # 当前价
    "high",
    "low",
    "buy",         # 竞买价（买一）
    "sell",        # 竞卖价（卖一）
    "turnover",    # 成交量（股）
    "volume",      # 成交额（元）
    "bid1_volume", "bid1",
    "bid2_volume", "bid2",
    "bid3_volume", "bid3",
    "bid4_volume", "bid4",
    "bid5_volume", "bid5",
    "ask1_volume", "ask1",
    "ask2_volume", "ask2",
    "ask3_volume", "ask3",
    "ask4_volume", "ask4",
    "ask5_volume", "ask5",
    "date",
    "time",
]

# 数值列（解析时自动转换类型）
NUMERIC_COLUMNS = [
    "open", "close", "now", "high", "low", "buy", "sell",
    "turnover", "volume",
    "bid1_volume", "bid1", "bid2_volume", "bid2",
    "bid3_volume", "bid3", "bid4_volume", "bid4",
    "bid5_volume", "bid5",
    "ask1_volume", "ask1", "ask2_volume", "ask2",
    "ask3_volume", "ask3", "ask4_volume", "ask4",
    "ask5_volume", "ask5",
]

# ============================================================
# 涨跌停系数
# ============================================================

GEM_PREFIXES = ("300", "301")   # 创业板 20%
STAR_PREFIXES = ("688",)        # 科创板 20%
LIMIT_RATIO_MAIN = 0.10         # 主板 10%
LIMIT_RATIO_GEM_STAR = 0.20     # 创业板/科创板 20%

# 复盘指标复用常量
CORE_INDEXES = {
    "000001.SH": "上证指数",
    "399001.SZ": "深证成指",
    "399006.SZ": "创业板指",
    "000016.SH": "上证50",
    "000905.SH": "中证500",
}
GEM_PREFIX = "30"
STAR_PREFIX = "68"
HIGH_VOLATILITY_PCT = 5.0
HIGH_VOLATILITY_TOP_N = 50

# ============================================================
# 文件监听配置
# ============================================================

WATCHER_POLL_INTERVAL = 1.0     # 轮询间隔（秒）

# ============================================================
# 日志配置
# ============================================================

LOG_DIR = PROJECT_ROOT / "logs"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 确保目录存在
ALERT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
REVIEW_DAILY_DIR.mkdir(parents=True, exist_ok=True)
REVIEW_SCHEMA_DIR.mkdir(parents=True, exist_ok=True)
