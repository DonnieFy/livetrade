"""
数据采集适配层 — 直接读取已有项目的数据文件，不复制数据
"""
import pandas as pd
from pathlib import Path

from config import (
    STOCK_BASIC_FILE, KLINES_DAILY_FILE,
    INDEX_BASIC_FILE, INDEX_KLINES_FILE,
    TICKS_DATA_DIR, CORE_INDEXES,
)


class DataCollector:
    """统一数据访问接口，适配各个已有数据源"""

    def __init__(self, date: str | None = None):
        """
        Args:
            date: 目标日期 "YYYY-MM-DD"，默认为数据中最新交易日
        """
        self._stock_basic: pd.DataFrame | None = None
        self._klines: pd.DataFrame | None = None
        self._index_klines: pd.DataFrame | None = None
        self._date = date

    # ============================================================
    # 懒加载
    # ============================================================

    @property
    def stock_basic(self) -> pd.DataFrame:
        if self._stock_basic is None:
            self._stock_basic = self._load_gz(STOCK_BASIC_FILE)
            if "symbol" in self._stock_basic.columns:
                self._stock_basic["symbol"] = (
                    self._stock_basic["symbol"].astype(str).str.zfill(6)
                )
        return self._stock_basic

    @property
    def klines(self) -> pd.DataFrame:
        if self._klines is None:
            self._klines = self._load_gz(KLINES_DAILY_FILE)
            if not self._klines.empty:
                if "date" in self._klines.columns:
                    self._klines["date"] = pd.to_datetime(
                        self._klines["date"]
                    ).dt.strftime("%Y-%m-%d")
                if "symbol" in self._klines.columns:
                    self._klines["symbol"] = (
                        self._klines["symbol"].astype(str).str.zfill(6)
                    )
        return self._klines

    @property
    def index_klines(self) -> pd.DataFrame:
        if self._index_klines is None:
            self._index_klines = self._load_gz(INDEX_KLINES_FILE)
            if not self._index_klines.empty and "date" in self._index_klines.columns:
                self._index_klines["date"] = pd.to_datetime(
                    self._index_klines["date"]
                ).dt.strftime("%Y-%m-%d")
        return self._index_klines

    @property
    def date(self) -> str | None:
        """目标交易日期"""
        if self._date:
            return self._date
        if not self.klines.empty and "date" in self.klines.columns:
            self._date = self.klines["date"].max()
        return self._date

    # ============================================================
    # 公开接口
    # ============================================================

    def get_stock_klines(self, days: int = 20, date: str | None = None) -> pd.DataFrame:
        """获取最近 N 个交易日的个股日线数据"""
        if self.klines.empty:
            return pd.DataFrame()
        target = date or self.date
        scope = self.klines
        if target:
            scope = scope[scope["date"] <= target]
        dates = sorted(scope["date"].unique(), reverse=True)[:days]
        return scope[scope["date"].isin(dates)].copy()

    def get_index_klines(self, days: int = 20, date: str | None = None) -> pd.DataFrame:
        """获取核心指数最近 N 个交易日的日线数据"""
        df = self.index_klines
        if df.empty:
            return pd.DataFrame()
        # 只保留核心指数
        core_codes = list(CORE_INDEXES.keys())
        df = df[df["ts_code"].isin(core_codes)].copy()
        target = date or self.date
        if target:
            df = df[df["date"] <= target]
        dates = sorted(df["date"].unique(), reverse=True)[:days]
        return df[df["date"].isin(dates)].copy()

    def get_day_klines(self, date: str | None = None) -> pd.DataFrame:
        """获取指定日期的全市场日线数据"""
        target = date or self.date
        if not target or self.klines.empty:
            return pd.DataFrame()
        return self.klines[self.klines["date"] == target].copy()

    def get_prev_day_klines(self, date: str | None = None) -> pd.DataFrame:
        """获取目标日期的前一个交易日数据"""
        target = date or self.date
        if not target or self.klines.empty:
            return pd.DataFrame()
        dates = sorted(self.klines["date"].unique())
        try:
            idx = dates.index(target)
            if idx == 0:
                return pd.DataFrame()
            prev_date = dates[idx - 1]
            return self.klines[self.klines["date"] == prev_date].copy()
        except ValueError:
            return pd.DataFrame()

    def get_stock_name(self, symbol: str) -> str:
        """根据股票代码获取名称"""
        s = str(symbol).zfill(6)
        if self.stock_basic.empty or "name" not in self.stock_basic.columns:
            return s
        match = self.stock_basic[self.stock_basic["symbol"] == s]
        if match.empty:
            return s
        return str(match.iloc[0]["name"])

    def get_trading_dates(self, n: int = 20, date: str | None = None) -> list[str]:
        """获取最近 N 个交易日列表（降序）"""
        if self.klines.empty or "date" not in self.klines.columns:
            return []
        target = date or self.date
        scope = self.klines
        if target:
            scope = scope[scope["date"] <= target]
        return sorted(scope["date"].unique(), reverse=True)[:n]

    def is_st(self, symbol: str) -> bool:
        """判断是否为 ST 股票"""
        name = self.get_stock_name(symbol)
        return "ST" in name.upper()

    # ============================================================
    # 内部工具
    # ============================================================

    @staticmethod
    def _load_gz(filepath: Path) -> pd.DataFrame:
        if not filepath.exists():
            print(f"⚠️  数据文件不存在: {filepath}")
            return pd.DataFrame()
        return pd.read_csv(filepath, compression="gzip", encoding="utf-8-sig")
