"""Loaders for daily bars, ticks, and theme knowledge."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import pandas as pd

from .constants import (
    BASIC_FILE,
    DEFAULT_KNOWLEDGE_ROOT,
    DEFAULT_TICKS_ROOT,
    KLINE_FILE,
    TICK_COLUMNS,
)


def load_daily_klines(path: Path = KLINE_FILE) -> pd.DataFrame:
    """Load daily bars from the local kline project."""
    df = pd.read_csv(path, compression="gzip", encoding="utf-8-sig")
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    numeric_cols = [
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "pct_chg",
        "change",
        "turnover",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.sort_values(["symbol", "date"]).reset_index(drop=True)


def load_stock_basic(path: Path = BASIC_FILE) -> pd.DataFrame:
    """Load stock basic table."""
    df = pd.read_csv(path, compression="gzip", encoding="utf-8-sig")
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    return df


def _normalize_tick_symbol(code: str) -> str:
    code = str(code)
    if code.startswith(("sh", "sz", "bj")) and len(code) >= 8:
        return code[-6:]
    return code.zfill(6)


def locate_tick_file(date: str, ticks_root: Path = DEFAULT_TICKS_ROOT) -> Path | None:
    """Locate the intraday continuous trading file for a date."""
    # Support both compressed (.csv.gz) and uncompressed (.csv) files,
    # and both path layouts (with and without extra 'data/' subdirectory).
    for base in [ticks_root, ticks_root / "data"]:
        for ext in [".csv.gz", ".csv"]:
            candidate = base / date / f"{date}_trading{ext}"
            if candidate.exists():
                return candidate
    return None


def load_ticks_for_date(date: str, ticks_root: Path = DEFAULT_TICKS_ROOT) -> pd.DataFrame:
    """Load a single day's continuous trading ticks."""
    path = locate_tick_file(date, ticks_root=ticks_root)
    if path is None:
        return pd.DataFrame(columns=TICK_COLUMNS)

    is_gz = path.suffix == ".gz"
    df = pd.read_csv(
        path,
        names=TICK_COLUMNS,
        header=None,
        compression="gzip" if is_gz else None,
        encoding="utf-8",
    )
    df["symbol"] = df["code"].map(_normalize_tick_symbol)
    df["datetime"] = pd.to_datetime(df["date"] + " " + df["time"])

    numeric_cols = [col for col in TICK_COLUMNS if col not in {"code", "name", "date", "time"}]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.sort_values(["symbol", "datetime"]).reset_index(drop=True)


def _extract_concept_names(raw: object) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        names: list[str] = []
        for item in raw:
            if isinstance(item, str):
                names.append(item.strip())
            elif isinstance(item, dict):
                value = str(item.get("name", "")).strip()
                if value:
                    names.append(value)
        return names
    return []


def _safe_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


@lru_cache(maxsize=2)
def load_theme_knowledge(knowledge_root: Path = DEFAULT_KNOWLEDGE_ROOT) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load symbol-theme relations and stock metadata from merged JSON drafts.

    Returns
    -------
    theme_map:
        columns: symbol, name, theme, theme_source, industry
    stock_meta:
        columns: symbol, name, industry, market_cap_total_e8, market_cap_float_e8
    """
    merged_dir = knowledge_root / "data" / "drafts" / "merged"
    records: list[dict] = []
    stock_meta_records: list[dict] = []

    for path in sorted(merged_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue

        symbol = str(payload.get("code", path.stem)).zfill(6)
        name = str(payload.get("name", "")).strip()
        info = payload.get("info", {}) or {}
        industry = str(payload.get("industry", "") or info.get("行业", "")).strip()
        market_cap_total = (
            _safe_float(info.get("market_cap_total"))
            or _safe_float(info.get("总市值"))
            or _safe_float(info.get("total_market_cap"))
        )
        market_cap_float = (
            _safe_float(info.get("market_cap_float"))
            or _safe_float(info.get("流通市值"))
            or _safe_float(info.get("float_market_cap"))
        )

        stock_meta_records.append(
            {
                "symbol": symbol,
                "name": name,
                "industry": industry,
                "market_cap_total_e8": market_cap_total,
                "market_cap_float_e8": market_cap_float,
            }
        )

        for theme in _extract_concept_names(payload.get("concepts_ths")):
            records.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "theme": theme,
                    "theme_source": "ths",
                    "industry": industry,
                }
            )

        for theme in _extract_concept_names(payload.get("concepts_eastmoney")):
            records.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "theme": theme,
                    "theme_source": "eastmoney",
                    "industry": industry,
                }
            )

        if industry:
            records.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "theme": industry,
                    "theme_source": "industry",
                    "industry": industry,
                }
            )

    theme_map = pd.DataFrame(records).drop_duplicates(["symbol", "theme", "theme_source"])
    stock_meta = pd.DataFrame(stock_meta_records).drop_duplicates("symbol")
    return theme_map, stock_meta
