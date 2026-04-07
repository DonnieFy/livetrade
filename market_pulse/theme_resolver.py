# -*- coding: utf-8 -*-
"""
盘面脉搏 — 题材解析器

多源题材映射，按优先级合并：
1. review/src/strategy_quant/loaders.py::load_theme_knowledge() — 主映射
2. knowledge/stock-vector-knowledge/data/jiuyangongshe/industry.json — 增量
3. stock_basic.csv.gz — name 映射兜底

提供：
    resolve_primary_theme(symbol) -> str | None
    resolve_themes(symbol) -> list[str]
    aggregate_top_themes(symbols, top_n=3) -> list[ThemeBucket]
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ThemeBucket:
    """题材聚合桶。"""
    theme: str
    count: int = 0
    symbols: list[str] = field(default_factory=list)
    names: list[str] = field(default_factory=list)
    avg_pct_chg: float = 0.0


def _normalize_name(name: str) -> str:
    """名称归一化：去空格、全角转半角、统一大小写。"""
    name = name.strip()
    name = unicodedata.normalize("NFKC", name)
    name = re.sub(r"\s+", "", name)
    return name


def _symbol_to_code(symbol: str) -> str:
    """6 位纯数字 symbol 或已带前缀 code 转为标准带前缀 code。"""
    if symbol and len(symbol) > 2 and symbol[:2] in ("sh", "sz", "bj"):
        return symbol
    pure = symbol.zfill(6)
    if pure.startswith(("4", "8", "92")):
        return f"bj{pure}"
    if pure.startswith(("0", "1", "2", "3")):
        return f"sz{pure}"
    return f"sh{pure}"


def _code_to_symbol(code: str) -> str:
    """带前缀 code 转为 6 位纯数字 symbol。"""
    if code and len(code) > 2 and code[:2] in ("sh", "sz", "bj"):
        return code[2:]
    return code.zfill(6)


class ThemeResolver:
    """多源题材解析器。"""

    def __init__(self):
        # symbol(6位) -> list[str] 题材列表
        self._theme_map: dict[str, list[str]] = {}
        # symbol(6位) -> name
        self._name_map: dict[str, str] = {}
        # symbol(6位) -> industry
        self._industry_map: dict[str, str] = {}
        # 热门题材集合（用于 primary_theme 优先级判断）
        self._hot_themes: set[str] = set()
        # 行业关键词 -> 标准题材名
        self._keyword_to_theme: dict[str, str] = {}
        self._loaded = False

    @property
    def loaded(self) -> bool:
        return self._loaded

    def load(
        self,
        knowledge_root: Path,
        industry_file: Path,
        stock_basic_file: Path,
        hot_theme_names: list[str] | None = None,
    ) -> None:
        """加载所有数据源并合并题材映射。

        参数:
            knowledge_root: knowledge/ 目录（含 stock-vector-knowledge）
            industry_file: industry.json 路径
            stock_basic_file: stock_basic.csv.gz 路径
            hot_theme_names: 昨日热门题材名称列表（用于优先级判断）
        """
        # 1. 加载 stock_basic (name 映射)
        self._load_stock_basic(stock_basic_file)

        # 2. 加载 theme_knowledge (主映射)
        self._load_theme_knowledge(knowledge_root)

        # 3. 加载 industry.json (增量)
        self._load_industry_json(industry_file)

        # 4. 设置热门题材
        if hot_theme_names:
            self._hot_themes = set(hot_theme_names)

        self._loaded = True
        logger.info(
            f"[ThemeResolver] 加载完成 — "
            f"映射股票: {len(self._theme_map)}, "
            f"热门题材: {len(self._hot_themes)}"
        )

    def _load_stock_basic(self, path: Path) -> None:
        """加载 stock_basic.csv.gz。"""
        if not path.exists():
            logger.warning(f"stock_basic 文件不存在: {path}")
            return
        try:
            df = pd.read_csv(path, compression="gzip", encoding="utf-8-sig", dtype={"symbol": str})
            if "symbol" in df.columns:
                df["symbol"] = df["symbol"].astype(str).str.zfill(6)
            for _, row in df.iterrows():
                sym = str(row.get("symbol", "")).zfill(6)
                name = str(row.get("name", "")).strip()
                if sym and name:
                    self._name_map[sym] = name
        except Exception as e:
            logger.error(f"加载 stock_basic 失败: {e}")

    def _load_theme_knowledge(self, knowledge_root: Path) -> None:
        """加载 theme_knowledge 主映射。"""
        try:
            from review.src.strategy_quant.loaders import load_theme_knowledge
            theme_map_df, _ = load_theme_knowledge(knowledge_root=knowledge_root)
            for sym, group in theme_map_df.groupby("symbol"):
                sym_str = str(sym).zfill(6)
                themes = group["theme"].dropna().unique().tolist()
                if sym_str not in self._theme_map:
                    self._theme_map[sym_str] = []
                self._theme_map[sym_str].extend(themes)
                # 也补充 name
                name = group.iloc[0].get("name", "")
                if name and sym_str not in self._name_map:
                    self._name_map[sym_str] = str(name)
                industry = group.iloc[0].get("industry", "")
                if industry:
                    self._industry_map[sym_str] = str(industry)
        except Exception as e:
            logger.warning(f"加载 theme_knowledge 失败: {e}")

    def _load_industry_json(self, path: Path) -> None:
        """加载 industry.json 作为增量题材源。"""
        if not path.exists():
            logger.warning(f"industry.json 不存在: {path}")
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            items = data if isinstance(data, list) else data.get("data", data.get("items", []))
            if isinstance(items, dict):
                items = list(items.values())

            for item in items:
                if not isinstance(item, dict):
                    continue
                title = str(item.get("title", "")).strip()
                keywords = str(item.get("keyword", "")).strip()
                if not title and not keywords:
                    continue

                # 提取标准题材名（从 title 去掉编号前缀）
                theme_name = re.sub(r"^\d+\s*", "", title).strip()
                if not theme_name:
                    continue

                # 建立 keyword -> theme 映射
                self._keyword_to_theme[theme_name] = theme_name
                if keywords:
                    for kw in keywords.split(","):
                        kw = kw.strip()
                        if kw:
                            self._keyword_to_theme[kw] = theme_name

        except Exception as e:
            logger.error(f"加载 industry.json 失败: {e}")

    def resolve_themes(self, symbol: str) -> list[str]:
        """获取某股票的所有题材。

        参数:
            symbol: 6位数字或带前缀代码

        返回:
            去重后的题材列表
        """
        pure = _code_to_symbol(symbol)
        themes = self._theme_map.get(pure, [])
        # 去重保序
        seen = set()
        result = []
        for t in themes:
            if t not in seen:
                seen.add(t)
                result.append(t)
        return result

    def resolve_primary_theme(self, symbol: str) -> str | None:
        """获取某股票的主题材。

        优先级：
        1. 如果该股命中昨日 action_analysis 热门题材，优先取该题材
        2. 否则取 theme_knowledge 里第一个非行业题材
        3. 再不行取 industry
        4. 最后尝试 industry.json keyword 匹配股票名称

        参数:
            symbol: 6位数字或带前缀代码

        返回:
            主题材名称，无则 None
        """
        pure = _code_to_symbol(symbol)
        themes = self.resolve_themes(pure)

        if not themes:
            # 尝试用名称在 keyword 表中匹配
            name = self._name_map.get(pure, "")
            if name:
                norm = _normalize_name(name)
                for kw, theme in self._keyword_to_theme.items():
                    if kw in norm or norm in kw:
                        return theme
            return None

        # 优先取热门题材
        for t in themes:
            if t in self._hot_themes:
                return t

        # 取第一个非纯行业题材
        industry = self._industry_map.get(pure, "")
        for t in themes:
            if t != industry:
                return t

        return themes[0] if themes else None

    def get_name(self, symbol: str) -> str:
        """获取股票名称。"""
        pure = _code_to_symbol(symbol)
        return self._name_map.get(pure, pure)

    def aggregate_top_themes(
        self,
        symbol_pct_pairs: list[tuple[str, float]],
        top_n: int = 3,
    ) -> list[ThemeBucket]:
        """对一组股票按题材聚合，返回 TOP-N 题材桶。

        参数:
            symbol_pct_pairs: [(symbol_or_code, pct_chg), ...]
            top_n: 返回前 N 个题材

        返回:
            按 count 降序排列的 ThemeBucket 列表
        """
        # theme -> {"symbols": [...], "pct_sum": float, "count": int, "names": [...]}
        buckets: dict[str, dict[str, Any]] = {}

        for symbol_or_code, pct_chg in symbol_pct_pairs:
            pure = _code_to_symbol(symbol_or_code)
            theme = self.resolve_primary_theme(pure)
            if not theme:
                continue
            name = self._name_map.get(pure, pure)

            if theme not in buckets:
                buckets[theme] = {
                    "symbols": [],
                    "pct_sum": 0.0,
                    "count": 0,
                    "names": [],
                }
            b = buckets[theme]
            b["count"] += 1
            b["pct_sum"] += pct_chg
            if pure not in b["symbols"]:
                b["symbols"].append(pure)
            if len(b["names"]) < 5:
                b["names"].append(name)

        # 排序并构建结果
        sorted_themes = sorted(buckets.items(), key=lambda x: x[1]["count"], reverse=True)

        result = []
        for theme, b in sorted_themes[:top_n]:
            avg_pct = b["pct_sum"] / b["count"] if b["count"] > 0 else 0.0
            result.append(ThemeBucket(
                theme=theme,
                count=b["count"],
                symbols=b["symbols"],
                names=b["names"][:3],
                avg_pct_chg=round(avg_pct, 2),
            ))

        return result
