# -*- coding: utf-8 -*-
"""信号题材增强。

统一给股票类 Alert 追加板块、题材和静态同题材相关个股。
"""

from __future__ import annotations

import logging
import re

import config
from market_pulse.theme_resolver import ThemeResolver
from strategy_base import Alert

logger = logging.getLogger(__name__)

_RESOLVER: ThemeResolver | None = None
_LOAD_FAILED = False


def format_alert_with_theme(alert: Alert) -> str:
    """格式化 Alert，并在可用时追加题材信息。"""
    line = alert.format_line()
    if not getattr(config, "ALERT_THEME_INFO_ENABLED", True):
        return line
    if not _is_stock_code(alert.code):
        return line

    resolver = _get_resolver()
    if resolver is None or not resolver.loaded:
        return line

    try:
        extra_lines = _build_theme_lines(
            alert.code,
            resolver,
            theme_limit=getattr(config, "ALERT_THEME_MAX_COUNT", 6),
            related_limit=getattr(config, "ALERT_THEME_RELATED_LIMIT", 5),
        )
    except Exception as e:
        logger.warning("信号题材增强失败: %s %s: %s", alert.code, alert.name, e)
        return line

    if not extra_lines:
        return line
    return "\n".join([line, *extra_lines])


def warmup_theme_resolver() -> None:
    """预热题材解析器缓存，避免首条信号写出时阻塞。"""
    if getattr(config, "ALERT_THEME_INFO_ENABLED", True):
        _get_resolver()


def _get_resolver() -> ThemeResolver | None:
    global _RESOLVER, _LOAD_FAILED

    if _RESOLVER is not None:
        return _RESOLVER
    if _LOAD_FAILED:
        return None

    resolver = ThemeResolver()
    try:
        resolver.load(
            knowledge_root=config.VECTOR_PROJECT,
            industry_file=config.INDUSTRY_FILE,
            stock_basic_file=config.STOCK_BASIC_FILE,
        )
    except Exception as e:
        _LOAD_FAILED = True
        logger.warning("加载信号题材解析器失败: %s", e)
        return None

    if not getattr(resolver, "_theme_map", None):
        _LOAD_FAILED = True
        logger.warning("加载信号题材解析器失败: 题材映射为空")
        return None

    _RESOLVER = resolver
    return _RESOLVER


def _build_theme_lines(
    code: str,
    resolver: ThemeResolver,
    theme_limit: int,
    related_limit: int,
) -> list[str]:
    themes = resolver.resolve_themes(code)
    if not themes:
        return ["  题材: 暂无映射"]

    primary = resolver.resolve_primary_theme(code)
    industry = resolver.resolve_industry(code)

    lines: list[str] = []
    sector_parts = [part for part in (primary, industry) if part]
    if sector_parts:
        lines.append(f"  板块: {' / '.join(sector_parts)}")

    shown_themes = themes[:max(theme_limit, 1)]
    lines.append(f"  题材: {' / '.join(shown_themes)}")

    if primary:
        related = resolver.resolve_related_by_theme(
            primary,
            exclude_symbol=code,
            limit=related_limit,
        )
        if related:
            related_text = "、".join(
                f"{_clean_name(name)}({symbol})"
                for symbol, name in related
            )
            lines.append(f"  相关: {primary}[{related_text}]")

    return lines


def _is_stock_code(code: str) -> bool:
    text = str(code or "").strip()
    if not text or text.lower() == "market":
        return False
    if re.fullmatch(r"(sh|sz|bj)\d{6}", text):
        return True
    return bool(re.fullmatch(r"\d{6}", text))


def _clean_name(name: str) -> str:
    return re.sub(r"\s+", "", str(name or "").strip())
