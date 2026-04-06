from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

import config

REVIEW_STRATEGY_ALIASES: dict[str, tuple[str, ...]] = {
    "trend_breakout": ("trend_revaluation", "mainline_low_absorption"),
    "auction_strength": ("main_rise_resonance", "new_mainline_breakout"),
    "ice_point_repair": ("ice_repair", "mispriced_recovery"),
    "auction_limit_chase": ("fast_rotation_scalp",),
}


def _sort_dates(paths: list[Path]) -> list[str]:
    return sorted(path.name for path in paths if path.is_dir())


def list_review_dates() -> list[str]:
    if not config.REVIEW_DAILY_DIR.exists():
        return []
    return _sort_dates(list(config.REVIEW_DAILY_DIR.iterdir()))


def review_day_dir(review_date: str) -> Path:
    return config.REVIEW_DAILY_DIR / review_date


def machine_file(review_date: str) -> Path:
    return review_day_dir(review_date) / config.REVIEW_MACHINE_FILENAME


def analyst_file(review_date: str) -> Path:
    return review_day_dir(review_date) / config.REVIEW_ANALYST_FILENAME


def report_file(review_date: str) -> Path:
    return review_day_dir(review_date) / config.REVIEW_REPORT_FILENAME


def default_analyst_payload(review_date: str) -> dict[str, Any]:
    return {
        "date": review_date,
        "market_regime": "",
        "emotion_phase": "",
        "trend_bias": "",
        "is_ice_point": None,
        "main_themes": [],
        "secondary_themes": [],
        "avoid_themes": [],
        "active_strategies": [],
        "focus_watchlist": [],
        "tomorrow_observation_points": [],
        "risk_notes": [],
        "manual_overrides": {
            "strategy_candidates": {},
            "strategy_excludes": {},
        },
    }


def ensure_review_day(review_date: str) -> Path:
    day_dir = review_day_dir(review_date)
    day_dir.mkdir(parents=True, exist_ok=True)
    return day_dir


def ensure_analyst_file(review_date: str) -> Path:
    path = analyst_file(review_date)
    if path.exists():
        return path

    ensure_review_day(review_date)
    payload = default_analyst_payload(review_date)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(payload, f, allow_unicode=True, sort_keys=False)
    return path


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@dataclass
class ReviewData:
    trade_date: str
    review_date: str = ""
    machine: dict[str, Any] = field(default_factory=dict)
    analyst: dict[str, Any] = field(default_factory=dict)

    @property
    def available(self) -> bool:
        return bool(self.review_date and (self.machine or self.analyst))

    def get_manual_candidates(self, slug: str) -> list[str]:
        overrides = self.analyst.get("manual_overrides", {})
        candidates = overrides.get("strategy_candidates", {}).get(slug, [])
        return [str(code) for code in candidates]

    def get_machine_candidates(self, *slugs: str) -> list[str]:
        candidates_by_strategy = self.machine.get("strategy_quant", {}).get(
            "candidates_by_strategy", {}
        )
        symbols: list[str] = []
        for slug in slugs:
            rows = candidates_by_strategy.get(slug, [])
            for row in rows:
                symbol = str(row.get("symbol", "")).strip()
                if symbol:
                    symbols.append(symbol)
        return list(dict.fromkeys(symbols))

    def get_focus_watchlist_symbols(self, *slugs: str) -> list[str]:
        wanted = {slug for slug in slugs if slug}
        symbols: list[str] = []
        for item in self.analyst.get("focus_watchlist", []):
            if not isinstance(item, dict):
                continue
            strategy = str(item.get("strategy", "")).strip()
            if wanted and strategy and strategy not in wanted:
                continue
            symbol = str(item.get("symbol", "")).strip()
            if symbol:
                symbols.append(symbol)
        return list(dict.fromkeys(symbols))

    def get_strategy_candidates(self, slug: str) -> list[str]:
        aliases = REVIEW_STRATEGY_ALIASES.get(slug, ())
        manual = self.get_manual_candidates(slug)
        watchlist = self.get_focus_watchlist_symbols(slug, *aliases)
        machine = self.get_machine_candidates(*aliases)
        return list(dict.fromkeys([*manual, *watchlist, *machine]))

    def is_strategy_excluded(self, slug: str) -> bool:
        overrides = self.analyst.get("manual_overrides", {})
        exclude_value = overrides.get("strategy_excludes", {}).get(slug)
        if exclude_value is None:
            return False
        if isinstance(exclude_value, str):
            return exclude_value.lower() == "all"
        if isinstance(exclude_value, list):
            return any(str(item).lower() == "all" for item in exclude_value)
        return bool(exclude_value)

    def get_active_strategy_slugs(self) -> set[str]:
        raw = self.analyst.get("active_strategies", [])
        active: set[str] = set()
        for item in raw:
            if isinstance(item, str):
                active.add(item)
                continue
            if isinstance(item, dict) and item.get("slug"):
                active.add(str(item["slug"]))
        return active


def resolve_review_date(trade_date: str) -> str:
    dates = list_review_dates()
    if not dates:
        return ""

    older = [date for date in dates if date < trade_date]
    if older:
        return older[-1]

    not_newer = [date for date in dates if date <= trade_date]
    if not_newer:
        return not_newer[-1]

    return dates[-1]


def load_review(review_date: str, *, trade_date: str = "") -> ReviewData:
    if not review_date:
        return ReviewData(trade_date=trade_date)

    return ReviewData(
        trade_date=trade_date,
        review_date=review_date,
        machine=_load_json(machine_file(review_date)),
        analyst=_load_yaml(analyst_file(review_date)),
    )


def load_review_for_trade(trade_date: str) -> ReviewData:
    review_date = resolve_review_date(trade_date)
    return load_review(review_date, trade_date=trade_date)
