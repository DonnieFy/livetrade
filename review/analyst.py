from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml

from review.runtime import analyst_file, default_analyst_payload, ensure_analyst_file


def load_analyst(review_date: str) -> dict[str, Any]:
    path = ensure_analyst_file(review_date)
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_analyst(
    review_date: str,
    payload: dict[str, Any],
    *,
    merge: bool = True,
) -> Path:
    current = load_analyst(review_date)
    base = current if merge else default_analyst_payload(review_date)
    validated = validate_payload(payload)
    merged = deep_merge(base, validated)

    path = analyst_file(review_date)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(merged, f, allow_unicode=True, sort_keys=False)
    return path


def validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    allowed_keys = set(default_analyst_payload("1970-01-01").keys())
    unknown = sorted(set(payload.keys()) - allowed_keys)
    if unknown:
        raise ValueError(f"analyst payload 包含未知字段: {', '.join(unknown)}")

    if "active_strategies" in payload:
        payload["active_strategies"] = _normalize_active_strategies(
            payload["active_strategies"]
        )
    if "focus_watchlist" in payload:
        payload["focus_watchlist"] = _normalize_focus_watchlist(
            payload["focus_watchlist"]
        )
    if "main_themes" in payload:
        payload["main_themes"] = _normalize_theme_items(payload["main_themes"])
    if "secondary_themes" in payload:
        payload["secondary_themes"] = _normalize_theme_items(payload["secondary_themes"])
    if "manual_overrides" in payload:
        payload["manual_overrides"] = _normalize_manual_overrides(
            payload["manual_overrides"]
        )

    return payload


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in patch.items():
        if (
            isinstance(value, dict)
            and isinstance(result.get(key), dict)
        ):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _normalize_theme_items(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        raise ValueError("theme 列表必须是 list")
    normalized: list[dict[str, Any]] = []
    for item in items:
        if isinstance(item, str):
            normalized.append({"name": item, "stance": "", "note": ""})
            continue
        if not isinstance(item, dict):
            raise ValueError("theme 项必须是 string 或 mapping")
        normalized.append(
            {
                "name": str(item.get("name", "")).strip(),
                "stance": str(item.get("stance", "")).strip(),
                "note": str(item.get("note", "")).strip(),
            }
        )
    return normalized


def _normalize_active_strategies(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        raise ValueError("active_strategies 必须是 list")
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        if isinstance(item, str):
            normalized.append({"slug": item, "priority": index, "reason": ""})
            continue
        if not isinstance(item, dict):
            raise ValueError("active_strategies 项必须是 string 或 mapping")
        normalized.append(
            {
                "slug": str(item.get("slug", "")).strip(),
                "priority": int(item.get("priority", index) or index),
                "reason": str(item.get("reason", "")).strip(),
            }
        )
    return normalized


def _normalize_focus_watchlist(items: Any) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        raise ValueError("focus_watchlist 必须是 list")
    normalized: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("focus_watchlist 项必须是 mapping")
        normalized.append(
            {
                "symbol": str(item.get("symbol", "")).strip(),
                "name": str(item.get("name", "")).strip(),
                "tags": list(item.get("tags", []) or []),
                "strategy": str(item.get("strategy", "")).strip(),
                "note": str(item.get("note", "")).strip(),
            }
        )
    return normalized


def _normalize_manual_overrides(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("manual_overrides 必须是 mapping")
    strategy_candidates = value.get("strategy_candidates", {}) or {}
    strategy_excludes = value.get("strategy_excludes", {}) or {}
    if not isinstance(strategy_candidates, dict) or not isinstance(strategy_excludes, dict):
        raise ValueError("manual_overrides 下的 strategy_candidates / strategy_excludes 必须是 mapping")
    return {
        "strategy_candidates": strategy_candidates,
        "strategy_excludes": strategy_excludes,
    }


def _load_json_file(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("JSON 输入必须是 object")
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge structured analyst judgments into analyst.yaml")
    parser.add_argument("--date", required=True, help="Review date, format YYYY-MM-DD")
    parser.add_argument("--from-json", help="JSON file containing structured analyst payload")
    parser.add_argument("--set-json", help="Inline JSON object containing structured analyst payload")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Replace analyst.yaml instead of merging into the existing file",
    )
    args = parser.parse_args()

    if not args.from_json and not args.set_json:
        raise SystemExit("必须提供 --from-json 或 --set-json")

    payload: dict[str, Any] = {}
    if args.from_json:
        payload = _load_json_file(args.from_json)
    if args.set_json:
        inline = json.loads(args.set_json)
        if not isinstance(inline, dict):
            raise SystemExit("--set-json 必须是 JSON object")
        payload = deep_merge(payload, inline)

    path = save_analyst(args.date, payload, merge=not args.replace)
    print(path)


if __name__ == "__main__":
    main()
