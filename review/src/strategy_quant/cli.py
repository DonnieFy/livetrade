"""Minimal CLI for the local strategy engine."""

from __future__ import annotations

import argparse
import json

from .strategies import run_all_strategies


def main() -> None:
    parser = argparse.ArgumentParser(description="Run A-share strategy engine on a trade date.")
    parser.add_argument("--date", required=True, help="Trade date, e.g. 2026-03-06")
    parser.add_argument("--top-n", type=int, default=10, help="Top candidates per strategy")
    parser.add_argument("--output", default="", help="Optional JSON output path")
    args = parser.parse_args()

    result = run_all_strategies(args.date, top_n=args.top_n)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(args.output)
        return

    for slug, payload in result.items():
        print(f"\n=== {slug} ===")
        print(f"candidates: {len(payload['candidates'])}")
        for row in payload["candidates"][: args.top_n]:
            print(
                row.get("symbol", ""),
                row.get("name", ""),
                f"score={row.get('score', 0):.2f}",
                row.get("primary_theme", ""),
            )


if __name__ == "__main__":
    main()
