from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REVIEW_ROOT = Path(__file__).resolve().parent

for path in [PROJECT_ROOT, REVIEW_ROOT]:
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

import config
from review.runtime import ensure_analyst_file, ensure_review_day, machine_file
from src.data_collector import DataCollector
from src.indicators.action_loader import compute_action_indicators, compute_action_trend
from src.indicators.board_stats import compute_board_stats
from src.indicators.emotion_cycle import compute_emotion_cycle
from src.indicators.hot_sectors import compute_hot_sectors
from src.indicators.index_status import compute_index_status
from src.indicators.star_stocks import compute_star_stocks
from src.strategy_quant.strategies import run_all_strategies


def run(date: str | None = None, top_n: int = 5) -> dict[str, Any]:
    dc = DataCollector(date=date)
    target_date = dc.date
    if not target_date:
        raise RuntimeError("无可用交易日数据，请先更新基础数据。")

    print("=" * 60)
    print(f"Review Machine Builder — {target_date}")
    print("=" * 60)

    index_status = compute_index_status(dc)
    emotion_cycle = compute_emotion_cycle(dc)
    board_stats = compute_board_stats(dc)
    hot_sectors = compute_hot_sectors(dc)
    star_stocks = compute_star_stocks(dc)
    action_analysis = compute_action_indicators(target_date, klines=dc.klines)
    action_trend = compute_action_trend(dc.get_trading_dates(10), klines=dc.klines)
    strategies_result = run_all_strategies(
        target_date,
        top_n=top_n,
        klines_df=dc.klines,
        basic_df=dc.stock_basic,
    )

    payload = build_machine_payload(
        target_date=target_date,
        index_status=index_status,
        emotion_cycle=emotion_cycle,
        board_stats=board_stats,
        hot_sectors=hot_sectors,
        star_stocks=star_stocks,
        action_analysis=action_analysis,
        action_trend=action_trend,
        strategies_result=strategies_result,
    )

    day_dir = ensure_review_day(target_date)
    output_file = machine_file(target_date)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    analyst_path = ensure_analyst_file(target_date)

    print(f"machine.json: {output_file}")
    print(f"analyst.yaml: {analyst_path}")
    print(f"review dir: {day_dir}")
    return payload


def build_machine_payload(
    *,
    target_date: str,
    index_status: dict[str, Any],
    emotion_cycle: dict[str, Any],
    board_stats: dict[str, Any],
    hot_sectors: dict[str, Any],
    star_stocks: dict[str, Any],
    action_analysis: dict[str, Any],
    action_trend: dict[str, Any],
    strategies_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "meta": {
            "trade_date": target_date,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "version": "1.0.0",
        },
        "market": {
            "indexes": index_status.get("indexes", {}),
            "breadth": index_status.get("breadth", {}),
            "turnover": index_status.get("volume", {}),
        },
        "board_stats": {
            "limit_up_count": board_stats.get("limit_up_count", 0),
            "limit_down_count": board_stats.get("limit_down_count", 0),
            "hit_limit_up_count": board_stats.get("hit_limit_up_count", 0),
            "broken_board_count": board_stats.get("broken_board_count", 0),
            "seal_rate": board_stats.get("seal_rate", 0.0),
            "broken_board_rate": board_stats.get("broken_board_rate", 0.0),
            "consecutive_board_count": board_stats.get("consecutive_board_count", 0),
            "max_board_height": board_stats.get("max_board_height", 0),
            "consecutive_board_ladder": emotion_cycle.get("consecutive_board", {}).get(
                "ladder", {}
            ),
            "first_board_premium": board_stats.get("first_board_premium"),
            "yesterday_limit_up_performance": board_stats.get(
                "yesterday_limit_up_performance", {}
            ),
            "emotion_5d_matrix": board_stats.get("emotion_5d_matrix", []),
        },
        "stocks": {
            "limit_up": emotion_cycle.get("limit_up_stocks", []),
            "limit_down": emotion_cycle.get("limit_down_stocks", []),
            "broken_board": board_stats.get("broken_board_stocks", []),
            "board_breakers": board_stats.get("board_breakers", []),
            "high_volatility": star_stocks.get("high_volatility_top", []),
            "highest_board": star_stocks.get("highest_board", []),
        },
        "themes": {
            "hot_sectors": hot_sectors,
            "action_analysis": action_analysis,
            "action_trend": action_trend,
            "sector_clusters_hint": hot_sectors.get("svk_command_hint", ""),
        },
        "strategy_quant": _summarize_strategy_quant(strategies_result),
    }


def _summarize_strategy_quant(strategies_result: dict[str, Any]) -> dict[str, Any]:
    candidates_by_strategy: dict[str, list[dict[str, Any]]] = {}
    top_candidates: list[dict[str, Any]] = []

    for slug, payload in strategies_result.items():
        candidates = payload.get("candidates", [])
        candidates_by_strategy[slug] = candidates
        for candidate in candidates[:3]:
            row = dict(candidate)
            row["strategy"] = slug
            top_candidates.append(row)

    top_candidates.sort(
        key=lambda item: float(item.get("score", 0) or 0),
        reverse=True,
    )

    return {
        "candidates_by_strategy": candidates_by_strategy,
        "top_candidates": top_candidates[:20],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build review machine facts for one trade date.")
    parser.add_argument("--date", default="", help="Trade date, default latest available date")
    parser.add_argument("--top-n", type=int, default=5, help="Top candidates per strategy")
    args = parser.parse_args()
    run(date=args.date or None, top_n=args.top_n)


if __name__ == "__main__":
    main()
