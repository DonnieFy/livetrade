# -*- coding: utf-8 -*-
"""
盘面脉搏 — 报告格式化

只负责格式化，不做业务判断。
每个时点报告拆成 1-3 条消息（Alert），按主题块拆分。
"""

from __future__ import annotations

from typing import Any

from .calculators import (
    GroupPremiumResult,
)
from .theme_resolver import ThemeBucket


def _fmt_pct(pct: float) -> str:
    """格式化涨跌幅，带正号。"""
    if pct > 0:
        return f"+{pct:.2f}%"
    return f"{pct:.2f}%"


def _fmt_volume(vol: float) -> str:
    """格式化成交额（元转亿）。"""
    yi = vol / 1e8
    if yi >= 100:
        return f"{yi:.0f}亿"
    if yi >= 10:
        return f"{yi:.1f}亿"
    if yi >= 1:
        return f"{yi:.2f}亿"
    return f"{vol/1e4:.0f}万"


def _fmt_stock(sym: str, name: str, pct: float) -> str:
    return f"{name}({_fmt_pct(pct)})"


# ============================================================
# T1 报告（09:25 竞价结束）
# ============================================================

def build_t1_premium_report(
    premiums: list[GroupPremiumResult],
    board_ladder_premiums: dict[int, GroupPremiumResult],
) -> str:
    """T1-A: 昨日分组溢价。"""
    lines = ["📊 T1 竞价溢价快照"]

    # 昨日分组溢价
    for p in premiums:
        if p.sample_count == 0:
            lines.append(f"  {p.group_name}: 无样本")
            continue
        lines.append(
            f"  {p.group_name}({p.sample_count}只): "
            f"均{_fmt_pct(p.avg_pct)} 中位{_fmt_pct(p.median_pct)} "
            f"高{p.high_open_count}/平{p.flat_open_count}/低{p.low_open_count}"
        )
        if p.top3:
            top_str = " ".join(
                _fmt_stock(t["symbol"], t["name"], t["pct"]) for t in p.top3
            )
            lines.append(f"    最强: {top_str}")
        if p.bottom3:
            bot_str = " ".join(
                _fmt_stock(t["symbol"], t["name"], t["pct"]) for t in p.bottom3
            )
            lines.append(f"    最弱: {bot_str}")

    # 连板梯队溢价
    if board_ladder_premiums:
        lines.append("")
        lines.append("  连板梯队溢价:")
        for height in sorted(board_ladder_premiums.keys(), reverse=True):
            p = board_ladder_premiums[height]
            if p.sample_count == 0:
                continue
            top_names = " ".join(
                _fmt_stock(t["symbol"], t["name"], t["pct"]) for t in p.top3[:2]
            )
            lines.append(
                f"    {p.group_name}({p.sample_count}只): "
                f"均{_fmt_pct(p.avg_pct)} {top_names}"
            )

    return "\n".join(lines)


def build_t1_market_report(
    breadth: dict[str, Any],
    volume_top_themes: list[ThemeBucket],
    pct_top_themes: list[ThemeBucket],
    theme_performance: list[dict[str, Any]] | None = None,
) -> str:
    """T1-B: 市场温度 + 竞价聚合。"""
    lines = ["🌡 T1 市场温度"]

    # 市场温度
    lines.append(
        f"  红盘率 {breadth.get('red_rate', 0)}% "
        f"(↑{breadth.get('up', 0)} ↓{breadth.get('down', 0)}) "
        f"涨停{breadth.get('limit_up', 0)} 跌停{breadth.get('limit_down', 0)}"
    )
    lines.append(
        f"  成交额 {_fmt_volume(breadth.get('total_volume', 0))} "
        f"均涨{_fmt_pct(breadth.get('avg_pct', 0))}"
    )

    # 竞价量能 TOP 题材
    if volume_top_themes:
        lines.append("")
        lines.append("  竞价量能TOP题材:")
        for i, tb in enumerate(volume_top_themes, 1):
            names_str = "/".join(tb.names)
            lines.append(
                f"    {i}. {tb.theme}({tb.count}只) "
                f"均{_fmt_pct(tb.avg_pct_chg)} [{names_str}]"
            )

    # 竞价涨幅 TOP 题材
    if pct_top_themes:
        lines.append("")
        lines.append("  竞价涨幅TOP题材:")
        for i, tb in enumerate(pct_top_themes, 1):
            names_str = "/".join(tb.names)
            lines.append(
                f"    {i}. {tb.theme}({tb.count}只) "
                f"均{_fmt_pct(tb.avg_pct_chg)} [{names_str}]"
            )

    # 昨日热门题材今日表现
    if theme_performance:
        lines.append("")
        lines.append("  昨日热门题材今日竞价:")
        for tp in theme_performance[:5]:
            lines.append(
                f"    {tp['name']}({tp['total_count']}只): "
                f"红盘率{tp['red_rate']}% 均{_fmt_pct(tp['avg_pct'])}"
            )

    return "\n".join(lines)


# ============================================================
# T2 报告（09:30 开盘）
# ============================================================

def build_t2_report(
    slippage_stocks: list[dict[str, Any]],
    breadth: dict[str, Any] | None = None,
) -> str:
    """T2: 竞价兑现偏离 / 假高开预警。"""
    lines = ["⚠ T2 开盘兑现"]

    if breadth:
        lines.append(
            f"  红盘率 {breadth.get('red_rate', 0)}% "
            f"涨停{breadth.get('limit_up', 0)} 跌停{breadth.get('limit_down', 0)}"
        )

    if not slippage_stocks:
        lines.append("  无明显假高开")
    else:
        lines.append(f"  假高开预警({len(slippage_stocks)}只):")
        for s in slippage_stocks:
            lines.append(
                f"    {s['name']}: 竞价{_fmt_pct(s['auction_pct'])} → "
                f"开盘{_fmt_pct(s['open_pct'])} (偏离{s['slippage']:.1f}点)"
            )

    return "\n".join(lines)


# ============================================================
# T3 报告（09:33 开盘3分钟）
# ============================================================

def build_t3_rise_fall_report(
    rise_themes: list[ThemeBucket],
    fall_themes: list[ThemeBucket],
) -> str:
    """T3-A: 涨跌幅 TOP 题材聚合。"""
    lines = ["📈 T3 开盘3分钟方向"]

    if rise_themes:
        lines.append("  最强方向:")
        for i, tb in enumerate(rise_themes, 1):
            names_str = "/".join(tb.names)
            lines.append(
                f"    {i}. {tb.theme}({tb.count}只) "
                f"均{_fmt_pct(tb.avg_pct_chg)} [{names_str}]"
            )

    if fall_themes:
        lines.append("  最弱方向:")
        for i, tb in enumerate(fall_themes, 1):
            names_str = "/".join(tb.names)
            lines.append(
                f"    {i}. {tb.theme}({tb.count}只) "
                f"均{_fmt_pct(tb.avg_pct_chg)} [{names_str}]"
            )

    return "\n".join(lines)


def build_t3_risk_report(
    high_board_results: list[dict[str, Any]],
    big_face_results: list[dict[str, Any]],
    mainline_results: list[dict[str, Any]],
) -> str:
    """T3-B: 高标承接 + 大面股 + 主线延续。"""
    lines = ["🔍 T3 风险与高标"]

    # 高标承接
    if high_board_results:
        lines.append("  高标承接:")
        for hb in high_board_results:
            auction_str = ""
            if hb.get("auction_pct") is not None:
                auction_str = f" 竞价{_fmt_pct(hb['auction_pct'])}"
            lines.append(
                f"    {hb['name']}({hb['board_count']}板): "
                f"现{_fmt_pct(hb['current_pct'])}{auction_str} [{hb['status']}]"
            )

    # 大面股
    if big_face_results:
        lines.append(f"  大面股({len(big_face_results)}只):")
        for bf in big_face_results[:5]:
            lines.append(f"    {bf['name']} {_fmt_pct(bf['pct'])}")

    # 主线延续
    if mainline_results:
        lines.append("  主线延续:")
        for ml in mainline_results:
            leader_str = ""
            if ml.get("leader"):
                leader_str = f" 龙头{ml['leader']['name']}{_fmt_pct(ml['leader']['pct'])}"
            lines.append(
                f"    {ml['name']}({ml['total_count']}只): "
                f"红盘率{ml['red_rate']}% 涨停{ml['limit_up_count']}{leader_str}"
            )

    return "\n".join(lines)


# ============================================================
# 工具函数
# ============================================================

def split_report_lines(text: str, max_chars: int = 900) -> list[str]:
    """将长报告按行拆分为多条消息，每条不超过 max_chars。

    按空行边界拆分，避免截断句子。
    """
    if len(text) <= max_chars:
        return [text]

    chunks = []
    current_lines = []
    current_len = 0

    for line in text.split("\n"):
        line_len = len(line) + 1  # +1 for \n
        if current_len + line_len > max_chars and current_lines:
            chunks.append("\n".join(current_lines))
            current_lines = []
            current_len = 0
        current_lines.append(line)
        current_len += line_len

    if current_lines:
        chunks.append("\n".join(current_lines))

    return chunks
