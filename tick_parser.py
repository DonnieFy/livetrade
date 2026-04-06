# -*- coding: utf-8 -*-
"""
Livetrade — CSV tick 数据解析

将 ashares-ticks 写入的 CSV 原始行解析为 pandas DataFrame。
"""

import io
import csv
import logging

import pandas as pd

import config

logger = logging.getLogger(__name__)


def parse_csv_lines(lines: list[str]) -> pd.DataFrame:
    """将 CSV 文本行列表解析为 DataFrame。

    参数:
        lines: 原始 CSV 行（无列头），每行对应一只股票的一帧快照

    返回:
        DataFrame，列名为 config.CSV_COLUMNS，数值列已转换类型
    """
    if not lines:
        return pd.DataFrame(columns=config.CSV_COLUMNS)

    # 用 csv.reader 处理可能存在的引号转义
    reader = csv.reader(io.StringIO("".join(lines)))
    rows = list(reader)

    df = pd.DataFrame(rows, columns=config.CSV_COLUMNS)

    # 数值列类型转换
    for col in config.NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def parse_csv_text(text: str) -> pd.DataFrame:
    """将 CSV 文本块解析为 DataFrame。"""
    lines = text.splitlines(keepends=True)
    return parse_csv_lines(lines)


def extract_tick_time(df: pd.DataFrame) -> str | None:
    """提取该帧的 time 值（取第一行）。"""
    if df.empty or "time" not in df.columns:
        return None
    return str(df.iloc[0]["time"]).strip()


def calc_pct_change(df: pd.DataFrame) -> pd.DataFrame:
    """为 DataFrame 计算涨跌幅列 pct_chg = (now - close) / close * 100。

    注意: CSV 中的 close 列实际是 **昨收盘价**。
    """
    df = df.copy()
    df["pct_chg"] = ((df["now"] - df["close"]) / df["close"] * 100).round(4)
    return df


def calc_limit_up_price(df: pd.DataFrame) -> pd.DataFrame:
    """计算涨停价并添加 limit_up_price 和 is_limit_up 列。"""
    df = df.copy()

    def _limit_ratio(code: str) -> float:
        # code 格式: "sh600000" / "sz300750"
        pure = code[2:] if len(code) > 2 else code
        if pure.startswith(config.GEM_PREFIXES) or pure.startswith(config.STAR_PREFIXES):
            return config.LIMIT_RATIO_GEM_STAR
        return config.LIMIT_RATIO_MAIN

    df["limit_ratio"] = df["code"].apply(_limit_ratio)
    df["limit_up_price"] = (df["close"] * (1 + df["limit_ratio"])).round(2)
    df["limit_down_price"] = (df["close"] * (1 - df["limit_ratio"])).round(2)
    df["is_limit_up"] = df["now"] >= df["limit_up_price"]
    df["is_limit_down"] = df["now"] <= df["limit_down_price"]

    return df
