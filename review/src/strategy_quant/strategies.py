"""Executable trading strategies built on top of local A-share data."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
import pandas as pd

from .constants import DEFAULT_KNOWLEDGE_ROOT, DEFAULT_TICKS_ROOT
from .features import (
    attach_best_theme,
    build_elasticity_preference,
    build_intraday_snapshot,
    build_market_environment,
    build_mispriced_recovery_signals,
    build_stock_signals,
    build_theme_snapshot,
    prepare_daily_features,
)
from .loaders import load_daily_klines, load_stock_basic, load_theme_knowledge, load_ticks_for_date


@dataclass(frozen=True)
class StrategyCard:
    slug: str
    name: str
    thesis: str
    environment: str
    stock_selection: str
    buy_timing: str
    observe_after_buy: str


STRATEGY_CARDS: dict[str, StrategyCard] = {
    "trend_revaluation": StrategyCard(
        slug="trend_revaluation",
        name="产业趋势与基本面波段",
        thesis="依托长期产业趋势（AI算力/半导体/出海/机器人等）或基本面边际变化（涨价/业绩预期），跟随机构和中长线趋势资金的审美，博弈估值重构的波段红利。",
        environment="指数处于牛市中继或宽幅震荡，成交量>1万亿可支撑大票运行；市场风格偏向有业绩/逻辑支撑的'机构市'（短线投机受监管压制时尤其有效）。",
        stock_selection="板块核心中军或有独立逻辑的细分龙头，流通市值适中偏大。走势'领涨抗跌'：指数跌时横盘抗跌在MA5/MA10上方，指数企稳时率先创阶段新高。",
        buy_timing="绝不追高加速段。买点：(1)新高突破后回踩MA5/MA10低吸；(2)指数恐慌跳水但个股有主动承接时埋伏；(3)板块出现合理分歧回踩关键均线时介入。",
        observe_after_buy="核心中军是否放量破位（跌破关键均线且无修复→减仓）；指数与板块是否共振走强；产业端是否有持续消息催化或订单验证落地。",
    ),
    "new_mainline_breakout": StrategyCard(
        slug="new_mainline_breakout",
        name="新周期破局龙",
        thesis="轮动混沌期后，首只突破前期高度压制的破局龙标志新周期开端。破局龙出现意味着大周期有望进入新主升，跟随最先发起进攻的先锋。",
        environment="前期处于轮动期或冰点期，连板高度长期压制在3-4板以下；突然出现连板突破前高（如突破6板），且有明确题材支撑。",
        stock_selection="突破高度限制的先锋龙头（小中盘优先），同题材中出现容量中军配合上涨者更优。画像完整：龙头+中军+助攻(一字)+跟风齐全。",
        buy_timing="破局确认日或次日弱转强竞价高开跟进。前日烂板次日竞价超预期（红开或高开秒板）为核心买点。",
        observe_after_buy="高度是否持续抬升；旧周期是否退潮让位；题材二三梯队是否跟上；涨停溢价指数是否持续为正。",
    ),
    "main_rise_resonance": StrategyCard(
        slug="main_rise_resonance",
        name="情绪连板接力与主升共振",
        thesis="基于筹码博弈与情绪周期：龙头、中军、板块宽度三者共振时风险收益比最佳。游资集中火力打造高辨识度妖股/空间板龙头，博弈短线情绪溢价与强者恒强。",
        environment="连板晋级率上升，全市场连板高度不断突破(≥7板)；涨停家数增加、封板率>60%；赚钱效应集中在少数高辨识度个股，进入情绪加速期/高潮期。",
        stock_selection="严格筛选全市场最高空间板(空间龙)或板块内绝对先锋；有中军配合(无中军不主线)。放弃无辨识度的中位跟风股(极易吃大面)。",
        buy_timing="分歧转一致的弱转强节点：前日烂板/爆量分歧→次日竞价超预期高开秒板。板块共振时打板跟随。严格避开连续高潮后的缩量加速一字接力。",
        observe_after_buy="板块一二三板梯队是否完整；龙头是否高位'爆量滞涨'或'尾盘漏单'；同梯队中位股是否大面积跌停(亏钱效应放大)；监管异动规则对高标的压制。",
    ),
    "mainline_low_absorption": StrategyCard(
        slug="mainline_low_absorption",
        name="主线分歧低吸",
        thesis="主线叙事未结束时，最好的买点常出现在分歧而非高潮。当核心龙头出现合理分歧但板块逻辑未崩，低吸最强分支核心享受后续修复溢价。",
        environment="主线题材仍然完整（龙头未死/板块热度未枯竭），市场非全面崩溃，出现局部分歧而非系统性退潮。",
        stock_selection="最强分支核心、技术图形高效、接近新高、回撤浅、盘中有明确承接。走势要求缩量回调而非放量下跌。",
        buy_timing="指数或板块洗盘时低吸；竞价不佳但未破位的次日；个股回踩MA5/MA10时低吸布局。",
        observe_after_buy="核心龙头是否率先反弹；分支是否跟随向上；盘中低点是否抬升；缩量跌放量涨特征是否持续。",
    ),
    "ice_repair": StrategyCard(
        slug="ice_repair",
        name="极值冰点反核与超跌反弹",
        thesis="物极必反。连续退潮出现大规模跌停/恐慌抛售后，做空动能衰竭，博弈情绪拐点修复的反弹溢价。",
        environment="连续'双冰'或'多冰'：跌停家数激增(>30家)、连板高度压至2-3板、下跌>4000家、涨停溢价连续为负(≥3天)。",
        stock_selection="前期跌幅够深且有基本面支撑的错杀核心大票；冰点日跌停板被大资金撬板/分时承接极强的人气老龙；冰点日逆势抗跌率先翻红的破局试错票。",
        buy_timing="情绪冰点日尾盘(抢先手防次日高开)；分歧次日核按钮深水区(下杀-5%甚至跌停附近)低吸；盘中个股从水下直线拉起完成弱转强时半路跟。",
        observe_after_buy="次日指数/情绪是否V型回升；买入个股是否快速拉红站稳均线；若修复极弱、量能萎缩或反抽无力→果断止损，拒绝格局。",
    ),
    "catchup_rotation": StrategyCard(
        slug="catchup_rotation",
        name="高低切与补涨轮动",
        thesis="资金天然厌恶风险。老龙头涨幅过大/面临监管/资金兑现时，资金自发向低位流动。博弈同题材低位补涨或全新低位题材的破局。",
        environment="情绪周期进入'退潮期'初期或'补涨期'：老龙头断板/放量巨震/关小黑屋，高位容错率极低，但板块整体热度尚未枯竭。",
        stock_selection="与老龙同概念属性(或暗线)的低位小盘股（1进2阶段）；或伴随突发新闻催化的全新题材先锋首板。",
        buy_timing="老龙头断板/跳水当日及次日果断切低位，介入1进2（打换手板或大单一字）；轮动预判中提前潜伏，盘中放量拉升时半路跟。",
        observe_after_buy="老龙头是否A杀跌停(老龙核死→补涨票也会失败)；新买入低位票次日竞价是否拿到全市场最大封单(确认补涨共识)；是否有更强新题材卡位吸血。",
    ),
    "fast_rotation_scalp": StrategyCard(
        slug="fast_rotation_scalp",
        name="量化弹性套利(20cm/30cm)",
        thesis="适应量化主导+主板异动新规限制的生态。量化偏好高波动、热点标签的弹性品种。跟随量化'触发式'点火，吃日内超额弹性和次日情绪溢价。",
        environment="主板10cm连板生态恶劣(接力吃大面、异动监管压制)，存量活跃资金转向创业板/科创板/北交所寻找弹性与流动性。",
        stock_selection="当前最热主线概念(主板龙头属性发散)的300/688标的，流通盘偏小适中，K线折叠向上(大阳-调整-反包)或沿MA5趋势上行。",
        buy_timing="板块情绪启动或高低切回流时半路追击日内最先拉升的弹性先锋；或人气票大涨后回踩MA5/MA10时低吸做N字反包。快进快出，不追高位加速。",
        observe_after_buy="主板锚定龙头是否坚挺(龙头崩→弹性跟风瞬间跳水)；次日量化资金是否继续接力推高；冲高滞涨/板块分歧加大→及时止盈，切忌在量化票里格局长情。",
    ),
    "mispriced_recovery": StrategyCard(
        slug="mispriced_recovery",
        name="错杀修复套利",
        thesis="近1-2日连续涨停的个股因情绪退潮、指数杀跌或所属板块中某个弱势标签拖累被错杀至跌停，但自身拥有其他仍在运行的热点题材。跌停日盘中出现明确做多动作（撬板、反弹、放量），次日情绪回暖时高概率修复。核心本质：市场用A属性定价，但个股真实价值锚定的是仍在运行的B属性。",
        environment="市场并非全面崩溃，而是局部退潮（某板块回调但其他板块延续）。情绪指标非深度冰点，次日有回暖预期。跌停个股数量适中（5-30家），非系统性暴跌。",
        stock_selection="严格四条件筛选：(1)前1-2日有涨停记录(证明市场认可度)；(2)当日跌停或大跌>7%(被错杀)；(3)盘中tick数据显示做多动作(撬板、反弹幅度>3%、低点放量)；(4)自身题材至少有一个匹配当日theme_support_score>50的热门板块(证明有其他热点属性锚)。",
        buy_timing="次日竞价阶段。若竞价高开(>-3%)或出现买盘堆积，可在9:25集合竞价果断买入；若低开则观察9:30后5分钟，出现快速拉升过零轴时半路跟进。",
        observe_after_buy="命中的热点题材板块当日是否继续走强(热点熄火则逻辑崩塌→止损)；个股是否在30分钟内站上均价线（不能→减仓）；同板块是否有其他标的联动上涨(孤军奋战→谨慎)。",
    ),
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


class StrategyEngine:
    """Load local datasets once and compute strategy candidates by date."""

    def __init__(
        self,
        ticks_root=DEFAULT_TICKS_ROOT,
        knowledge_root=DEFAULT_KNOWLEDGE_ROOT,
        *,
        klines_df: pd.DataFrame | None = None,
        basic_df: pd.DataFrame | None = None,
    ):
        self.klines = klines_df if klines_df is not None else load_daily_klines()
        self.basic = basic_df if basic_df is not None else load_stock_basic()
        self.theme_map, self.stock_meta = load_theme_knowledge(knowledge_root)
        self.daily_features = prepare_daily_features(self.klines, basic=self.basic, stock_meta=self.stock_meta)
        self.market_env = build_market_environment(self.daily_features)
        self.stock_signals = build_stock_signals(self.daily_features)
        self.ticks_root = ticks_root

    def _get_prev_date(self, date: str) -> str | None:
        dates = self.market_env["date"].tolist()
        if date not in dates:
            return None
        idx = dates.index(date)
        if idx == 0:
            return None
        return dates[idx - 1]

    def _get_day_bundle(self, date: str) -> dict[str, Any]:
        env_row = self.market_env[self.market_env["date"] == date]
        if env_row.empty:
            raise ValueError(f"date {date} not found in daily kline data")
        env = env_row.iloc[0].to_dict()
        prev_date = self._get_prev_date(date)
        prev_env = self.market_env[self.market_env["date"] == prev_date].iloc[0].to_dict() if prev_date else {}

        day_stock = self.stock_signals[self.stock_signals["date"] == date].copy()
        theme_snapshot = build_theme_snapshot(day_stock, self.theme_map)
        day_stock = attach_best_theme(day_stock, self.theme_map, theme_snapshot)

        ticks = load_ticks_for_date(date, self.ticks_root)
        intraday = build_intraday_snapshot(ticks)
        if not intraday.empty:
            day_stock = day_stock.merge(
                intraday[
                    [
                        "symbol",
                        "open_strength",
                        "first_5m_return",
                        "morning_drawdown",
                        "morning_rebound",
                        "close_from_open_tick",
                        "bid_ask_imbalance",
                        "intraday_support_signal",
                    ]
                ],
                on="symbol",
                how="left",
            )
        else:
            for col in [
                "open_strength",
                "first_5m_return",
                "morning_drawdown",
                "morning_rebound",
                "close_from_open_tick",
                "bid_ask_imbalance",
                "intraday_support_signal",
            ]:
                day_stock[col] = np.nan if col != "intraday_support_signal" else False

        elasticity = build_elasticity_preference(day_stock)
        return {
            "date": date,
            "env": env,
            "prev_env": prev_env,
            "day_stock": day_stock,
            "theme_snapshot": theme_snapshot,
            "intraday": intraday,
            "elasticity": elasticity,
        }

    @staticmethod
    def _finalize(df: pd.DataFrame, strategy: str, top_n: int = 20) -> pd.DataFrame:
        if df.empty:
            return df
        cols = [
            "strategy",
            "score",
            "symbol",
            "name",
            "primary_theme",
            "theme_support_score",
            "pct_chg",
            "limit_up_streak",
            "rs_5",
            "rs_20",
            "close_vs_ma20",
            "amount_vs_ma5",
            "open_strength",
            "first_5m_return",
            "morning_rebound",
            "intraday_support_signal",
            "market_cap_total_e8",
        ]
        keep_cols = [c for c in cols if c in df.columns]
        out = df.copy()
        out["strategy"] = strategy
        return out.sort_values(["score", "theme_support_score", "rs_20"], ascending=[False, False, False]).head(top_n)[keep_cols]

    def strategy_trend_revaluation(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        env = bundle["env"]
        day_stock = bundle["day_stock"]
        if env["emotion_phase"] == "ice":
            return pd.DataFrame()
        cond = (
            day_stock["trend_core_signal"].fillna(False)
            & (day_stock["liquidity_regime"].fillna(env["liquidity_regime"]) != "shrinking")
        ) if "liquidity_regime" in day_stock.columns else day_stock["trend_core_signal"].fillna(False)
        df = day_stock[cond].copy()
        if df.empty:
            return df
        df["score"] = (
            df["rs_20"].fillna(0) * 100
            + df["rs_5"].fillna(0) * 40
            + df["close_vs_ma20"].fillna(0) * 80
            + df["theme_support_score"].fillna(30) * 0.5
            + (df["intraday_support_signal"].eq(True).astype(int) * 5)
        )
        return self._finalize(df, "trend_revaluation", top_n=top_n)

    def strategy_new_mainline_breakout(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        env = bundle["env"]
        prev_env = bundle["prev_env"]
        day_stock = bundle["day_stock"]
        if not (env["height_breakout"] or (env["market_height"] >= 5 and _safe_float(prev_env.get("market_height")) <= 4)):
            return pd.DataFrame()
        df = day_stock[
            (day_stock["limit_up_streak"] >= max(3, int(env["market_height"]) - 1))
            & (day_stock["breakout_pioneer_signal"].fillna(False))
        ].copy()
        if df.empty:
            return df
        cap_penalty = np.where(df["market_cap_total_e8"].fillna(120) <= 120, 5, 0)
        df["score"] = (
            df["limit_up_streak"] * 12
            + df["theme_support_score"].fillna(30) * 0.6
            + df["rs_5"].fillna(0) * 60
            + cap_penalty
        )
        return self._finalize(df, "new_mainline_breakout", top_n=top_n)

    def strategy_main_rise_resonance(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        env = bundle["env"]
        day_stock = bundle["day_stock"]
        if not (env["is_main_rise"] or (env["emotion_score"] >= 60 and env["amount_vs_ma5"] >= 0.95)):
            return pd.DataFrame()
        df = day_stock[
            (day_stock["theme_support_score"].fillna(0) >= 55)
            & (
                day_stock["is_limit_up"].fillna(False)
                | day_stock["trend_core_signal"].fillna(False)
                | day_stock["breakout_pioneer_signal"].fillna(False)
            )
        ].copy()
        if df.empty:
            return df
        df["score"] = (
            df["theme_support_score"].fillna(0) * 0.7
            + df["limit_up_streak"].fillna(0) * 8
            + df["rs_20"].fillna(0) * 40
            + df["amount_vs_ma5"].fillna(0) * 5
        )
        return self._finalize(df, "main_rise_resonance", top_n=top_n)

    def strategy_mainline_low_absorption(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        env = bundle["env"]
        day_stock = bundle["day_stock"]
        if env["emotion_phase"] == "ice":
            return pd.DataFrame()
        df = day_stock[
            day_stock["low_absorption_signal"].fillna(False)
            & (day_stock["theme_support_score"].fillna(0) >= 50)
            & ~day_stock["climax_risk_signal"].fillna(False)
        ].copy()
        if df.empty:
            return df
        intraday_bonus = (
            df["intraday_support_signal"].eq(True).astype(int) * 8
            + df["morning_rebound"].fillna(0) * 200
        )
        df["score"] = (
            df["theme_support_score"].fillna(0) * 0.6
            + df["rs_20"].fillna(0) * 40
            + (0.10 + df["distance_to_20d_high"].fillna(-0.10)).clip(0, 0.10) * 100
            + intraday_bonus
        )
        return self._finalize(df, "mainline_low_absorption", top_n=top_n)

    def strategy_ice_repair(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        env = bundle["env"]
        prev_env = bundle["prev_env"]
        day_stock = bundle["day_stock"]
        if not (env["is_ice_point"] or env["is_repair"] or bool(prev_env.get("is_ice_point", False))):
            return pd.DataFrame()
        df = day_stock[
            day_stock["weak_to_strong_signal"].fillna(False)
            | (
                (day_stock["gap_up"].fillna(0) > 0.02)
                & (day_stock["first_5m_return"].fillna(0) > 0)
                & (day_stock["intraday_support_signal"].eq(True))
            )
        ].copy()
        if df.empty:
            return df
        df["score"] = (
            df["gap_up"].fillna(0) * 100
            + df["first_5m_return"].fillna(0) * 120
            + df["morning_rebound"].fillna(0) * 180
            + df["amount_vs_ma5"].fillna(0) * 8
            + df["theme_support_score"].fillna(20) * 0.3
        )
        return self._finalize(df, "ice_repair", top_n=top_n)

    def strategy_catchup_rotation(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        env = bundle["env"]
        day_stock = bundle["day_stock"]
        if env["emotion_phase"] not in {"rotation", "neutral", "climax"}:
            return pd.DataFrame()
        df = day_stock[
            (day_stock["theme_support_score"].fillna(0).between(40, 80))
            & (day_stock["ret_20d"].fillna(0).between(0.05, 0.50))
            & (day_stock["rs_20"].fillna(0) > 0.03)
            & (
                day_stock["is_limit_up"].fillna(False)
                | day_stock["is_new_high_20"].fillna(False)
                | day_stock["breakout_pioneer_signal"].fillna(False)
            )
            & ~day_stock["trend_core_signal"].fillna(False)
        ].copy()
        if df.empty:
            return df
        laggard_bonus = (0.50 - df["ret_20d"].fillna(0)).clip(0, 0.50) * 40
        df["score"] = (
            df["theme_support_score"].fillna(0) * 0.5
            + df["rs_5"].fillna(0) * 40
            + laggard_bonus
            + df["limit_up_streak"].fillna(0) * 6
        )
        return self._finalize(df, "catchup_rotation", top_n=top_n)

    def strategy_fast_rotation_scalp(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        env = bundle["env"]
        day_stock = bundle["day_stock"]
        elasticity = bundle["elasticity"]
        if not (env["emotion_phase"] == "rotation" or elasticity["elasticity_preference"] in {"20cm", "30cm"}):
            return pd.DataFrame()
        elastic_mask = day_stock["symbol"].str.startswith(("30", "68", "4", "8", "920"))
        df = day_stock[
            elastic_mask
            & (
                day_stock["is_limit_up"].fillna(False)
                | (day_stock["pct_chg"].fillna(0) > 8)
                | day_stock["intraday_support_signal"].eq(True)
                | (day_stock["first_5m_return"].fillna(0) > 0.02)
            )
            & (day_stock["theme_support_score"].fillna(0) >= 30)
        ].copy()
        if df.empty:
            return df
        df["score"] = (
            df["pct_chg"].fillna(0) * 2.0
            + df["first_5m_return"].fillna(0) * 100
            + df["morning_rebound"].fillna(0) * 150
            + df["theme_support_score"].fillna(0) * 0.3
            + df["bid_ask_imbalance"].fillna(1.0) * 3
        )
        return self._finalize(df, "fast_rotation_scalp", top_n=top_n)

    def strategy_mispriced_recovery(self, bundle: dict[str, Any], top_n: int = 20) -> pd.DataFrame:
        """错杀修复套利策略.

        筛选近1-2日曾连续涨停但当日被错杀至跌停/大跌的个股，
        结合盘后tick数据分析做多动作，交叉匹配热门题材，
        排除已经趋势破位（连续大阴线）的标的，
        输出次日竞价买入候选标的。
        """
        date = bundle["date"]
        day_stock = bundle["day_stock"]
        theme_snapshot = bundle["theme_snapshot"]

        if day_stock.empty:
            return pd.DataFrame()

        # ── Step 1: 识别"近期连续涨停+当日大跌"的候选 ──
        all_dates = sorted(self.daily_features["date"].unique())
        try:
            date_idx = list(all_dates).index(date)
        except ValueError:
            return pd.DataFrame()

        prev_dates = []
        if date_idx >= 1:
            prev_dates.append(all_dates[date_idx - 1])
        if date_idx >= 2:
            prev_dates.append(all_dates[date_idx - 2])

        if not prev_dates:
            return pd.DataFrame()

        # 找前1-2日有涨停的股票，并记录其连板数
        prev_data = self.daily_features[
            (self.daily_features["date"].isin(prev_dates))
        ][["symbol", "date", "is_limit_up", "limit_up_streak", "pct_chg", "close", "ma5"]].copy()

        # 取每个symbol在prev_dates中最后一天的streak
        prev_lu_data = prev_data[prev_data["is_limit_up"].fillna(False)].copy()
        if prev_lu_data.empty:
            return pd.DataFrame()

        # 对每个symbol，取最近日的连板数作为其涨停记录
        prev_lu_best = (
            prev_lu_data.sort_values("date")
            .drop_duplicates("symbol", keep="last")
            [["symbol", "limit_up_streak", "pct_chg"]]
            .rename(columns={
                "limit_up_streak": "prior_streak",
                "pct_chg": "prev_day_pct_chg",
            })
        )

        prev_lu_symbols = prev_lu_best["symbol"].unique()

        # 当日跌停或大跌（pct_chg < -7%）
        df = day_stock[
            (day_stock["symbol"].isin(prev_lu_symbols))
            & (
                day_stock["is_limit_down"].fillna(False)
                | (day_stock["pct_chg"].fillna(0) < -7.0)
            )
        ].copy()

        if df.empty:
            return pd.DataFrame()

        # 合入前日连板数据
        df = df.merge(prev_lu_best, on="symbol", how="left")

        # ── Step 1b: 走势质量过滤 ──

        # 过滤1: 要求前日至少有2连板（单日涨停跌回更像反弹失败而非错杀）
        df = df[df["prior_streak"].fillna(0) >= 2].copy()
        if df.empty:
            return pd.DataFrame()

        # 过滤2: 排除「连续下杀」——前一日已经大跌(pct < -3%)的，
        # 说明是退潮趋势而非首次错杀
        if date_idx >= 1:
            prev_date = all_dates[date_idx - 1]
            prev_day_all = self.daily_features[
                self.daily_features["date"] == prev_date
            ][["symbol", "pct_chg"]].rename(columns={"pct_chg": "d_minus_1_pct"})
            df = df.merge(prev_day_all, on="symbol", how="left")
            # 如果昨日就已经大跌(< -3%)，说明不是首次错杀
            df = df[df["d_minus_1_pct"].fillna(0) >= -3.0].copy()
            if df.empty:
                return pd.DataFrame()
        else:
            df["d_minus_1_pct"] = np.nan

        # 过滤3: MA5趋势检查——排除已经趋势破位的标的
        # 条件: 当日收盘仍在MA5上方，或次日涨8%可以突破MA5
        close_vs_ma5 = np.where(
            df["ma5"].fillna(0) > 0,
            df["close"] / df["ma5"] - 1.0,
            np.nan,
        )
        recovery_target = df["close"] * 1.08  # 次日涨8%预估价
        can_breach_ma5 = recovery_target > df["ma5"].fillna(recovery_target + 1)
        still_above_ma5 = pd.Series(close_vs_ma5, index=df.index) > -0.01  # 允许小幅偏差

        df["close_vs_ma5"] = close_vs_ma5
        df = df[still_above_ma5 | can_breach_ma5].copy()
        if df.empty:
            return pd.DataFrame()

        # ── Step 2: 分析盘后tick做多动作 ──
        ticks = load_ticks_for_date(date, self.ticks_root)
        candidate_symbols = df["symbol"].tolist()

        recovery_signals = build_mispriced_recovery_signals(
            ticks, candidate_symbols, self.daily_features, date
        )

        if not recovery_signals.empty:
            df = df.merge(recovery_signals, on="symbol", how="left")
        else:
            df["recovery_from_low"] = 0.0
            df["limit_down_open_count"] = 0
            df["volume_surge_at_low"] = 0.0
            df["close_above_low_pct"] = 0.0
            df["has_recovery_signal"] = False

        # ── Step 3: 题材交叉匹配 ──
        hot_themes = set()
        if not theme_snapshot.empty:
            hot = theme_snapshot[theme_snapshot["theme_support_score"] >= 50]
            hot_themes = set(hot["theme"].tolist())

        theme_map = self.theme_map
        matched_info: list[dict] = []
        for sym in df["symbol"].tolist():
            stock_themes = set(theme_map[theme_map["symbol"] == sym]["theme"].tolist())
            matched = stock_themes & hot_themes
            if matched and not theme_snapshot.empty:
                matched_scores = theme_snapshot[
                    theme_snapshot["theme"].isin(matched)
                ].sort_values("theme_support_score", ascending=False)
                best_score = float(matched_scores.iloc[0]["theme_support_score"]) if not matched_scores.empty else 0
                best_themes = ", ".join(list(matched)[:3])
            else:
                best_score = 0
                best_themes = ""
            matched_info.append({
                "symbol": sym,
                "matched_hot_theme_count": len(matched),
                "matched_hot_themes": best_themes,
                "matched_theme_score": best_score,
            })

        matched_df = pd.DataFrame(matched_info)
        df = df.merge(matched_df, on="symbol", how="left")

        # ── Step 4: 过滤 — 至少匹配一个热门题材 ──
        # tick做多信号在无tick数据时放宽（仅检查K线形态）
        has_ticks = not ticks.empty
        if has_ticks:
            df = df[
                (df["has_recovery_signal"].fillna(False))
                & (df["matched_hot_theme_count"].fillna(0) >= 1)
            ].copy()
        else:
            # 无tick数据时仅靠K线和题材筛选
            df = df[
                (df["matched_hot_theme_count"].fillna(0) >= 1)
            ].copy()

        if df.empty:
            return pd.DataFrame()

        # ── Step 5: 评分 ──
        df["score"] = (
            df["recovery_from_low"].fillna(0) * 200
            + df["limit_down_open_count"].fillna(0) * 15
            + df["matched_theme_score"].fillna(0) * 0.5
            + df["volume_surge_at_low"].fillna(0) * 10
            + df["close_above_low_pct"].fillna(0) * 100
            + df["prior_streak"].fillna(0) * 20            # 连板越多辨识度越高
            + np.clip(df["close_vs_ma5"].fillna(0), 0, 0.20) * 150  # 仍在MA5上方加分
        )

        # 添加额外输出列
        extra_cols = [
            "recovery_from_low", "limit_down_open_count",
            "volume_surge_at_low", "close_above_low_pct",
            "matched_hot_themes", "matched_hot_theme_count",
            "matched_theme_score",
            "prior_streak", "close_vs_ma5",
        ]
        keep = [
            "strategy", "score", "symbol", "name",
            "primary_theme", "theme_support_score",
            "pct_chg", "limit_up_streak",
            "rs_5", "rs_20",
        ] + extra_cols
        keep = [c for c in keep if c in df.columns]
        out = df.copy()
        out["strategy"] = "mispriced_recovery"
        return out.sort_values("score", ascending=False).head(top_n)[keep]

    def run(self, date: str, top_n: int = 20) -> dict[str, dict[str, Any]]:
        bundle = self._get_day_bundle(date)
        outputs = {
            "trend_revaluation": self.strategy_trend_revaluation(bundle, top_n=top_n),
            "new_mainline_breakout": self.strategy_new_mainline_breakout(bundle, top_n=top_n),
            "main_rise_resonance": self.strategy_main_rise_resonance(bundle, top_n=top_n),
            "mainline_low_absorption": self.strategy_mainline_low_absorption(bundle, top_n=top_n),
            "ice_repair": self.strategy_ice_repair(bundle, top_n=top_n),
            "catchup_rotation": self.strategy_catchup_rotation(bundle, top_n=top_n),
            "fast_rotation_scalp": self.strategy_fast_rotation_scalp(bundle, top_n=top_n),
            "mispriced_recovery": self.strategy_mispriced_recovery(bundle, top_n=top_n),
        }
        result: dict[str, dict[str, Any]] = {}
        for slug, df in outputs.items():
            result[slug] = {
                "card": asdict(STRATEGY_CARDS[slug]),
                "date": date,
                "environment": bundle["env"],
                "elasticity": bundle["elasticity"],
                "top_themes": bundle["theme_snapshot"].head(10).to_dict("records"),
                "candidates": df.to_dict("records"),
            }
        return result


def run_all_strategies(
    date: str,
    top_n: int = 20,
    *,
    klines_df: pd.DataFrame | None = None,
    basic_df: pd.DataFrame | None = None,
) -> dict[str, dict[str, Any]]:
    """Convenience entry point for one-off runs.

    If *klines_df* / *basic_df* are supplied they are forwarded to
    ``StrategyEngine`` so the heavy CSV files are not loaded again.
    """
    engine = StrategyEngine(klines_df=klines_df, basic_df=basic_df)
    return engine.run(date=date, top_n=top_n)
