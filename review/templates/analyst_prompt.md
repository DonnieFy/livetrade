# Analyst JSON Prompt

你现在要为 A 股复盘系统产出一份 **结构化主观结论 JSON**，供下面这个命令直接写入：

```bash
cd /home/fy/myown/livetrade
python3 -m review.analyst --date YYYY-MM-DD --from-json /tmp/analyst_update.json
```

## 输入材料

你会拿到：
- `machine.json`
- `analyst_schema.yaml`
- `analyst_update.template.json`
- 可选的 `review.md` 草稿或额外盘面说明

## 输出要求

1. 只输出 **一个** `json` fenced code block。
2. JSON 顶层必须是 object。
3. 字段只允许来自 `analyst_schema.yaml`。
4. 不要输出解释、分析过程、注释、额外文字。
5. 没把握的字段宁可留空字符串、空数组或 `null`，不要编造。
6. `active_strategies` 必须按优先级排序。
7. `focus_watchlist` 里的 `strategy` 必须对应实盘策略 slug：
   - `trend_breakout`
   - `auction_strength`
   - `ice_point_repair`
   - `auction_limit_chase`

## 枚举约束

- `market_regime`: `trend_up` / `trend_down` / `range` / `range_weak` / `range_strong` / `""`
- `emotion_phase`: `ice` / `repair` / `warming` / `hot` / `cooling` / `""`
- `trend_bias`: `bullish` / `neutral` / `bearish` / `""`
- `main_themes[].stance` / `secondary_themes[].stance`: `main` / `watch` / `avoid` / `""`

## 输出模板

直接按这个结构填：

```json
{
  "market_regime": "",
  "emotion_phase": "",
  "trend_bias": "",
  "is_ice_point": null,
  "main_themes": [],
  "secondary_themes": [],
  "avoid_themes": [],
  "active_strategies": [],
  "focus_watchlist": [],
  "tomorrow_observation_points": [],
  "risk_notes": [],
  "manual_overrides": {
    "strategy_candidates": {},
    "strategy_excludes": {}
  }
}
```

## 填写建议

- `market_regime`、`emotion_phase`、`trend_bias` 要尽量稳定、可执行。
- `active_strategies` 只保留 1 到 3 个最主要策略。
- `focus_watchlist` 只放次日真正要盯的核心标的，不要泛化。
- 若明确不做某策略，可在 `manual_overrides.strategy_excludes` 中写：

```json
{
  "ice_point_repair": ["all"]
}
```

- 若要手工指定候选池，可在 `manual_overrides.strategy_candidates` 中写：

```json
{
  "trend_breakout": ["002082", "600488"]
}
```
