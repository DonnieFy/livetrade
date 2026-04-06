---
name: daily-review
description: A股每日综合复盘 — 融合指标特征、题材时间线、动作库情绪及八大策略的深度复盘框架
---

# A股每日复盘 SKILL

## 触发方式

当用户提到"复盘"、"今日复盘"、"review"、"盘后分析"等关键词时执行此 SKILL。

## 前置条件

- 确认各种底层数据皆已更新（`ashares-k-lines` 基础日线数据）
- 确认资金动作库与行业时间线文件存在且已更新（涵盖 `industry.json`, `timeline.json` 及 `action/YYYY-MM-DD.json` 等）。

## 执行步骤

### Step 1: 计算并载入核心前置数据

```bash
cd /home/fy/myown/livetrade
python3 -m review.runner
```

执行后将产出当日的基础面及初步程序化盘后数据文件：
- 重点文件：`/home/fy/myown/livetrade/review/daily/YYYY-MM-DD/machine.json`
- 同目录还会自动初始化：`/home/fy/myown/livetrade/review/daily/YYYY-MM-DD/analyst.yaml`
- `machine.json` 已包含：指数状态、情绪周期、炸板/断板统计、热门板块、明星股、**九阳公社异动分析（action_analysis）**、异动趋势（action_trend）、八大策略量化结果（含错杀修复套利策略）。

### Step 2: 解析异动特征与情绪指标 (Indicator Module)

Step 1 的 `machine.json` 中已程序化读取并计算：
- **涨停数量**、**连板数量**、**最高连板高度**、**涨停股总成交金额**
- **炸板数量**、**炸板率**、**断板股列表**
- **按板块统计**的涨停与连板分布

AI Agent 应在此基础上结合 `themes.action_trend` 趋势数据进行定性分析：
  - **横向强度比较**：哪些板块涨停数激增？哪些正在衰退？
  - **整体情绪评估**：依据涨停股容纳的"成交金额"与高度，综合判断市场情绪处于冰点、修复、主升发酵，还是退潮卡位期。

### Step 3: 题材深度挖掘与时间线前瞻预测 (Theme Mining Module)

分别读取并分析以下两个重点的底层常情库：
1. **行业全景**：`/home/fy/myown/knowledge/stock-vector-knowledge/data/jiuyangongshe/industry.json`
2. **事件时间线**：`/home/fy/myown/knowledge/stock-vector-knowledge/data/jiuyangongshe/timeline.json`

将上述百科及未来催化事件内容，同今日异动爆发的方向进行交叉验证，挖掘明确以下维度的题材：
- **刚启动/最新发酵**：识别出那些在最新异动和行业异动中首次发力的题材。
- **持续性久/资金深**：找出有主线背景支撑的、容量够大且有连板核心票接力的方向。
- **预期有催化且当下发生异动**：基于 `timeline.json` 中的未来事件日期，观察哪些事件方向的板块在今日出现了先发"抢跑"异动。

### Step 4: 板块聚类与分支细读

获取 Step 1 `machine.json` 中给出的最新聚类命令参数（即 `themes.sector_clusters_hint`），随后运行向量聚类：
```bash
cd /home/fy/myown/knowledge/stock-vector-knowledge
source .venv/bin/activate
# 把那条长长的主命令粘贴至此运行
```
查阅聚类产出的"语义簇"，并将它们作为论据挂载到 Step 3 发掘出的"题材"分支上，寻找特定流向的助攻点。

### Step 5: 策略匹配与触发选股指引 (Strategy Matching)

阅读**策略操作手册**：`/home/fy/myown/livetrade/.agents/skills/daily-review/STRATEGIES.md`

调用您的分析智能，将今日的**当前市场环境状态**（结合 Step 1~4 得出的指数状况、高度压制分布、情绪阶段周期）与手册内的 8 大策略要求进行打分匹配：
- **筛选当日前排策略**：选出最适合今日盘面环境、目前已生效或适合明日开仓的 1 到 2 个核心策略。
- **定位战法对应股票池 (Watchlist)**：根据挑出的策略的《个股选择》和《买点时机》要素，在今日热点爆发标的和梯队中挑选具体的股票标的。
- **阐明盘中观察点**：根据《买后观察》梳理这几只标的明天盘中需要盯牢的核心动作。

### Step 6: 自动生成多维度战略复盘报告

更新 `analyst.yaml`，并将复盘报告输出至：`/home/fy/myown/livetrade/review/daily/YYYY-MM-DD/review.md`

更新 `analyst.yaml` 时，字段结构必须遵循示例模板：
- `/home/fy/myown/livetrade/review/templates/analyst.example.yaml`
- `/home/fy/myown/livetrade/review/schemas/analyst_schema.yaml`
- 建议先生成结构化 JSON，再通过统一入口写回：
  - `/home/fy/myown/livetrade/review/templates/analyst_update.example.json`
  - `/home/fy/myown/livetrade/review/templates/analyst_update.template.json`

必须严格遵守以下架构生成报告内容：

```markdown
# 📊 A股核心逻辑复盘 — YYYY-MM-DD

> 生成时间：YYYY-MM-DD HH:MM

---

## 一、大势状态与5日情绪节奏矩阵 📈🎭

### 大势状态
[结合大盘基础数据和 action 异动数据]
一句话定性：
1. 当前大势（震荡市/趋势市/冰点市），评价依据大盘放量还是缩量？是否站上均线？走震荡还是主推？
2. 交易场景（纯情绪博弈/机构趋势/高低切轮动），连板压制高度、连板数量和涨停股容量(成交额)传递了什么信号？情绪明确处于哪个周期阶段？
3. 5日节奏模式（如"冰点后修复→强修复→分歧→修复→分歧"）。

### 5日情绪节奏矩阵

| 日期 | 成交额 | 大面 | 大肉 | 涨停/跌停 | 炸板率 | 红盘% | 连板数 | 最高板 | 日定性 |
|------|--------|------|------|-----------|--------|-------|--------|--------|--------|
| ... | ... | ... | ... | ... | ... | ... | ... | ... | 冰点/修复/分歧/高潮 |

### 五维评估

| 维度 | 判断 | 核心依据 |
|------|------|---------|
| 大势(风控) | 偏多/中性/偏空 | 5日成交额均值、量能趋势 |
| 节奏(周期) | 上升/高潮/分歧/退潮 | 封板率、炸板率、首板溢价 |
| 板块地位(空间) | 主线确认/新主线候选/无共识 | 连板龙与当日最强板块对齐度 |
| 逻辑发展性(质量) | 硬逻辑/纯情绪/混合 | 产业催化验证、业绩支撑 |
| 个股强度(弹性) | 有核心/分散/缺乏辨识度 | 龙头辨识度、中军配合度 |

## 二、题材层：板块纵深与推演 🔥⏳
[结合 action 异动、svk 聚类、industry 及 timeline 事件库]
- **刚启动/新发酵**：首次出现在异动中的新方向
- **主线资金深潜**：多日趋势数据中持续活跃、成交额大、涨幅趋势强的板块
- **退潮方向**：5日/10日趋势明确走弱的板块（警示不碰）
- **时间线催化交叉**：timeline.json 中未来事件 vs 今日异动的先发抢跑

## 三、标的层：连板梯队深度分析 ⭐
[列出今日最亮眼的连板梯队、强成交巨无霸以及高波动个股，重点关注的“领涨极点”或“强中军”梳理。]

### 连板梯队

| 个股 | 高度 | 晋级路径 | 首封特征 | 主要归因 | 定性判断 |
|------|------|---------|---------|---------|---------|
| ... | N板 | 首板→二板→...→N板(含日期) | 几点封板/一字/换手 | 题材方向 | 情绪龙/题材龙/跟风 |

将连板股分为三类：
1. **情绪总龙**：全市场最高标，博弈纯高度空间
2. **题材连板龙**：与当日最强板块对齐，有产业逻辑支撑
3. **跟风/穿越票**：偏离主线，靠情绪穿越

### 20日涨幅靠前个股深度点评
对20日涨幅排名前列的个股逐一点评，区分：
- **真趋势核心**：20日和10日趋势都极强，不靠涨停做趋势
- **高位钝化**：20日强但近3-5日明显钝化/回落
- **快速冲上来的新启动**：仅10日强，更像是新启动或二波

## 四、策略层：战法匹配与明日剧本推演 🎯
[引入 strategies.py 八大策略的五维量化体系]
- **当前最佳匹配策略**（1-2个）
- **环境吻合度解析**：策略五维 vs 当前市场环境逐条对照
- **目标Watchlist**：表格列出标的/定位/买入逻辑
- **明日核心观察锚点**（3-5条具体可操作的盘中观察指标）

### 风险提示
列出2-3条可能导致判断失效的风险场景
```

## 九阳公社 Action 数据字段说明

> **⚠️ 关键区分：action 数据中的 `day` 和 `edition` 不等于 emotion_cycle 中的"连板"概念！**

| 字段 | 含义 | 示例 |
|------|------|------|
| `day` | **观察天数** — 该股在九阳公社异动追踪中被持续关注的天数（包含非涨停日） | `day=5` 表示已被关注5天 |
| `edition` | **期间涨停次数** — 在被关注的 `day` 天内，实际封涨停板的次数（不要求连续） | `edition=3` 表示5天内涨停了3次 |
| `num` | 人类可读标签，格式为 "X天Y板" | `"5天3板"` |

### day/edition vs 连板的区别

- **action 的 "5天3板"**：5天观察期内累计涨停3次，中间可能断开（如涨停-涨停-未涨停-未涨停-涨停）
- **emotion_cycle 的连板**：必须是连续交易日每天都涨停封板，中间不能断开

因此在写复盘报告时：
- 描述连板梯队（如"3连板"、"5连板"）应以 `emotion_cycle.consecutive_board` 为准
- action 数据的 day/edition 用于衡量一只股票的题材活跃持续度和阶段涨停频率
- 切勿将 action 的 `edition=3` 直接写成"3连板"，应写成"5天内涨停3次"或"5天3板"

## 注意事项

1. **切勿生造数据**：客观提取各种数据文件的结论。
2. **严丝合缝闭环**：策略层必须体现"环境→策略→选股→买点"的心法链路。
3. **输出干练**：字字珠玑，结论明确，可实盘直接按策略开仓为准。
4. **深度分析连板股**：不是简单列出，而是分析晋级路径、封板质量、与主线对齐度，给出分类判断。连板数据以 `emotion_cycle.consecutive_board` 为唯一权威来源。
5. **五维评估**：每次复盘必须包含五维评估表，给出当前市场的结构化定性。
6. **区分数据源语义**：action 的 day/edition 表示"观察天数/期间涨停次数"，不是连板；emotion_cycle 的 board_count 才是真正的连板数。
7. **同步更新主观层**：在生成 `review.md` 后，同步维护 `analyst.yaml` 中的 `market_regime`、`emotion_phase`、`is_ice_point`、`active_strategies`、`focus_watchlist`、`tomorrow_observation_points`、`risk_notes` 等字段，供次日实盘直接读取。
8. **优先写结构化结论**：若长文复盘与结构化字段冲突，以 `analyst.yaml` 里的结构化字段为实盘唯一准绳。
9. **不要直接手改 analyst.yaml**：请先整理为 JSON object，然后执行：

```bash
cd /home/fy/myown/livetrade
python3 -m review.analyst --date YYYY-MM-DD --from-json /tmp/analyst_update.json
```

如需整体覆盖而不是增量合并，追加 `--replace`。

## 结构化输出协议

在生成长文 `review.md` 之前，必须先产出一份 **结构化 JSON**，字段只允许来自：
- `/home/fy/myown/livetrade/review/schemas/analyst_schema.yaml`

推荐直接从这里复制骨架：
- `/home/fy/myown/livetrade/review/templates/analyst_update.template.json`
- `/home/fy/myown/livetrade/review/templates/analyst_prompt.md`

最低要求：
- `market_regime`
- `emotion_phase`
- `trend_bias`
- `is_ice_point`
- `active_strategies`
- `focus_watchlist`
- `tomorrow_observation_points`
- `risk_notes`

建议流程：

1. 先根据 `machine.json` 完成盘面分析。
2. 优先参考 `/home/fy/myown/livetrade/review/templates/analyst_prompt.md`，先输出一份完整 JSON，保存到 `/tmp/analyst_update.json`。
3. 执行：

```bash
cd /home/fy/myown/livetrade
python3 -m review.analyst --date YYYY-MM-DD --from-json /tmp/analyst_update.json
```

4. 读取写回后的 `analyst.yaml`，确认字段已生效。
5. 最后再生成 `review.md` 长文复盘。

若需要在对话中先展示结构化结论，必须先给出一个单独的 `json` fenced code block，且内容必须能直接写入 `review.analyst`。
