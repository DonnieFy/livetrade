# 盘面脉搏策略开发方案（修订版）

## 结论

Claude 的方案方向是对的，尤其是把系统目标从“选股信号”切到“盘面观察”，这一步非常关键；但如果按原方案直接做，落到当前仓库里会有几个明显问题：

1. 只改 `strategy_config.yaml` 还不够，`engine.py` 会优先读取 `analyst.yaml.active_strategies`，所以“禁用现有策略”必须同时处理配置层和复盘覆盖层。
2. 当前策略抽象是“逐只股票输出 Alert”，而 `market_pulse` 更像“按时间点输出一份盘面报告”，需要从一开始就按“快照报告”来设计状态管理和输出格式。
3. 题材归因如果只依赖 `industry.json.keyword`，命中率和稳定性都不够，应该采用“多源题材映射”方案：优先复用现有 `review/src/strategy_quant/loaders.py` 里的 `load_theme_knowledge()`，再用 `industry.json` 补充最新叙事。
4. 竞价、开盘、开盘 3 分钟、半小时轮询这几个时点都需要“只触发一次”的状态机，否则同一分钟内多帧 tick 会重复推送。
5. 你列的 9 个观察项已经很好，但还缺少几个真正能帮助早盘决策的横截面：市场温度、开盘兑现/回落、连板高标承接、大面股、昨日主线延续度。

基于这些实际情况，我建议先做一个“事实快照版”的 `market_pulse`，第一期先把 T1/T2/T3 跑通，T4 放到第二期。

## 已处理

- 已将 [strategy_config.yaml](/home/fy/myown/livetrade/strategy_config.yaml) 中当前两个启用策略改为禁用。
- 已将 [analyst.yaml](/home/fy/myown/livetrade/review/daily/2026-04-02/analyst.yaml) 中的 `active_strategies` 清空，避免复盘覆盖层继续强行启用旧策略。

## 设计目标

`market_pulse` 不是“给出买点”，而是“在关键时点给出一张高密度、可直接指导交易的盘面事实卡片”。

输出应该满足三个原则：

1. 全部基于本地数据和明确规则，不依赖主观判断。
2. 每个时点只输出一次，信息密度高但长度可控。
3. 报告既能描述市场整体，也能快速落到“哪些方向强、哪些方向弱、哪里有风险”。

## 建议的数据源优先级

### 1. 昨日分组事实

优先使用 `review/daily/{prev_date}/machine.json`：

- `stocks.limit_up`: 昨日涨停股
- `stocks.broken_board`: 昨日炸板股
- `stocks.board_breakers`: 昨日断板股
- `board_stats.consecutive_board_ladder`: 昨日连板梯队
- `themes.action_analysis.sectors`: 昨日动作最强题材
- `themes.hot_sectors`: 昨日高波动 / 涨停聚焦集合

这部分比重新临时扫日线更稳，因为复盘口径已经统一过。

### 2. 当日实时事实

优先使用 engine 每帧给到策略的：

- `frame`: 当帧全市场 tick
- `ctx.stock_snapshots`: 当前全市场快照
- `ctx.market`: 全市场统计
- `ctx.tick_history`: 开盘后回看 3 分钟等短周期计算

### 3. 题材映射

不要只依赖 `industry.json`。

建议建立统一 `ThemeResolver`，数据源按优先级合并：

1. `review/src/strategy_quant/loaders.py::load_theme_knowledge()`
2. `knowledge/stock-vector-knowledge/data/jiuyangongshe/industry.json`
3. `stock_basic.csv.gz` 提供 `symbol -> name` 基础映射

原因：

- `load_theme_knowledge()` 已经把 merged drafts 里的概念、行业、同花顺/东财题材整理成 `symbol-theme` 映射，稳定性高。
- `industry.json` 更新快，更适合补“最新叙事题材”。
- `stock_basic.csv.gz` 里的名称存在空格和 `A/B/*ST` 等展示差异，需要做名称归一化，不能直接字符串硬匹配。

## 关键口径定义

这部分建议在开发前先定死，否则后面报告数字会飘。

## 开发前建议先补的两个底层一致性问题

### 1. 涨跌停口径要和复盘模块统一

当前实盘运行链路里的 `engine.py` / `tick_parser.py` 只区分主板和创业板/科创板，但复盘模块已经额外处理了：

- 北交所 30cm
- ST 5cm
- 新股前 5 个交易日不设涨跌停

如果 `market_pulse` 要统计实时涨停数、跌停数、高标承接，这个口径不统一会直接让盘中观察和盘后复盘对不上。

建议在开发 `market_pulse` 前，先抽一个统一的 limit helper，给实盘和复盘共用。

### 2. `StockSnapshot` 最好显式补上 `volume`

当前 `engine.py` 会给 `snap.volume` 赋值，但 `StockSnapshot` 数据类里并没有显式声明这个字段。

虽然 Python 现在还能跑，但这会让：

- 类型提示不稳定
- 后续重构容易踩坑
- `market_pulse` 做横截面统计时可读性变差

建议把 `volume` 或更明确的 `amount` 字段正式放进 `StockSnapshot`。

### 溢价

统一定义：

- `auction_premium_pct = (auction_last_price / yesterday_close - 1) * 100`
- `open_premium_pct = (open_price / yesterday_close - 1) * 100`
- `t3_return_pct = (price_at_09_33 / yesterday_close - 1) * 100`

其中：

- T1 使用 `09:25:00` 的最后一帧竞价价格。
- T2 使用开盘第一帧价格。
- T3 使用 `<= 09:33:00` 的最后一帧价格。

### 竞价量能 TOP50

用 `09:25` 时刻的 `volume` 排序即可，因为竞价阶段累计成交额就是竞价成交额。

### 3 分钟涨跌幅 TOP50

用 `09:33` 时刻快照里的 `pct_chg` 排序，不必从 `09:30` 单独再算一遍相对收益，先保证和全市场口径一致。

### 题材聚合

一个股票可能对应多个题材，因此不要简单“一票一题材”。

建议：

- 先给每只股票取 `primary_theme`，用于榜单统计。
- 同时保留 `all_themes`，用于详细展开。
- `primary_theme` 选择规则：
  1. 如果该股命中昨日 `action_analysis` 热门题材，优先取该题材。
  2. 否则取 `theme_knowledge` 里得分最高的题材。
  3. 再不行才回退到 `industry.json` 命中的第一个标准化题材。

这样结果更接近“今天市场实际在怎么讲故事”，而不只是字面匹配。

## 观察维度（修订版）

你原来的 9 项全部保留，我建议扩成 14 项，其中前 9 项是第一优先级。

### A. 昨日分组今日表现

1. 昨日首板溢价
2. 昨日炸板股溢价
3. 昨日断板股溢价
4. 昨日连板梯队溢价
5. 昨日热门题材今日表现

建议每组都输出：

- 样本数
- 平均涨幅
- 中位数涨幅
- 高开数 / 平开数 / 低开数
- 最强 3 只 / 最弱 3 只

中位数很重要，因为这类样本很容易被少数极端股带偏。

### B. 当日实时结构

6. 竞价量能 TOP50 的题材聚合 top3
7. 竞价涨幅 TOP50 的题材聚合 top3
8. 开盘后 3 分钟涨幅 TOP50 的题材聚合 top3
9. 开盘后 3 分钟跌幅 TOP50 的题材聚合 top3

建议每个题材输出：

- 命中股票数
- 代表股 3 只
- 该题材样本平均涨幅

### C. 决策辅助维度

10. 市场温度计

- 红盘率
- 涨跌比
- 涨停 / 跌停 / 炸板数
- 全市场成交额
- 主板 / 创业板 / 北交所分别强弱

11. 竞价兑现偏离

- 9:25 高开很强，但 9:30 开盘明显回落的个股
- 用于识别“假高开”和骗炮型开盘

12. 高标承接观察

- 昨日最高板、次高板、核心辨识度股今天竞价 / 开盘 / 3 分钟表现
- 这个维度对你的交易指导价值很高，建议单独输出

13. 大面股监控

- 跌幅超过 5%
- 且属于昨日涨停、昨日炸板、昨日高位股

14. 主线延续度

- 昨日主线题材今天有多少只红盘
- 有多少只冲板 / 涨停
- 龙头是否继续领涨

这个维度实际上比“题材均值”更接近交易直觉。

## 时间节点建议

第一期不要一次做满全天，建议分两步。

### 第一期

- T1 `09:25`
- T2 `09:30`
- T3 `09:33`

### 第二期

- T4 `10:00 / 10:30 / 11:00 / 13:30 / 14:00 / 14:30`

原因：

- 你真正需要快速定调的时间窗口就是竞价结束到开盘前 3 分钟。
- T4 指标会牵涉涨停追踪、题材轮动、炸板回封、午后回流，复杂度明显更高。
- 先把盘前和开盘初期做准，收益最高。

## 和当前代码结构对齐的落地方案

### 推荐文件结构

```text
livetrade/
  strategies/
    market_pulse.py
  market_pulse/
    __init__.py
    timepoints.py
    theme_resolver.py
    review_snapshot.py
    calculators.py
    report_builder.py
```

### 模块职责

#### `strategies/market_pulse.py`

策略入口，只做：

- `prepare()` 里加载依赖
- `on_tick()` 里判断时间节点
- 调用计算器
- 组装成一条或多条 `Alert`

#### `market_pulse/theme_resolver.py`

负责：

- 加载 `load_theme_knowledge()`
- 解析 `industry.json`
- 读取 `stock_basic.csv.gz`
- 建立 `symbol -> themes`
- 建立标准化题材别名表

建议输出：

```python
resolve_primary_theme(symbol: str) -> str | None
resolve_themes(symbol: str) -> list[str]
aggregate_top_themes(symbols: list[str], top_n: int = 3) -> list[ThemeBucket]
```

#### `market_pulse/review_snapshot.py`

负责把 `machine.json` 里你关心的字段抽成统一对象，避免策略层到处写深层字典路径。

建议封装：

- 昨日首板列表
- 昨日炸板列表
- 昨日断板列表
- 昨日连板梯队
- 昨日热门题材及 constituent
- 昨日高标列表

#### `market_pulse/calculators.py`

放所有纯计算逻辑：

- `calc_group_premium()`
- `calc_theme_performance()`
- `top_n_by_volume()`
- `top_n_by_pct_chg()`
- `calc_market_breadth()`
- `calc_open_slippage()`
- `calc_big_face_risk()`

这里尽量做成纯函数，便于单测。

#### `market_pulse/report_builder.py`

只负责格式化，不做业务判断。

建议支持：

- `build_t1_report(...)`
- `build_t2_report(...)`
- `build_t3_report(...)`
- `split_report_lines(text, max_chars=900)`

## 需要补的一层状态管理

Claude 那版没有明确写这层，但实际很重要。

建议在 `ctx.state` 里维护：

```python
{
  "fired_timepoints": set(),
  "auction_0925_snapshot": {},
  "open_0930_snapshot": {},
  "latest_0933_snapshot": {},
}
```

用途：

- 防止同一时点重复发送
- 给 T2 计算“竞价 vs 开盘偏离”
- 给 T3 计算“开盘后 3 分钟表现”

## 输出方式建议

当前 `AlertWriter` 和 `signal_monitor.py` 已经支持多行文本，所以第一期不必重构输出链路。

建议直接让 `market_pulse` 生成 1 条“盘面报告型 Alert”：

```python
Alert(
    code="market",
    name="盘面",
    strategy_slug="market_pulse",
    strategy_name="盘面脉搏",
    message=multi_line_report,
)
```

这样改动最小，可以先跑起来。

但要注意一件事：

- QQ 消息过长时，建议按 section 拆成 2 到 3 条 Alert，而不是在 notifier 层做截断。

所以更稳的方案是：

- T1 报告拆成 `昨日溢价` + `竞价聚合/市场温度`
- T3 报告拆成 `涨跌幅聚合` + `风险与高标`

## 具体开发步骤

### Step 0. 静音旧策略

- 关闭配置层策略
- 清空复盘覆盖层 `active_strategies`

这一步已经处理。

### Step 1. 打基础设施

- 新建 `market_pulse/` 包
- 实现 `ReviewSnapshot`
- 实现 `ThemeResolver`
- 实现时间点状态机

验收标准：

- 能从当前仓库直接加载前一交易日复盘
- 能对任意股票给出主题映射
- 能正确判断 T1/T2/T3 是否已触发

### Step 2. 先做 T1 报告

先实现：

- 首板 / 炸板 / 断板 / 连板溢价
- 昨日热门题材今日竞价表现
- 竞价量能 / 涨幅 TOP50 题材聚合
- 市场温度计

验收标准：

- `09:25` 只触发一次
- 文本长度控制在 2 条 QQ 消息以内

### Step 3. 再做 T2/T3

T2：

- 竞价 vs 开盘偏离
- 假高开预警

T3：

- 涨幅 TOP50 题材聚合
- 跌幅 TOP50 题材聚合
- 高标承接
- 大面股监控
- 主线延续度

### Step 4. 补回测验证

建议新增：

- `backtest/test_market_pulse.py`
- 固定日期 snapshot 的断言

要验证的不是“收益”，而是“报告内容是否与事实一致”。

## 测试建议

### 单元测试

优先测这些纯函数：

- `ThemeResolver` 的 symbol/name 命中
- `calc_group_premium()`
- `aggregate_top_themes()`
- `calc_open_slippage()`

### 集成测试

选 2 到 3 个有代表性的交易日回放：

- 冰点日
- 修复日
- 主升日

验证：

- 时间点是否只触发一次
- T1/T2/T3 是否都能产出报告
- 报告中的样本数与均值是否可复算

## 对原方案的具体修正建议

### 1. `industry.json` 匹配方案

你的想法“`industry.json` + `stock_basic.csv.gz` 做 code-name 双向匹配”是合理的，但不应该是唯一方案。

更好的做法是：

1. 先用现成的 `load_theme_knowledge()` 做主映射。
2. 再把 `industry.json` 作为增量补丁源。
3. 名称统一做 normalize，例如去空格、全角转半角、保留 `*ST` 标记。

### 2. 输出拆分

不要按“每个观察维度单独一条”发，那样消息太碎。

建议：

- 每个时间点控制在 1 到 3 条消息
- 按主题块拆，而不是按函数拆

### 3. T4 频率

建议放到第二期再上，频率维持每 30 分钟一次即可。

如果第一期就加 T4，会把“开盘观察器”做成一个过宽的东西，反而拖慢交付。

## 最终建议

最值得优先落地的不是“把 13 个维度一次做全”，而是先把下面这 6 件事做准：

1. 昨日四类股票分组的竞价溢价
2. 昨日主线题材今天是否延续
3. 竞价量能 TOP50 在讲什么题材
4. 开盘 3 分钟最强方向和最弱方向
5. 高标今天是加强、分歧还是补跌
6. 有没有明显的大面风险和假高开

如果这 6 个点做得稳定，`market_pulse` 就已经能显著提升你每天开盘前 3 分钟的判断效率了。
