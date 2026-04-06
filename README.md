# Livetrade — A股实盘监控系统

实时监控 tick 数据变化，运行可配置的策略集，输出符合条件的股票信号；同时内置盘后复盘模块，统一管理 `machine.json` 和 `analyst.yaml`。

## 功能特性

- **增量文件监听**：轮询检测 `ashares-ticks` 的 CSV 文件变化，增量读取新数据
- **三阶段感知**：竞价（auction_open）、盘中（trading）、尾盘竞价（auction_close）
- **可扩展策略框架**：`@register_strategy` 装饰器自动注册，策略文件放入 `strategies/` 即可
- **灵活策略配置**：YAML 配置每日启用/禁用、候选股池、参数、启用阶段、时间段
- **回测模式**：使用历史 `.csv.gz` 数据按帧回放，验证策略有效性
- **复盘内置化**：盘后自动指标与次日主观判断统一收口到 `review/daily/YYYY-MM-DD/`
- **信号转发**：输出到 `output/{date}/{time}.txt`，供下游程序监控转发

## 快速开始

```bash
# 安装依赖
pip install -r requirements.txt

# 实盘运行（交易日启动）
python main.py

# 回测历史数据
python backtest.py --date 2026-03-26

# 生成复盘机器层数据
python -m review.runner --date 2026-03-31

# 将结构化主观判断合并进 analyst.yaml
python -m review.analyst --date 2026-03-31 --from-json /tmp/analyst_update.json
```

## 项目结构

```
├── config.py              # 全局配置
├── main.py                # 实盘主入口
├── backtest.py            # 回测入口
├── review/                # 复盘模块（machine.json / analyst.yaml / review.md）
├── engine.py              # 核心调度引擎
├── tick_watcher.py        # 文件监听（轮询 / 回放）
├── tick_parser.py         # CSV 解析
├── context.py             # 策略上下文（MarketContext + StrategyContext）
├── strategy_base.py       # 策略基类、Alert、注册器
├── alert_writer.py        # 信号输出
├── strategy_config.yaml   # 策略运行配置（每日更新）
├── .agents/skills/        # Claude Code / Codex 复盘 skill
├── strategies/            # 策略实现
│   ├── __init__.py        # 自动发现
│   ├── trend_breakout.py  # 产业趋势突破
│   ├── auction_strength.py # 竞价强度异动
│   └── ice_point_repair.py # 冰点修复
├── strategy_example/      # 策略参考资料
├── output/                # 信号输出目录
└── logs/                  # 日志目录
```

## 策略配置

编辑 `strategy_config.yaml` 控制默认策略行为：

```yaml
strategies:
  trend_breakout:
    enabled: true                     # 是否启用
    phases: ["trading"]               # 运行阶段
    time_range: ["09:30", "10:30"]    # 运行时间段（null=全时段）
    candidates: ["sh600000"]          # 候选股池（空=全市场）
    params:                           # 策略自定义参数
      breakout_pct: 0.02
```

## 扩展策略

在 `strategies/` 目录下新建 `.py` 文件：

```python
from strategy_base import BaseStrategy, Alert, register_strategy

@register_strategy
class MyStrategy(BaseStrategy):
    slug = "my_strategy"
    name = "我的策略"
    description = "策略描述"

    def prepare(self, ctx):
        # 启动时做重型初始化（读 K 线、查向量库等）
        pass

    def on_tick(self, frame, ctx):
        # 每帧调用，返回 Alert 列表
        return []
```

然后在 `strategy_config.yaml` 中添加配置即可。

## 复盘数据

每日复盘数据统一放在 `review/daily/YYYY-MM-DD/`：

- `machine.json`：程序自动计算的客观事实，包含涨停/跌停、炸板/断板、连板梯队、市场成交额、热门板块、量化候选等
- `analyst.yaml`：你或模型维护的主观判断，包含市场状态、情绪周期、次日主看题材、启用策略、watchlist、风险提示等
- `review.md`：盘后长文复盘报告

`engine.py` 启动时会优先读取最近一份可用复盘数据，将其注入策略上下文。`analyst.yaml` 中的 `active_strategies` 和 `manual_overrides` 会覆盖默认配置。

主观层推荐工作流：

1. 先参考 [analyst_update.template.json](/home/fy/myown/livetrade/review/templates/analyst_update.template.json) 生成结构化 JSON
2. 再执行 `python -m review.analyst --date YYYY-MM-DD --from-json /tmp/analyst_update.json`
3. 最后生成 `review.md`

```
[产业趋势突破] sh600000 浦发银行 | 突破20日新高 15.20→15.60, 涨幅2.63%
[竞价强度异动] sz300750 宁德时代 | 昨日涨停今日高开3.5%, 买盘积极(比2.1)
```

## 信号监控服务

基于 Linux 内核 `inotify` 的轻量级文件监控，当策略引擎输出信号文件时，通过 **QQ Bot REST API** 实时推送到你的 QQ。

**核心特性：**
- 0 token 运行成本（纯系统调用 + HTTP REST API）
- Linux 内核级 inotify 事件驱动（非轮询）
- QQ Bot API 直连（~100ms 延迟），不依赖 OpenClaw Gateway
- access_token 自动缓存与刷新，支持全天运行
- systemd timer 周一至周五 09:15 自动启动，15:05 自动退出

### 快速使用

```bash
# 1. 配置 QQ Bot 凭证
cp .env.example .env
nano .env  # 填写 QQBOT_CLIENT_SECRET 和 QQBOT_TARGET_OPENID

# 2. 安装依赖
pip install inotify_simple

# 3. 测试通知通道
python signal_monitor.py --test

# 4. 手动启动监控
python signal_monitor.py

# 5. 一键安装 systemd 定时任务（可选）
bash scripts/install_monitor.sh
```

### 相关文件

```
├── signal_monitor.py          # inotify 监控主脚本
├── notifier.py                # QQ Bot REST API 直连客户端
├── .env                       # QQ Bot 凭证（不入 git）
├── .env.example               # 凭证模板
├── systemd/                   # systemd 服务单元
│   ├── livetrade-signal-monitor.service
│   └── livetrade-signal-monitor.timer
└── scripts/
    └── install_monitor.sh     # 一键部署脚本
```
