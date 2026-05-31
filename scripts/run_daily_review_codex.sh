#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_DIR="/home/fy/myown/livetrade"
DATABASE_DIR="/home/fy/myown/database"
KNOWLEDGE_DIR="/home/fy/myown/knowledge"
SKILL_FILE="$PROJECT_DIR/.agents/skills/daily-review/SKILL.md"
LOG_DIR="$PROJECT_DIR/logs"

export TZ="${TZ:-Asia/Shanghai}"

REVIEW_DATE="${REVIEW_DATE:-$(date +%F)}"
REVIEW_DIR="$PROJECT_DIR/review/daily/$REVIEW_DATE"
MACHINE_FILE="$REVIEW_DIR/machine.json"
ANALYST_FILE="$REVIEW_DIR/analyst.yaml"
REVIEW_FILE="$REVIEW_DIR/review.md"
LAST_MESSAGE_FILE="$LOG_DIR/daily_review_${REVIEW_DATE}.final.md"
LOG_FILE="$LOG_DIR/daily_review_${REVIEW_DATE}.log"
RUNNER_TIMEOUT="${RUNNER_TIMEOUT:-45m}"

mkdir -p "$LOG_DIR"

find_codex() {
    if [[ -n "${CODEX_BIN:-}" ]]; then
        printf '%s\n' "$CODEX_BIN"
        return
    fi

    if command -v codex >/dev/null 2>&1; then
        command -v codex
        return
    fi

    local nvm_codex="/home/fy/.nvm/versions/node/v22.19.0/bin/codex"
    if [[ -x "$nvm_codex" ]]; then
        printf '%s\n' "$nvm_codex"
        return
    fi

    return 1
}

main() {
    local started_at codex_bin
    started_at="$(date '+%F %T')"

    echo
    echo "==== livetrade daily review started at $started_at ===="
    echo "project: $PROJECT_DIR"
    echo "review_date: $REVIEW_DATE"

    if [[ ! -d "$PROJECT_DIR" ]]; then
        echo "ERROR: project directory not found: $PROJECT_DIR" >&2
        return 1
    fi

    if [[ ! -f "$SKILL_FILE" ]]; then
        echo "ERROR: skill file not found: $SKILL_FILE" >&2
        return 1
    fi

    if ! codex_bin="$(find_codex)"; then
        echo "ERROR: codex CLI not found. Set CODEX_BIN=/path/to/codex." >&2
        return 127
    fi

    if [[ ! -x "$codex_bin" ]]; then
        echo "ERROR: codex CLI is not executable: $codex_bin" >&2
        return 127
    fi

    if [[ "${SKIP_IF_REVIEW_EXISTS:-1}" == "1" && -s "$REVIEW_FILE" ]]; then
        echo "review already exists, skip: $REVIEW_FILE"
        echo "set SKIP_IF_REVIEW_EXISTS=0 to force a rerun"
        return 0
    fi

    echo "building machine facts: python3 -m review.runner --date $REVIEW_DATE"
    if command -v timeout >/dev/null 2>&1; then
        timeout "$RUNNER_TIMEOUT" python3 -m review.runner --date "$REVIEW_DATE"
    else
        python3 -m review.runner --date "$REVIEW_DATE"
    fi
    local runner_status=$?

    if (( runner_status != 0 )); then
        echo "ERROR: review.runner exited with status $runner_status" >&2
        return "$runner_status"
    fi

    if [[ ! -s "$MACHINE_FILE" || ! -s "$ANALYST_FILE" ]]; then
        echo "ERROR: review.runner did not generate required files:" >&2
        echo "  machine: $MACHINE_FILE" >&2
        echo "  analyst: $ANALYST_FILE" >&2
        return 1
    fi

    local codex_args=(
        exec
        -C "$PROJECT_DIR"
        --color never
        --ephemeral
        -c 'approval_policy="never"'
        -o "$LAST_MESSAGE_FILE"
    )

    if [[ "${CODEX_BYPASS_SANDBOX:-0}" == "1" ]]; then
        codex_args+=(--dangerously-bypass-approvals-and-sandbox)
    else
        codex_args+=(--sandbox "${CODEX_SANDBOX:-workspace-write}")
        [[ -d "$DATABASE_DIR" ]] && codex_args+=(--add-dir "$DATABASE_DIR")
        [[ -d "$KNOWLEDGE_DIR" ]] && codex_args+=(--add-dir "$KNOWLEDGE_DIR")
    fi

    if [[ -n "${CODEX_MODEL:-}" ]]; then
        codex_args+=(-m "$CODEX_MODEL")
    fi

    echo "codex: $codex_bin"
    echo "log: $LOG_FILE"
    echo "last_message: $LAST_MESSAGE_FILE"

    "$codex_bin" "${codex_args[@]}" - <<PROMPT
你正在由 systemd 定时任务非交互运行，请不要等待人工确认。

请严格执行这个 Codex skill：
$SKILL_FILE

目标：
1. 对目标交易日 $REVIEW_DATE 做 A股每日综合复盘。
2. 外层脚本已经在 $PROJECT_DIR 运行 python3 -m review.runner --date $REVIEW_DATE，并生成/更新：
   - $MACHINE_FILE
   - $ANALYST_FILE
3. 按 skill 要求读取 machine.json、industry.json、timeline.json、action 数据、策略手册，以及必要的聚类结果。
4. 更新 $PROJECT_DIR/review/daily/$REVIEW_DATE/analyst.yaml。
5. 生成 $PROJECT_DIR/review/daily/$REVIEW_DATE/review.md。

约束：
- 如果发现 $REVIEW_DATE 不是可用交易日，或当日基础数据/action 数据缺失，请明确记录原因，不要编造行情。
- 不要重复运行长时间的 review.runner；如需确认，只检查上述文件内容。
- 这是自动任务，请自行完成必要检查、命令执行、文件写入和验证。
- 不要修改与复盘无关的源码文件。
- 严禁在终端输出大段 JSON、完整数据表或长日志；任何探查命令必须把输出控制在 120 行以内。
- 如需整理大量 machine/action/industry/timeline 数据，优先用 Python 读取后只打印聚合摘要，或写入 review/daily/$REVIEW_DATE/ 下的临时摘要文件再读取。
- 不要把完整 machine.json、industry.json、timeline.json、action JSON 粘贴进对话上下文；只提取复盘所需结论。
- 如某个探索命令失败，请修正后继续，不要把失败栈和大量中间数据反复输出。
- 最终回复只需概括产物路径、是否成功、以及无法完成时的阻塞原因。
PROMPT
    local codex_status=$?

    if (( codex_status != 0 )); then
        echo "ERROR: codex exited with status $codex_status" >&2
        return "$codex_status"
    fi

    if [[ ! -s "$REVIEW_FILE" ]]; then
        echo "ERROR: review report was not generated or is empty: $REVIEW_FILE" >&2
        if [[ -s "$LAST_MESSAGE_FILE" ]]; then
            echo "last codex message:"
            sed -n '1,120p' "$LAST_MESSAGE_FILE"
        fi
        return 1
    fi

    echo "review generated: $REVIEW_FILE"
}

set +e
main 2>&1 | tee -a "$LOG_FILE"
status=${PIPESTATUS[0]}
set -e

echo "==== livetrade daily review finished with status $status at $(date '+%F %T') ====" | tee -a "$LOG_FILE"
exit "$status"
