#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
USER_SYSTEMD_DIR="$HOME/.config/systemd/user"

SERVICE_NAME="livetrade-daily-review.service"
TIMER_NAME="livetrade-daily-review.timer"

echo "Installing livetrade daily review timer..."

if [[ ! -f "$PROJECT_DIR/.agents/skills/daily-review/SKILL.md" ]]; then
    echo "ERROR: daily-review skill file not found." >&2
    exit 1
fi

if [[ ! -x "$PROJECT_DIR/scripts/run_daily_review_codex.sh" ]]; then
    chmod +x "$PROJECT_DIR/scripts/run_daily_review_codex.sh"
fi

mkdir -p "$USER_SYSTEMD_DIR"
cp "$PROJECT_DIR/systemd/$SERVICE_NAME" "$USER_SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/$TIMER_NAME" "$USER_SYSTEMD_DIR/"

systemctl --user daemon-reload
systemctl --user enable --now "$TIMER_NAME"

echo
echo "Installed."
echo
echo "Timer status:"
systemctl --user status "$TIMER_NAME" --no-pager
echo
echo "Useful commands:"
echo "  systemctl --user list-timers '$TIMER_NAME'"
echo "  systemctl --user start '$SERVICE_NAME'"
echo "  journalctl --user -u '$SERVICE_NAME' -f"
echo "  tail -f '$PROJECT_DIR/logs/daily_review_$(date +%F).log'"
echo
echo "If this machine must run the timer while you are logged out, enable linger once:"
echo "  sudo loginctl enable-linger $USER"
