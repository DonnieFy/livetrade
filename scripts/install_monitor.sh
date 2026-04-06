#!/bin/bash
# Livetrade 信号监控服务 — 一键安装脚本
#
# 用法: bash scripts/install_monitor.sh
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "📦 安装 Python 依赖..."
pip install inotify_simple

echo ""
echo "📋 检查 .env 配置..."
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "⚠️  未找到 .env 文件"
    echo "   请复制 .env.example 并填写实际配置:"
    echo "   cp .env.example .env && nano .env"
    exit 1
fi
echo "✅ .env 已存在"

echo ""
echo "🔧 安装 systemd 用户级服务..."
mkdir -p ~/.config/systemd/user/
cp "$PROJECT_DIR/systemd/livetrade-signal-monitor.service" ~/.config/systemd/user/
cp "$PROJECT_DIR/systemd/livetrade-signal-monitor.timer" ~/.config/systemd/user/

systemctl --user daemon-reload
systemctl --user enable livetrade-signal-monitor.timer
systemctl --user start livetrade-signal-monitor.timer

echo ""
echo "✅ 安装完成！"
echo ""
echo "📊 定时器状态:"
systemctl --user status livetrade-signal-monitor.timer --no-pager
echo ""
echo "🧪 测试通知通道:"
echo "   python3 signal_monitor.py --test"
echo ""
echo "🔍 手动启动监控:"
echo "   python3 signal_monitor.py"
echo ""
echo "📋 查看日志:"
echo "   journalctl --user -u livetrade-signal-monitor.service -f"
