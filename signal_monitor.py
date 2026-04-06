# -*- coding: utf-8 -*-
"""
Livetrade — inotify 信号文件监控服务

利用 Linux 内核 inotify 实时监听策略信号输出文件，
新文件产生时通过 QQ Bot REST API 直连推送。

用法:
    python signal_monitor.py              # 监控今日信号
    python signal_monitor.py --test       # 发送测试消息后退出
    python signal_monitor.py --date 2026-04-07  # 指定日期
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path

# 将项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

import config
from notifier import QQBotNotifier

# inotify_simple 延迟导入，便于给出友好提示
try:
    from inotify_simple import INotify, flags as iflags
except ImportError:
    print(
        "错误: 缺少 inotify_simple 模块\n"
        "请执行: pip install inotify_simple",
        file=sys.stderr,
    )
    sys.exit(1)

logger = logging.getLogger("signal_monitor")


# ======================================================================
# .env 文件加载（零依赖实现，不需要 python-dotenv）
# ======================================================================

def load_dotenv(env_path: Path) -> None:
    """加载 .env 文件到 os.environ（仅补充，不覆盖已有变量）。"""
    if not env_path.is_file():
        return
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # 不覆盖已有环境变量
            if key and key not in os.environ:
                os.environ[key] = value


# ======================================================================
# 日志初始化
# ======================================================================

def setup_logging(date_string: str) -> None:
    """初始化日志：文件 + 控制台。"""
    os.makedirs(config.LOG_DIR, exist_ok=True)

    log_file = os.path.join(config.LOG_DIR, f"signal_monitor_{date_string}.log")

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # 避免重复添加 handler
    if root.handlers:
        return

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(
        logging.Formatter(config.LOG_FORMAT, datefmt=config.LOG_DATE_FORMAT)
    )

    root.addHandler(fh)
    root.addHandler(ch)


# ======================================================================
# 信号监控核心
# ======================================================================

class SignalMonitor:
    """基于 inotify 的信号文件监控器。"""

    def __init__(
        self,
        date_string: str,
        notifier: QQBotNotifier,
        stop_time: str = "15:05",
    ):
        self.date_string = date_string
        self.notifier = notifier
        self.stop_time = stop_time
        self.output_base = Path(config.ALERT_OUTPUT_DIR)
        self.target_dir = self.output_base / date_string
        self.processed: set[str] = set()
        self.total_signals = 0
        self._running = True
        self._inotify: INotify | None = None

        # 注册信号处理
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

    def _handle_signal(self, signum: int, _frame) -> None:
        sig_name = signal.Signals(signum).name
        logger.info(f"收到 {sig_name}，正在停止...")
        self._running = False

    def _should_stop(self) -> bool:
        """检查是否到达停止时间。"""
        if not self._running:
            return True
        now = datetime.now().strftime("%H:%M")
        return now >= self.stop_time

    def _process_signal_file(self, filepath: Path) -> None:
        """读取并转发一个信号文件。"""
        filename = filepath.name

        if filename in self.processed:
            return

        if not filename.endswith(".txt"):
            return

        self.processed.add(filename)

        try:
            content = filepath.read_text(encoding="utf-8").strip()
        except Exception as e:
            logger.error(f"读取信号文件失败 {filepath}: {e}")
            return

        if not content:
            logger.debug(f"空信号文件，跳过: {filename}")
            return

        # 构造通知消息
        time_str = filename.replace("_", ":").replace(".txt", "")
        message = (
            f"📢 策略信号 [{self.date_string} {time_str}]\n"
            f"{'─' * 30}\n"
            f"{content}"
        )

        logger.info(f"发现信号文件: {filename}")
        success = self.notifier.send(message)

        if success:
            self.total_signals += 1
            logger.info(f"✅ 信号已推送: {filename} (累计 {self.total_signals})")
        else:
            logger.error(f"❌ 信号推送失败: {filename}")

    def _scan_existing_files(self) -> None:
        """扫描目录中已存在的文件（处理启动前已产生的信号）。"""
        if not self.target_dir.is_dir():
            return

        existing = sorted(self.target_dir.glob("*.txt"))
        if existing:
            logger.info(f"发现 {len(existing)} 个已存在的信号文件，开始处理...")
            for f in existing:
                self._process_signal_file(f)

    def _wait_for_directory(self, ino: INotify) -> bool:
        """等待今日信号目录创建。

        如果目录已存在则立即返回 True。
        否则监听 output/ 目录的子目录创建事件。
        """
        if self.target_dir.is_dir():
            logger.info(f"信号目录已存在: {self.target_dir}")
            return True

        logger.info(f"信号目录尚未创建，等待中: {self.target_dir}")

        # 确保 output/ 目录存在
        self.output_base.mkdir(parents=True, exist_ok=True)

        # 监听 output/ 的子目录创建
        parent_wd = ino.add_watch(
            str(self.output_base),
            iflags.CREATE | iflags.ISDIR,
        )

        while not self._should_stop():
            # 二次检查（可能在 add_watch 前创建了）
            if self.target_dir.is_dir():
                logger.info(f"信号目录已出现: {self.target_dir}")
                ino.rm_watch(parent_wd)
                return True

            events = ino.read(timeout=1000)  # 1 秒超时
            for event in events:
                if event.name == self.date_string:
                    logger.info(f"信号目录已创建: {self.target_dir}")
                    ino.rm_watch(parent_wd)
                    return True

        ino.rm_watch(parent_wd)
        return False

    def run(self) -> None:
        """主运行循环。"""
        logger.info("=" * 60)
        logger.info(f"信号监控启动 — {self.date_string}")
        logger.info(f"监听目录: {self.target_dir}")
        logger.info(f"停止时间: {self.stop_time}")
        logger.info("=" * 60)

        # 发送启动通知
        self.notifier.send_startup_notice(self.date_string)

        ino = INotify()
        self._inotify = ino

        try:
            # 1. 等待今日目录就绪
            if not self._wait_for_directory(ino):
                logger.info("到达停止时间，且信号目录未出现，退出")
                return

            # 2. 扫描已存在的文件
            self._scan_existing_files()

            # 3. 监听新文件
            watch_wd = ino.add_watch(
                str(self.target_dir),
                iflags.CLOSE_WRITE,
            )
            logger.info("inotify 监听已就绪，等待信号文件...")

            while not self._should_stop():
                events = ino.read(timeout=1000)
                for event in events:
                    if event.name and event.name.endswith(".txt"):
                        filepath = self.target_dir / event.name
                        # 短暂等待确保文件写入完毕
                        time.sleep(0.05)
                        self._process_signal_file(filepath)

            ino.rm_watch(watch_wd)

        except Exception as e:
            logger.error(f"监控异常: {e}", exc_info=True)
        finally:
            ino.close()
            self._inotify = None

        # 发送停止通知
        self.notifier.send_shutdown_notice(self.date_string, self.total_signals)

        logger.info("=" * 60)
        logger.info(
            f"信号监控结束 — 当日共转发 {self.total_signals} 条信号"
        )
        logger.info("=" * 60)


# ======================================================================
# CLI 入口
# ======================================================================

def create_notifier() -> QQBotNotifier:
    """从环境变量创建 QQBotNotifier。"""
    app_id = os.environ.get("QQBOT_APP_ID", "")
    client_secret = os.environ.get("QQBOT_CLIENT_SECRET", "")
    target_openid = os.environ.get("QQBOT_TARGET_OPENID", "")

    if not all([app_id, client_secret, target_openid]):
        print(
            "错误: 缺少 QQ Bot 配置\n"
            "请创建 .env 文件（参考 .env.example）并设置:\n"
            "  QQBOT_APP_ID=...\n"
            "  QQBOT_CLIENT_SECRET=...\n"
            "  QQBOT_TARGET_OPENID=...",
            file=sys.stderr,
        )
        sys.exit(1)

    return QQBotNotifier(
        app_id=app_id,
        client_secret=client_secret,
        target_openid=target_openid,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Livetrade 信号文件监控服务（inotify + QQ Bot）"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="指定监控日期（YYYY-MM-DD），默认今日",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="发送测试消息后退出",
    )
    parser.add_argument(
        "--stop-time",
        type=str,
        default=config.SIGNAL_MONITOR_STOP_TIME,
        help="监控停止时间（HH:MM），默认 15:05",
    )
    args = parser.parse_args()

    date_string = args.date or datetime.now().strftime("%Y-%m-%d")

    # 加载 .env
    load_dotenv(PROJECT_ROOT / ".env")

    # 初始化日志
    setup_logging(date_string)

    # 创建通知器
    notifier = create_notifier()

    # 测试模式
    if args.test:
        logger.info("测试模式：发送测试消息...")
        success = notifier.send(
            f"🧪 测试消息\n"
            f"📅 {date_string}\n"
            f"✅ QQ Bot 通知通道正常"
        )
        if success:
            logger.info("✅ 测试消息发送成功")
        else:
            logger.error("❌ 测试消息发送失败")
            sys.exit(1)
        return

    # 正式监控
    monitor = SignalMonitor(
        date_string=date_string,
        notifier=notifier,
        stop_time=args.stop_time,
    )
    monitor.run()


if __name__ == "__main__":
    main()
