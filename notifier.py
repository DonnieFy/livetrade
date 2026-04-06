# -*- coding: utf-8 -*-
"""
Livetrade — QQ Bot REST API 直连通知器

零外部依赖（仅 stdlib），直接调用 QQ Bot 开放平台 API 发送消息。

API 流程：
  1. POST bots.qq.com/app/getAppAccessToken → access_token（缓存，自动刷新）
  2. POST api.sgroup.qq.com/v2/users/{openid}/messages → 发送 C2C 消息

参考实现：~/.openclaw/extensions/qqbot/src/api.ts
"""

from __future__ import annotations

import json
import logging
import random
import time
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)

# QQ Bot API 端点
_TOKEN_URL = "https://bots.qq.com/app/getAppAccessToken"
_API_BASE = "https://api.sgroup.qq.com"

# Token 提前刷新余量（秒）
_TOKEN_REFRESH_AHEAD = 5 * 60


class QQBotNotifier:
    """QQ Bot C2C 消息直连通知器。

    使用 appId + clientSecret 鉴权，直接通过 HTTP REST API 发送消息。
    access_token 自动缓存并在过期前自动刷新，支持全天运行。
    """

    def __init__(
        self,
        app_id: str,
        client_secret: str,
        target_openid: str,
        *,
        max_retries: int = 1,
        request_timeout: int = 15,
    ):
        """
        参数:
            app_id: QQ Bot 应用 ID
            client_secret: QQ Bot 应用密钥
            target_openid: 目标用户 openid
            max_retries: 发送失败时重试次数
            request_timeout: HTTP 请求超时（秒）
        """
        self.app_id = app_id
        self.client_secret = client_secret
        self.target_openid = target_openid
        self.max_retries = max_retries
        self.request_timeout = request_timeout

        self._token: str | None = None
        self._token_expires_at: float = 0

    # ------------------------------------------------------------------
    # Token 管理
    # ------------------------------------------------------------------

    def _ensure_token(self) -> str:
        """确保 access_token 有效，过期前自动刷新。"""
        now = time.time()
        if self._token and now < self._token_expires_at - _TOKEN_REFRESH_AHEAD:
            return self._token

        logger.info("正在获取 QQ Bot access_token...")
        payload = json.dumps({
            "appId": self.app_id,
            "clientSecret": self.client_secret,
        }).encode("utf-8")

        req = urllib.request.Request(
            _TOKEN_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError) as e:
            logger.error(f"获取 access_token 失败: {e}")
            raise

        token = data.get("access_token")
        expires_in = int(data.get("expires_in", 7200))

        if not token:
            raise RuntimeError(f"access_token 为空: {data}")

        self._token = token
        self._token_expires_at = now + expires_in
        logger.info(
            f"access_token 已缓存，有效期 {expires_in}s，"
            f"将在 {expires_in - _TOKEN_REFRESH_AHEAD}s 后自动刷新"
        )
        return token

    # ------------------------------------------------------------------
    # 消息发送
    # ------------------------------------------------------------------

    def _gen_msg_seq(self) -> int:
        """生成消息序号（0~65535）。"""
        time_part = int(time.time() * 1000) % 100000000
        rand_part = random.randint(0, 65535)
        return (time_part ^ rand_part) % 65536

    def send(self, content: str) -> bool:
        """发送文本消息到目标用户。

        参数:
            content: 消息文本内容

        返回:
            True 发送成功，False 发送失败
        """
        if not content or not content.strip():
            logger.warning("消息内容为空，跳过发送")
            return False

        last_error: Exception | None = None

        for attempt in range(1 + self.max_retries):
            try:
                token = self._ensure_token()
                url = f"{_API_BASE}/v2/users/{self.target_openid}/messages"

                payload = json.dumps({
                    "content": content,
                    "msg_type": 0,
                    "msg_seq": self._gen_msg_seq(),
                }).encode("utf-8")

                req = urllib.request.Request(
                    url,
                    data=payload,
                    headers={
                        "Authorization": f"QQBot {token}",
                        "Content-Type": "application/json",
                    },
                    method="POST",
                )

                with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                    resp_data = json.loads(resp.read().decode("utf-8"))

                logger.debug(f"消息发送成功: {resp_data}")
                return True

            except urllib.error.HTTPError as e:
                last_error = e
                body = ""
                try:
                    body = e.read().decode("utf-8")
                except Exception:
                    pass
                logger.warning(
                    f"发送失败 (attempt {attempt + 1}): "
                    f"HTTP {e.code} — {body}"
                )

                # Token 过期(401)时清除缓存，下次重试会刷新
                if e.code == 401:
                    self._token = None
                    self._token_expires_at = 0

            except Exception as e:
                last_error = e
                logger.warning(
                    f"发送失败 (attempt {attempt + 1}): {e}"
                )

            # 重试前等一下
            if attempt < self.max_retries:
                delay = 1.0 * (attempt + 1)
                time.sleep(delay)

        logger.error(f"消息发送最终失败: {last_error}")
        return False

    def send_startup_notice(self, date_string: str) -> bool:
        """发送启动通知。"""
        return self.send(
            f"📡 Livetrade 信号监控已启动\n"
            f"📅 {date_string}\n"
            f"🔍 监听目录: output/{date_string}/\n"
            f"⏰ 将在 15:05 自动退出"
        )

    def send_shutdown_notice(self, date_string: str, total_signals: int) -> bool:
        """发送停止通知。"""
        return self.send(
            f"🔴 Livetrade 信号监控已停止\n"
            f"📅 {date_string}\n"
            f"📊 当日共转发 {total_signals} 条信号"
        )
