"""
Telegram Gateway — the entry point for the entire harness.

You send a message to Telegram → bot receives it → dispatches to harness.

Setup:
    1. Create a bot via @BotFather on Telegram
    2. Set TELEGRAM_BOT_TOKEN env var
    3. Run: python -m gateway.telegram_bot

Usage:
    Send to your bot:  "帮我写一个 React 登录页面"
    Bot auto-dispatches to the right AI based on task difficulty.

Auto code (full bot): use /dev in bot.py — see pipeline/auto_dev_orchestrator.py.
"""

from __future__ import annotations

import os
import json
import asyncio
import logging
from dataclasses import dataclass
from typing import Callable, Awaitable

logger = logging.getLogger(__name__)

# Lightweight Telegram Bot — no heavy dependencies, just urllib
import urllib.request
import urllib.error


@dataclass
class TelegramMessage:
    chat_id: int
    text: str
    user_id: int
    username: str
    message_id: int


class TelegramBot:
    """
    Minimal Telegram bot that receives messages and routes them to the harness.
    No external dependencies — uses urllib only.
    """

    def __init__(self, token: str | None = None):
        self.token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        if not self.token:
            raise ValueError(
                "Set TELEGRAM_BOT_TOKEN env var or pass token directly.\n"
                "Get one from @BotFather on Telegram."
            )
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self._handler: Callable[[TelegramMessage], Awaitable[str]] | None = None
        self._offset = 0

    def on_message(self, handler: Callable[[TelegramMessage], Awaitable[str]]):
        """Register the message handler. Should return response text."""
        self._handler = handler
        return handler

    def _api_call(self, method: str, params: dict = None) -> dict:
        """Call Telegram Bot API."""
        url = f"{self.base_url}/{method}"
        if params:
            data = json.dumps(params).encode()
            req = urllib.request.Request(
                url, data=data,
                headers={"Content-Type": "application/json"},
            )
        else:
            req = urllib.request.Request(url)

        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                body = resp.read().decode()
                return json.loads(body)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 30))
                logger.warning(f"Telegram API rate limited, waiting {retry_after}s...")
                import time
                time.sleep(min(retry_after, 120))
                # Retry once
                try:
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return json.loads(resp.read().decode())
                except Exception:
                    return {"ok": False, "description": "retry failed"}
            raise

    def send_message(self, chat_id: int, text: str, parse_mode: str = "Markdown"):
        """Send a message back to the user."""
        if not text:
            text = "(empty response)"
        # Truncate if too long for Telegram (4096 char limit)
        if len(text) > 4000:
            text = text[:4000] + "\n\n... (truncated)"
        try:
            self._api_call("sendMessage", {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode,
            })
        except Exception:
            # Markdown parsing can fail on arbitrary text; retry without parse_mode
            try:
                self._api_call("sendMessage", {
                    "chat_id": chat_id,
                    "text": text,
                })
            except Exception as e:
                logger.error(f"Failed to send message to {chat_id}: {e}")

    def send_status(self, chat_id: int, status: str):
        """Send a status update (typing indicator + optional message)."""
        try:
            self._api_call("sendChatAction", {
                "chat_id": chat_id,
                "action": "typing",
            })
        except Exception as e:
            logger.debug(f"Failed to send typing indicator: {e}")
        if status and status != "typing":
            self.send_message(chat_id, status)

    def _parse_update(self, update: dict) -> TelegramMessage | None:
        """Parse a Telegram update into our message format."""
        msg = update.get("message", {})
        text = msg.get("text", "")
        if not text:
            return None

        user = msg.get("from", {})
        chat = msg.get("chat")
        if not chat or "id" not in chat:
            return None
        # Only handle private chats; ignore groups/channels
        chat_type = chat.get("type", "private")
        if chat_type not in ("private",):
            logger.debug(f"Ignoring message from {chat_type} chat {chat.get('id')}")
            return None
        return TelegramMessage(
            chat_id=chat["id"],
            text=text,
            user_id=user.get("id", 0),
            username=user.get("username", "unknown"),
            message_id=msg.get("message_id", 0),
        )

    async def _async_api_call(self, method: str, params: dict = None) -> dict:
        """Non-blocking wrapper around _api_call for use in async context."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._api_call, method, params)

    async def poll(self):
        """Long-polling loop to receive messages."""
        logger.info("Telegram bot started. Waiting for messages...")
        while True:
            try:
                result = await self._async_api_call("getUpdates", {
                    "offset": self._offset,
                    "timeout": 30,
                })
                for update in result.get("result", []):
                    uid = update.get("update_id")
                    if uid is None:
                        continue
                    self._offset = uid + 1
                    msg = self._parse_update(update)
                    if msg and self._handler:
                        # send_status uses blocking urllib — run in executor
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, self.send_status, msg.chat_id, "typing")
                        try:
                            response = await self._handler(msg)
                            await loop.run_in_executor(None, self.send_message, msg.chat_id, response)
                        except Exception as e:
                            logger.error(f"Handler error: {e}")
                            await loop.run_in_executor(None, self.send_message, msg.chat_id, f"Error: {str(e)[:300]}")
            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(5)

    def run(self):
        """Start the bot (blocking)."""
        asyncio.run(self.poll())
