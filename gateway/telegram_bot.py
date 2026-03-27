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
import urllib.parse


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

        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode())

    def send_message(self, chat_id: int, text: str, parse_mode: str = "Markdown"):
        """Send a message back to the user."""
        # Truncate if too long for Telegram (4096 char limit)
        if len(text) > 4000:
            text = text[:4000] + "\n\n... (truncated)"
        self._api_call("sendMessage", {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode,
        })

    def send_status(self, chat_id: int, status: str):
        """Send a status update (typing indicator + message)."""
        self._api_call("sendChatAction", {
            "chat_id": chat_id,
            "action": "typing",
        })

    def _parse_update(self, update: dict) -> TelegramMessage | None:
        """Parse a Telegram update into our message format."""
        msg = update.get("message", {})
        text = msg.get("text", "")
        if not text:
            return None

        user = msg.get("from", {})
        return TelegramMessage(
            chat_id=msg["chat"]["id"],
            text=text,
            user_id=user.get("id", 0),
            username=user.get("username", "unknown"),
            message_id=msg.get("message_id", 0),
        )

    async def poll(self):
        """Long-polling loop to receive messages."""
        logger.info("Telegram bot started. Waiting for messages...")
        while True:
            try:
                result = self._api_call("getUpdates", {
                    "offset": self._offset,
                    "timeout": 30,
                })
                for update in result.get("result", []):
                    self._offset = update["update_id"] + 1
                    msg = self._parse_update(update)
                    if msg and self._handler:
                        self.send_status(msg.chat_id, "typing")
                        try:
                            response = await self._handler(msg)
                            self.send_message(msg.chat_id, response)
                        except Exception as e:
                            logger.error(f"Handler error: {e}")
                            self.send_message(msg.chat_id, f"Error: {e}")
            except Exception as e:
                logger.error(f"Polling error: {e}")
                await asyncio.sleep(5)

    def run(self):
        """Start the bot (blocking)."""
        asyncio.run(self.poll())
