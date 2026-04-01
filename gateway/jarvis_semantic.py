"""
Lightweight plain-text routing for the gateway (no nested asyncio.run).

Heuristic intent when no LLM keys are configured; optional LLM can be wired later.
"""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from typing import Any

_AUTO_DEV_HINT = re.compile(
    r"(写代码|编程|实现|重构|修复\s*bug|AUTO_DEV|自动开发|代码库|加一个\s*\w+)",
    re.I,
)


def llm_backend_configured() -> bool:
    import os

    return bool(
        (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
        or (os.environ.get("OPENAI_API_KEY") or "").strip()
    )


_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)


def user_semantic_lock(uid: int) -> asyncio.Lock:
    return _locks[int(uid)]


async def classify_intent(text: str, *, uid: int) -> dict[str, Any]:
    t = (text or "").strip()
    if _AUTO_DEV_HINT.search(t):
        return {"intent": "AUTO_DEV", "extracted_requirement": t}
    if any(k in t.upper() for k in ("买", "卖", "平仓", "BUY", "SELL", "CLOSE")):
        return {"intent": "TRADE", "extracted_requirement": t}
    return {"intent": "CHAT", "extracted_requirement": t}


async def execute_trade_from_user_text(
    text: str, *, uid: int, user_mode: str
) -> tuple[bool, str]:
    return False, "请在主机器人或专用交易流程中下单；网关面板仅展示与引擎控制。"


async def chat_reply(text: str, *, uid: int) -> tuple[str, str]:
    if llm_backend_configured():
        return "", "未接入对话模型实现（仅意图路由）；请用 /dev 或看板按钮。"
    return (
        "💬 已收到。配置 ANTHROPIC_API_KEY 或 OPENAI_API_KEY 后可启用完整对话；"
        "开发类需求请用 `/dev …` 或直接描述要改的代码。",
        "",
    )
