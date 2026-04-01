"""
MOE 语义路由：TRADE / 复杂意图不阻塞 PTB 更新循环。

- TRADE：立即回复辩论提示，``asyncio.create_task`` 后台执行交易语义链，结束用 ``send_message`` 回传。
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from telegram.ext.filters import MessageFilter

if TYPE_CHECKING:
    from telegram import Bot

logger = logging.getLogger(__name__)

MOE_DEBATE_ACK = (
    "🧠 智囊团（保守/激进/风控）正在紧急辩论该指令..."
)


def split_command_line(text: str) -> tuple[str, list[str]]:
    parts = (text or "").strip().split()
    if not parts:
        return "", []
    head = parts[0]
    cmd = head.split("@", 1)[0].lower() if head.startswith("/") else head.lower()
    if cmd and not cmd.startswith("/"):
        cmd = "/" + cmd
    return cmd, parts[1:]


async def run_trade_moe_background(
    bot: Bot,
    chat_id: int,
    text: str,
    *,
    uid: int,
    user_mode: str,
) -> None:
    """后台跑完整交易语义链（当前即 execute_trade_from_user_text）。"""
    from gateway.jarvis_semantic import execute_trade_from_user_text

    try:
        _ok, msg = await execute_trade_from_user_text(
            text, uid=uid, user_mode=user_mode
        )
        await bot.send_message(chat_id=chat_id, text=(msg or "")[:4096])
    except Exception as e:
        logger.exception("MOE trade background: %s", e)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"❌ 智囊团合议异常：{e!s}"[:4096],
            )
        except Exception:
            pass


def schedule_trade_moe_nonblocking(
    application: Any,
    bot: Bot,
    chat_id: int,
    text: str,
    *,
    uid: int,
    user_mode: str,
) -> None:
    application.create_task(
        run_trade_moe_background(
            bot, chat_id, text, uid=uid, user_mode=user_mode
        ),
        name="moe_trade_gate",
    )


class _NonStartTradeSlashFilter(MessageFilter):
    """斜杠命令且非 /start、/trade、/t（已由 CommandHandler 处理）。"""

    __slots__ = ()

    def filter(self, message):  # type: ignore[override]
        if not message or not message.text:
            return False
        if not message.text.strip().startswith("/"):
            return False
        cmd, _ = split_command_line(message.text)
        return cmd not in ("/start", "/trade", "/t")


NON_START_TRADE_SLASH = _NonStartTradeSlashFilter()
