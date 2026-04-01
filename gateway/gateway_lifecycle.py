"""
Shared PTB lifecycle bits for panel (`telegram_bot`) and terminal (`terminal_ui`).

Env:
  GATEWAY_AUTO_RESEARCH=1 — start ``auto_research.run_experiment_loop`` in-process.
  GATEWAY_AUTO_RESEARCH_NOTIFY_CHAT_ID — optional Telegram chat id for experiment notices.
  JARVIS_QUEUE_SESSION_COMMANDER=1 — on successful dev task, append to ``jarvis_pending_commands`` (see ``session_commander.py``).
"""

from __future__ import annotations

import asyncio
import logging
import os

from telegram import BotCommand
from telegram.ext import Application

logger = logging.getLogger(__name__)

BOT_DATA_AUTO_RESEARCH_TASK_KEY = "_gw_auto_research_task"


def standard_bot_commands() -> list[BotCommand]:
    """
    菜单文案与职责一一对应：命令走 CommandHandler，绝不与纯文本语义层混用。
    描述控制在 Telegram 菜单可读长度内，并标明「非聊天语义」边界。
    """
    return [
        BotCommand(
            "start",
            "面板：持仓·引擎·刷新（非闲聊）",
        ),
        BotCommand(
            "trade",
            "指引：自然语言买卖（非 /dev）",
        ),
        BotCommand(
            "config",
            "风控·纸/实盘（非造物）",
        ),
        BotCommand(
            "help",
            "命令清单·环境变量（只读）",
        ),
        BotCommand(
            "feed",
            "专用：链接/正文情绪",
        ),
        BotCommand(
            "dev",
            "专用：/dev 写代码·因子",
        ),
    ]


async def sync_slash_command_menu(bot) -> None:
    """delete_my_commands + set_my_commands — BotFather 菜单与面板/终端一致。"""
    commands = standard_bot_commands()
    try:
        await bot.delete_my_commands()
    except Exception:
        logger.exception("delete_my_commands 失败，将继续尝试 set_my_commands")
    try:
        success = await bot.set_my_commands(commands)
        if success:
            logger.info("✅ 战术指挥菜单已强制全局同步")
        else:
            logger.warning("set_my_commands 未返回成功标志: %r", success)
    except Exception:
        logger.exception("set_my_commands 失败 — Bot 菜单描述可能未更新")


def mark_gateway_user_activity() -> None:
    """Ping auto_research idle timer（主 bot 与网关共用）。"""
    try:
        import auto_research

        auto_research.mark_user_active()
    except Exception:
        pass


def auto_research_env_enabled() -> bool:
    v = (os.environ.get("GATEWAY_AUTO_RESEARCH") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


async def send_auto_research_telegram_notify(application: Application, text: str) -> None:
    raw = (os.environ.get("GATEWAY_AUTO_RESEARCH_NOTIFY_CHAT_ID") or "").strip()
    if not raw or not raw.isdigit():
        return
    try:
        await application.bot.send_message(chat_id=int(raw), text=text[:4096])
    except Exception:
        logger.debug("GATEWAY_AUTO_RESEARCH notify failed", exc_info=True)


async def start_auto_research_background(application: Application) -> None:
    if not auto_research_env_enabled():
        return
    if application.bot_data.get(BOT_DATA_AUTO_RESEARCH_TASK_KEY):
        logger.debug("auto_research task already running")
        return
    try:
        import auto_research

        async def _notify(t: str) -> None:
            await send_auto_research_telegram_notify(application, t)

        task = application.create_task(
            auto_research.run_experiment_loop(send_status=_notify)
        )
        application.bot_data[BOT_DATA_AUTO_RESEARCH_TASK_KEY] = task
        logger.info(
            "Gateway: auto_research 后台循环已启动（GATEWAY_AUTO_RESEARCH=1；"
            "可选 GATEWAY_AUTO_RESEARCH_NOTIFY_CHAT_ID）"
        )
    except Exception:
        logger.exception("Gateway: 启动 auto_research 失败")


async def cancel_auto_research_background(application: Application) -> None:
    task = application.bot_data.pop(BOT_DATA_AUTO_RESEARCH_TASK_KEY, None)
    if task is None or task.done():
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.debug("auto_research task join", exc_info=True)
