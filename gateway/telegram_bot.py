"""
Telegram Gateway — PTB entry with two UI modes:

1. **panel** (default) — MarkdownV2 templates + `GW_CB:*` callbacks from `gateway.tg_panel`;
   秒回占位后 `asyncio.create_task` 拉取链上/刷新持仓，禁止在 CallbackQuery 协程内 await 耗时 IO。

2. **terminal** — Bloomberg-style state machine: `ConversationHandler`, global
   contract-address interceptor, 2s API throttle on hot refresh. See
   `gateway.terminal_ui`.

Run:   python -m gateway.telegram_bot
Env:   TELEGRAM_BOT_TOKEN (required)
       GATEWAY_UI=panel|terminal   (default: panel)
       GATEWAY_TELEGRAM_USER_IDS="123,456" optional allow-list (empty = any user)
       TERMINAL_REDIS_URL or REDIS_URL optional session mirror for terminal mode
       TG_DEV_TIMEOUT_SEC optional seconds for `/dev` Claude CLI (default: 600)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import Awaitable, Callable

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from gateway.tg_panel import (
    GW_CB,
    tg_gw_build_back_keyboard,
    tg_gw_build_main_keyboard,
    tg_gw_build_positions_keyboard,
    tg_gw_escape_v2,
    tg_gw_render_callback_pending_text,
    tg_gw_render_home_text,
    tg_gw_render_positions_text,
    tg_gw_render_strategy_text,
)
from tracker.session_store import SessionStore

logger = logging.getLogger(__name__)


@dataclass
class TelegramMessage:
    """Legacy shape kept for imports from `gateway` package."""

    chat_id: int
    text: str
    user_id: int
    username: str
    message_id: int


@dataclass
class TelegramCallbackQuery:
    """Minimal callback shape for harness code that imports from `gateway`."""

    chat_id: int
    user_id: int
    data: str
    message_id: int


def _allowed_user_ids() -> set[int]:
    raw = (os.environ.get("GATEWAY_TELEGRAM_USER_IDS") or "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part.isdigit():
            out.add(int(part))
    return out


_ALLOWED = _allowed_user_ids()

_STORE: SessionStore | None = None


def _session_store_lock(application: Application) -> asyncio.Lock:
    return application.bot_data.setdefault("_gw_session_store_init_lock", asyncio.Lock())


async def _session_store_async(application: Application) -> SessionStore:
    """Lazy SessionStore; constructor does disk IO — keep off the event loop."""
    global _STORE
    lock = _session_store_lock(application)
    async with lock:
        if _STORE is None:
            _STORE = await asyncio.to_thread(SessionStore)
        return _STORE


def _is_authorized(user_id: int) -> bool:
    if not _ALLOWED:
        return True
    return int(user_id) in _ALLOWED


def _gw_pos_refresh_lock(app: Application, chat_id: int, message_id: int) -> asyncio.Lock:
    locks: dict[tuple[int, int], asyncio.Lock] = app.bot_data.setdefault("_gw_pos_locks", {})
    key = (chat_id, message_id)
    if key not in locks:
        locks[key] = asyncio.Lock()
    return locks[key]


def _gw_portfolio_refresh_sem(application: Application) -> asyncio.Semaphore:
    return application.bot_data.setdefault("_gw_portfolio_refresh_sem", asyncio.Semaphore(1))


def _parse_gw_callback(data: str) -> tuple[str, str] | None:
    if not data or not data.startswith(f"{GW_CB}:"):
        return None
    rest = data[len(GW_CB) + 1 :]
    if rest.startswith("mode:"):
        mode = rest.split(":", 1)[1]
        return ("mode", mode)
    if rest == "home":
        return ("home", "")
    if rest == "pos":
        return ("pos", "")
    if rest == "strat":
        return ("strat", "")
    return None


async def _safe_edit(
    query,
    text: str,
    reply_markup,
    parse_mode: str = "MarkdownV2",
) -> None:
    try:
        await query.edit_message_text(
            text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
        )
    except Exception as e:
        logger.debug("edit_message_text failed: %s", e)
        try:
            await query.edit_message_text(
                text,
                reply_markup=reply_markup,
            )
        except Exception as e2:
            logger.warning("edit_message_text plain fallback failed: %s", e2)


async def _bot_edit_markdown_v2(bot, chat_id: int, message_id: int, text: str, reply_markup) -> None:
    try:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode="MarkdownV2",
        )
    except Exception:
        await bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=reply_markup,
        )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        await update.message.reply_text("⛔ 未授权使用此网关机器人。")
        return
    store = await _session_store_async(context.application)
    mode = store.get_trade_mode(uid)
    text = tg_gw_render_home_text(mode)
    kb = tg_gw_build_main_keyboard(mode)
    await update.message.reply_text(
        text,
        reply_markup=kb,
        parse_mode="MarkdownV2",
    )


async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)


_DEV_CMD = re.compile(r"^/dev(?:@\w+)?\s*(.*)$", re.IGNORECASE | re.DOTALL)


async def cmd_dev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Run local `claude -p` from repo root; report git status when finished."""
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        await update.message.reply_text("⛔ 未授权使用此网关机器人。")
        return
    raw = (update.message.text or "").strip()
    m = _DEV_CMD.match(raw)
    prompt = (m.group(1) if m else "").strip()
    if not prompt:
        await update.message.reply_text("用法：`/dev <你的开发需求>`")
        return

    await update.message.reply_text(
        "🚀 收到架构师指令，本地 Claude CLI 开发进程已唤醒，正在后台阅览全库并重构代码，请等待战报..."
    )
    bot = context.bot
    chat_id = update.message.chat_id

    async def _run_bridge() -> None:
        from pipeline.tg_dev_bridge import format_telegram_report, run_dev_prompt

        try:
            result = await run_dev_prompt(prompt)
            if result.ok and result.modified_files:
                text = "✅ 自动编程完成。您的代码库已被修改。"
            else:
                text = format_telegram_report(result)
        except Exception as e:
            logger.exception("cmd_dev bridge: %s", e)
            text = f"❌ 桥接执行异常：{e!s}"
        text = text[:4096]
        try:
            await bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            logger.warning("cmd_dev report send failed: %s", e)

    asyncio.create_task(_run_bridge())


def _gw_pending_markup(action: str, mode: str):
    """加载态下保留对应屏的键盘，避免用户卡在空白页。"""
    if action == "strat":
        return tg_gw_build_back_keyboard()
    if action == "pos":
        return tg_gw_build_positions_keyboard()
    return tg_gw_build_main_keyboard(mode)


async def _followup_positions_refresh(
    application: Application,
    bot,
    chat_id: int,
    message_id: int,
    uid: int,
) -> None:
    """Single-flight portfolio refresh; second edit when data is fresh."""
    from trading import portfolio_snapshot

    sem = _gw_portfolio_refresh_sem(application)
    store = await _session_store_async(application)
    try:
        async with sem:
            await portfolio_snapshot.refresh_once()
    except Exception:
        logger.exception("gateway positions background refresh")
    m = store.get_trade_mode(uid)
    try:
        snap = portfolio_snapshot.get_snapshot()
        await _bot_edit_markdown_v2(
            bot,
            chat_id,
            message_id,
            tg_gw_render_positions_text(m, snap, refreshing=False),
            tg_gw_build_positions_keyboard(),
        )
    except Exception as e:
        logger.warning("gateway positions follow-up edit failed: %s", e)


async def handle_gateway_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or not query.from_user or not query.message:
        return
    uid = query.from_user.id
    if not _is_authorized(uid):
        await query.answer("⛔ 未授权", show_alert=True)
        return

    parsed = _parse_gw_callback(query.data or "")
    if not parsed:
        await query.answer()
        return

    action, arg = parsed
    store = await _session_store_async(context.application)
    mode = store.get_trade_mode(uid)

    await query.answer()

    # 零延迟：先占位，禁止在本协程内 await 链上刷新 / 大模型
    await _safe_edit(
        query,
        tg_gw_render_callback_pending_text(),
        _gw_pending_markup(action, mode),
    )

    bot = context.bot
    application = context.application
    chat_id = query.message.chat_id
    message_id = query.message.message_id

    async def _gw_panel_work() -> None:
        try:
            if action == "mode":
                new_mode = "live" if arg == "live" else "paper"
                await asyncio.to_thread(store.set_trade_mode, uid, new_mode)
                m = new_mode
                await _bot_edit_markdown_v2(
                    bot,
                    chat_id,
                    message_id,
                    tg_gw_render_home_text(m),
                    tg_gw_build_main_keyboard(m),
                )
                return

            if action == "home":
                m = store.get_trade_mode(uid)
                await _bot_edit_markdown_v2(
                    bot,
                    chat_id,
                    message_id,
                    tg_gw_render_home_text(m),
                    tg_gw_build_main_keyboard(m),
                )
                return

            if action == "strat":
                m = store.get_trade_mode(uid)
                await _bot_edit_markdown_v2(
                    bot,
                    chat_id,
                    message_id,
                    tg_gw_render_strategy_text(m),
                    tg_gw_build_back_keyboard(),
                )
                return

            if action == "pos":
                lock = _gw_pos_refresh_lock(application, chat_id, message_id)
                async with lock:
                    m = store.get_trade_mode(uid)
                    from trading import portfolio_snapshot

                    snap = await asyncio.to_thread(portfolio_snapshot.get_snapshot)
                    await _bot_edit_markdown_v2(
                        bot,
                        chat_id,
                        message_id,
                        tg_gw_render_positions_text(m, snap, refreshing=True),
                        tg_gw_build_positions_keyboard(),
                    )
                asyncio.create_task(
                    _followup_positions_refresh(
                        application, bot, chat_id, message_id, uid
                    )
                )
                return
        except Exception as e:
            logger.exception("gateway panel work: %s", e)
            try:
                err_body = tg_gw_escape_v2(f"❌ 面板更新失败：{e!s}")
                await _bot_edit_markdown_v2(
                    bot,
                    chat_id,
                    message_id,
                    err_body,
                    tg_gw_build_main_keyboard(store.get_trade_mode(uid)),
                )
            except Exception as e2:
                logger.warning("gateway panel error edit failed: %s", e2)

    asyncio.create_task(_gw_panel_work())


class TelegramBot:
    """
    Compatibility wrapper: builds a PTB `Application` and runs polling.

    For the previous harness hook style (`on_message` + `poll`), use `bot.py` instead;
    this class focuses on the inline trading panel.
    """

    def __init__(self, token: str | None = None):
        self.token = token or os.environ.get("TELEGRAM_BOT_TOKEN", "")
        if not self.token:
            raise ValueError(
                "Set TELEGRAM_BOT_TOKEN or pass token=...\n"
                "Create a bot with @BotFather."
            )
        self._handler: Callable[[TelegramMessage], Awaitable[str]] | None = None
        self._offset = 0

    def on_message(self, handler: Callable[[TelegramMessage], Awaitable[str]]):
        """Reserved for API compatibility; panel mode does not use text dispatch."""
        self._handler = handler
        return handler

    def build_application(self) -> Application:
        app = (
            Application.builder()
            .token(self.token)
            .build()
        )
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("panel", cmd_panel))
        app.add_handler(CommandHandler("dev", cmd_dev))
        pat = re.compile(rf"^{re.escape(GW_CB)}:")
        app.add_handler(CallbackQueryHandler(handle_gateway_callback, pattern=pat))
        return app

    async def poll(self) -> None:
        """Run gateway panel until the asyncio task is cancelled."""
        app = self.build_application()
        await app.initialize()
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("Gateway Telegram panel polling…")
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise
        finally:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()

    def run(self) -> None:
        logging.basicConfig(
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
            level=logging.INFO,
        )
        app = self.build_application()
        app.run_polling(allowed_updates=Update.ALL_TYPES)


def main() -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        level=logging.INFO,
    )
    mode = (os.environ.get("GATEWAY_UI") or "panel").strip().lower()
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise SystemExit("TELEGRAM_BOT_TOKEN is required.")

    if mode in ("terminal", "bloomberg", "conv", "state"):
        from gateway.terminal_ui import build_terminal_application

        build_terminal_application(token).run_polling(allowed_updates=Update.ALL_TYPES)
        return

    TelegramBot(token).run()


if __name__ == "__main__":
    main()
