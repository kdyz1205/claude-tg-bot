"""
Telegram Gateway — PTB entry with two UI modes:

1. **panel** (default) — dashboard-backed trading home / positions / strategy
   (`GW_CB:*` callbacks, in-place `edit_message_text`).

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
import threading
from dataclasses import dataclass
from typing import Callable, Awaitable

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from dashboard import (
    GW_CB,
    tg_gw_build_back_keyboard,
    tg_gw_build_main_keyboard,
    tg_gw_build_positions_keyboard,
    tg_gw_render_home_text,
    tg_gw_render_positions_stale_with_refresh_banner,
    tg_gw_render_positions_text,
    tg_gw_render_strategy_text,
    tg_gw_sanitize_for_markdown,
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
_STORE_INIT_LOCK = threading.Lock()


def _is_authorized(user_id: int) -> bool:
    if not _ALLOWED:
        return True
    return int(user_id) in _ALLOWED


def _session_store() -> SessionStore:
    """Single process-wide store so prefs are not re-read from disk on every tap."""
    global _STORE
    with _STORE_INIT_LOCK:
        if _STORE is None:
            _STORE = SessionStore()
        return _STORE


def _gw_pos_refresh_lock(app: Application, chat_id: int, message_id: int) -> asyncio.Lock:
    locks: dict[tuple[int, int], asyncio.Lock] = app.bot_data.setdefault("_gw_pos_locks", {})
    key = (chat_id, message_id)
    if key not in locks:
        locks[key] = asyncio.Lock()
    return locks[key]


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
    parse_mode: str = "Markdown",
) -> None:
    if parse_mode == "Markdown":
        text = tg_gw_sanitize_for_markdown(text)
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


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        await update.message.reply_text("⛔ 未授权使用此网关机器人。")
        return
    store = _session_store()
    mode = store.get_trade_mode(uid)
    text = tg_gw_sanitize_for_markdown(tg_gw_render_home_text(mode))
    kb = tg_gw_build_main_keyboard(mode)
    await update.message.reply_text(
        text,
        reply_markup=kb,
        parse_mode="Markdown",
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
    store = _session_store()
    mode = store.get_trade_mode(uid)

    await query.answer()

    if action == "mode":
        new_mode = "live" if arg == "live" else "paper"
        store.set_trade_mode(uid, new_mode)
        mode = new_mode
        await _safe_edit(
            query,
            tg_gw_render_home_text(mode),
            tg_gw_build_main_keyboard(mode),
        )
        return

    if action == "home":
        await _safe_edit(
            query,
            tg_gw_render_home_text(mode),
            tg_gw_build_main_keyboard(mode),
        )
        return

    if action == "strat":
        await _safe_edit(
            query,
            tg_gw_render_strategy_text(mode),
            tg_gw_build_back_keyboard(),
        )
        return

    if action == "pos":
        try:
            from trading import portfolio_snapshot

            snap_now = portfolio_snapshot.get_snapshot()
        except Exception:
            snap_now = None

        await _safe_edit(
            query,
            tg_gw_render_positions_stale_with_refresh_banner(mode, snap_now),
            tg_gw_build_positions_keyboard(),
        )

        bot = context.bot
        application = context.application
        chat_id = query.message.chat_id
        message_id = query.message.message_id

        async def _fetch_and_refresh() -> None:
            lock = _gw_pos_refresh_lock(application, chat_id, message_id)
            async with lock:
                try:
                    from trading import portfolio_snapshot

                    await portfolio_snapshot.refresh_once()
                    snap = portfolio_snapshot.get_snapshot()
                except Exception as e:
                    logger.exception("gateway positions refresh: %s", e)
                    snap = None
                mode_now = _session_store().get_trade_mode(uid)
                body = tg_gw_sanitize_for_markdown(
                    tg_gw_render_positions_text(mode_now, snap)
                )
                try:
                    await bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=body,
                        reply_markup=tg_gw_build_positions_keyboard(),
                        parse_mode="Markdown",
                    )
                except Exception:
                    try:
                        await bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=body,
                            reply_markup=tg_gw_build_positions_keyboard(),
                        )
                    except Exception as e2:
                        logger.warning("background edit positions failed: %s", e2)

        asyncio.create_task(_fetch_and_refresh())
        return


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
