"""
Telegram Gateway — lightweight **trading panel** entry (paper / live, positions, strategy).

Uses python-telegram-bot (same stack as bot.py). All callback routes **edit the same
message** via `edit_message_text` — no callback-driven `reply_text` spam.

Heavy chain / exchange reads run in `asyncio.create_task` after showing a loading line.

Run:  python -m gateway.telegram_bot
Env:  TELEGRAM_BOT_TOKEN (required)
      GATEWAY_TELEGRAM_USER_IDS="123,456" optional allow-list (empty = any user)
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
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
    tg_gw_render_positions_loading_text,
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


def _is_authorized(user_id: int) -> bool:
    if not _ALLOWED:
        return True
    return int(user_id) in _ALLOWED


def _session_store() -> SessionStore:
    return SessionStore()


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
    mode = store.get_telegram_panel_mode(uid)
    text = tg_gw_render_home_text(mode)
    kb = tg_gw_build_main_keyboard(mode)
    await update.message.reply_text(
        text,
        reply_markup=kb,
        parse_mode="Markdown",
    )


async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)


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
    mode = store.get_telegram_panel_mode(uid)

    await query.answer()

    if action == "mode":
        new_mode = "live" if arg == "live" else "paper"
        store.set_telegram_panel_mode(uid, new_mode)
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
        await _safe_edit(
            query,
            tg_gw_render_positions_loading_text(mode),
            tg_gw_build_positions_keyboard(),
        )

        bot = context.bot
        chat_id = query.message.chat_id
        message_id = query.message.message_id

        async def _fetch_and_refresh() -> None:
            try:
                from trading import portfolio_snapshot

                await portfolio_snapshot.refresh_once()
                snap = portfolio_snapshot.get_snapshot()
            except Exception as e:
                logger.exception("gateway positions refresh: %s", e)
                snap = None
            mode_now = _session_store().get_telegram_panel_mode(uid)
            body = tg_gw_render_positions_text(mode_now, snap)
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
    TelegramBot().run()


if __name__ == "__main__":
    main()
