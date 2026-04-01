"""
Telegram Gateway — PTB entry with two UI modes:

1. **panel** (default) — MarkdownV2 + ``gw:*`` callbacks; **zero-delay** callback path:
   ``answer`` → plain ``⏳ 正在切换…`` (keep keyboard) → ``asyncio.create_task`` for all IO.

2. **terminal** — Bloomberg-style state machine. See ``gateway.terminal_ui``.

Run:   python -m gateway.telegram_bot

Command taxonomy for the **full** bot (bindings, /help, Telegram menu) lives in
``tg_registry`` at repo root; this gateway exposes a slim /start|/panel|/dev surface only.
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

from gateway.tg_front import (
    GW_CB,
    build_back_keyboard,
    build_main_keyboard,
    build_positions_keyboard,
    escape_v2,
    render_home_text,
    render_positions_text,
    render_strategy_text,
)
from tracker.session_store import SessionStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Single in-memory mode for the gateway panel (single-operator; instant UI).
# Persisted to SessionStore in background on change. Hydrated from disk on /start.
# ---------------------------------------------------------------------------
USER_MODE: str = "paper"
GOD_ENGINE_ACTIVE: bool = False


def _normalize_mode(raw: str | None) -> str:
    return "live" if (raw or "").lower() == "live" else "paper"


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
    if rest == "engine:start":
        return ("engine", "start")
    return None


async def _safe_edit_markdown(
    bot,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup,
) -> None:
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


async def _instant_callback_ack(query) -> None:
    """Plain-text placeholder; keeps current keyboard so user can tap again."""
    try:
        await query.edit_message_text(
            "⏳ 正在切换…",
            reply_markup=query.message.reply_markup if query.message else None,
        )
    except Exception as e:
        logger.debug("instant callback ack edit failed: %s", e)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global USER_MODE
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        await update.message.reply_text("⛔ 未授权使用此网关机器人。")
        return
    store = await _session_store_async(context.application)
    USER_MODE = _normalize_mode(store.get_trade_mode(uid))
    text = render_home_text(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE)
    kb = build_main_keyboard(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE)
    await update.message.reply_text(
        text,
        reply_markup=kb,
        parse_mode="MarkdownV2",
    )


async def cmd_panel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await cmd_start(update, context)


_DEV_CMD = re.compile(r"^/dev(?:@\w+)?\s*(.*)$", re.IGNORECASE | re.DOTALL)


async def cmd_dev(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
        from pipeline.tg_dev_bridge import (
            format_telegram_report,
            run_dev_prompt_with_stream,
        )

        async def _stream_chunk(t: str) -> None:
            try:
                await bot.send_message(chat_id=chat_id, text=t[:4090])
            except Exception as ex:
                logger.debug("cmd_dev stream chunk: %s", ex)

        try:
            result = await run_dev_prompt_with_stream(
                prompt, _stream_chunk, timeout_sec=600, min_interval_sec=3.0
            )
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


async def _persist_user_mode(application: Application, uid: int, mode: str) -> None:
    try:
        store = await _session_store_async(application)
        await asyncio.to_thread(store.set_trade_mode, uid, mode)
    except Exception:
        logger.exception("persist trade mode failed uid=%s", uid)


async def _followup_positions_refresh(
    application: Application,
    bot,
    chat_id: int,
    message_id: int,
) -> None:
    from trading import portfolio_snapshot

    sem = _gw_portfolio_refresh_sem(application)
    try:
        async with sem:
            await portfolio_snapshot.refresh_once()
    except Exception:
        logger.exception("gateway positions background refresh")
    m = USER_MODE
    try:
        snap = portfolio_snapshot.get_snapshot_for_gateway()
        await _safe_edit_markdown(
            bot,
            chat_id,
            message_id,
            render_positions_text(m, snap, refreshing=False),
            build_positions_keyboard(),
        )
    except Exception as e:
        logger.warning("gateway positions follow-up edit failed: %s", e)


async def _gw_panel_work(
    application: Application,
    bot,
    chat_id: int,
    message_id: int,
    uid: int,
    action: str,
    arg: str,
) -> None:
    global USER_MODE, GOD_ENGINE_ACTIVE
    try:
        if action == "mode":
            new_mode = "live" if arg == "live" else "paper"
            USER_MODE = new_mode
            asyncio.create_task(_persist_user_mode(application, uid, new_mode))
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_home_text(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE),
                build_main_keyboard(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE),
            )
            return

        if action == "home":
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_home_text(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE),
                build_main_keyboard(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE),
            )
            return

        if action == "engine" and arg == "start":
            if USER_MODE != "live":
                await _safe_edit_markdown(
                    bot,
                    chat_id,
                    message_id,
                    escape_v2("请先切换到 🔴 真金实盘，再启动奇点引擎。"),
                    build_main_keyboard(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE),
                )
                return
            from pipeline.god_orchestrator import start_autonomous_engine

            async def _god_alert(text: str) -> None:
                try:
                    await bot.send_message(chat_id=chat_id, text=text[:4096])
                except Exception as e:
                    logger.debug("god alert send: %s", e)

            started = await start_autonomous_engine(alert_sender=_god_alert, paper_mode=False)
            if started:
                GOD_ENGINE_ACTIVE = True
                await _safe_edit_markdown(
                    bot,
                    chat_id,
                    message_id,
                    render_home_text(USER_MODE, god_engine_active=True),
                    build_main_keyboard(USER_MODE, god_engine_active=True),
                )
            else:
                await _safe_edit_markdown(
                    bot,
                    chat_id,
                    message_id,
                    escape_v2("奇点引擎已在运行中（或未成功启动）。"),
                    build_main_keyboard(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE),
                )
            return

        if action == "strat":
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_strategy_text(USER_MODE),
                build_back_keyboard(),
            )
            return

        if action == "pos":
            lock = _gw_pos_refresh_lock(application, chat_id, message_id)
            async with lock:
                from trading import portfolio_snapshot

                snap = portfolio_snapshot.get_snapshot_for_gateway()
                await _safe_edit_markdown(
                    bot,
                    chat_id,
                    message_id,
                    render_positions_text(USER_MODE, snap, refreshing=True),
                    build_positions_keyboard(),
                )
            asyncio.create_task(
                _followup_positions_refresh(application, bot, chat_id, message_id)
            )
            return
    except Exception as e:
        logger.exception("gateway panel work: %s", e)
        try:
            err_body = escape_v2(f"❌ 面板更新失败：{e!s}")
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                err_body,
                build_main_keyboard(USER_MODE, god_engine_active=GOD_ENGINE_ACTIVE),
            )
        except Exception as e2:
            logger.warning("gateway panel error edit failed: %s", e2)


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
    await query.answer()

    # 秒回：本协程内零 SessionStore / 零网络
    await _instant_callback_ack(query)

    bot = context.bot
    application = context.application
    chat_id = query.message.chat_id
    message_id = query.message.message_id

    asyncio.create_task(
        _gw_panel_work(application, bot, chat_id, message_id, uid, action, arg)
    )


class TelegramBot:
    """Compatibility wrapper: builds a PTB `Application` and runs polling."""

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
        self._handler = handler
        return handler

    def build_application(self) -> Application:
        app = Application.builder().token(self.token).build()
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("panel", cmd_panel))
        app.add_handler(CommandHandler("dev", cmd_dev))
        pat = re.compile(rf"^{re.escape(GW_CB)}:")
        app.add_handler(CallbackQueryHandler(handle_gateway_callback, pattern=pat))
        return app

    async def poll(self) -> None:
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
