"""
Telegram Gateway — PTB：极简全自动看板。

- 唯一主入口：`/start`（MarkdownV2 + ``gw:*`` 回调）。
- UI 只读内存/文件缓存：`portfolio_snapshot.get_snapshot_for_gateway`、
  ``trade_scheduler.read_scheduler_state``、``live_trader.get_live_stats``；不在回调协程里
  ``await`` 链上或 OKX 实时拉取。重刷新通过后台 ``asyncio.create_task(refresh_once)`` 触发。
- 引擎启停：`TradeScheduler`（live/paper 由 ``USER_MODE`` 决定）；紧急停止后调用
  ``hard_risk_kill.hard_kill`` 尝试 OKX 全平。

Run:   python -m gateway.telegram_bot
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from telegram import Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from pipeline.tg_dev_bridge import (
    process_chaos_immunity_task,
    process_dev_task,
    process_wallet_clone_task,
)

from gateway.tg_front import (
    GW_CB,
    build_dashboard_keyboard,
    build_risk_keyboard,
    escape_v2,
    render_dashboard_text,
    render_risk_settings_text,
)
from tracker.session_store import SessionStore

logger = logging.getLogger(__name__)

USER_MODE: str = "paper"
_TS: Any = None
_TS_LOCK = asyncio.Lock()

_STORE: SessionStore | None = None


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
    return set()


_ALLOWED = _allowed_user_ids()


async def _gw_post_init(application: Application) -> None:
    """PTB lifecycle hook: polling loop is ready."""
    logger.info("Gateway Telegram post_init — polling loop ready")


async def _gw_post_shutdown(application: Application) -> None:
    try:
        from pipeline.god_orchestrator import stop_autonomous_engine

        await stop_autonomous_engine()
    except Exception:
        logger.exception("Gateway post_shutdown: god engine stop failed")


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


def _parse_gw_callback(data: str) -> tuple[str, str] | None:
    if not data or not data.startswith(f"{GW_CB}:"):
        return None
    rest = data[len(GW_CB) + 1 :]
    if rest in ("dash", "home"):
        return ("dash", "")
    if rest == "engine_start":
        return ("engine_start", "")
    if rest == "engine_stop":
        return ("engine_stop", "")
    if rest == "refresh":
        return ("refresh", "")
    if rest == "risk":
        return ("risk", "")
    if rest.startswith("mode:"):
        return ("mode", rest.split(":", 1)[1])
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


def _dashboard_sync() -> tuple[dict, dict, dict]:
    from trading import portfolio_snapshot

    import trade_scheduler

    import live_trader

    snap = portfolio_snapshot.get_snapshot_for_gateway()
    st = trade_scheduler.read_scheduler_state()
    stats = live_trader.get_live_stats()
    return snap, st, stats


async def _edit_main_dashboard(bot, chat_id: int, message_id: int) -> None:
    snap, st, stats = _dashboard_sync()
    await _safe_edit_markdown(
        bot,
        chat_id,
        message_id,
        render_dashboard_text(USER_MODE, snap, st, stats),
        build_dashboard_keyboard(bool(st.get("active"))),
    )


async def _persist_user_mode(application: Application, uid: int, mode: str) -> None:
    try:
        store = await _session_store_async(application)
        await asyncio.to_thread(store.set_trade_mode, uid, mode)
    except Exception:
        logger.exception("persist trade mode failed uid=%s", uid)


async def _get_trade_scheduler(bot, chat_id: int):
    global _TS
    import trade_scheduler as ts_mod

    async def _send(msg: str) -> None:
        try:
            await bot.send_message(chat_id=chat_id, text=(msg or "")[:4096])
        except Exception:
            logger.exception("TradeScheduler notify")

    async with _TS_LOCK:
        if _TS is None:
            _TS = ts_mod.TradeScheduler(send_func=_send)
        return _TS


async def _run_engine_start(bot, chat_id: int, message_id: int) -> None:
    try:
        sch = await _get_trade_scheduler(bot, chat_id)
        mode = "live" if USER_MODE == "live" else "paper"
        if sch.running:
            try:
                await bot.send_message(chat_id=chat_id, text="ℹ️ 全自动引擎已在运行。")
            except Exception:
                logger.debug("engine_start already running notify")
        else:
            await sch.start(mode=mode)
    except Exception as e:
        logger.exception("engine_start: %s", e)
        try:
            await bot.send_message(chat_id=chat_id, text=f"❌ 启动失败：{e!s}"[:4096])
        except Exception:
            pass
    await _edit_main_dashboard(bot, chat_id, message_id)


async def _run_engine_stop(bot, chat_id: int, message_id: int) -> None:
    global _TS
    try:
        if _TS is not None and _TS.running:
            await _TS.stop()
        from trading.hard_risk_kill import hard_kill
        from trading.okx_executor import OKXExecutor

        ex = OKXExecutor()
        r = await hard_kill(ex, reason="telegram_gateway_emergency_stop")
        ok_n = int(r.get("ok_closes", 0) or 0)
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=f"⏹ 已停止调度并请求 OKX 平仓，成功腿数：{ok_n}。",
            )
        except Exception:
            pass
    except Exception as e:
        logger.exception("engine_stop: %s", e)
        try:
            await bot.send_message(chat_id=chat_id, text=f"❌ 停止/平仓异常：{e!s}"[:4096])
        except Exception:
            pass
    await _edit_main_dashboard(bot, chat_id, message_id)


async def _run_refresh_assets(bot, chat_id: int, message_id: int) -> None:
    try:
        from trading import portfolio_snapshot

        asyncio.create_task(portfolio_snapshot.refresh_once())
    except Exception:
        logger.exception("enqueue refresh_once")
    await _edit_main_dashboard(bot, chat_id, message_id)


async def _gw_panel_work(
    application: Application,
    bot,
    chat_id: int,
    message_id: int,
    uid: int,
    action: str,
    arg: str,
) -> None:
    global USER_MODE
    try:
        if action == "dash":
            await _edit_main_dashboard(bot, chat_id, message_id)
            return
        if action == "mode":
            USER_MODE = _normalize_mode(arg)
            asyncio.create_task(_persist_user_mode(application, uid, USER_MODE))
            import live_trader

            cfg = live_trader._load_config()
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_risk_settings_text(USER_MODE, cfg),
                build_risk_keyboard(USER_MODE),
            )
            return
        if action == "risk":
            import live_trader

            cfg = live_trader._load_config()
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_risk_settings_text(USER_MODE, cfg),
                build_risk_keyboard(USER_MODE),
            )
            return
        if action == "refresh":
            await _run_refresh_assets(bot, chat_id, message_id)
            return
        if action == "engine_start":
            await _run_engine_start(bot, chat_id, message_id)
            return
        if action == "engine_stop":
            await _run_engine_stop(bot, chat_id, message_id)
            return
    except Exception as e:
        logger.exception("gateway panel work: %s", e)
        try:
            err_body = escape_v2(f"❌ 面板更新失败：{e!s}")
            snap, st, stats = _dashboard_sync()
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                err_body,
                build_dashboard_keyboard(bool(st.get("active"))),
            )
        except Exception as e2:
            logger.warning("gateway panel error edit failed: %s", e2)


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
    snap, st, stats = _dashboard_sync()
    text = render_dashboard_text(USER_MODE, snap, st, stats)
    kb = build_dashboard_keyboard(bool(st.get("active")))
    await update.message.reply_text(
        text,
        reply_markup=kb,
        parse_mode="MarkdownV2",
    )


_DEV_CMD = re.compile(r"^/dev(?:@\w+)?\s*(.*)$", re.IGNORECASE | re.DOTALL)


async def cmd_feed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """喂养通道：链接或新闻正文 → 极速情绪 + 实体；极端看涨 + 流动性则事件小单。"""
    global USER_MODE
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        await update.message.reply_text("⛔ 未授权使用此网关机器人。")
        return
    store = await _session_store_async(context.application)
    USER_MODE = _normalize_mode(store.get_trade_mode(uid))
    from gateway.sentiment_feed import process_sentiment_feed

    parts = context.args or []
    blob = " ".join(parts).strip()
    if not blob and update.message.reply_to_message:
        blob = (update.message.reply_to_message.text or "").strip()
    if not blob:
        await update.message.reply_text(
            "用法：/feed <推特/新闻链接 或 正文>\n"
            "也可回复一条消息发送 /feed（引用原消息）。\n"
            "单独发一条 http(s) 链接也会自动走本通道。"
        )
        return
    await update.message.reply_text("⚡ Jarvis 极速模型分析中…")
    try:
        out = await process_sentiment_feed(blob, user_mode=USER_MODE)
    except Exception as e:
        logger.exception("cmd_feed: %s", e)
        out = f"❌ 分析失败: {e!s}"
    await update.message.reply_text(out[:4096])


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
        await update.message.reply_text(
            "用法：`/dev <你的开发需求>`\n也可直接发自然语言，Jarvis 会自动识别开发意图。"
        )
        return

    from gateway.jarvis_semantic import classify_intent

    row = await classify_intent(prompt, uid=uid)
    if str(row.get("intent") or "").upper() == "CHAOS_IMMUNITY":
        await update.message.reply_text(
            "🧪 已排队混沌抗压免疫任务（模拟盘 + 后台电池）。"
            "断连模拟时长：CHAOS_API_BLACKOUT_SEC（默认 10s）。"
        )
        _gw_schedule(
            context.application,
            process_chaos_immunity_task(
                bot=context.bot,
                chat_id=update.message.chat_id,
                uid=uid,
                dev_timeout_sec=900,
                min_interval_sec=3.0,
            ),
        )
        return

    if str(row.get("intent") or "").upper() == "WALLET_CLONE":
        addr = row.get("extracted_address")
        if not addr:
            await update.message.reply_text("未识别到有效的 0x 钱包地址。")
            return
        await update.message.reply_text(
            "🔭 已启动后台「对手盘行为克隆」：拉取近 100 笔交易与买入前窗口链上特征…"
        )
        _gw_schedule(
            context.application,
            process_wallet_clone_task(
                bot=context.bot,
                chat_id=update.message.chat_id,
                wallet_address=str(addr),
                timeout_sec=600,
                min_interval_sec=3.0,
            ),
        )
        return

    sub_intent = row.get("sub_intent")

    await update.message.reply_text(
        "🧠 已理解您的战略意图，正在后台唤醒造物主引擎编写代码..."
    )
    _gw_schedule(
        context.application,
        process_dev_task(
            bot=context.bot,
            chat_id=update.message.chat_id,
            prompt=prompt,
            timeout_sec=600,
            min_interval_sec=3.0,
            sub_intent=sub_intent,
        ),
    )


async def handle_plain_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """非命令纯文本：CHAT / TRADE / AUTO_DEV。"""
    global USER_MODE
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    from gateway.jarvis_semantic import (
        chat_reply,
        classify_intent,
        execute_trade_from_user_text,
        user_semantic_lock,
    )
    from gateway.sentiment_feed import is_single_url_message, process_sentiment_feed

    store = await _session_store_async(context.application)
    USER_MODE = _normalize_mode(store.get_trade_mode(uid))
    application = context.application

    if is_single_url_message(text):
        await update.message.reply_text("⚡ Jarvis 拉取链接并分析情绪…")
        try:
            out = await process_sentiment_feed(text, user_mode=USER_MODE)
        except Exception as e:
            logger.exception("sentiment_feed url shortcut: %s", e)
            out = f"❌ 分析失败: {e!s}"
        await update.message.reply_text(out[:4096])
        return

    async with user_semantic_lock(uid):
        row = await classify_intent(text, uid=uid)
        intent = str(row.get("intent") or "CHAT").upper()

        if intent == "CHAOS_IMMUNITY":
            await update.message.reply_text(
                "🧪 已排队混沌抗压免疫任务（模拟盘 + 后台电池）。"
                "断连模拟时长：CHAOS_API_BLACKOUT_SEC（默认 10s）。"
            )
            _gw_schedule(
                application,
                process_chaos_immunity_task(
                    bot=context.bot,
                    chat_id=update.message.chat_id,
                    uid=uid,
                    dev_timeout_sec=900,
                    min_interval_sec=3.0,
                ),
            )
            return

        if intent == "WALLET_CLONE":
            addr = row.get("extracted_address")
            if not addr:
                await update.message.reply_text("未识别到有效的 0x 钱包地址。")
                return
            await update.message.reply_text(
                "🔭 已启动后台「对手盘行为克隆」：拉取近 100 笔交易与买入前窗口链上特征…"
            )
            _gw_schedule(
                application,
                process_wallet_clone_task(
                    bot=context.bot,
                    chat_id=update.message.chat_id,
                    wallet_address=str(addr),
                    timeout_sec=600,
                    min_interval_sec=3.0,
                ),
            )
            return

        if intent == "AUTO_DEV":
            req = (row.get("extracted_requirement") or "").strip() or text
            sub_intent = row.get("sub_intent")
            await update.message.reply_text(
                "🧠 已理解您的战略意图，正在后台唤醒造物主引擎编写代码..."
            )
            _gw_schedule(
                application,
                process_dev_task(
                    bot=context.bot,
                    chat_id=update.message.chat_id,
                    prompt=req,
                    timeout_sec=600,
                    min_interval_sec=3.0,
                    sub_intent=sub_intent,
                ),
            )
            return

        if intent == "TRADE":
            _ok, msg = await execute_trade_from_user_text(
                text, uid=uid, user_mode=USER_MODE
            )
            await update.message.reply_text(msg[:4096])
            return

        reply, err = await chat_reply(text, uid=uid)
        if err:
            await update.message.reply_text(f"（模型不可用：{err[:800]}）")
            return
        if reply:
            await update.message.reply_text(reply[:4096])


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

    await query.answer()

    bot = context.bot
    application = context.application
    chat_id = query.message.chat_id
    message_id = query.message.message_id

    action, arg = parsed
    _gw_schedule(
        application,
        _gw_panel_work(application, bot, chat_id, message_id, uid, action, arg),
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
        app = (
            Application.builder()
            .token(self.token)
            .post_init(_gw_post_init)
            .post_shutdown(_gw_post_shutdown)
            .build()
        )
        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("feed", cmd_feed))
        app.add_handler(CommandHandler("dev", cmd_dev))
        app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_plain_text)
        )
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
