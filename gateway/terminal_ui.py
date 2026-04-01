"""
Terminal-style Telegram UI: ConversationHandler state machine, in-place edits,
address interceptor, and Telegram API throttling.

Slash menu与可选 ``auto_research`` 后台与面板共用 ``gateway.gateway_lifecycle``（
``GATEWAY_AUTO_RESEARCH`` / ``GATEWAY_AUTO_RESEARCH_NOTIFY_CHAT_ID``）。
全局命令（group -2）：``/help`` ``/trade`` ``/config`` ``/feed`` ``/dev``（复用 ``telegram_bot`` 处理器）。

Callback routing (prefix ``term:`` to avoid clashes with bot.py):
  term:main_menu
  term:view_positions
  term:position_refresh
  term:close_position:{id}
  term:settings
  term:snipe_setup
  term:snipe_refresh
  term:buy_evm:{address}   — stub until wired to dex_trader
"""

from __future__ import annotations

import asyncio
import functools
import logging
import os
import re
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Callable, Optional, Union

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from gateway.gateway_lifecycle import (
    cancel_auto_research_background,
    mark_gateway_user_activity,
    start_auto_research_background,
    sync_slash_command_menu,
)
from gateway.telegram_bot import cmd_config, cmd_dev, cmd_feed, cmd_help, cmd_trade

logger = logging.getLogger(__name__)

_GLOBAL_COMMAND_GROUP = -2


def _allowed_user_ids() -> set[int]:
    raw = (os.environ.get("GATEWAY_TELEGRAM_USER_IDS") or "").strip()
    if not raw:
        return set()
    out: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        p = part.strip()
        if p.isdigit():
            out.add(int(p))
    return out


_ALLOWED_UIDS = _allowed_user_ids()


def _is_authorized(user_id: int) -> bool:
    if not _ALLOWED_UIDS:
        return True
    return int(user_id) in _ALLOWED_UIDS


# ── Optional Redis (fallback: in-process dict) ──────────────────────────────
try:
    import redis  # type: ignore

    _redis_mod = redis
except ImportError:
    _redis_mod = None

_REDIS_CLIENT: Any = None


def _get_redis():
    global _REDIS_CLIENT
    if _redis_mod is None:
        return None
    url = os.environ.get("TERMINAL_REDIS_URL") or os.environ.get("REDIS_URL")
    if not url:
        return None
    if _REDIS_CLIENT is None:
        _REDIS_CLIENT = _redis_mod.from_url(url, decode_responses=True)
    return _REDIS_CLIENT


# ── Conversation states ─────────────────────────────────────────────────────


class TerminalState(IntEnum):
    MAIN_MENU = 0
    POSITION_VIEW = 1
    SNIPE_SETUP = 2
    SETTINGS = 3


# ── User session (in-memory + optional Redis mirror for multi-worker) ───────


@dataclass
class TerminalSession:
    """Per-user terminal session. Used for Bloomberg-style panel continuity."""

    user_id: int
    active_screen: str = "main"  # main | positions | snipe | settings
    panel_chat_id: Optional[int] = None
    panel_message_id: Optional[int] = None
    snipe_token_address: Optional[str] = None
    snipe_preset_sol: float = 0.1
    meta: dict[str, Any] = field(default_factory=dict)


_SESSIONS: dict[int, TerminalSession] = {}
_MAIN_MENU_REFRESH_LAST: dict[int, float] = {}


def _session_key(uid: int) -> str:
    return f"tg:terminal:session:{uid}"


def get_session(user_id: int) -> TerminalSession:
    if user_id in _SESSIONS:
        return _SESSIONS[user_id]
    r = _get_redis()
    if r:
        try:
            raw = r.hgetall(_session_key(user_id))
            if raw:
                s = TerminalSession(
                    user_id=user_id,
                    active_screen=raw.get("active_screen", "main"),
                    panel_chat_id=int(raw["panel_chat_id"]) if raw.get("panel_chat_id") else None,
                    panel_message_id=int(raw["panel_message_id"])
                    if raw.get("panel_message_id")
                    else None,
                    snipe_token_address=raw.get("snipe_token_address") or None,
                    snipe_preset_sol=float(raw.get("snipe_preset_sol", "0.1")),
                )
                _SESSIONS[user_id] = s
                return s
        except Exception as e:
            logger.debug("Redis session read failed: %s", e)
    s = TerminalSession(user_id=user_id)
    _SESSIONS[user_id] = s
    return s


def persist_session(s: TerminalSession) -> None:
    _SESSIONS[s.user_id] = s
    r = _get_redis()
    if not r:
        return
    try:
        mapping = {
            "active_screen": s.active_screen,
            "panel_chat_id": str(s.panel_chat_id or ""),
            "panel_message_id": str(s.panel_message_id or ""),
            "snipe_token_address": s.snipe_token_address or "",
            "snipe_preset_sol": str(s.snipe_preset_sol),
        }
        r.hset(_session_key(s.user_id), mapping=mapping)
        r.expire(_session_key(s.user_id), 86400 * 7)
    except Exception as e:
        logger.debug("Redis session write failed: %s", e)


def remember_panel(chat_id: int, message_id: int, user_id: int) -> None:
    s = get_session(user_id)
    s.panel_chat_id = chat_id
    s.panel_message_id = message_id
    persist_session(s)


# ── Throttle: max one effective edit per user per handler per interval ──────


def telegram_api_throttle(
    interval_sec: float = 2.0,
    *,
    action_key: Optional[str] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator for callback-driven handlers that call Telegram edit APIs.
    Enforces minimum ``interval_sec`` between successful runs per (user, key).
    """

    _last: dict[tuple[int, str], float] = {}

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        key_name = action_key or fn.__name__

        @functools.wraps(fn)
        async def wrapper(
            update: Update, context: ContextTypes.DEFAULT_TYPE, *a: Any, **kw: Any
        ):
            q = update.callback_query
            uid = update.effective_user.id if update.effective_user else 0
            now = time.monotonic()
            slot = (uid, key_name)
            prev = _last.get(slot, 0.0)
            if now - prev < interval_sec:
                if q:
                    await q.answer("⏳ 请求过快（≤2s），已节流", show_alert=False)
                return False  # caller interprets as "throttled"
            _last[slot] = now
            return await fn(update, context, *a, **kw)

        return wrapper

    return decorator


# ── Address detection ─────────────────────────────────────────────────────────

EVM_ADDR_RE = re.compile(r"\b(0x[a-fA-F0-9]{40})\b")
# Solana base58 (no 0, O, I, l); typical mint 32–44 chars
SOL_ADDR_RE = re.compile(r"\b([1-9A-HJ-NP-Za-km-z]{32,44})\b")


def extract_contract_addresses(text: str) -> tuple[Optional[str], Optional[str]]:
    """Return (evm_address_or_none, sol_address_or_none) from first matches."""
    if not text:
        return None, None
    evm_m = EVM_ADDR_RE.search(text.strip())
    sol_m = SOL_ADDR_RE.search(text.strip())
    evm = evm_m.group(1) if evm_m else None
    sol = sol_m.group(1) if sol_m else None
    if evm and sol:
        # Prefer EVM if both match same span unlikely; keep first in string
        if text.index(evm) <= text.index(sol):
            sol = None
        else:
            evm = None
    return evm, sol


async def _rug_report_evm(address: str) -> str:
    try:
        from skills.sk_rug_pull_detector import scan_contract_bytecode
    except ImportError:
        return f"📍 `{address}`\n\n_（sk_rug_pull_detector 未加载）_"

    try:
        out = await scan_contract_bytecode(address)
        return out.get("formatted_report") or str(out)
    except Exception as e:
        logger.exception("rug scan failed")
        return f"📍 `{address}`\n\n⚠️ 扫描异常: `{e!s}`"


def _solana_stub_report(address: str) -> str:
    return (
        f"◎ **Solana mint**\n`{address}`\n\n"
        "链上静态扫描（sk_rug_pull_detector）当前面向 **EVM**。\n"
        "已识别地址格式；详细风控请接入 Jupiter / 持仓模块后扩展。"
    )


def build_token_analysis_keyboard(evm_address: Optional[str], sol_address: Optional[str]) -> InlineKeyboardMarkup:
    rows = []
    if evm_address:
        short = evm_address[:10]
        rows.append(
            [
                InlineKeyboardButton("🛒 买入 0.05 ETH", callback_data=f"term:buy_evm:{short}"),
                InlineKeyboardButton("🛒 买入 0.1 ETH", callback_data=f"term:buy_evm:{short}:0.1"),
            ]
        )
    if sol_address:
        rows.append(
            [
                InlineKeyboardButton("🛒 买入 0.1 SOL", callback_data=f"term:buy_sol:{sol_address[:8]}"),
                InlineKeyboardButton("🛒 买入 0.5 SOL", callback_data=f"term:buy_sol:{sol_address[:8]}:0.5"),
            ]
        )
    rows.append([InlineKeyboardButton("⬅️ 终端主页", callback_data="term:main_menu")])
    return InlineKeyboardMarkup(rows)


# ── Dashboard & Snipe keyboards ─────────────────────────────────────────────


def keyboard_dashboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📊 持仓", callback_data="term:view_positions"),
                InlineKeyboardButton("🎯 狙击", callback_data="term:snipe_setup"),
            ],
            [
                InlineKeyboardButton("⚙️ 设置", callback_data="term:settings"),
                InlineKeyboardButton("🔄 刷新", callback_data="term:main_menu_refresh"),
            ],
        ]
    )


def keyboard_snipe_setup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔁 刷新狙击页", callback_data="term:snipe_refresh")],
            [
                InlineKeyboardButton("0.05 SOL", callback_data="term:snipe_amt:0.05"),
                InlineKeyboardButton("0.1 SOL", callback_data="term:snipe_amt:0.1"),
                InlineKeyboardButton("0.5 SOL", callback_data="term:snipe_amt:0.5"),
            ],
            [InlineKeyboardButton("⬅️ 返回主页", callback_data="term:main_menu")],
        ]
    )


def keyboard_positions_demo() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔄 刷新浮亏", callback_data="term:position_refresh")],
            [
                InlineKeyboardButton("平仓 demo-1", callback_data="term:close_position:demo1"),
                InlineKeyboardButton("平仓 demo-2", callback_data="term:close_position:demo2"),
            ],
            [InlineKeyboardButton("⬅️ 返回主页", callback_data="term:main_menu")],
        ]
    )


def keyboard_settings() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ 返回主页", callback_data="term:main_menu")]]
    )


def _dashboard_text() -> str:
    return (
        "📈 **终端 · Dashboard**\n"
        "━━━━━━━━━━━━━━━━\n"
        "快捷操作已启用；菜单切换全部就地刷新。\n"
        "粘贴 **EVM / Solana** 合约地址可唤出分析面板。"
    )


def _positions_text() -> str:
    now = time.strftime("%H:%M:%S")
    return (
        "📊 **持仓视图**（演示）\n"
        f"更新时间 `{now}`\n"
        "━━━━━━━━━━━━━━━━\n"
        "• demo-1: +0.00%  （接入实盘后替换）\n"
        "• demo-2: -0.00%\n"
    )


def _snipe_text(s: TerminalSession) -> str:
    amt = s.snipe_preset_sol
    tok = s.snipe_token_address or "（粘贴代币地址或从分析面板跳转）"
    return (
        "🎯 **狙击设置**\n"
        "━━━━━━━━━━━━━━━━\n"
        f"预设金额: **{amt} SOL**\n"
        f"目标: `{tok}`\n"
    )


def _settings_text() -> str:
    return "⚙️ **设置**\n━━━━━━━━━━━━━━━━\n终端状态机已启用；更多选项可接 bot.py 配置。"


async def _edit_or_send_panel(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    chat_id: int,
    text: str,
    reply_markup: InlineKeyboardMarkup,
) -> None:
    s = get_session(user_id)
    if s.panel_message_id and s.panel_chat_id == chat_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=s.panel_message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode="Markdown",
            )
            return
        except Exception as e:
            logger.debug("edit_message_text failed, sending new: %s", e)
    m = await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        reply_markup=reply_markup,
        parse_mode="Markdown",
    )
    remember_panel(chat_id, m.message_id, user_id)


# ── Handlers ─────────────────────────────────────────────────────────────────-


async def _terminal_post_init(application: Application) -> None:
    logger.info("Terminal UI post_init — menu sync + optional auto_research")
    await sync_slash_command_menu(application.bot)
    await start_auto_research_background(application)


async def _terminal_post_shutdown(application: Application) -> None:
    await cancel_auto_research_background(application)
    try:
        from pipeline.god_orchestrator import stop_autonomous_engine

        await stop_autonomous_engine()
    except Exception:
        logger.exception("Terminal post_shutdown: god engine stop failed")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not update.effective_chat or not update.effective_user:
        return ConversationHandler.END
    uid = update.effective_user.id
    if not _is_authorized(uid):
        if update.message:
            await update.message.reply_text("⛔ 未授权使用此网关机器人。")
        return ConversationHandler.END
    mark_gateway_user_activity()
    chat_id = update.effective_chat.id
    s = get_session(uid)
    s.active_screen = "main"
    persist_session(s)
    if s.panel_message_id and s.panel_chat_id == chat_id:
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=s.panel_message_id,
                text=_dashboard_text(),
                reply_markup=keyboard_dashboard(),
                parse_mode="Markdown",
            )
            return TerminalState.MAIN_MENU
        except Exception as e:
            logger.debug("cmd_start edit failed, sending new: %s", e)
    m = await update.effective_message.reply_text(
        _dashboard_text(),
        reply_markup=keyboard_dashboard(),
        parse_mode="Markdown",
    )
    remember_panel(m.chat_id, m.message_id, uid)
    return TerminalState.MAIN_MENU


async def _terminal_rug_followup_evm(
    context: ContextTypes.DEFAULT_TYPE,
    uid: int,
    chat_id: int,
    evm: str,
) -> None:
    """Heavy rug scan off the MessageHandler hot path (instant placeholder first)."""
    try:
        body = await _rug_report_evm(evm)
        panel = f"{body}"
        kb = build_token_analysis_keyboard(evm, None)
        await _edit_or_send_panel(context, uid, chat_id, panel, kb)
    except Exception as e:
        logger.warning("terminal rug follow-up failed: %s", e)


async def global_address_interceptor(
    update: Update, context: ContextTypes.DEFAULT_TYPE,
) -> None:
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    if not _is_authorized(update.effective_user.id):
        return
    mark_gateway_user_activity()
    text = update.message.text or ""
    if text.strip().startswith("/"):
        return
    evm, sol = extract_contract_addresses(text)
    if not evm and not sol:
        return

    uid = update.effective_user.id
    chat_id = update.effective_chat.id

    if evm:
        panel = f"📍 `{evm}`\n\n⏳ 链上静态扫描进行中…"
        kb = build_token_analysis_keyboard(evm, None)
        await _edit_or_send_panel(context, uid, chat_id, panel, kb)
        asyncio.create_task(_terminal_rug_followup_evm(context, uid, chat_id, evm))
    else:
        panel = _solana_stub_report(sol or "")
        kb = build_token_analysis_keyboard(None, sol)
        await _edit_or_send_panel(context, uid, chat_id, panel, kb)

    s = get_session(uid)
    s.active_screen = "token"
    persist_session(s)
    raise ApplicationHandlerStop


@telegram_api_throttle(2.0, action_key="position_refresh")
async def on_position_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Union[int, bool]:
    """Throttle wrapper answers only when throttled; otherwise caller answers after edit."""
    q = update.callback_query
    if not q:
        return TerminalState.POSITION_VIEW
    uid = update.effective_user.id if update.effective_user else 0
    try:
        await q.edit_message_text(
            _positions_text(),
            reply_markup=keyboard_positions_demo(),
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.debug("position refresh edit: %s", e)
    s = get_session(uid)
    s.active_screen = "positions"
    persist_session(s)
    return TerminalState.POSITION_VIEW


async def on_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    q = update.callback_query
    if not q or not q.data or not update.effective_user:
        return ConversationHandler.END
    uid = update.effective_user.id
    if not _is_authorized(uid):
        await q.answer("⛔ 未授权", show_alert=True)
        return ConversationHandler.END
    mark_gateway_user_activity()
    data = q.data
    s = get_session(uid)

    if data == "term:main_menu" or data == "term:main_menu_refresh":
        if data == "term:main_menu_refresh":
            now = time.monotonic()
            prev = _MAIN_MENU_REFRESH_LAST.get(uid, 0.0)
            if now - prev < 2.0:
                await q.answer("⏳ 刷新过快", show_alert=False)
                return TerminalState.MAIN_MENU
            _MAIN_MENU_REFRESH_LAST[uid] = now
        s.active_screen = "main"
        persist_session(s)
        await q.answer()
        await q.edit_message_text(
            _dashboard_text(),
            reply_markup=keyboard_dashboard(),
            parse_mode="Markdown",
        )
        return TerminalState.MAIN_MENU

    if data == "term:view_positions":
        s.active_screen = "positions"
        persist_session(s)
        await q.answer()
        await q.edit_message_text(
            _positions_text(),
            reply_markup=keyboard_positions_demo(),
            parse_mode="Markdown",
        )
        return TerminalState.POSITION_VIEW

    if data == "term:position_refresh":
        res = await on_position_refresh(update, context)
        if res is False:
            return TerminalState.POSITION_VIEW
        await q.answer()
        return res  # type: ignore[return-value]

    if data.startswith("term:close_position:"):
        pid = data.split(":", 2)[-1]
        await q.answer(f"平仓指令已记录: {pid}", show_alert=True)
        return TerminalState.POSITION_VIEW

    if data == "term:settings":
        s.active_screen = "settings"
        persist_session(s)
        await q.answer()
        await q.edit_message_text(
            _settings_text(),
            reply_markup=keyboard_settings(),
            parse_mode="Markdown",
        )
        return TerminalState.SETTINGS

    if data == "term:snipe_setup" or data == "term:snipe_refresh":
        s.active_screen = "snipe"
        persist_session(s)
        await q.answer()
        await q.edit_message_text(
            _snipe_text(s),
            reply_markup=keyboard_snipe_setup(),
            parse_mode="Markdown",
        )
        return TerminalState.SNIPE_SETUP

    if data.startswith("term:snipe_amt:"):
        amt = float(data.split(":")[-1])
        s.snipe_preset_sol = amt
        persist_session(s)
        await q.answer()
        await q.edit_message_text(
            _snipe_text(s),
            reply_markup=keyboard_snipe_setup(),
            parse_mode="Markdown",
        )
        return TerminalState.SNIPE_SETUP

    if data.startswith("term:buy_evm:") or data.startswith("term:buy_sol:"):
        await q.answer("买入路由待接入 dex_trader / live_trader", show_alert=True)
        return TerminalState.MAIN_MENU

    await q.answer()
    logger.warning("Unknown callback: %s", data)
    return TerminalState.MAIN_MENU


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("已取消 / 会话结束。发送 /start 打开终端。")
    return ConversationHandler.END


async def on_non_address_text(
    update: Update, context: ContextTypes.DEFAULT_TYPE,
) -> int:
    """Keep conversation state; no extra messages (terminal stays in-place)."""
    uid = update.effective_user.id if update.effective_user else 0
    mark_gateway_user_activity()
    s = get_session(uid)
    return {
        "main": TerminalState.MAIN_MENU,
        "positions": TerminalState.POSITION_VIEW,
        "snipe": TerminalState.SNIPE_SETUP,
        "settings": TerminalState.SETTINGS,
        "token": TerminalState.MAIN_MENU,
    }.get(s.active_screen, TerminalState.MAIN_MENU)


_TERM_PATTERN = r"^term:"


def _terminal_callback_handler() -> CallbackQueryHandler:
    """New instance per ConversationHandler slot (PTB requirement)."""
    return CallbackQueryHandler(on_callback_router, pattern=_TERM_PATTERN)


def _terminal_text_handler() -> MessageHandler:
    return MessageHandler(filters.TEXT & ~filters.COMMAND, on_non_address_text)


def build_terminal_application(token: str) -> Application:
    app = (
        Application.builder()
        .token(token)
        .post_init(_terminal_post_init)
        .post_shutdown(_terminal_post_shutdown)
        .build()
    )

    for cmd, fn in (
        ("help", cmd_help),
        ("trade", cmd_trade),
        ("config", cmd_config),
        ("feed", cmd_feed),
        ("dev", cmd_dev),
    ):
        app.add_handler(CommandHandler(cmd, fn), group=_GLOBAL_COMMAND_GROUP)

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", cmd_start),
            _terminal_callback_handler(),
        ],
        states={
            TerminalState.MAIN_MENU: [
                _terminal_callback_handler(),
                _terminal_text_handler(),
            ],
            TerminalState.POSITION_VIEW: [
                _terminal_callback_handler(),
                _terminal_text_handler(),
            ],
            TerminalState.SNIPE_SETUP: [
                _terminal_callback_handler(),
                _terminal_text_handler(),
            ],
            TerminalState.SETTINGS: [
                _terminal_callback_handler(),
                _terminal_text_handler(),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
        name="terminal_conv",
        persistent=False,
        allow_reentry=True,
    )

    app.add_handler(conv, group=0)
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, global_address_interceptor),
        group=-1,
    )
    return app
