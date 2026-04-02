"""
Telegram Gateway — PTB：极简全自动看板。

- 主入口：`/start`（MarkdownV2 + ``gw:*`` 回调）；交易类斜杠委托 ``bot.py`` 同名处理器。
- UI 只读内存/文件缓存：看板/速报快路径用 ``portfolio_snapshot.get_local_cache()``（零网络）；
  其它用 ``get_snapshot_for_gateway``、``trade_scheduler.read_scheduler_state``、
  ``live_trader.get_live_stats``；不在回调协程里
  ``await`` 链上或 OKX 实时拉取。重刷新通过后台 ``asyncio.create_task(refresh_once)`` 触发。
- 引擎启停：`TradeScheduler`（live/paper 由 ``USER_MODE`` 决定）；紧急停止后调用
  ``hard_risk_kill.hard_kill`` 尝试 OKX 全平。

环境变量（可选）：
  ``GATEWAY_TELEGRAM_USER_IDS`` — 逗号分隔 uid，空则不限。
  ``GATEWAY_AUTO_RESEARCH`` — 设为 1/true 时在本进程启动 ``auto_research`` 空闲实验循环。
  ``GATEWAY_AUTO_RESEARCH_NOTIFY_CHAT_ID`` — 实验结果 Telegram 通知 chat id。
  ``JARVIS_INTENT_LLM`` — 设为 1/true 且已配置 OpenAI/Anthropic 时，对「疑似闲聊」做一次 LLM 意图纠错。
  ``JARVIS_INTENT_MODEL`` — 覆盖默认小模型（OpenAI 默认 ``gpt-4o-mini``，Anthropic 默认 haiku）。
  ``JARVIS_QUEUE_SESSION_COMMANDER`` — 造物任务成功且产生文件变更时，向 ``jarvis_pending_commands`` 入队。
  ``JARVIS_QUEUE_DRAIN_SESSION`` — 入队项显式 ``drain_session``（覆盖路由表）。
  ``SESSION_COMMANDER_JARVIS_FILTER_SESSION`` — 本机 watch 只消费 resolve 后等于该名的任务（并行 drain）。
  市值提醒：自然语言或 ``/mcap_watches`` / ``/mcap_unwatch N``；轮询 ``MCAP_WATCH_POLL_SEC``（默认 90）、
  穿越滞回 ``MCAP_WATCH_HYSTERESIS``（默认 0.985）；数据为 DexScreener 参考市值。
  Jarvis 闲聊默认最长等待见 ``JARVIS_CHAT_MAX_SEC``（默认 180）；单轮硬上限仍可用 ``JARVIS_CHAT_TIMEOUT_SEC`` 覆盖。
  若出现「网络异常」类推送：多为 Telegram 瞬断或双进程抢 polling（勿同时开两个 ``run.py``）。
  ``AUTO_RESEARCH_LAB`` / ``AUTO_RESEARCH_LAB_ROTATE`` / ``AUTO_RESEARCH_SKIP_IDLE`` — 见 ``python auto_research.py``。
  配置总线：写 ``session_commander_config.json`` 的 ``active_skills``；God 引擎用 **watchdog** 监听 JSON 并 ``reload_skills``。
  斜杠：``/start`` 网关面板；``/trade``、``/t`` 委托 ``bot.trade_dashboard_command``。
  其它 ``/…`` 与纯文本相同，默认走 Jarvis 语义路由；**FAST_MAP**（语义路由最前）+ **FAST_COMMANDS** 在 LLM 与 ``chat_reply`` 之前拦截：
  中文「卖出/买入/下单…」→ ``/trade`` 面板；「持仓/资产/余额/面板…」→ ``render_dashboard_text``（内存 ``get_local_cache()``）；「状态/监控」→ ``render_status_brief_text``；「刷新…」→ 仅后台 ``refresh_once``，面板仍读缓存；斜杠同理。``FAST_MAP`` 在 ``_jarvis_semantic_route`` 最前硬匹配，**不经** ``classify_intent``。

Run:   python -m gateway.telegram_bot
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from telegram import ForceReply, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, NetworkError, TimedOut
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
    build_manual_hub_keyboard,
    build_okx_lab_keyboard,
    build_onchain_hub_keyboard,
    build_risk_keyboard,
    escape_v2,
    render_dashboard_plain_text,
    render_dashboard_text,
    render_status_brief_text,
    render_manual_hub_text,
    render_okx_lab_text,
    render_onchain_hub_text,
    render_risk_settings_text,
)
from gateway.gateway_lifecycle import (
    cancel_auto_research_background,
    mark_gateway_user_activity,
    start_auto_research_background,
    sync_slash_command_menu,
)
from gateway.handlers.router import (
    MOE_DEBATE_ACK,
    schedule_trade_moe_nonblocking,
)
from tracker.session_store import SessionStore

logger = logging.getLogger(__name__)

_GW_RISK_PENDING_KEY = "_gw_risk_pending"
_MCW_PENDING_KEY = "_mcap_watch_pending"
_GW_EVOLVER = None  # infinite_evolver.InfiniteEvolver | None

# 止盈/止损/滑点必须先选轨道（gw:risk_q → gw:risk_apply），禁止 CEX 误用链上土狗参数
_RISK_BRANCH_KINDS: frozenset[str] = frozenset({"sl", "tp", "slip"})
_RISK_APPLY_MAP: dict[tuple[str, str], str] = {
    ("cex", "sl"): "cex_stop_loss_pct",
    ("onchain", "sl"): "stop_loss_pct",
    ("cex", "tp"): "cex_take_profit_pct",
    ("onchain", "tp"): "take_profit_pct",
    ("cex", "slip"): "cex_max_slippage_bps",
    ("onchain", "slip"): "max_slippage_bps",
}

_RISK_EDIT_KEYS: frozenset[str] = frozenset(
    {
        "max_trade_pct",
        "max_trade_sol",
        "max_positions",
        "daily_loss_limit_pct",
        "min_liquidity_usd",
    }
)


def _load_gateway_env() -> None:
    """Load repo-root ``.env`` then CWD ``.env`` (``override=False`` — shell env wins)."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    root = Path(__file__).resolve().parent.parent
    p = root / ".env"
    if p.is_file():
        load_dotenv(p, override=False)
    load_dotenv(override=False)


# Fast path：长官一键平账（脊髓反射，不经 Jarvis LLM）
_LEDGER_RESYNC_TRIGGERS_CN = ("校准", "平账", "同步账本")


def _text_triggers_ledger_resync(text: str) -> bool:
    if not (text or "").strip():
        return False
    raw = text.strip()
    low = raw.lower()
    if "resync" in low:
        return True
    for w in _LEDGER_RESYNC_TRIGGERS_CN:
        if w in raw:
            return True
    return False


# 极速反射区：交易中文子串（有序）+ FAST_COMMANDS 看板/刷新；不经 classify_intent / chat_reply
_FAST_TRADE_CN: tuple[tuple[str, str], ...] = (
    ("卖出", "trade_panel"),
    ("卖币", "trade_panel"),
    ("抛售", "trade_panel"),
    ("平仓", "trade_panel"),
    ("买入", "trade_panel"),
    ("买币", "trade_panel"),
    ("下单", "trade_panel"),
    ("开仓", "trade_panel"),
    ("交易面板", "trade_panel"),
    ("极速交易", "trade_panel"),
)

# Jarvis 入口最前硬匹配：不经 LLM / classify_intent（与 ``_resolve_fast_action`` 语义一致）
FAST_MAP: dict[str, tuple[str, ...]] = {
    "dashboard": ("持仓", "资产", "余额", "/portfolio"),
    "status_brief": ("状态", "监控", "/status"),
}


def _jarvis_fast_map_match(text: str) -> str | None:
    """
    Return ``dashboard`` | ``status_brief`` | ``None``.
    Portfolio keys are checked before status so e.g.「资产状态」→ 看板。
    """
    raw = (text or "").strip()
    if not raw:
        return None
    low = raw.lower()
    parts = low.split()
    head = parts[0] if parts else low

    for action, needles in (
        ("dashboard", FAST_MAP["dashboard"]),
        ("status_brief", FAST_MAP["status_brief"]),
    ):
        for n in needles:
            if n.startswith("/"):
                nl = n.lower()
                if head == nl or head.startswith(f"{nl}@"):
                    return action
            elif n in raw:
                return action
    return None


# 看板：``dashboard`` = 快照看板；``status_brief`` = 状态速报；``refresh_dashboard`` = 仅后台 refresh_once
FAST_COMMANDS: dict[str, tuple[str, ...]] = {
    "refresh_dashboard": ("刷新", "刷新资产", "重载快照", "更新快照", "/refresh"),
    "status_brief": ("状态", "监控"),
    "dashboard": (
        "持仓",
        "仓位",
        "余额",
        "资产",
        "面板",
        "看板",
        "主控",
        "/portfolio",
        "/panel",
    ),
}


def _resolve_fast_action(text: str) -> str | None:
    """
    Return ``trade_panel`` | ``refresh_dashboard`` | ``dashboard`` | ``status_brief`` | ``None``.
    刷新优先于看板子串；交易子串在刷新之后、看板之前。斜杠支持 ``@botname``。
    """
    raw = (text or "").strip()
    if not raw:
        return None
    low = raw.lower()
    parts = low.split()
    head = parts[0] if parts else low

    if head in ("/trade",) or head.startswith("/trade@"):
        return "trade_panel"
    if head in ("/t",) or head.startswith("/t@"):
        return "trade_panel"
    if "/refresh" in low or head.startswith("/refresh"):
        return "refresh_dashboard"
    if head in ("/status",) or head.startswith("/status@"):
        return "status_brief"
    if "/portfolio" in low or head.startswith("/portfolio"):
        return "dashboard"
    if "/panel" in low or head.startswith("/panel"):
        return "dashboard"

    for sub in FAST_COMMANDS["refresh_dashboard"]:
        if sub.startswith("/"):
            if sub.lower() in low:
                return "refresh_dashboard"
        elif sub in raw:
            return "refresh_dashboard"

    for needle, action in _FAST_TRADE_CN:
        if needle in raw:
            return action

    for sub in FAST_COMMANDS["status_brief"]:
        if sub.startswith("/"):
            if sub.lower() in low:
                return "status_brief"
        elif sub in raw:
            return "status_brief"

    for sub in FAST_COMMANDS["dashboard"]:
        if sub.startswith("/"):
            if sub.lower() in low:
                return "dashboard"
        elif sub in raw:
            return "dashboard"

    return None


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

_GW_PORTFOLIO_BG_TASK = "_gw_portfolio_snapshot_loop"


def _gw_schedule(application: Application, coro) -> None:
    application.create_task(coro, name="gw_bg")


async def _gw_post_init(application: Application) -> None:
    """PTB lifecycle hook: polling loop is ready; force-refresh slash command menu."""
    logger.info(
        "Gateway Telegram: polling loop ready — Telegram API link up (post_init)."
    )
    await sync_slash_command_menu(application.bot)
    await start_auto_research_background(application)

    # 与主 bot.py 一致：网关独立进程也要拉 OKX + 链上快照，否则 /start 永远看到默认 $0
    try:
        from trading import portfolio_snapshot

        await portfolio_snapshot.refresh_once()
        if _GW_PORTFOLIO_BG_TASK not in application.bot_data:
            application.bot_data[_GW_PORTFOLIO_BG_TASK] = application.create_task(
                portfolio_snapshot.run_background_loop(10.0),
                name="gw_portfolio_snapshot_loop",
            )
        logger.info("Gateway: portfolio_snapshot initial refresh + 10s background loop started")
    except Exception as e:
        logger.warning("Gateway: portfolio_snapshot loop not started: %s", e)


async def _gw_post_shutdown(application: Application) -> None:
    await cancel_auto_research_background(application)

    t = application.bot_data.pop(_GW_PORTFOLIO_BG_TASK, None)
    if t is not None and not t.done():
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.debug("gw portfolio loop join", exc_info=True)

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


def _gw_risk_pending_map(application: Application) -> dict[int, str]:
    return application.bot_data.setdefault(_GW_RISK_PENDING_KEY, {})  # type: ignore[return-value]


def _mcw_pending_map(application: Application) -> dict[int, Any]:
    return application.bot_data.setdefault(_MCW_PENDING_KEY, {})  # type: ignore[return-value]


def _risk_param_label(key: str) -> str:
    return {
        "max_trade_pct": "MaxTradeSize (%)",
        "max_trade_sol": "MaxTradeSOL",
        "max_positions": "MaxPosition",
        "daily_loss_limit_pct": "DailyLossLimit (%)",
        "stop_loss_pct": "链上 StopLoss (%)",
        "take_profit_pct": "链上 TakeProfit (%)",
        "max_slippage_bps": "链上 滑点 (bps)",
        "min_liquidity_usd": "MinLiquidity (USD)",
        "cex_take_profit_pct": "CEX TakeProfit (%)",
        "cex_stop_loss_pct": "CEX StopLoss (%)",
        "cex_max_slippage_bps": "CEX 滑点 (bps)",
    }.get(key, key)


def _risk_branch_cn(kind: str) -> str:
    return {"sl": "止损 %", "tp": "止盈 %", "slip": "滑点 bps"}.get(kind, kind)


def _format_cfg_value(cfg: dict, key: str) -> str:
    v = cfg.get(key)
    if key == "max_trade_sol" and v is None:
        return "null（仅按百分比）"
    return repr(v)


def _coerce_risk_value(key: str, raw: str) -> tuple[bool, Any, str]:
    t = raw.strip().replace(",", "")
    low = t.lower()
    if key == "max_trade_sol":
        if low in ("", "none", "null", "-", "无", "清空", "nil"):
            return True, None, ""
        try:
            v = float(t)
            if v <= 0:
                return False, None, "SOL 硬顶须 > 0，或回复 none 清空"
            return True, round(v, 6), ""
        except ValueError:
            return False, None, "需要数字或 none"

    if key == "max_slippage_bps":
        try:
            v = int(float(t))
            if v < 1 or v > 5000:
                return False, None, "bps 建议 1–5000"
            return True, v, ""
        except ValueError:
            return False, None, "需要整数 bps"

    if key == "max_positions":
        try:
            v = int(float(t))
            if v < 1 or v > 100:
                return False, None, "并发仓位 1–100"
            return True, v, ""
        except ValueError:
            return False, None, "需要整数"

    try:
        v = float(t)
    except ValueError:
        return False, None, "需要数字"

    if key == "max_trade_pct" and not (0 < v <= 200):
        return False, None, "max_trade_pct 建议 (0, 200]"
    if key in (
        "daily_loss_limit_pct",
        "stop_loss_pct",
        "take_profit_pct",
        "cex_stop_loss_pct",
        "cex_take_profit_pct",
    ) and not (0 < v <= 100):
        return False, None, "百分比建议 (0, 100]"
    if key == "min_liquidity_usd" and v < 0:
        return False, None, "不能为负"

    if key == "min_liquidity_usd":
        return True, float(v), ""
    return True, float(v), ""


async def _try_consume_risk_edit_reply(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    if not update.effective_user or not update.message:
        return False
    uid = int(update.effective_user.id)
    mp = _gw_risk_pending_map(context.application)
    key = mp.get(uid)
    if not key:
        return False

    text = (update.message.text or "").strip()
    ok, val, err = _coerce_risk_value(key, text)
    if not ok:
        try:
            await update.message.reply_text(
                f"❌ {err}\n仍等待 `{key}`；可前往风控页或发送正确数值。"
                [:4096]
            )
        except Exception:
            pass
        return True

    import live_trader

    cfg = live_trader._load_config()
    cfg[key] = val
    live_trader._save_config(cfg)
    mp.pop(uid, None)
    try:
        await update.message.reply_text(
            f"✅ 参数已热更新：`{key}` → `{val}`\n（已写入 _live_config.json）"[:4096]
        )
    except Exception:
        pass
    return True


async def _run_gw_lab_action(
    application: Application,
    bot,
    chat_id: int,
    sub: str,
) -> None:
    global _GW_EVOLVER

    async def _notify(msg: str) -> None:
        try:
            await bot.send_message(chat_id=chat_id, text=(msg or "")[:4096])
        except Exception:
            logger.debug("gw lab notify failed", exc_info=True)

    if sub == "evolve":
        try:
            from infinite_evolver import InfiniteEvolver
        except Exception as e:
            await _notify(f"❌ 无法加载 InfiniteEvolver：{e!s}")
            return

        if _GW_EVOLVER is not None and getattr(_GW_EVOLVER, "_running", False):
            await _notify("ℹ️ 自动进化已在运行中。")
            return

        _GW_EVOLVER = InfiniteEvolver(send_func=_notify)
        _GW_EVOLVER.start()
        await _notify("🧬 已启动 InfiniteEvolver 后台循环（默认 30min/次 sweep）。")
        return

    if sub == "train":
        try:
            import auto_train
        except Exception as e:
            await _notify(f"❌ 无法加载 auto_train：{e!s}")
            return

        async def send_status(t: str) -> None:
            await _notify(t)

        async def _train_job():
            await auto_train.run_training(
                "code_edit", send_status, max_tasks=4, loops=1, _internal=False
            )

        _gw_schedule(application, _train_job())
        await _notify("🧠 已排队 auto_train（code_edit，短轮次），请留意后续消息。")
        return

    if sub == "factor":
        await _notify("🧪 Factor Forge 已排队（FACTOR_FORGE 造物主）…")

        async def _factor_job():
            await process_dev_task(
                bot=bot,
                chat_id=chat_id,
                prompt=(
                    "挖掘稳健量化因子：多周期动量 + 波动过滤 + 成交量确认；"
                    "输出单一 BaseSkill（sk_ 前缀），含 buy_confidence / sell_confidence。"
                ),
                sub_intent="FACTOR_FORGE",
            )

        _gw_schedule(application, _factor_job())
        return

    if sub == "alpha":
        root = Path(__file__).resolve().parent.parent
        sk_dir = root / "skills"
        n = len(list(sk_dir.glob("sk_*.py"))) if sk_dir.is_dir() else 0
        ev_on = (
            _GW_EVOLVER is not None and getattr(_GW_EVOLVER, "_running", False)
        )
        await _notify(
            f"📊 Alpha 状态\n"
            f"· 本地 skills/sk_*.py：{n} 个\n"
            f"· InfiniteEvolver：{'🟢 运行中' if ev_on else '⚪ 未启动'}\n"
        )
        return

    await _notify(f"❌ 未知实验室指令：{sub!r}")


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
    if rest == "full_sync":
        return ("full_sync", "")
    if rest == "risk_cancel":
        return ("risk_cancel", "")
    if rest == "manual_hub":
        return ("manual_hub", "")
    if rest == "manual_okx":
        return ("manual_okx", "")
    if rest == "manual_onchain":
        return ("manual_onchain", "")
    if rest.startswith("risk_edit:"):
        return ("risk_edit", rest[10:])
    if rest.startswith("risk_q:"):
        return ("risk_q", rest[7:])
    if rest.startswith("risk_apply:"):
        return ("risk_apply", rest[11:])
    if rest.startswith("lab:"):
        return ("lab", rest[4:])
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

    snap = portfolio_snapshot.get_local_cache()
    st = trade_scheduler.read_scheduler_state()
    stats = live_trader.get_live_stats()
    return snap, st, stats


def _dashboard_sync_fast_path() -> tuple[dict, dict, dict]:
    """Fast path / Pepe: in-memory portfolio cache only — **no** ``refresh_once``, **no** Redis."""
    from trading import portfolio_snapshot

    import trade_scheduler

    import live_trader

    snap = portfolio_snapshot.get_local_cache()
    st = trade_scheduler.read_scheduler_state()
    stats = live_trader.get_live_stats()
    return snap, st, stats


async def _dispatch_fast_action(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    uid: int,
    action: str,
) -> None:
    """Execute fast path: trade panel / 看板 / 速报（无 Jarvis ``chat_reply`` / ``classify_intent``）。"""
    mark_gateway_user_activity()
    global USER_MODE
    store = await _session_store_async(context.application)
    USER_MODE = _normalize_mode(store.get_trade_mode(uid))

    if action == "trade_panel":
        await cmd_trade(update, context)
        return

    if action == "refresh_dashboard":
        try:
            from trading import portfolio_snapshot

            asyncio.create_task(portfolio_snapshot.refresh_once())
        except Exception:
            logger.exception("fast path refresh_once enqueue")

    if action not in ("refresh_dashboard", "dashboard", "status_brief"):
        return

    # 只读 ``get_local_cache()``；刷新类仅后台排队 refresh_once，主流程不 await 拉链上/OKX
    snap, st, stats = _dashboard_sync_fast_path()
    try:
        kb = build_dashboard_keyboard(bool(st.get("active")))
        if action == "status_brief":
            body = render_status_brief_text(USER_MODE, snap, st, stats)
        else:
            body = render_dashboard_text(USER_MODE, snap, st, stats)
        await update.message.reply_text(
            body,
            reply_markup=kb,
            parse_mode="MarkdownV2",
        )
    except Exception as e:
        logger.exception("fast action dispatch failed action=%s", action)
        try:
            await update.message.reply_text(f"⚡ 快路径渲染失败：{e!s}"[:4096])
        except Exception:
            pass


async def _edit_main_dashboard(bot, chat_id: int, message_id: int) -> None:
    snap, st, stats = _dashboard_sync_fast_path()
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
        if action == "manual_hub":
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_manual_hub_text(),
                build_manual_hub_keyboard(),
            )
            return
        if action == "manual_okx":
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_okx_lab_text(),
                build_okx_lab_keyboard(),
            )
            return
        if action == "manual_onchain":
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                render_onchain_hub_text(),
                build_onchain_hub_keyboard(),
            )
            return
        if action == "full_sync":
            try:
                from trading import portfolio_snapshot

                asyncio.create_task(portfolio_snapshot.refresh_once())
            except Exception:
                logger.exception("full_sync enqueue")
            await _edit_main_dashboard(bot, chat_id, message_id)
            return
        if action == "risk_cancel":
            _gw_risk_pending_map(application).pop(uid, None)
            try:
                await bot.send_message(chat_id=chat_id, text="❎ 已取消参数编辑。"[:4096])
            except Exception:
                pass
            await _edit_main_dashboard(bot, chat_id, message_id)
            return
        if action == "risk_q":
            if arg not in _RISK_BRANCH_KINDS:
                await _edit_main_dashboard(bot, chat_id, message_id)
                return
            hint = _risk_branch_cn(arg)
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "您要修改 [🏦 OKX（CEX）] 还是 [🔗 链上（DEX）] 的参数？\n"
                        f"项目：{hint}\n"
                        "点选下方按钮后继续。"
                    )[:4096],
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    "🏦 OKX（CEX 轨道）",
                                    callback_data=f"{GW_CB}:risk_apply:cex:{arg}",
                                ),
                                InlineKeyboardButton(
                                    "🔗 链上（DEX 轨道）",
                                    callback_data=f"{GW_CB}:risk_apply:onchain:{arg}",
                                ),
                            ],
                            [
                                InlineKeyboardButton(
                                    "↩️ 返回风控页",
                                    callback_data=f"{GW_CB}:risk",
                                ),
                            ],
                        ]
                    ),
                )
            except Exception:
                logger.exception("risk_q pick_track message")
            return
        if action == "risk_apply":
            parts = arg.split(":", 1)
            if len(parts) != 2:
                await _edit_main_dashboard(bot, chat_id, message_id)
                return
            track, kind = parts[0].strip().lower(), parts[1].strip().lower()
            fkey = _RISK_APPLY_MAP.get((track, kind))
            if not fkey:
                await _edit_main_dashboard(bot, chat_id, message_id)
                return
            import live_trader

            cfg = live_trader._load_config()
            cur = _format_cfg_value(cfg, fkey)
            _gw_risk_pending_map(application)[uid] = fkey
            label = _risk_param_label(fkey)
            track_cn = "OKX·CEX" if track == "cex" else "链上·DEX"
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"✏️ [{track_cn}] 调整 {label}（`{fkey}`）\n"
                        f"当前值：{cur}\n"
                        "请直接回复一条消息，内容为纯数值。"
                        "\n· max_trade_sol 可回复 none 清空硬顶"
                    )[:4096],
                    reply_markup=ForceReply(
                        selective=True,
                        input_field_placeholder="输入数值",
                    ),
                )
            except Exception:
                logger.exception("risk_apply force_reply")
            return
        if action == "risk_edit":
            if arg not in _RISK_EDIT_KEYS:
                await _edit_main_dashboard(bot, chat_id, message_id)
                return
            import live_trader

            cfg = live_trader._load_config()
            cur = _format_cfg_value(cfg, arg)
            _gw_risk_pending_map(application)[uid] = arg
            label = _risk_param_label(arg)
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"✏️ 调整 {label}（`{arg}`）\n"
                        f"当前值：{cur}\n"
                        "请直接回复一条消息，内容为纯数值。"
                        "\n· max_trade_sol 可回复 none 清空硬顶"
                    )[:4096],
                    reply_markup=ForceReply(
                        selective=True,
                        input_field_placeholder="输入数值",
                    ),
                )
            except Exception:
                logger.exception("risk_edit force_reply")
            return
        if action == "lab":
            await _run_gw_lab_action(application, bot, chat_id, arg)
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
            snap, st, stats = _dashboard_sync_fast_path()
            await _safe_edit_markdown(
                bot,
                chat_id,
                message_id,
                err_body,
                build_dashboard_keyboard(bool(st.get("active"))),
            )
        except Exception as e2:
            logger.warning("gateway panel error edit failed: %s", e2)


def _make_bot_delegate(handler_attr: str):
    """把 ``bot`` 模块里已有的 CommandHandler 逻辑挂到网关进程上。"""

    async def _handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_user or not update.message:
            return
        if not _is_authorized(update.effective_user.id):
            await update.message.reply_text("⛔ 未授权使用此网关机器人。")
            return
        mark_gateway_user_activity()
        try:
            import bot as _bot_mod

            fn = getattr(_bot_mod, handler_attr, None)
            if fn is None:
                await update.message.reply_text(f"❌ 内部错误：无处理器 {handler_attr}")
                return
            await fn(update, context)
        except Exception as e:
            logger.exception("gateway delegate %s failed", handler_attr)
            try:
                await update.message.reply_text(
                    f"❌ 命令执行失败: {e!s}"[:800]
                )
            except Exception:
                pass

    return _handler


# 仅 /trade 委托 bot.py；其余斜杠不走 CommandHandler，由 Jarvis 语义路由处理。
# ``terminal_ui`` 仍 ``from gateway.telegram_bot import cmd_trade``
cmd_trade = _make_bot_delegate("trade_dashboard_command")


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    global USER_MODE
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        await update.message.reply_text("⛔ 未授权使用此网关机器人。")
        return
    mark_gateway_user_activity()
    try:
        store = await _session_store_async(context.application)
        USER_MODE = _normalize_mode(store.get_trade_mode(uid))
    except Exception:
        logger.exception("cmd_start: session store failed; keeping USER_MODE=%s", USER_MODE)

    snap, st, stats = _dashboard_sync_fast_path()
    text = render_dashboard_text(USER_MODE, snap, st, stats)
    plain = render_dashboard_plain_text(USER_MODE, snap, st, stats)
    kb = build_dashboard_keyboard(bool(st.get("active")))
    for attempt in range(3):
        try:
            await update.message.reply_text(
                text,
                reply_markup=kb,
                parse_mode="MarkdownV2",
            )
            return
        except (NetworkError, TimedOut) as e:
            logger.warning(
                "cmd_start: Telegram send failed (attempt %s): %s",
                attempt + 1,
                e,
            )
            if attempt < 2:
                await asyncio.sleep(0.8 + attempt * 0.7)
                continue
            break
        except BadRequest as e:
            logger.warning("cmd_start: MarkdownV2 rejected: %s", e)
            break
        except Exception:
            logger.exception("cmd_start: unexpected error sending MarkdownV2 panel")
            break
    for attempt in range(3):
        try:
            await update.message.reply_text(plain[:4096], reply_markup=kb)
            return
        except (NetworkError, TimedOut) as e:
            logger.warning(
                "cmd_start: plain panel send failed (attempt %s): %s",
                attempt + 1,
                e,
            )
            if attempt < 2:
                await asyncio.sleep(0.8 + attempt * 0.7)
                continue
            break
        except Exception as e:
            logger.exception("cmd_start: plain fallback failed: %s", e)
            break
    try:
        await update.message.reply_text(
            "🤖 奇点量化终端\n\n暂无法发送完整面板（Telegram 或格式异常）。"
            "请几秒后重试 /start，或检查网络。",
            reply_markup=kb,
        )
    except Exception:
        logger.exception("cmd_start: minimal stub send failed")


async def try_mcap_watch_user_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE, uid: int
) -> bool:
    """
    市值提醒：列表 / 取消 / 自然语言登记（DexScreener 轮询）。
    返回 True 表示已处理，勿再进 Jarvis。
    """
    if not update.message:
        return False
    from trading import mcap_watch as mw

    text = (update.message.text or "").strip()
    if not text:
        return False

    if mw.parse_list_intent(text):
        await update.message.reply_text(mw.format_watch_list(uid))
        return True

    cancel_i = mw.parse_cancel_intent(text)
    if cancel_i is not None:
        ok = mw.delete_watch_by_user_index(uid, cancel_i)
        await update.message.reply_text(
            "✅ 已取消该序号提醒。"
            if ok
            else "❌ 序号无效，发送 /mcap_watches 查看列表。"
        )
        return True

    parsed = mw.parse_mcap_watch_intent(text)
    if not parsed:
        return False

    try:
        candidates = await mw.dexscreener_search_candidates(parsed.token_query)
    except Exception as e:
        logger.exception("mcap_watch dex search")
        await update.message.reply_text(f"❌ 搜索异常：{e!s}"[:800])
        return True

    if not candidates:
        await update.message.reply_text(
            f"未在 DexScreener 找到「{parsed.token_query}」。请发合约地址 / mint 再试。"
        )
        return True

    chat_id = update.effective_chat.id
    if len(candidates) == 1:
        c0 = candidates[0]
        if mw.has_active_duplicate(
            uid, str(c0["address"]), parsed.threshold_usd, parsed.direction
        ):
            await update.message.reply_text(
                "ℹ️ 已有相同标的、方向与阈值的未触发提醒，无需重复添加。"
            )
            return True
        rec = mw.make_watch_record(
            user_id=uid, chat_id=chat_id, candidate=c0, parsed=parsed
        )
        mw.add_watch(rec)
        d_zh = "≥" if parsed.direction == "above" else "≤"
        cur = float(c0.get("mcap") or 0)
        await update.message.reply_text(
            "✅ 已登记市值提醒\n"
            f"{c0.get('symbol', '?')} ({c0.get('chain', '')}) 当前参考市值 "
            f"${mw.format_usd_compact(cur)} USD\n"
            f"触发条件：{d_zh} ${mw.format_usd_compact(parsed.threshold_usd)} USD\n"
            f"后台约每 {int(mw.POLL_INTERVAL_SEC)}s 轮询 DexScreener（可设 MCAP_WATCH_POLL_SEC）。\n"
            "/mcap_watches 查看列表。"
        )
        return True

    nonce = secrets.token_hex(4)
    _mcw_pending_map(context.application)[uid] = {
        "nonce": nonce,
        "expires": time.time() + mw.PENDING_TTL_SEC,
        "candidates": candidates[:5],
        "parsed": {
            "token_query": parsed.token_query,
            "threshold_usd": parsed.threshold_usd,
            "direction": parsed.direction,
            "anchor_usd": parsed.anchor_usd,
            "source_text": parsed.source_text,
        },
    }
    buttons: list[list[InlineKeyboardButton]] = []
    for i, c in enumerate(candidates[:5]):
        liq = float(c.get("liquidity_usd") or 0)
        liq_k = max(liq / 1e3, 0.001)
        sym = str(c.get("symbol") or "?")[:10]
        ch = str(c.get("chain") or "")[:8]
        label = f"{i + 1}.{sym} {ch} L${liq_k:.0f}k"
        if len(label) > 56:
            label = label[:53] + "…"
        buttons.append(
            [InlineKeyboardButton(label, callback_data=f"mcapw:p:{nonce}:{i}")]
        )
    cond = "市值突破" if parsed.direction == "above" else "市值跌破"
    await update.message.reply_text(
        f"找到多个池子，请点选对应代币（按流动性排序）：\n"
        f"条件：{cond} ${mw.format_usd_compact(parsed.threshold_usd)} USD",
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return True


async def handle_mcap_watch_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or not query.data or not query.from_user or not query.message:
        return
    uid = query.from_user.id
    if not _is_authorized(uid):
        await query.answer("⛔ 未授权", show_alert=True)
        return
    data = query.data
    if not data.startswith("mcapw:p:"):
        return
    parts = data.split(":")
    if len(parts) != 4:
        await query.answer()
        return
    _, _, nonce, idx_s = parts
    try:
        idx = int(idx_s)
    except ValueError:
        await query.answer()
        return

    from trading import mcap_watch as mw

    pmap = _mcw_pending_map(context.application)
    pend = pmap.get(uid)
    if not pend or pend.get("nonce") != nonce:
        await query.answer("已失效，请重发提醒指令", show_alert=True)
        return
    if time.time() > float(pend.get("expires") or 0):
        pmap.pop(uid, None)
        await query.answer("选择已超时", show_alert=True)
        return

    cands = pend.get("candidates") or []
    if idx < 0 or idx >= len(cands):
        await query.answer("无效选项", show_alert=True)
        return

    cand = cands[idx]
    parsed = mw.parsed_watch_from_dict(pend.get("parsed") or {})
    chat_id = query.message.chat_id

    if mw.has_active_duplicate(
        uid, str(cand["address"]), parsed.threshold_usd, parsed.direction
    ):
        pmap.pop(uid, None)
        await query.answer("已有相同提醒", show_alert=True)
        try:
            await query.edit_message_text("ℹ️ 已有相同标的、方向与阈值的未触发提醒。")
        except BadRequest:
            pass
        return

    rec = mw.make_watch_record(
        user_id=uid, chat_id=chat_id, candidate=cand, parsed=parsed
    )
    mw.add_watch(rec)
    pmap.pop(uid, None)
    await query.answer("已登记")
    d_zh = "≥" if parsed.direction == "above" else "≤"
    cur = float(cand.get("mcap") or 0)
    try:
        await query.edit_message_text(
            "✅ 已登记市值提醒\n"
            f"{cand.get('symbol', '?')} ({cand.get('chain', '')}) 当前参考市值 "
            f"${mw.format_usd_compact(cur)} USD\n"
            f"触发条件：{d_zh} ${mw.format_usd_compact(parsed.threshold_usd)} USD\n"
            f"约每 {int(mw.POLL_INTERVAL_SEC)}s 轮询；/mcap_watches 查看列表。"
        )
    except BadRequest:
        pass


async def _jarvis_semantic_route(
    update: Update, context: ContextTypes.DEFAULT_TYPE, *, uid: int
) -> None:
    """Jarvis：纯文本或未注册斜杠（/start、/trade 已由 CommandHandler 吃掉）。"""
    global USER_MODE
    if not update.message:
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    # FAST_MAP：脊髓反射，禁止进入 classify_intent / 任何 LLM
    jm = _jarvis_fast_map_match(text)
    if jm == "dashboard":
        await _dispatch_fast_action(update, context, uid=uid, action="dashboard")
        return
    if jm == "status_brief":
        await _dispatch_fast_action(update, context, uid=uid, action="status_brief")
        return

    # 看板 / 主控：整句命中即走快路径，禁止进入意图分类或 LLM
    if text in ("看板", "主控"):
        await _dispatch_fast_action(update, context, uid=uid, action="dashboard")
        return

    fa = _resolve_fast_action(text)
    if fa:
        await _dispatch_fast_action(update, context, uid=uid, action=fa)
        return

    if _text_triggers_ledger_resync(text):
        try:
            import live_trader as _lt

            res = await _lt.force_resync_ledger()
        except Exception as e:
            logger.exception("ledger resync fast path: unexpected error")
            await update.message.reply_text(
                f"❌ 平账指令执行异常（账本未修改）：{e!s}"[:4096]
            )
            return
        if res.get("ok"):
            await update.message.reply_text(
                "✅ 遵命长官！实盘引擎的基准资金已强制与您的真实钱包同步，历史盈亏基准已重新校准。"
            )
        else:
            err = str(res.get("error") or "")
            if err == "wallet_not_configured":
                fail = (
                    "❌ 平账指令无法执行：尚未配置链上钱包。请先 /wallet_setup。"
                    "引擎账本未作任何修改。"
                )
            else:
                fail = (
                    "❌ 平账指令无法执行：无法可靠读取链上 SOL 余额（网络或 RPC）。"
                    "引擎账本未作任何修改。"
                )
            await update.message.reply_text(fail)
        return

    if await try_mcap_watch_user_message(update, context, uid):
        return

    from gateway.jarvis_semantic import (
        chat_reply,
        classify_intent,
        maybe_mount_skill_after_auto_dev,
        update_config_active_skill,
        user_semantic_lock,
    )
    from gateway.sentiment_feed import is_single_url_message, process_sentiment_feed

    store = await _session_store_async(context.application)
    USER_MODE = _normalize_mode(store.get_trade_mode(uid))
    application = context.application

    status_msg = None
    try:
        if is_single_url_message(text):
            status_msg = await update.message.reply_text("⚡ Jarvis 拉取链接并分析情绪…")
            try:
                out = await process_sentiment_feed(text, user_mode=USER_MODE)
            except Exception as e:
                logger.exception("sentiment_feed url shortcut: %s", e)
                out = f"❌ 分析失败: {e!s}"
            await status_msg.edit_text(out[:4096])
            return

        async with user_semantic_lock(uid):
            status_msg = await update.message.reply_text("⚡ Jarvis 正在解析...")
            row = await classify_intent(text, uid=uid)
            intent = str(row.get("intent") or "CHAT").upper()
            logger.debug(
                "jarvis_route uid=%s intent=%s sub=%s reasoning=%s",
                uid,
                intent,
                row.get("sub_intent"),
                row.get("reasoning"),
            )

            if intent == "CHAOS_IMMUNITY":
                await status_msg.edit_text(
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

            if intent == "CONFIG_BUS":
                from gateway.config_bus import append_lab_nudge_to_queue, apply_safe_config_patch

                patches = row.get("config_patch") or {}
                lines: list[str] = []
                if patches:
                    ok, msg = apply_safe_config_patch(patches)
                    lines.append(f"⚙️ 配置总线：{'✅' if ok else '❌'} {msg}")
                lp = row.get("lab_prompt")
                if lp:
                    ok2, msg2 = append_lab_nudge_to_queue(str(lp))
                    lines.append(f"🧪 炼丹队列：{'✅' if ok2 else '❌'} {msg2}")
                await status_msg.edit_text(
                    "\n".join(lines) if lines else "✅ 已处理（无写入项）。"
                )
                return

            if intent == "RUN_SKILL":
                sid = str(row.get("skill_id") or "").strip()
                ok_m, msg_m = update_config_active_skill(sid)
                await status_msg.edit_text(
                    f"⚔️ 已写入 `active_skills`：`{sid}`\n"
                    f"配置总线：{'✅' if ok_m else '❌'} {msg_m}"
                )
                return

            if intent == "WALLET_CLONE":
                addr = row.get("extracted_address")
                if not addr:
                    await status_msg.edit_text("未识别到有效的 0x 钱包地址。")
                    return
                await status_msg.edit_text(
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
                await status_msg.edit_text(
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
                maybe_mount_skill_after_auto_dev(text, req)
                return

            if intent == "TRADE":
                await status_msg.edit_text(MOE_DEBATE_ACK)
                schedule_trade_moe_nonblocking(
                    application,
                    context.bot,
                    update.message.chat_id,
                    text,
                    uid=uid,
                    user_mode=USER_MODE,
                )
                return

            reply, err = await chat_reply(text, uid=uid)
            if err:
                await status_msg.edit_text(f"（模型不可用：{err[:800]}）")
                return
            final = (reply or "").strip()
            if not final:
                final = (
                    "（Jarvis 未返回可展示的正文；请检查模型配置或稍后重试。）"
                )
            await status_msg.edit_text(final[:4096])
    except Exception as e:
        logger.exception("handle_plain_text failed uid=%s", uid)
        err_reply = f"❌ 解析异常: {e!s}"[:4096]
        try:
            if status_msg is not None:
                await status_msg.edit_text(err_reply)
            else:
                await update.message.reply_text(err_reply)
        except Exception:
            try:
                await update.message.reply_text(err_reply)
            except Exception:
                pass


async def handle_plain_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """非命令纯文本 → Jarvis。"""
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        return
    mark_gateway_user_activity()
    text = (update.message.text or "").strip()
    if not text:
        return
    if await _try_consume_risk_edit_reply(update, context):
        return
    await _jarvis_semantic_route(update, context, uid=uid)


async def handle_slash_as_semantic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """除 /start、/trade、/t 外，其余斜杠整段交给 Jarvis（与纯文本同路由）。"""
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    if not _is_authorized(uid):
        return
    mark_gateway_user_activity()
    text = (update.message.text or "").strip()
    if not text:
        return
    await _jarvis_semantic_route(update, context, uid=uid)


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
    mark_gateway_user_activity()

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
        # CommandHandler 仅 /start /trade；其它斜杠由 Jarvis 语义层处理（与纯文本同路径）
        from gateway.handlers.router import NON_START_TRADE_SLASH

        app.add_handler(CommandHandler("start", cmd_start))
        app.add_handler(CommandHandler("trade", cmd_trade))
        app.add_handler(CommandHandler("t", cmd_trade))
        app.add_handler(MessageHandler(NON_START_TRADE_SLASH, handle_slash_as_semantic))
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
    _load_gateway_env()
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        level=logging.INFO,
    )
    mode = (os.environ.get("GATEWAY_UI") or "panel").strip().lower()
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        logger.error(
            "TELEGRAM_BOT_TOKEN missing: put it in repo-root .env or export it before "
            "running python -m gateway.telegram_bot."
        )
        raise SystemExit("TELEGRAM_BOT_TOKEN is required.")
    logger.info(
        "Gateway Telegram: TELEGRAM_BOT_TOKEN loaded from environment (communications online; len=%d).",
        len(token),
    )
    logger.info(
        "Fast Path Initialized: FAST_MAP + FAST_COMMANDS (dashboard/status_brief/refresh) + trade_panel."
    )

    if mode in ("terminal", "bloomberg", "conv", "state"):
        from gateway.terminal_ui import build_terminal_application

        build_terminal_application(token).run_polling(allowed_updates=Update.ALL_TYPES)
        return

    TelegramBot(token).run()


if __name__ == "__main__":
    main()
