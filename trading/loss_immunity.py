"""
Loss streak → post-trade analysis → LLM reflexion → learned risk guard injection.

Trigger: **3 consecutive losing closes** within **1 hour** (exit_time window).
Actions: suspend strategy opens, run ``PostTradeAnalyzer``, call reflexion for diagnosis
+ defensive ``guard(ctx)``, append to ``learned_risk_guards``, optional TG notify.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

_CONSEC_LOSSES = 3
_WINDOW_SEC = float(os.environ.get("LOSS_IMMUNITY_WINDOW_SEC", "3600"))
_SUSPEND_SEC = float(os.environ.get("LOSS_IMMUNITY_SUSPEND_SEC", "14400"))
_MIN_PIPELINE_INTERVAL = float(os.environ.get("LOSS_IMMUNITY_DEBOUNCE_SEC", "7200"))

_notify: Optional[Callable[[str], Awaitable[None]]] = None
_suspend_until: dict[str, float] = {}
_last_pipeline_at: dict[str, float] = {}
_inflight: set[str] = set()


def set_telegram_notify(fn: Callable[[str], Awaitable[None]] | None) -> None:
    global _notify
    _notify = fn


def strategy_id_from_executor(ex: Any) -> str:
    env_id = (os.environ.get("ACTIVE_STRATEGY_ID") or "").strip()
    if env_id:
        return env_id
    try:
        raw = json.dumps(ex.state.strategy_params, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
    except Exception:
        return "default_strategy"


def is_strategy_suspended(strategy_id: str) -> tuple[bool, str]:
    until = _suspend_until.get(strategy_id)
    if until is None:
        return False, ""
    if time.time() >= until:
        _suspend_until.pop(strategy_id, None)
        return False, ""
    left = int(until - time.time())
    return True, f"strategy_suspended_loss_immunity({left}s)"


def suspend_strategy(strategy_id: str, seconds: float | None = None) -> None:
    sec = float(seconds if seconds is not None else _SUSPEND_SEC)
    _suspend_until[strategy_id] = time.time() + max(60.0, sec)


def _trade_to_dict(t: Any) -> dict[str, Any]:
    return {
        "symbol": t.symbol,
        "side": t.side,
        "entry_price": t.entry_price,
        "exit_price": t.exit_price,
        "size": t.size,
        "pnl_pct": t.pnl_pct,
        "pnl_usd": t.pnl_usd,
        "entry_time": t.entry_time,
        "exit_time": t.exit_time,
        "reason": t.reason,
    }


def _last_three_consecutive_losses_within_window(history: list) -> list | None:
    if len(history) < _CONSEC_LOSSES:
        return None
    streak: list = []
    for t in reversed(history):
        pnl = getattr(t, "pnl_usd", None)
        if pnl is None:
            pnl = getattr(t, "pnl_pct", 0)
            if isinstance(pnl, (int, float)) and pnl >= 0:
                break
        if isinstance(pnl, (int, float)) and pnl < 0:
            streak.append(t)
            if len(streak) >= _CONSEC_LOSSES:
                break
        else:
            break
    if len(streak) < _CONSEC_LOSSES:
        return None
    streak.reverse()
    span = float(streak[-1].exit_time) - float(streak[0].exit_time)
    if span > _WINDOW_SEC:
        return None
    return streak


def after_trade_closed(executor: Any, record: Any) -> None:
    """Sync hook from ``close_position`` — schedules async pipeline if tripwire."""
    strategy_id = strategy_id_from_executor(executor)
    hist = list(executor.state.trade_history)
    trip = _last_three_consecutive_losses_within_window(hist)
    if not trip:
        return
    if record not in trip:
        return
    if trip[-1] is not record:
        return

    suspend_strategy(strategy_id)

    now = time.time()
    if now - _last_pipeline_at.get(strategy_id, 0) < _MIN_PIPELINE_INTERVAL:
        logger.debug("loss_immunity: debounce skip for %s", strategy_id)
        return
    if strategy_id in _inflight:
        return

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        logger.warning("loss_immunity: no asyncio loop — skip reflexion pipeline")
        suspend_strategy(strategy_id)
        return

    _inflight.add(strategy_id)
    loop.create_task(
        _run_immunity_pipeline(executor, trip, strategy_id),
        name=f"loss_immunity_{strategy_id[:8]}",
    )


async def _run_immunity_pipeline(executor: Any, three_trades: list, strategy_id: str) -> None:
    try:
        _last_pipeline_at[strategy_id] = time.time()

        trades_d = [_trade_to_dict(t) for t in three_trades]
        try:
            from trading_skills.post_trade_analyzer import analyze_trades_as_dicts

            batch = analyze_trades_as_dicts(trades_d)
        except Exception as e:
            logger.exception("post_trade_analyzer: %s", e)
            batch = {"error": str(e), "trades": trades_d}

        market_snapshot = await _snapshot_market(executor)
        payload = {
            "strategy_id": strategy_id,
            "window_sec": _WINDOW_SEC,
            "trades": trades_d,
            "post_trade_batch": batch,
            "market_snapshot": market_snapshot,
        }

        try:
            from agents.reflexion import reflect_trading_loss_immunity

            reflex = await reflect_trading_loss_immunity(payload)
        except Exception as e:
            logger.exception("reflexion: %s", e)
            reflex = {
                "diagnosis": f"reflexion_error:{e!s}",
                "defensive_code": (
                    "def guard(ctx):\n"
                    "    # fallback: block ultra-low confidence\n"
                    "    return float(ctx.get('confidence', 1.0) or 1.0) < 0.25\n"
                ),
                "summary_zh": "模型反思失败，已启用保守置信度门槛。",
            }

        diag = str(reflex.get("diagnosis") or "unknown")
        code = str(reflex.get("defensive_code") or "").strip()
        summary = str(reflex.get("summary_zh") or "已记录防御规则。")

        try:
            from trading import learned_risk_guards as lrg

            ok = lrg.append_guard(source=code, diagnosis=diag, strategy_id=strategy_id)
            if not ok:
                logger.error("learned_risk_guards rejected LLM code")
        except Exception as e:
            logger.exception("append_guard: %s", e)

        msg = (
            f"长官，系统已学会防御假突破陷阱。\n"
            f"诊断：{diag[:400]}\n"
            f"{summary[:300]}"
        )
        if _notify:
            try:
                await _notify(msg)
            except Exception as e:
                logger.warning("loss_immunity TG notify: %s", e)
        logger.warning("loss_immunity pipeline complete: %s", diag[:200])
    finally:
        _inflight.discard(strategy_id)


async def _snapshot_market(executor: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ts": time.time(), "symbols": {}}
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        try:
            px = await executor.get_price(sym)
            out["symbols"][sym] = {"last": px}
        except Exception:
            out["symbols"][sym] = {"last": None}
    return out
