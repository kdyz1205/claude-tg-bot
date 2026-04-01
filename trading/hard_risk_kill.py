"""
Hard risk kill — independent of LLM / strategy brain.

Cancels registered asyncio tasks that represent in-flight opens (e.g. delta-neutral
legs) and closes all positions on the given OKXExecutor. Triggered from drawdown
guardian status and OKXExecutor circuit breakers.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from typing import TYPE_CHECKING, Set

if TYPE_CHECKING:
    from trading.okx_executor import OKXExecutor

logger = logging.getLogger(__name__)

_tasks_lock = threading.Lock()
_registered: Set[asyncio.Task] = set()
_kill_lock = asyncio.Lock()


def register_trading_task(task: asyncio.Task) -> None:
    """Register a task that should be cancelled on hard kill (opens / hedges)."""
    with _tasks_lock:
        _registered.add(task)

    def _done(t: asyncio.Task) -> None:
        with _tasks_lock:
            _registered.discard(t)

    task.add_done_callback(_done)


async def cancel_registered_trading_tasks() -> int:
    with _tasks_lock:
        pending = [t for t in _registered if not t.done()]
    for t in pending:
        t.cancel()
    for t in pending:
        try:
            await t
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.debug("registered task cleanup: %s", e)
    return len(pending)


async def hard_kill(executor: OKXExecutor, reason: str) -> dict:
    """
    Cancel registered trading tasks, reload executor state, flatten all positions.
    Serialized so concurrent triggers do not interleave closes.
    """
    async with _kill_lock:
        try:
            from profit_tracker import record_risk_kill_event

            record_risk_kill_event("hard_kill", {"reason": reason[:500]})
        except Exception as e:
            logger.debug("risk event log skipped: %s", e)

        n = await cancel_registered_trading_tasks()
        try:
            executor.load_state()
        except Exception as e:
            logger.warning("hard_kill load_state: %s", e)

        closes = await executor.close_all_positions(reason=f"HARD_KILL:{reason[:200]}")
        ok_n = sum(1 for r in closes if r.get("ok"))
        logger.error(
            "HARD_KILL reason=%s tasks_cancelled=%d positions_closed_ok=%d/%d",
            reason[:200],
            n,
            ok_n,
            len(closes),
        )
        return {"tasks_cancelled": n, "close_results": closes, "ok_closes": ok_n}
