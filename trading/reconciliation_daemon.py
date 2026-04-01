"""
OKX live ledger ↔ exchange reconciliation (ghost positions / stale local rows).

Runs periodically (default 15 min): pulls OKX positions REST, merges into ``agent_state.json``
via ``OKXExecutor.reconcile_state_with_exchange``. DEX leg: ``dex_trader.refresh_positions()``
so Jupiter-tracked open spots match chain (best-effort; not a full delta-neutral audit).
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SEC = 900.0


async def run_reconciliation_once(
    *,
    notify: Optional[Callable[[str], Awaitable[None]]] = None,
) -> dict[str, Any]:
    """Single pass: OKX reconcile + DEX refresh from chain."""
    summary: dict[str, Any] = {"okx": {}, "dex_refresh": "skipped"}

    try:
        from trading.okx_executor import OKXExecutor

        ex = OKXExecutor()
        ex.load_state()
        okx_r = await ex.reconcile_state_with_exchange()
        summary["okx"] = okx_r

        if (
            not okx_r.get("skipped")
            and (okx_r.get("added") or okx_r.get("removed") or okx_r.get("adjusted"))
            and notify
        ):
            lines = ["📒 对账 OKX↔本地账本"]
            if okx_r.get("added"):
                lines.append(f"补齐仓位: {okx_r['added']}")
            if okx_r.get("removed"):
                lines.append(f"移除(所上已平): {okx_r['removed']}")
            if okx_r.get("adjusted"):
                lines.append(f"修正: {okx_r['adjusted']}")
            await notify("\n".join(lines)[:3900])
    except Exception as e:
        logger.warning("reconciliation OKX leg: %s", e, exc_info=True)
        summary["okx"] = {"ok": False, "error": str(e)[:200]}

    if (os.getenv("RECONCILE_DEX_REFRESH") or "1").strip().lower() not in ("0", "false", "no"):
        try:
            import dex_trader as dex

            await asyncio.wait_for(dex.refresh_positions(), timeout=35.0)
            summary["dex_refresh"] = "ok"
        except ImportError:
            summary["dex_refresh"] = "no_dex_trader"
        except Exception as e:
            logger.debug("reconciliation DEX refresh: %s", e)
            summary["dex_refresh"] = f"err:{e}"[:80]

    return summary


async def run_reconciliation_loop(
    interval_sec: float = DEFAULT_INTERVAL_SEC,
    *,
    notify: Optional[Callable[[str], Awaitable[None]]] = None,
) -> None:
    """Never returns; swallows errors and sleeps ``interval_sec`` between passes."""
    interval_sec = max(60.0, float(interval_sec))
    while True:
        try:
            await run_reconciliation_once(notify=notify)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning("reconciliation_daemon loop: %s", e, exc_info=True)
        await asyncio.sleep(interval_sec)
