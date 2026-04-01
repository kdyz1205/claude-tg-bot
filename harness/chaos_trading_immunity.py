"""
Trading chaos immunity battery — builds on ``harness.chaos_monkey`` patterns.

Runs **paper** (or mocked-live) scenarios in an **isolated** ``agent_state.json``
path so production state is untouched. Produces a JSON report for Telegram +
optional LLM repair prompts.

Scenarios:
  - Flash crash (90% mark) then ``hard_risk_kill.hard_kill`` flatten.
  - Simulated OKX price API blackout: ``get_price`` stalls / returns None then recovers.
  - ``reconcile_state_with_exchange`` clears local ghosts when exchange is flat (mocked live).
  - Limping short leg: ``limping_fuse_flatten_short`` in paper with a local short.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Callable
from unittest.mock import AsyncMock, patch

logger = logging.getLogger(__name__)

# Realistic blackout; override with CHAOS_API_BLACKOUT_SEC=0.15 for fast CI
_DEFAULT_BLACKOUT = "10"


@asynccontextmanager
async def isolated_okx_state_file() -> AsyncIterator[Path]:
    """Point ``trading.okx_executor.STATE_FILE`` at a temp file for the duration."""
    import trading.okx_executor as ox

    fd, path = tempfile.mkstemp(prefix="chaos_agent_", suffix=".json")
    os.close(fd)
    tmp = Path(path)
    try:
        with patch.object(ox, "STATE_FILE", tmp):
            yield tmp
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _blackout_sec() -> float:
    return float(os.getenv("CHAOS_API_BLACKOUT_SEC", _DEFAULT_BLACKOUT))


async def _scenario_flash_crash_hard_kill() -> dict[str, Any]:
    """90% drop on mark → hard_kill should still flatten in paper."""
    from trading.hard_risk_kill import hard_kill
    from trading.okx_executor import OKXExecutor

    name = "flash_crash_90pct_then_hard_kill"
    t0 = time.perf_counter()
    async with isolated_okx_state_file():
        ex = OKXExecutor()
        ex.load_state()
        ex.state.mode = "paper"
        ex.state.is_alive = True
        ex.state.equity = 20_000.0
        ex.state.cash = 20_000.0
        ex.state.positions.clear()

        px_open = 100_000.0
        px_crash = 10_000.0

        async def price_crash(sym: str):
            return px_crash

        async def price_open(sym: str):
            return px_open

        with patch.object(ex, "get_price", new=AsyncMock(side_effect=price_open)):
            op = await ex.open_position("BTCUSDT", "long", 1500.0)
        if not op.get("ok"):
            return {
                "name": name,
                "ok": False,
                "error": f"open_failed:{op}",
                "elapsed_sec": round(time.perf_counter() - t0, 3),
            }

        with patch.object(ex, "get_price", new=AsyncMock(side_effect=price_crash)):
            await ex.update_positions()

        with patch.object(ex, "get_price", new=AsyncMock(side_effect=price_crash)):
            hk = await hard_kill(ex, "chaos_immunity:flash_crash_90pct")

        npos = len(ex.state.positions)
        ok_flat = npos == 0 and hk.get("ok_closes", 0) >= 1
        return {
            "name": name,
            "ok": bool(ok_flat),
            "positions_after": npos,
            "hard_kill": hk,
            "elapsed_sec": round(time.perf_counter() - t0, 3),
        }


async def _scenario_okx_blackout_hard_kill() -> dict[str, Any]:
    """
    First get_price calls: sleep (blackout) + None — mimics API dead.
    Later calls return stable price so hard_kill can complete if code retries once.
    """
    from trading.hard_risk_kill import hard_kill
    from trading.okx_executor import OKXExecutor

    name = "okx_price_api_blackout_then_hard_kill"
    t0 = time.perf_counter()
    blackout = _blackout_sec()
    stable = 50_000.0
    calls = {"n": 0}

    async def flaky(sym: str):
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(blackout)
            return None
        return stable

    async with isolated_okx_state_file():
        ex = OKXExecutor()
        ex.load_state()
        ex.state.mode = "paper"
        ex.state.is_alive = True
        ex.state.equity = 20_000.0
        ex.state.cash = 20_000.0
        ex.state.positions.clear()

        with patch.object(ex, "get_price", new=AsyncMock(side_effect=lambda s: stable)):
            op = await ex.open_position("ETHUSDT", "long", 800.0)
        if not op.get("ok"):
            return {
                "name": name,
                "ok": False,
                "error": f"open_failed:{op}",
                "blackout_sec": blackout,
                "elapsed_sec": round(time.perf_counter() - t0, 3),
            }

        hk1: dict[str, Any] = {}
        with patch.object(ex, "get_price", new=AsyncMock(side_effect=flaky)):
            hk1 = await hard_kill(ex, "chaos_immunity:okx_blackout")

        npos_mid = len(ex.state.positions)
        hk2: dict[str, Any] = {}
        if npos_mid > 0:
            with patch.object(ex, "get_price", new=AsyncMock(return_value=stable)):
                hk2 = await hard_kill(ex, "chaos_immunity:okx_blackout_retry")

        npos = len(ex.state.positions)
        return {
            "name": name,
            "ok": bool(npos == 0),
            "positions_after": npos,
            "blackout_sec": blackout,
            "price_calls": calls["n"],
            "hard_kill_first": hk1,
            "hard_kill_second": hk2,
            "recovery_second_kill": bool(npos_mid > 0 and npos == 0),
            "note": "First kill may fail when get_price is dead; second kill after API recovery should flatten.",
            "elapsed_sec": round(time.perf_counter() - t0, 3),
        }


async def _scenario_reconcile_clears_local_ghost() -> dict[str, Any]:
    """Mocked live: exchange flat, local still has row → reconcile removes."""
    from trading.okx_executor import OKXExecutor, Position

    name = "reconcile_state_clears_stale_local"
    t0 = time.perf_counter()
    async with isolated_okx_state_file():
        ex = OKXExecutor()
        ex.load_state()
        ex.state.mode = "live"
        ex.state.positions["BTCUSDT"] = Position(
            symbol="BTCUSDT",
            side="long",
            size=500.0,
            entry_price=90_000.0,
            entry_time=time.time(),
        )

        async def empty_positions():
            return []

        with patch.object(ex, "has_api_keys", return_value=True):
            with patch.object(
                ex, "get_exchange_positions", new=AsyncMock(side_effect=empty_positions)
            ):
                with patch.object(
                    ex, "get_account_balance", new=AsyncMock(return_value={"ok": False})
                ):
                    r = await ex.reconcile_state_with_exchange()

        npos = len(ex.state.positions)
        removed = r.get("removed") or []
        ok = npos == 0 and "BTCUSDT" in removed
        return {
            "name": name,
            "ok": bool(ok),
            "positions_after": npos,
            "reconcile": r,
            "elapsed_sec": round(time.perf_counter() - t0, 3),
        }


async def _scenario_limping_insufficient_balance_short() -> dict[str, Any]:
    """
    Simulates «做空端 Insufficient Balance» aftermath: only the hedge short exists locally;
    ``limping_fuse_flatten_short`` must flatten it in paper (maps to close_position).
    """
    from trading.okx_executor import OKXExecutor, Position

    name = "limping_short_insufficient_balance_fuse"
    t0 = time.perf_counter()
    async with isolated_okx_state_file():
        ex = OKXExecutor()
        ex.load_state()
        ex.state.mode = "paper"
        ex.state.equity = 50_000.0
        ex.state.cash = 44_000.0
        ex.state.positions["SOLUSDT"] = Position(
            symbol="SOLUSDT",
            side="short",
            size=600.0,
            entry_price=150.0,
            entry_time=time.time(),
        )

        with patch.object(ex, "get_price", new=AsyncMock(return_value=150.0)):
            fuse = await ex.limping_fuse_flatten_short(
                "SOLUSDT",
                reason="chaos_insufficient_balance_short",
                max_verify_rounds=3,
            )

        npos = len(ex.state.positions)
        return {
            "name": name,
            "ok": bool(fuse.get("ok") and npos == 0),
            "fuse": fuse,
            "positions_after": npos,
            "elapsed_sec": round(time.perf_counter() - t0, 3),
        }


async def run_chaos_immunity_battery(
    *,
    run_reconciliation_daemon_sample: bool = True,
) -> dict[str, Any]:
    """
    Execute all scenarios sequentially; optionally call ``run_reconciliation_once``
    (OKX leg may skip in paper — reported explicitly).
    """
    from trading import reconciliation_daemon as rd

    started = time.time()
    scenarios: list[Callable[[], Any]] = [
        _scenario_flash_crash_hard_kill,
        _scenario_okx_blackout_hard_kill,
        _scenario_reconcile_clears_local_ghost,
        _scenario_limping_insufficient_balance_short,
    ]
    results: list[dict[str, Any]] = []
    for fn in scenarios:
        try:
            results.append(await fn())
        except Exception as e:
            logger.exception("chaos scenario %s", getattr(fn, "__name__", fn))
            results.append(
                {
                    "name": getattr(fn, "__name__", "unknown"),
                    "ok": False,
                    "error": str(e)[:500],
                }
            )

    reco: dict[str, Any] = {"skipped": True}
    if run_reconciliation_daemon_sample:
        import trading.okx_executor as ox

        fd, path = tempfile.mkstemp(prefix="chaos_reco_", suffix=".json")
        os.close(fd)
        tmp = Path(path)
        try:
            with patch.object(ox, "STATE_FILE", tmp):
                reco = await rd.run_reconciliation_once(notify=None)
        except Exception as e:
            reco = {"ok": False, "error": str(e)[:300]}
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass

    passed = sum(1 for r in results if r.get("ok"))
    report = {
        "kind": "chaos_immunity_battery",
        "started_at": started,
        "blackout_sec_config": _blackout_sec(),
        "scenarios": results,
        "reconciliation_daemon_once": reco,
        "summary": {
            "total": len(results),
            "passed": passed,
            "failed": len(results) - passed,
            "all_ok": passed == len(results),
        },
    }
    return report


def format_chaos_report_telegram(report: dict[str, Any], max_len: int = 3800) -> str:
    lines = [
        "🧪 混沌抗压免疫 — 报告摘要",
        f"通过 {report.get('summary', {}).get('passed', 0)}/"
        f"{report.get('summary', {}).get('total', 0)} 场景",
        "",
    ]
    for s in report.get("scenarios") or []:
        mark = "✅" if s.get("ok") else "❌"
        lines.append(f"{mark} {s.get('name', '?')}")
        if not s.get("ok") and s.get("error"):
            lines.append(f"   err: {s['error'][:200]}")
        if not s.get("ok") and s.get("note"):
            lines.append(f"   note: {s['note'][:200]}")
    lines.append("")
    lines.append("对账守护单次抽样: " + json.dumps(report.get("reconciliation_daemon_once"), ensure_ascii=False)[:800])
    text = "\n".join(lines)
    return text[:max_len]


def build_chaos_immunity_repair_prompt(report: dict[str, Any]) -> str:
    """Prompt for local CLI to patch resilience (hard_kill / reconcile / close paths)."""
    blob = json.dumps(report, ensure_ascii=False, indent=2)[:28_000]
    return f"""你是交易系统韧性工程师。以下为 **混沌抗压免疫测试** 的 JSON 报告（假盘/模拟灾难）。

## 报告
```json
{blob}
```

## 要求
1. 阅读 ``trading/hard_risk_kill.py``、``trading/reconciliation_daemon.py``、``trading/okx_executor.py`` 中与全平、对账、``get_price`` 失败相关的路径。
2. 针对 **ok=false** 的场景做**最小必要**修改：例如 ``close_all_positions`` / ``hard_kill`` 在价格源短暂不可用时的有限次重试与退避；或 ``reconcile_state_with_exchange`` 在异常时的降级策略（勿改变正常实盘语义）。
3. 可扩展 ``harness/chaos_monkey.py`` 的故障枚举以便复现（可选）。
4. 修改后运行 ``python -m pytest tests/test_chaos_immunity.py -q``（若测试文件存在）。

不要编造测试结果；以报告中的 ``scenarios`` 为准。
"""


__all__ = [
    "build_chaos_immunity_repair_prompt",
    "format_chaos_report_telegram",
    "isolated_okx_state_file",
    "run_chaos_immunity_battery",
]
