"""
chaos_inspector.py — Meta-QA immune loop: AST policy scan, chaos probes,
Claude-driven repair with subprocess verification, git commit, and hot-reload.

Run standalone (24h daemon):
  python -m pipeline.chaos_inspector

One-shot health scan (no infinite loop):
  python -m pipeline.chaos_inspector --scan-once
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable, Coroutine

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

QUARANTINE_DIR = REPO_ROOT / "_quarantine_strategies"

DEFAULT_LOOP_INTERVAL = int(os.environ.get("CHAOS_INSPECTOR_INTERVAL_SEC", "3600"))
DEFAULT_AST_INTERVAL = int(os.environ.get("CHAOS_AST_SCAN_INTERVAL_SEC", "86400"))


async def run_daily_ast_scan(
    *,
    quarantine: bool = True,
    notify_fn: Callable[[str], Coroutine[Any, Any, None]] | None = None,
) -> dict:
    """
    Scan AI strategy paths; optionally quarantine violating files.
    """
    from pipeline.security_ast import PolicyViolation, quarantine_file, scan_strategy_tree

    report = scan_strategy_tree(REPO_ROOT)
    by_file: dict[str, list[PolicyViolation]] = {}
    for v in report.violations:
        by_file.setdefault(v.path, []).append(v)

    quarantined: list[str] = []
    if quarantine and by_file:
        for rel, viols in by_file.items():
            target = REPO_ROOT / rel
            if not target.is_file():
                continue
            reason = "; ".join(f"{x.rule}:{x.detail[:60]}" for x in viols[:5])
            dest = quarantine_file(target, QUARANTINE_DIR, reason)
            if dest:
                quarantined.append(str(dest.name))

    summary = {
        "scanned": len(report.scanned),
        "violations": len(report.violations),
        "quarantined": quarantined,
        "files_with_violations": list(by_file.keys()),
    }
    if notify_fn and report.violations:
        await notify_fn(
            f"🛡️ *AST 安全扫描*\n"
            f"已扫: {summary['scanned']} 文件\n"
            f"违规: {summary['violations']} 条\n"
            f"隔离: {len(quarantined)} 个文件"
        )
    logger.info("chaos_inspector: AST scan %s", summary)
    return summary


async def run_chaos_probe_cycle(
    monkey: Any,
    *,
    repair_target: str | None = None,
    notify_fn: Callable[[str], Coroutine[Any, Any, None]] | None = None,
    auto_repair: bool = False,
) -> dict:
    """
    Run sync probes (urllib / requests / sqlite) under each fault type.
    Expected: probes raise while chaos is active (resilience hooks see failures).
    """
    from harness.chaos_monkey import CHAOS_PROBES, ChaosFault, ChaosState, chaos_ai_response

    results: dict[str, object] = {}
    last_tb: str | None = None
    for fault, probe in CHAOS_PROBES.items():
        raised, tb = await monkey.run_probe_sync(probe, fault)
        if tb:
            last_tb = tb
        results[fault.name] = {"raised": raised, "has_tb": bool(tb)}

    st = ChaosState()
    st.active.add(ChaosFault.AI_HALLUCINATION)
    toxic = chaos_ai_response(st, "ok")
    results["AI_HALLUCINATION"] = {"toxic_len": len(toxic), "is_toxic": toxic != "ok"}

    if auto_repair and repair_target and last_tb:
        from self_repair import apply_traceback_repair

        path = REPO_ROOT / repair_target
        if path.is_file():
            ok = await apply_traceback_repair(
                str(path),
                last_tb,
                notify_fn=notify_fn,
                extra_context="chaos_inspector probe",
            )
            results["repair_attempted"] = ok

    return results


def commit_and_reload(file_path: Path, message: str) -> tuple[bool, str]:
    """Git-commit a repaired file and hot-reload its module if loaded."""
    from pipeline.git_merger import GitMerger
    import self_repair as sr

    rel = file_path.resolve().relative_to(REPO_ROOT)
    merger = GitMerger(str(REPO_ROOT))
    ok, msg = merger.commit_paths([str(rel)], message)
    if not ok and "Nothing to commit" not in msg:
        return False, msg

    mod_name = file_path.stem
    reload_ok, rmsg = sr._hot_reload_module(mod_name)
    return True, f"commit:{msg} reload:{reload_ok}:{rmsg}"


async def meta_repair_loop_iteration(
    monkey: Any,
    *,
    notify_fn: Callable[[str], Coroutine[Any, Any, None]] | None = None,
    do_ast: bool = False,
    auto_repair_file: str | None = None,
) -> dict:
    """One full iteration: optional AST scan, chaos cycle, optional repair."""
    out: dict[str, Any] = {}
    if do_ast:
        out["ast"] = await run_daily_ast_scan(quarantine=True, notify_fn=notify_fn)
    out["chaos"] = await run_chaos_probe_cycle(
        monkey,
        repair_target=auto_repair_file,
        notify_fn=notify_fn,
        auto_repair=bool(auto_repair_file),
    )
    return out


class ChaosInspector:
    """
    Background meta-QA controller. Event loop:
      1. Sleep until next tick (CHAOS_INSPECTOR_INTERVAL_SEC).
      2. Every CHAOS_AST_SCAN_INTERVAL_SEC, run AST scan + quarantine.
      3. Run chaos probes (fault injection smoke).
      4. (Optional) If linked with failing integration tests, call apply_traceback_repair + commit.
    """

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_ast: float = 0.0
        self._notify_fn: Callable[[str], Coroutine[Any, Any, None]] | None = None

    def set_notify_fn(self, fn: Callable[[str], Coroutine[Any, Any, None]] | None) -> None:
        self._notify_fn = fn

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="chaos_inspector")
        logger.info(
            "ChaosInspector started (loop=%ds ast=%ds)",
            DEFAULT_LOOP_INTERVAL,
            DEFAULT_AST_INTERVAL,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def run_full_health_scan_once(self, *, quarantine: bool = True) -> dict:
        """Single pass: AST + chaos summary (for operators / diagnose)."""
        from harness.chaos_monkey import ChaosMonkey

        monkey = ChaosMonkey()
        now = time.time()
        ast_summary = await run_daily_ast_scan(
            quarantine=quarantine,
            notify_fn=self._notify_fn,
        )
        chaos_summary = await run_chaos_probe_cycle(monkey, notify_fn=self._notify_fn)
        self._last_ast = now
        return {
            "timestamp": time.time(),
            "ast": ast_summary,
            "chaos": chaos_summary,
        }

    async def _loop(self) -> None:
        from harness.chaos_monkey import ChaosMonkey

        monkey = ChaosMonkey()
        await asyncio.sleep(30)
        while self._running:
            try:
                now = time.time()
                do_ast = (now - self._last_ast) >= DEFAULT_AST_INTERVAL
                await meta_repair_loop_iteration(
                    monkey,
                    notify_fn=self._notify_fn,
                    do_ast=do_ast,
                )
                if do_ast:
                    self._last_ast = now
            except Exception as exc:
                logger.error("ChaosInspector iteration error: %s", exc, exc_info=True)
            try:
                await asyncio.sleep(DEFAULT_LOOP_INTERVAL)
            except asyncio.CancelledError:
                break


async def _amain() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Chaos / meta-QA inspector")
    parser.add_argument(
        "--scan-once",
        action="store_true",
        help="Run one health scan (AST + chaos) and exit",
    )
    parser.add_argument(
        "--no-quarantine",
        action="store_true",
        help="With --scan-once: report AST violations without moving files",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run continuous loop (default if neither flag)",
    )
    args = parser.parse_args()
    insp = ChaosInspector()
    if args.scan_once:
        r = await insp.run_full_health_scan_once(quarantine=not args.no_quarantine)
        print(r)
        return
    await insp.start()
    await asyncio.Event().wait()


def main() -> None:
    asyncio.run(_amain())


inspector_singleton = ChaosInspector()

if __name__ == "__main__":
    main()
