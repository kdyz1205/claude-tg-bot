"""
Shared safety gates for infinite_evolver + smart_evolver:
  1) AST parse + static denylist (syntax / obvious escape & system calls)
  2) Backtest subprocess timeout (env EVOLVER_BACKTEST_TIMEOUT, default 30s)
  3) Asyncio fuse for heavy work (env EVOLVER_HEAVY_ASYNC_TIMEOUT, default 30s) — see get_heavy_async_timeout_sec
  4) Daily generation caps (env EVOLVER_MAX_DAILY_GENERATIONS, default 5)
"""

from __future__ import annotations

import ast
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger("evolver_firewall")

_BASE = Path(__file__).resolve().parent
QUOTA_FILE = _BASE / "_evolver_daily_quota.json"


def get_backtest_timeout_sec() -> int:
    try:
        v = int(os.environ.get("EVOLVER_BACKTEST_TIMEOUT", "30"))
    except ValueError:
        v = 30
    return max(5, min(v, 600))


def get_heavy_async_timeout_sec() -> float:
    """Ceiling for ``asyncio.wait_for`` around evolver CPU / subprocess work.

    Prevents AI-generated dead loops from stalling the Telegram bot event loop.
    Default 30s; override with ``EVOLVER_HEAVY_ASYNC_TIMEOUT`` (5–120).
    """
    try:
        v = float(os.environ.get("EVOLVER_HEAVY_ASYNC_TIMEOUT", "30"))
    except ValueError:
        v = 30.0
    return float(max(5.0, min(v, 120.0)))


def get_max_daily_generations() -> int:
    """Return max runs per UTC day; -1 means unlimited."""
    try:
        v = int(os.environ.get("EVOLVER_MAX_DAILY_GENERATIONS", "5"))
    except ValueError:
        v = 5
    if v < 0:
        return -1
    return min(v, 1000)


def _utc_date_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load_quota() -> dict[str, Any]:
    default = {
        "date_utc": _utc_date_str(),
        "smart_tasks": 0,
        "infinite_codegen": 0,
    }
    if not QUOTA_FILE.exists():
        return default
    try:
        raw = json.loads(QUOTA_FILE.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return default
        out = {**default, **raw}
        if out.get("date_utc") != _utc_date_str():
            return {**default, "date_utc": _utc_date_str()}
        return out
    except Exception as e:
        log.warning("quota load failed: %s", e)
        return default


def _save_quota(d: dict[str, Any]) -> None:
    d = dict(d)
    d["date_utc"] = _utc_date_str()
    tmp = str(QUOTA_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(d, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(QUOTA_FILE))
    except OSError as e:
        log.error("quota save failed: %s", e)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def try_acquire_daily_slot(kind: str) -> tuple[bool, str]:
    """
    kind: 'smart_task' | 'infinite_codegen'
    Returns (ok, message). Resets counters when UTC date changes.
    """
    cap = get_max_daily_generations()
    if cap < 0:
        return True, "unlimited"
    if cap == 0:
        return False, "EVOLVER_MAX_DAILY_GENERATIONS is 0 (all evolution blocked)"

    key = "smart_tasks" if kind == "smart_task" else "infinite_codegen"
    if key not in ("smart_tasks", "infinite_codegen"):
        return False, f"unknown quota kind: {kind}"

    data = _load_quota()
    used = int(data.get(key, 0))
    if used >= cap:
        return (
            False,
            f"daily {key} cap reached ({used}/{cap} UTC day {_utc_date_str()})",
        )
    data[key] = used + 1
    _save_quota(data)
    return True, f"{key} {data[key]}/{cap}"


def remaining_daily(kind: str) -> tuple[int, int]:
    cap = get_max_daily_generations()
    data = _load_quota()
    key = "smart_tasks" if kind == "smart_task" else "infinite_codegen"
    used = int(data.get(key, 0))
    return max(0, cap - used), cap


# ── AST firewall ─────────────────────────────────────────────────────────────

_FORBIDDEN_CALL_NAMES = frozenset({"eval", "exec", "__import__"})
_FORBIDDEN_IMPORT_MODULES = frozenset({
    "subprocess",
    "multiprocessing",
    "ctypes",
    "socket",
    "pty",
    "resource",
})
_FORBIDDEN_ATTR_CALLS = frozenset({
    "system",
    "popen",
    "spawn",
    "spawnlp",
    "execl",
    "execle",
    "execlp",
    "execv",
    "execve",
    "kill",
    "remove",
    "unlink",
    "rmtree",
    "chmod",
})


class _UnsafeAstVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.reason: str | None = None

    def fail(self, msg: str) -> None:
        if self.reason is None:
            self.reason = msg

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            base = (alias.name or "").split(".")[0]
            if base in _FORBIDDEN_IMPORT_MODULES:
                self.fail(f"forbidden import: {alias.name}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        if node.module:
            base = node.module.split(".")[0]
            if base in _FORBIDDEN_IMPORT_MODULES:
                self.fail(f"forbidden import from: {node.module}")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        f = node.func
        if isinstance(f, ast.Name) and f.id in _FORBIDDEN_CALL_NAMES:
            self.fail(f"forbidden call: {f.id}()")
        if isinstance(f, ast.Attribute) and f.attr in _FORBIDDEN_ATTR_CALLS:
            self.fail(f"forbidden call: .{f.attr}()")
        if isinstance(f, ast.Name) and f.id == "compile":
            # compile(src, name, "exec") — common sandbox escape
            if len(node.args) >= 3:
                mode = node.args[2]
                if isinstance(mode, ast.Constant) and mode.value == "exec":
                    self.fail("forbidden compile(..., 'exec')")
        self.generic_visit(node)


def validate_strategy_python_source(source: str, *, label: str = "") -> tuple[bool, str]:
    """
    Parse + walk AST. Returns (ok, detail).
    Intended for LLM-generated strategy scripts before subprocess execution.
    """
    if not source or not source.strip():
        return False, "empty source"
    try:
        tree = ast.parse(source, filename=label or "<strategy>")
    except SyntaxError as e:
        return False, f"syntax error: {e}"

    vis = _UnsafeAstVisitor()
    vis.visit(tree)
    if vis.reason:
        return False, vis.reason
    return True, "ok"


def validate_strategy_file(path: Path) -> tuple[bool, str]:
    try:
        src = path.read_text(encoding="utf-8")
    except OSError as e:
        return False, f"read error: {e}"
    ok, msg = validate_strategy_python_source(src, label=str(path))
    if not ok:
        return False, msg
    try:
        from pipeline.security_ast import scan_source

        viol = scan_source(src, rel_path=str(path))
    except Exception as e:
        log.warning("security_ast scan failed (allowing file): %s", e)
        return True, "ok"
    if viol:
        v0 = viol[0]
        return False, f"security_ast:{v0.rule} line {v0.line}: {v0.detail[:120]}"
    return True, "ok"
