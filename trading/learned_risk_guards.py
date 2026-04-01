"""
Learned defensive guards — LLM-suggested ``guard(ctx) -> bool`` snippets.

``True`` = **veto** (do not open position). Stored on disk and evaluated with
restricted builtins + AST sanity checks (no imports, no file/network calls).
"""

from __future__ import annotations

import ast
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

_BOT_DIR = Path(__file__).resolve().parent.parent
_GUARDS_FILE = _BOT_DIR / "_learned_risk_guards.json"
_LOCK = threading.RLock()

_ALLOWED_CALL_NAMES = frozenset(
    {"float", "int", "min", "max", "abs", "bool", "len", "round", "getattr"}
)


def _ast_guard_source_ok(source: str) -> tuple[bool, str]:
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as e:
        return False, f"syntax: {e}"

    if not tree.body or not isinstance(tree.body[0], ast.FunctionDef):
        return False, "must be a single top-level def guard(ctx):"
    fn = tree.body[0]
    if fn.name != "guard":
        return False, "function must be named guard"
    args = [a.arg for a in fn.args.args]
    if args != ["ctx"]:
        return False, "guard must take exactly one argument ctx"

    for n in ast.walk(fn):
        if isinstance(n, ast.FunctionDef) and n is not fn:
            return False, "nested functions not allowed"
        if isinstance(n, ast.Lambda):
            return False, "lambda not allowed"
        if isinstance(n, (ast.Import, ast.ImportFrom, ast.ClassDef, ast.Global, ast.Nonlocal)):
            return False, f"forbidden construct {type(n).__name__}"
        if isinstance(n, ast.Attribute):
            if isinstance(n.value, ast.Name) and n.value.id != "ctx":
                return False, "only ctx.* attribute access allowed"
        if isinstance(n, ast.Call):
            f = n.func
            if isinstance(f, ast.Name):
                if f.id not in _ALLOWED_CALL_NAMES:
                    return False, f"call {f.id!r} not allowed"
            elif isinstance(f, ast.Attribute):
                if not (
                    isinstance(f.value, ast.Name)
                    and f.value.id == "ctx"
                    and f.attr in ("get",)
                ):
                    return False, "only ctx.get(...) calls on ctx"
            else:
                return False, "complex call not allowed"
        if isinstance(n, ast.Subscript):
            if not (isinstance(n.value, ast.Name) and n.value.id == "ctx"):
                return False, "only ctx[...] subscripts allowed"
    return True, "ok"


def _load_raw() -> list[dict[str, Any]]:
    if not _GUARDS_FILE.exists():
        return []
    try:
        data = json.loads(_GUARDS_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get("guards"), list):
            return data["guards"]
    except Exception as e:
        logger.warning("learned_risk_guards load: %s", e)
    return []


def _save_raw(rows: list[dict[str, Any]]) -> None:
    tmp = str(_GUARDS_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"guards": rows, "updated_at": time.time()}, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, _GUARDS_FILE)
    except OSError as e:
        logger.warning("learned_risk_guards save: %s", e)


def _compile_guard(source: str) -> Callable[[dict[str, Any]], bool] | None:
    ok, msg = _ast_guard_source_ok(source)
    if not ok:
        logger.error("guard rejected: %s", msg)
        return None
    ns: dict[str, Any] = {
        "float": float,
        "int": int,
        "min": min,
        "max": max,
        "abs": abs,
        "bool": bool,
        "len": len,
        "round": round,
        "getattr": getattr,
        "__builtins__": {},
    }
    try:
        exec(compile(source, "<learned_guard>", "exec"), ns, ns)
        fn = ns.get("guard")
        if not callable(fn):
            return None
        return fn  # type: ignore[return-value]
    except Exception as e:
        logger.exception("guard compile failed: %s", e)
        return None


_GUARDS_CACHE: list[tuple[str, Callable[[dict[str, Any]], bool]]] = []


def reload_guards() -> int:
    global _GUARDS_CACHE
    rows = _load_raw()
    compiled: list[tuple[str, Callable[[dict[str, Any]], bool]]] = []
    for row in rows:
        src = (row.get("source") or "").strip()
        if not src:
            continue
        fn = _compile_guard(src)
        if fn is not None:
            compiled.append((row.get("id", "?"), fn))
    with _LOCK:
        _GUARDS_CACHE = compiled
    return len(_GUARDS_CACHE)


def append_guard(
    *,
    source: str,
    diagnosis: str,
    strategy_id: str = "",
) -> bool:
    """Validate, append, persist, reload."""
    src = source.strip()
    if not _compile_guard(src):
        return False
    gid = f"g_{int(time.time())}"
    row = {
        "id": gid,
        "source": src,
        "diagnosis": diagnosis[:2000],
        "strategy_id": strategy_id[:80],
        "added_at": time.time(),
    }
    with _LOCK:
        rows = _load_raw()
        rows.append(row)
        _save_raw(rows)
    reload_guards()
    logger.info("learned guard appended id=%s", gid)
    return True


def evaluate_all(ctx: dict[str, Any]) -> tuple[bool, str]:
    """
    Returns (allowed, reason). If any guard returns True, trade is vetoed.
    """
    with _LOCK:
        guards = list(_GUARDS_CACHE)
    if not guards:
        reload_guards()
        with _LOCK:
            guards = list(_GUARDS_CACHE)
    for gid, fn in guards:
        try:
            veto = bool(fn(ctx))
            if veto:
                return False, f"learned_guard:{gid}"
        except Exception as e:
            logger.warning("guard %s raised: %s", gid, e)
            return False, f"learned_guard_error:{gid}:{e!s}"[:200]
    return True, ""


# Load once at import
try:
    reload_guards()
except Exception:
    pass
