"""
self_repair.py — Proactive code health scanner and auto-repair engine.

Complements self_monitor.CodeSelfRepair (which handles runtime tracebacks)
by proactively scanning for:
  - Syntax errors (py_compile)
  - Import failures
  - Missing dependencies (auto pip-install)

Enforces success-rate gate: only auto-applies patches when historical
repair success rate >= MIN_AUTO_APPLY_RATE (80%), otherwise sends a
Telegram notification for human review.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import py_compile
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

BOT_DIR = Path(__file__).parent.resolve()
REPAIR_LOG = BOT_DIR / ".repair_log.jsonl"
SCAN_INTERVAL = 300          # seconds between proactive scans
MIN_AUTO_APPLY_RATE = 0.80   # 80% success rate required for auto-apply
CLAUDE_CMD = Path.home() / "AppData" / "Roaming" / "npm" / "claude.cmd"

# Files to skip (generated, lock, env)
_SKIP_PATTERNS = {".env", ".bot.lock", ".bot.pid"}
_SKIP_DIRS = {
    "__pycache__", ".git", ".claude", "skills",
    "agents", "docs", ".skill_library",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _collect_py_files() -> list[Path]:
    """Return all .py files in BOT_DIR (shallow — no deep third-party dirs)."""
    result = []
    for item in BOT_DIR.iterdir():
        if item.is_file() and item.suffix == ".py" and item.name not in _SKIP_PATTERNS:
            result.append(item)
    return sorted(result)


def _get_repair_stats() -> dict:
    """Read .repair_log.jsonl and compute success rate + total counts."""
    total = ok = 0
    if REPAIR_LOG.exists():
        try:
            with REPAIR_LOG.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        total += 1
                        if rec.get("success"):
                            ok += 1
                    except Exception:
                        pass
        except Exception as exc:
            logger.warning("self_repair: cannot read repair log: %s", exc)
    rate = ok / total if total else None  # None = no history yet
    return {"total": total, "ok": ok, "rate": rate}


_MAX_REPAIR_LOG_LINES = 5000


def _append_repair_log(record: dict) -> None:
    try:
        with REPAIR_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        # Truncate if too many lines
        try:
            with REPAIR_LOG.open("r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > _MAX_REPAIR_LOG_LINES:
                with REPAIR_LOG.open("w", encoding="utf-8") as f:
                    f.writelines(lines[-_MAX_REPAIR_LOG_LINES:])
        except Exception:
            pass
    except Exception as exc:
        logger.warning("self_repair: failed to write repair log: %s", exc)


# ---------------------------------------------------------------------------
# Syntax scanning
# ---------------------------------------------------------------------------

def scan_syntax_errors() -> list[dict]:
    """Run py_compile on every .py file. Returns list of error dicts."""
    errors = []
    for path in _collect_py_files():
        try:
            py_compile.compile(str(path), doraise=True)
        except py_compile.PyCompileError as exc:
            errors.append({
                "file": path.name,
                "path": str(path),
                "error_type": "SyntaxError",
                "error_msg": str(exc)[:300],
                "line": _extract_lineno(str(exc)),
            })
        except Exception as exc:
            logger.debug("self_repair: py_compile unexpected error for %s: %s", path.name, exc)
    return errors


def _extract_lineno(msg: str) -> int:
    """Try to parse line number from a py_compile error message."""
    import re
    m = re.search(r"line (\d+)", msg)
    return int(m.group(1)) if m else 0


# ---------------------------------------------------------------------------
# Import scanning
# ---------------------------------------------------------------------------

def scan_import_errors() -> list[dict]:
    """Attempt to import each .py file as a module. Returns error dicts."""
    errors = []
    for path in _collect_py_files():
        if path.name.startswith("_"):
            continue
        mod_name = path.stem
        try:
            # Re-import to catch currently broken imports
            if mod_name in sys.modules:
                # Check if already loaded without errors
                continue
            spec = importlib.util.spec_from_file_location(f"_selfrepair_check_{mod_name}", path)
            if spec is None:
                continue
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)  # type: ignore[union-attr]
        except ImportError as exc:
            errors.append({
                "file": path.name,
                "path": str(path),
                "error_type": "ImportError",
                "error_msg": str(exc)[:300],
                "missing_pkg": _extract_missing_pkg(str(exc)),
                "line": 0,
            })
        except Exception:
            pass  # Non-import errors handled by syntax scan or runtime repair
    return errors


def _extract_missing_pkg(msg: str) -> Optional[str]:
    """Extract package name from 'No module named X' message."""
    import re
    m = re.search(r"No module named '([^']+)'", msg)
    if m:
        return m.group(1).split(".")[0]
    return None


# ---------------------------------------------------------------------------
# Dependency auto-install
# ---------------------------------------------------------------------------

def auto_install_missing(pkg: str) -> bool:
    """Attempt to pip-install a missing package. Returns True on success."""
    logger.info("self_repair: auto-installing missing package: %s", pkg)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", pkg, "--quiet"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode == 0:
            logger.info("self_repair: installed %s successfully", pkg)
            return True
        logger.warning("self_repair: pip install %s failed: %s", pkg, result.stderr[:200])
        return False
    except Exception as exc:
        logger.warning("self_repair: auto_install error for %s: %s", pkg, exc)
        return False


# ---------------------------------------------------------------------------
# Claude-based syntax fix
# ---------------------------------------------------------------------------

async def _generate_syntax_fix(file_path: str, error_msg: str) -> Optional[str]:
    """Ask Claude CLI to suggest a fix for a syntax error. Returns fixed source or None."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            source = f.read()
    except Exception:
        return None

    prompt = (
        f"Fix the Python SyntaxError in this file:\n"
        f"Error: {error_msg}\n\n"
        f"Full source:\n```python\n{source[:6000]}\n```\n\n"
        f"Return ONLY the complete corrected Python source. "
        f"No explanations, no markdown fences."
    )
    try:
        proc = await asyncio.create_subprocess_exec(
            str(CLAUDE_CMD),
            "-p", prompt,
            "--output-format", "text",
            "--dangerously-skip-permissions",
            "--model", "claude-haiku-4-5-20251001",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(BOT_DIR),
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=120)
        if proc.returncode != 0:
            return None
        return stdout.decode("utf-8", errors="replace").strip()
    except Exception as exc:
        logger.warning("self_repair: _generate_syntax_fix error: %s", exc)
        return None


async def _apply_syntax_fix(
    file_path: str,
    fixed_source: str,
    error_info: dict,
    notify_fn=None,
) -> bool:
    """Validate and write fixed source. Respects success-rate gate."""
    stats = _get_repair_stats()

    # Success-rate gate
    if stats["rate"] is not None and stats["rate"] < MIN_AUTO_APPLY_RATE:
        msg = (
            f"⚠️ *自动修复被暂停*\n"
            f"文件: `{error_info['file']}`\n"
            f"错误: {error_info['error_msg'][:100]}\n"
            f"原因: 历史修复成功率 {stats['rate']:.0%} < 80% 阈值\n"
            f"需要人工审查 — 使用 /repair_status 查看详情"
        )
        logger.warning("self_repair: success rate %.0f%% < 80%%, skipping auto-apply for %s",
                       stats["rate"] * 100, error_info["file"])
        if notify_fn:
            await notify_fn(msg)
        _append_repair_log({
            "ts": datetime.now().isoformat(),
            "file": error_info["file"],
            "line": error_info.get("line", 0),
            "error_type": error_info["error_type"],
            "error_msg": error_info["error_msg"][:200],
            "confidence": 0.0,
            "diff": "",
            "success": False,
            "backed_up": False,
            "skipped_reason": f"success_rate_{stats['rate']:.2f}_below_threshold",
        })
        return False

    # Validate fixed source compiles
    import ast
    try:
        ast.parse(fixed_source)
    except SyntaxError as se:
        logger.warning("self_repair: generated fix still has SyntaxError: %s", se)
        _append_repair_log({
            "ts": datetime.now().isoformat(),
            "file": error_info["file"],
            "line": error_info.get("line", 0),
            "error_type": error_info["error_type"],
            "error_msg": error_info["error_msg"][:200],
            "confidence": 0.0,
            "diff": "",
            "success": False,
            "backed_up": False,
            "skipped_reason": "fix_still_has_syntax_error",
        })
        return False

    # Backup original (atomic: write to tmp, fsync, then rename)
    backup_path = file_path + f".bak.{int(time.time())}"
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            original = f.read()
        tmp_backup = backup_path + ".tmp"
        with open(tmp_backup, "w", encoding="utf-8") as f:
            f.write(original)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_backup, backup_path)
    except Exception as exc:
        logger.warning("self_repair: backup failed for %s: %s", file_path, exc)

    # Build diff
    import difflib
    diff_lines = list(difflib.unified_diff(
        original.splitlines(keepends=True),
        fixed_source.splitlines(keepends=True),
        fromfile=f"{error_info['file']} (original)",
        tofile=f"{error_info['file']} (fixed)",
        lineterm="",
    ))
    diff = "".join(diff_lines[:80])

    # Atomic write
    tmp = file_path + ".repair.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(fixed_source)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, file_path)
        logger.info("self_repair: applied syntax fix to %s", error_info["file"])
        _append_repair_log({
            "ts": datetime.now().isoformat(),
            "file": error_info["file"],
            "line": error_info.get("line", 0),
            "error_type": error_info["error_type"],
            "error_msg": error_info["error_msg"][:200],
            "confidence": 0.85,
            "diff": diff[:2000],
            "success": True,
            "backed_up": True,
            "source": "proactive_scan",
        })
        # Hot-reload the fixed module so changes take effect without restart
        mod_name = Path(file_path).stem
        reload_ok, reload_msg = _hot_reload_module(mod_name)
        if reload_ok:
            logger.info("self_repair: hot-reloaded module '%s'", mod_name)
        else:
            logger.warning("self_repair: hot-reload skipped for '%s': %s", mod_name, reload_msg)
        return True
    except Exception as exc:
        logger.warning("self_repair: write failed for %s: %s", file_path, exc)
        _append_repair_log({
            "ts": datetime.now().isoformat(),
            "file": error_info["file"],
            "line": error_info.get("line", 0),
            "error_type": error_info["error_type"],
            "error_msg": error_info["error_msg"][:200],
            "confidence": 0.0,
            "diff": diff[:2000],
            "success": False,
            "backed_up": True,
            "source": "proactive_scan",
        })
        return False


# ---------------------------------------------------------------------------
# Hot-reload helper
# ---------------------------------------------------------------------------

# Modules that must NOT be hot-reloaded (entry points / bot core)
_NO_RELOAD = {"bot", "run", "config", "self_repair"}


def _hot_reload_module(module_name: str) -> tuple[bool, str]:
    """
    Attempt to hot-reload a module that was just patched.
    Returns (success, reason_msg).
    Skips entry-point and core modules to avoid disrupting the running bot.
    """
    if module_name in _NO_RELOAD:
        return False, f"skipped (core/entry-point module)"
    if module_name not in sys.modules:
        return False, "not currently loaded — will load fresh on next import"
    try:
        importlib.reload(sys.modules[module_name])
        return True, "reloaded"
    except Exception as exc:
        return False, f"reload error: {exc}"


# ---------------------------------------------------------------------------
# ProactiveSelfRepair — background scanner
# ---------------------------------------------------------------------------

class ProactiveSelfRepair:
    """Periodically scans .py files for syntax and import errors; auto-repairs."""

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False
        self._notify_fn = None  # set via set_notify_fn()
        self._last_scan_errors: list[dict] = []
        self._last_scan_time: float = 0.0

    def set_notify_fn(self, fn) -> None:
        """Register a coroutine function(msg: str) for Telegram notifications."""
        self._notify_fn = fn

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._scan_loop(), name="proactive_self_repair")
        logger.info("ProactiveSelfRepair started (interval=%ds)", SCAN_INTERVAL)

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("ProactiveSelfRepair stopped")

    async def run_scan_now(self) -> dict:
        """Run a one-shot scan immediately and return results."""
        return await self._do_scan()

    def get_health_status(self) -> dict:
        """Return last scan results and repair stats."""
        stats = _get_repair_stats()
        return {
            "last_scan_time": self._last_scan_time,
            "last_scan_errors": self._last_scan_errors,
            "repair_stats": stats,
        }

    async def _scan_loop(self) -> None:
        # Initial delay so bot finishes startup first
        await asyncio.sleep(60)
        while self._running:
            try:
                await self._do_scan()
            except Exception as exc:
                logger.error("ProactiveSelfRepair scan error: %s", exc, exc_info=True)
            try:
                await asyncio.sleep(SCAN_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _do_scan(self) -> dict:
        """Run syntax + import scan, attempt repairs. Returns scan summary."""
        self._last_scan_time = time.time()
        results = {"syntax_errors": [], "import_errors": [], "fixed": [], "installed": []}

        # --- Syntax scan ---
        syntax_errors = scan_syntax_errors()
        results["syntax_errors"] = syntax_errors
        for err in syntax_errors:
            logger.warning("ProactiveSelfRepair: SyntaxError in %s: %s", err["file"], err["error_msg"][:80])
            fixed_source = await _generate_syntax_fix(err["path"], err["error_msg"])
            if fixed_source:
                success = await _apply_syntax_fix(err["path"], fixed_source, err, self._notify_fn)
                if success:
                    results["fixed"].append(err["file"])
                    if self._notify_fn:
                        await self._notify_fn(
                            f"🔧 *自动修复成功*\n"
                            f"文件: `{err['file']}`\n"
                            f"错误: SyntaxError\n"
                            f"已备份原文件并应用修复"
                        )

        # --- Import scan (only check files with no syntax errors) ---
        broken_files = {e["file"] for e in syntax_errors}
        import_errors = [
            e for e in scan_import_errors()
            if e["file"] not in broken_files
        ]
        results["import_errors"] = import_errors
        for err in import_errors:
            pkg = err.get("missing_pkg")
            if pkg:
                logger.info("ProactiveSelfRepair: missing package '%s' in %s", pkg, err["file"])
                installed = auto_install_missing(pkg)
                if installed:
                    results["installed"].append(pkg)
                    if self._notify_fn:
                        await self._notify_fn(
                            f"📦 *自动安装依赖*\n"
                            f"包: `{pkg}`\n"
                            f"触发文件: `{err['file']}`"
                        )
                else:
                    if self._notify_fn:
                        await self._notify_fn(
                            f"⚠️ *依赖安装失败*\n"
                            f"包: `{pkg}`\n"
                            f"文件: `{err['file']}`\n"
                            f"请手动运行: `pip install {pkg}`"
                        )

        self._last_scan_errors = syntax_errors + import_errors
        if syntax_errors or import_errors:
            logger.info(
                "ProactiveSelfRepair scan: %d syntax, %d import errors; fixed=%s installed=%s",
                len(syntax_errors), len(import_errors), results["fixed"], results["installed"],
            )
        else:
            logger.debug("ProactiveSelfRepair scan: all clean")
        return results


# Module-level singleton
proactive_repair = ProactiveSelfRepair()


# ---------------------------------------------------------------------------
# Code Evolution Engine — p3_17
# ---------------------------------------------------------------------------

_EVO_LOG = BOT_DIR / "_evolution_log.jsonl"
_PROPOSAL_FILE = BOT_DIR / "improvement_proposal.json"
_STAGING_DIR = BOT_DIR / "_patch_staging"

# Per-module timing registry: module_name -> list of elapsed seconds (rolling)
_module_timings: dict[str, list[float]] = {}
_module_errors: dict[str, int] = {}
_timing_lock: asyncio.Lock | None = None  # created lazily in async context
_MAX_MODULE_KEYS = 200  # cap distinct module names to prevent unbounded dict growth


def record_module_timing(module: str, elapsed_s: float) -> None:
    """Call from any module to report its response time."""
    bucket = _module_timings.setdefault(module, [])
    bucket.append(elapsed_s)
    if len(bucket) > 500:
        bucket[:] = bucket[-500:]
    # Cap total distinct modules
    if len(_module_timings) > _MAX_MODULE_KEYS:
        # Remove modules with fewest samples
        by_size = sorted(_module_timings, key=lambda k: len(_module_timings[k]))
        for k in by_size[:len(_module_timings) - _MAX_MODULE_KEYS]:
            del _module_timings[k]


def record_module_error(module: str) -> None:
    """Call from any module to report an error."""
    _module_errors[module] = _module_errors.get(module, 0) + 1
    # Cap total distinct modules
    if len(_module_errors) > _MAX_MODULE_KEYS:
        by_count = sorted(_module_errors, key=_module_errors.get)
        for k in by_count[:len(_module_errors) - _MAX_MODULE_KEYS]:
            del _module_errors[k]


_MAX_EVO_LOG_LINES = 2000


def _append_evo_log(record: dict) -> None:
    try:
        _EVO_LOG.parent.mkdir(exist_ok=True)
        with _EVO_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        # Truncate if too many lines
        try:
            with _EVO_LOG.open("r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > _MAX_EVO_LOG_LINES:
                with _EVO_LOG.open("w", encoding="utf-8") as f:
                    f.writelines(lines[-_MAX_EVO_LOG_LINES:])
        except Exception:
            pass
    except Exception as exc:
        logger.warning("evo_log write failed: %s", exc)


def _read_evo_log_week() -> list[dict]:
    """Return evolution log entries from the last 7 days."""
    records = []
    if not _EVO_LOG.exists():
        return records
    cutoff = datetime.now().timestamp() - 7 * 86400
    try:
        with _EVO_LOG.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    ts_str = rec.get("ts", "")
                    ts = datetime.fromisoformat(ts_str).timestamp() if ts_str else 0
                    if ts >= cutoff:
                        records.append(rec)
                except Exception:
                    pass
    except Exception as exc:
        logger.warning("evo_log read failed: %s", exc)
    return records


def _collect_module_profiles() -> list[dict]:
    """
    Build per-module performance profiles from:
    1. Timing registry (recorded by modules)
    2. Error counts (recorded by modules + repair log)
    3. repair_log per-file error counts
    """
    # Count errors per file from repair log
    repair_errors: dict[str, int] = {}
    if REPAIR_LOG.exists():
        try:
            with REPAIR_LOG.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                        fname = rec.get("file", "")
                        if fname:
                            repair_errors[fname] = repair_errors.get(fname, 0) + 1
                    except Exception:
                        pass
        except Exception:
            pass

    # Build profile for each .py file
    profiles = []
    for path in _collect_py_files():
        mod = path.stem
        fname = path.name
        timings = _module_timings.get(mod, [])
        avg_ms = (sum(timings) / len(timings) * 1000) if timings else 0.0
        runtime_errors = _module_errors.get(mod, 0)
        repair_err = repair_errors.get(fname, 0)
        total_errors = runtime_errors + repair_err
        try:
            size_kb = path.stat().st_size / 1024
        except Exception:
            size_kb = 0.0

        # Weighted score: higher = worse (more attention needed)
        score = total_errors * 3 + (avg_ms / 100) + size_kb * 0.05
        profiles.append({
            "module": mod,
            "file": fname,
            "path": str(path),
            "avg_response_ms": round(avg_ms, 1),
            "runtime_errors": runtime_errors,
            "repair_errors": repair_err,
            "total_errors": total_errors,
            "size_kb": round(size_kb, 1),
            "score": round(score, 2),
        })

    profiles.sort(key=lambda x: x["score"], reverse=True)
    return profiles


def _build_proposal(profiles: list[dict]) -> dict:
    """Pick top candidates and build improvement_proposal.json."""
    # Exclude trivially uninteresting files
    skip = {"__init__", "config", "run"}
    candidates = [p for p in profiles if p["module"] not in skip and p["total_errors"] > 0]
    top = candidates[:3] if candidates else profiles[:3]

    proposal = {
        "generated_at": datetime.now().isoformat(),
        "top_candidates": top,
        "target": top[0] if top else None,
    }
    try:
        with _PROPOSAL_FILE.open("w", encoding="utf-8") as f:
            json.dump(proposal, f, ensure_ascii=False, indent=2, default=str)
    except Exception as exc:
        logger.warning("evo_proposal write failed: %s", exc)
    return proposal


async def _generate_evolution_patch(module_path: str, module_name: str, issue_summary: str) -> str | None:
    """
    Ask Claude CLI to generate an optimized version of the module.
    Returns the new source code or None.
    """
    try:
        with open(module_path, "r", encoding="utf-8", errors="replace") as f:
            source = f.read()
    except Exception as exc:
        logger.warning("evo_patch: cannot read %s: %s", module_path, exc)
        return None

    prompt = (
        f"You are optimizing a Python module for a Telegram bot.\n"
        f"Module: {module_name}\n"
        f"Issue: {issue_summary}\n\n"
        f"Current source:\n```python\n{source[:8000]}\n```\n\n"
        f"Task: Improve this module by:\n"
        f"1. Reducing error frequency (fix common failure paths)\n"
        f"2. Adding better exception handling\n"
        f"3. Improving performance of slow paths\n"
        f"4. Keeping all existing public functions/classes intact\n\n"
        f"Return ONLY the complete improved Python source code. "
        f"No explanations, no markdown fences, no extra text. "
        f"The output must be valid Python that can be written directly to the file."
    )

    try:
        _STAGING_DIR.mkdir(exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            str(CLAUDE_CMD),
            "-p", prompt,
            "--output-format", "text",
            "--dangerously-skip-permissions",
            "--model", "claude-haiku-4-5-20251001",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(BOT_DIR),
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
        if proc.returncode != 0:
            logger.warning("evo_patch: claude CLI failed: %s", stderr.decode("utf-8", errors="replace")[:200])
            return None
        result = stdout.decode("utf-8", errors="replace").strip()
        # Strip accidental markdown fences if Claude adds them
        if result.startswith("```python"):
            result = result[9:]
        if result.startswith("```"):
            result = result[3:]
        if result.endswith("```"):
            result = result[:-3]
        return result.strip() or None
    except asyncio.TimeoutError:
        logger.warning("evo_patch: claude CLI timed out for %s", module_name)
        return None
    except Exception as exc:
        logger.warning("evo_patch: unexpected error for %s: %s", module_name, exc)
        return None


def _syntax_check(source: str) -> tuple[bool, str]:
    """Returns (ok, error_msg). Checks with ast.parse + py_compile temp file."""
    import ast
    try:
        ast.parse(source)
    except SyntaxError as se:
        return False, str(se)
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8") as tf:
            tf.write(source)
            tmp_path = tf.name
        py_compile.compile(tmp_path, doraise=True)
        os.unlink(tmp_path)
    except py_compile.PyCompileError as pce:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        return False, str(pce)
    except Exception as exc:
        return False, str(exc)
    return True, ""


def _get_module_error_rate(module: str, window_s: float = 600) -> float:
    """Return errors/minute for a module over the last window_s seconds, from evo log."""
    count = _module_errors.get(module, 0)
    if window_s <= 0:
        return 0.0
    return count / (window_s / 60)


class CodeEvolutionEngine:
    """
    Hourly self-evolution engine:
    1. Profile all modules — find worst performers
    2. Generate improvement_proposal.json
    3. Ask Claude CLI for optimized patch
    4. Stage in _patch_staging/, syntax-check, hot-replace
    5. Monitor for error rate spike — rollback if >20% worse
    """

    PROFILE_INTERVAL = 3600  # seconds between evolution runs
    MONITOR_PERIOD = 600     # seconds to watch after applying a patch

    _MAX_PATCH_TRACKING = 100

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None
        self._running = False
        self._notify_fn = None
        # Track error baselines per module (at patch time), capped
        self._patch_baselines: dict[str, int] = {}
        self._patch_backups: dict[str, str] = {}  # module -> backup path

    def set_notify_fn(self, fn) -> None:
        self._notify_fn = fn

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._evo_loop(), name="code_evolution_engine")
        logger.info("CodeEvolutionEngine started (interval=%ds)", self.PROFILE_INTERVAL)

    async def stop(self) -> None:
        self._running = False
        for t in (self._task, self._monitor_task):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
        self._task = None
        self._monitor_task = None

    async def run_now(self) -> dict:
        """Trigger one evolution cycle immediately."""
        return await self._do_evolve()

    async def _evo_loop(self) -> None:
        # Wait 5 minutes after startup before first run
        await asyncio.sleep(300)
        while self._running:
            try:
                await self._do_evolve()
            except Exception as exc:
                logger.error("CodeEvolutionEngine cycle error: %s", exc, exc_info=True)
            try:
                await asyncio.sleep(self.PROFILE_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _do_evolve(self) -> dict:
        result = {"status": "no_action", "target": None, "applied": False, "rolled_back": False}

        # 1. Profile
        profiles = _collect_module_profiles()
        proposal = _build_proposal(profiles)
        target = proposal.get("target")

        if not target or target["total_errors"] == 0:
            logger.debug("CodeEvolutionEngine: no error-prone modules found, skipping")
            result["status"] = "nothing_to_improve"
            return result

        result["target"] = target["module"]
        issue = (
            f"{target['total_errors']} errors, "
            f"avg {target['avg_response_ms']}ms response time, "
            f"{target['size_kb']}KB"
        )
        logger.info("CodeEvolutionEngine: targeting %s (%s)", target["module"], issue)

        if self._notify_fn:
            await self._notify_fn(
                f"🧬 *自进化引擎启动*\n"
                f"目标模块: `{target['module']}`\n"
                f"问题: {issue}\n"
                f"正在生成优化补丁..."
            )

        # 2. Generate patch via Claude CLI
        patch_source = await _generate_evolution_patch(target["path"], target["module"], issue)
        if not patch_source:
            logger.warning("CodeEvolutionEngine: patch generation failed for %s", target["module"])
            result["status"] = "patch_generation_failed"
            _append_evo_log({
                "ts": datetime.now().isoformat(),
                "module": target["module"],
                "issue": issue,
                "applied": False,
                "rolled_back": False,
                "reason": "patch_generation_failed",
            })
            return result

        # 3. Syntax check
        ok, err_msg = _syntax_check(patch_source)
        if not ok:
            logger.warning("CodeEvolutionEngine: patch for %s failed syntax check: %s", target["module"], err_msg)
            result["status"] = "syntax_check_failed"
            _append_evo_log({
                "ts": datetime.now().isoformat(),
                "module": target["module"],
                "issue": issue,
                "applied": False,
                "rolled_back": False,
                "reason": f"syntax_check_failed: {err_msg[:100]}",
            })
            if self._notify_fn:
                await self._notify_fn(
                    f"❌ *补丁语法检查失败*\n"
                    f"模块: `{target['module']}`\n"
                    f"错误: {err_msg[:120]}"
                )
            return result

        # 4. Stage patch
        _STAGING_DIR.mkdir(exist_ok=True)
        staged_path = _STAGING_DIR / f"{target['module']}.py"
        try:
            with staged_path.open("w", encoding="utf-8") as f:
                f.write(patch_source)
        except Exception as exc:
            logger.warning("CodeEvolutionEngine: failed to write staged patch: %s", exc)
            result["status"] = "staging_failed"
            return result

        # 5. Backup + hot-replace
        backup_path = str(target["path"]) + f".evobak.{int(time.time())}"
        try:
            with open(target["path"], "r", encoding="utf-8") as f:
                original = f.read()
            with open(backup_path, "w", encoding="utf-8") as f:
                f.write(original)
        except Exception as exc:
            logger.warning("CodeEvolutionEngine: backup failed for %s: %s", target["module"], exc)
            result["status"] = "backup_failed"
            return result

        self._patch_backups[target["module"]] = backup_path
        self._patch_baselines[target["module"]] = _module_errors.get(target["module"], 0)
        # Cap tracking dicts to prevent unbounded growth
        for d in (self._patch_backups, self._patch_baselines):
            if len(d) > self._MAX_PATCH_TRACKING:
                oldest_keys = list(d.keys())[:len(d) - self._MAX_PATCH_TRACKING]
                for k in oldest_keys:
                    del d[k]

        tmp = target["path"] + ".evotmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(patch_source)
            os.replace(tmp, target["path"])
        except Exception as exc:
            logger.warning("CodeEvolutionEngine: hot-replace failed for %s: %s", target["module"], exc)
            result["status"] = "replace_failed"
            _append_evo_log({
                "ts": datetime.now().isoformat(),
                "module": target["module"],
                "issue": issue,
                "applied": False,
                "rolled_back": False,
                "reason": f"replace_failed: {exc}",
            })
            return result

        result["applied"] = True
        result["status"] = "applied"
        logger.info("CodeEvolutionEngine: patch applied to %s", target["module"])

        if self._notify_fn:
            await self._notify_fn(
                f"✅ *补丁已应用*\n"
                f"模块: `{target['module']}`\n"
                f"备份: `{Path(backup_path).name}`\n"
                f"监控中 ({self.MONITOR_PERIOD}s)..."
            )

        # 6. Monitor for rollback
        rollback = await self._monitor_for_rollback(target["module"], issue)
        result["rolled_back"] = rollback

        _append_evo_log({
            "ts": datetime.now().isoformat(),
            "module": target["module"],
            "issue": issue,
            "applied": True,
            "rolled_back": rollback,
            "reason": "rollback_triggered" if rollback else "success",
            "backup_path": backup_path,
        })
        return result

    async def _monitor_for_rollback(self, module: str, issue: str) -> bool:
        """
        Watch module for MONITOR_PERIOD seconds.
        If error rate rises >20% above baseline, rollback.
        """
        baseline = self._patch_baselines.get(module, 0)
        check_interval = 60  # check every minute
        checks = self.MONITOR_PERIOD // check_interval

        for _ in range(checks):
            try:
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError:
                break

            current_errors = _module_errors.get(module, 0)
            new_errors = current_errors - baseline
            if new_errors > max(baseline * 0.20, 2):
                # Error rate spiked — rollback
                await self._do_rollback(module, issue, new_errors)
                return True

        logger.info("CodeEvolutionEngine: %s stable after patch, keeping", module)
        if self._notify_fn:
            await self._notify_fn(
                f"✅ *进化验证通过*\n"
                f"模块 `{module}` 监控期内稳定，补丁已保留"
            )
        return False

    async def _do_rollback(self, module: str, issue: str, new_errors: int) -> None:
        backup = self._patch_backups.get(module)
        if not backup or not Path(backup).exists():
            logger.error("CodeEvolutionEngine: cannot rollback %s — no backup", module)
            return

        module_path = str(BOT_DIR / f"{module}.py")
        try:
            with open(backup, "r", encoding="utf-8") as f:
                original = f.read()
            with open(module_path, "w", encoding="utf-8") as f:
                f.write(original)
            logger.warning("CodeEvolutionEngine: rolled back %s (new_errors=%d)", module, new_errors)
            if self._notify_fn:
                await self._notify_fn(
                    f"⏮️ *自动回滚*\n"
                    f"模块: `{module}`\n"
                    f"原因: 错误率升高 {new_errors} 次\n"
                    f"已还原备份版本"
                )
        except Exception as exc:
            logger.error("CodeEvolutionEngine: rollback write failed for %s: %s", module, exc)


# Module-level singleton for the evolution engine
code_evolution_engine = CodeEvolutionEngine()


# ---------------------------------------------------------------------------
# /evostatus formatting helper
# ---------------------------------------------------------------------------

def format_evostatus() -> str:
    """Format this week's self-evolution stats for Telegram."""
    records = _read_evo_log_week()

    total = len(records)
    applied = sum(1 for r in records if r.get("applied"))
    rolled_back = sum(1 for r in records if r.get("rolled_back"))
    success = applied - rolled_back
    success_rate = (success / applied * 100) if applied else 0.0

    # Best improvement = module with most errors that was successfully patched
    best = None
    for r in records:
        if r.get("applied") and not r.get("rolled_back"):
            if best is None or r.get("issue", "").count("errors") > 0:
                best = r

    lines = ["🧬 *代码自进化状态 (本周)*\n"]
    lines.append(f"📊 进化总次数: {total}")
    lines.append(f"✅ 成功应用: {applied}")
    lines.append(f"⏮️ 已回滚: {rolled_back}")
    lines.append(f"🎯 净成功: {success} ({success_rate:.0f}%)")

    if best:
        lines.append(f"\n🏆 *最大改进*")
        lines.append(f"  模块: `{best.get('module', '?')}`")
        lines.append(f"  问题: {best.get('issue', '?')[:80]}")
        ts = best.get("ts", "")[:19].replace("T", " ")
        lines.append(f"  时间: {ts}")

    # Show proposal if exists
    if _PROPOSAL_FILE.exists():
        try:
            with _PROPOSAL_FILE.open("r", encoding="utf-8") as f:
                prop = json.load(f)
            tgt = prop.get("target")
            if tgt:
                lines.append(f"\n🔍 *当前目标模块*")
                lines.append(f"  `{tgt['module']}` — 得分 {tgt['score']}, 错误 {tgt['total_errors']}")
                gen_at = prop.get("generated_at", "")[:19].replace("T", " ")
                lines.append(f"  分析时间: {gen_at}")
        except Exception:
            pass

    # Recent activity
    if records:
        lines.append(f"\n📋 *最近进化记录* (最新5条):")
        for rec in records[-5:][::-1]:
            ts = rec.get("ts", "")[:19].replace("T", " ")
            mod = rec.get("module", "?")
            applied_sym = "✅" if rec.get("applied") else "❌"
            rolled = " ⏮️回滚" if rec.get("rolled_back") else ""
            lines.append(f"  {applied_sym} `{ts}` `{mod}`{rolled}")
    else:
        lines.append("\n📋 本周暂无进化记录")

    result = "\n".join(lines)
    # Truncate for Telegram's 4096 char limit
    if len(result) > 4000:
        result = result[:4000] + "\n... (truncated)"
    return result


# ---------------------------------------------------------------------------
# /repair_status formatting helper
# ---------------------------------------------------------------------------

def format_repair_status(n_recent: int = 8) -> str:
    """Format a human-readable repair status report for Telegram."""
    health = proactive_repair.get_health_status()
    stats = health["repair_stats"]
    errors = health["last_scan_errors"]
    last_scan = health["last_scan_time"]

    lines = ["🔧 *代码自修复状态*\n"]

    # Repair statistics
    total = stats["total"]
    ok = stats["ok"]
    rate = stats["rate"]
    if total == 0:
        lines.append("📊 修复历史: 暂无记录")
    else:
        rate_str = f"{rate:.0%}" if rate is not None else "N/A"
        gate = "✅ 自动修复已启用" if (rate is None or rate >= MIN_AUTO_APPLY_RATE) else "⚠️ 自动修复暂停 (成功率过低)"
        lines.append(f"📊 历史修复: {ok}/{total} 成功 ({rate_str}) — {gate}")

    # Last scan
    if last_scan:
        ago = int(time.time() - last_scan)
        lines.append(f"🕐 上次扫描: {ago}秒前")
    else:
        lines.append("🕐 上次扫描: 尚未执行")

    # Current errors found in last scan
    if errors:
        lines.append(f"\n⚠️ *当前问题* ({len(errors)} 个):")
        for e in errors[:5]:
            lines.append(f"  • `{e['file']}` — {e['error_type']}: {e['error_msg'][:60]}")
    else:
        lines.append("\n✅ 代码健康: 无语法/导入错误")

    # Recent repairs
    if total > 0:
        lines.append(f"\n📋 *最近修复记录* (最新{min(n_recent, total)}条):")
        records: list[dict] = []
        try:
            if REPAIR_LOG.exists():
                with REPAIR_LOG.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                records.append(json.loads(line))
                            except Exception:
                                pass
        except Exception:
            pass
        for rec in records[-n_recent:][::-1]:
            ts = rec.get("ts", "?")[:19].replace("T", " ")
            ok_sym = "✅" if rec.get("success") else "❌"
            etype = rec.get("error_type", "?")
            fname = rec.get("file", "?")
            conf = rec.get("confidence", 0)
            skip = rec.get("skipped_reason", "")
            extra = f" [{skip}]" if skip else f" conf={conf:.0%}"
            lines.append(f"  {ok_sym} `{ts}` `{fname}` {etype}{extra}")

    result = "\n".join(lines)
    # Truncate for Telegram's 4096 char limit
    if len(result) > 4000:
        result = result[:4000] + "\n... (truncated)"
    return result


# ---------------------------------------------------------------------------
# Code Quality Analysis Engine — p3_22
# ---------------------------------------------------------------------------

_QUALITY_REPORT = BOT_DIR / ".code_quality_report.json"
_PENDING_PATCHES_DIR = BOT_DIR / ".pending_patches"


def _analyze_single_file(path: Path) -> dict:
    """Analyze one .py file for quality metrics. Returns dict with score 0-100."""
    import ast
    import re

    try:
        source = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return {
            "file": path.name, "score": 0,
            "todo_count": 0, "avg_func_lines": 0,
            "bare_excepts": 0, "dup_blocks": 0,
            "func_count": 0, "line_count": 0,
            "error": str(exc),
        }

    lines = source.splitlines()
    line_count = len(lines)

    # 1. TODO / FIXME / HACK count
    todo_count = len(re.findall(r'\b(TODO|FIXME|HACK|XXX)\b', source))

    # 2. AST-based metrics
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return {
            "file": path.name, "score": 0,
            "todo_count": todo_count, "avg_func_lines": 0,
            "bare_excepts": 0, "dup_blocks": 0,
            "func_count": 0, "line_count": line_count,
            "error": "syntax_error",
        }

    functions = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    func_line_counts = []
    for fn in functions:
        end = getattr(fn, "end_lineno", fn.lineno)
        func_line_counts.append(end - fn.lineno + 1)
    avg_func_lines = sum(func_line_counts) / len(func_line_counts) if func_line_counts else 0

    # 3. Exception handling: bare except (except: without type)
    bare_excepts = sum(
        1 for node in ast.walk(tree)
        if isinstance(node, ast.ExceptHandler) and node.type is None
    )

    # 4. Duplicate 3-line blocks (simplified check)
    chunk_counts: dict[str, int] = {}
    for i in range(len(lines) - 2):
        chunk = "\n".join(lines[i:i + 3]).strip()
        if len(chunk) > 40:
            chunk_counts[chunk] = chunk_counts.get(chunk, 0) + 1
    dup_blocks = sum(1 for v in chunk_counts.values() if v > 1)

    # Score: start at 100, deduct for issues
    score = 100
    score -= min(todo_count * 5, 20)                     # up to -20 for TODOs
    score -= min(bare_excepts * 5, 15)                   # up to -15 for bare excepts
    score -= min(max(avg_func_lines - 30, 0) * 0.5, 15) # up to -15 for long functions
    score -= min(dup_blocks * 3, 20)                     # up to -20 for duplicates
    score = max(0, min(100, int(score)))

    return {
        "file": path.name,
        "score": score,
        "todo_count": todo_count,
        "avg_func_lines": round(avg_func_lines, 1),
        "bare_excepts": bare_excepts,
        "dup_blocks": dup_blocks,
        "func_count": len(functions),
        "line_count": line_count,
    }


def analyze_code_quality() -> dict:
    """
    Scan all .py files, generate quality report, save to .code_quality_report.json.
    Returns the full report dict.
    """
    py_files = _collect_py_files()
    results = []
    for path in py_files:
        metrics = _analyze_single_file(path)
        metrics["path"] = str(path)
        results.append(metrics)

    results.sort(key=lambda x: x["score"])  # worst first

    low_quality = [r for r in results if r["score"] < 60]
    avg_score = round(sum(r["score"] for r in results) / len(results), 1) if results else 0

    report = {
        "generated_at": datetime.now().isoformat(),
        "file_count": len(results),
        "avg_score": avg_score,
        "low_quality_files": low_quality,
        "files": results,
    }

    try:
        tmp = str(_QUALITY_REPORT) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        os.replace(tmp, str(_QUALITY_REPORT))
        logger.info(
            "analyze_code_quality: %d files, avg=%.1f, low_quality=%d",
            len(results), avg_score, len(low_quality),
        )
    except Exception as exc:
        logger.warning("analyze_code_quality: failed to save report: %s", exc)

    return report


async def generate_quality_patches(notify_fn=None) -> list[str]:
    """
    For each file with quality score < 60, generate an improvement prompt
    via CodexCharger and save the suggestion to .pending_patches/.
    Returns list of patch filenames written.
    """
    if not _QUALITY_REPORT.exists():
        analyze_code_quality()

    try:
        report = json.loads(_QUALITY_REPORT.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("generate_quality_patches: cannot read report: %s", exc)
        return []

    low_quality = report.get("low_quality_files", [])
    if not low_quality:
        logger.info("generate_quality_patches: no low-quality files, skipping")
        return []

    _PENDING_PATCHES_DIR.mkdir(exist_ok=True)

    from codex_charger import CodexCharger
    charger = CodexCharger()
    written: list[str] = []

    for file_info in low_quality:
        file_path = file_info.get("path", "")
        file_name = file_info.get("file", "")
        score = file_info.get("score", 0)
        if not file_path or not Path(file_path).exists():
            continue

        try:
            source = Path(file_path).read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue

        issues = []
        if file_info.get("todo_count", 0) > 0:
            issues.append(f"{file_info['todo_count']} TODO/FIXME comments")
        if file_info.get("bare_excepts", 0) > 0:
            issues.append(f"{file_info['bare_excepts']} bare except clauses")
        if file_info.get("avg_func_lines", 0) > 30:
            issues.append(f"avg function length {file_info['avg_func_lines']} lines (too long)")
        if file_info.get("dup_blocks", 0) > 0:
            issues.append(f"{file_info['dup_blocks']} duplicate code blocks")

        issue_summary = "; ".join(issues) if issues else f"quality score {score}/100"

        prompt = (
            f"Code quality analysis found issues in `{file_name}` (score: {score}/100).\n"
            f"Issues: {issue_summary}\n\n"
            f"Current source:\n```python\n{source[:6000]}\n```\n\n"
            f"Please provide specific, actionable improvements to fix these quality issues:\n"
            f"1. Replace TODO comments with actual implementations or remove them\n"
            f"2. Replace bare `except:` with typed exceptions\n"
            f"3. Break long functions into smaller focused ones\n"
            f"4. Extract duplicate blocks into reusable helpers\n\n"
            f"Return the complete improved Python source code only, no explanations."
        )

        logger.info("generate_quality_patches: requesting improvement for %s (score=%d)", file_name, score)
        if notify_fn:
            await notify_fn(
                f"🔍 *代码质量分析*\n"
                f"文件: `{file_name}` (得分: {score}/100)\n"
                f"问题: {issue_summary[:120]}\n"
                f"正在生成改进建议..."
            )

        try:
            result = await asyncio.wait_for(charger.run_task(prompt), timeout=300)
        except asyncio.TimeoutError:
            logger.warning("generate_quality_patches: timeout for %s", file_name)
            continue
        except Exception as exc:
            logger.warning("generate_quality_patches: error for %s: %s", file_name, exc)
            continue

        if result.get("success") and result.get("output"):
            patch_name = f"{Path(file_name).stem}_quality_patch_{int(time.time())}.py"
            patch_path = _PENDING_PATCHES_DIR / patch_name
            meta_path = _PENDING_PATCHES_DIR / (patch_name + ".meta.json")
            try:
                patch_path.write_text(result["output"], encoding="utf-8")
                meta_path.write_text(
                    json.dumps({
                        "source_file": file_name,
                        "source_path": file_path,
                        "score": score,
                        "issues": issue_summary,
                        "generated_at": datetime.now().isoformat(),
                    }, indent=2),
                    encoding="utf-8",
                )
                written.append(patch_name)
                logger.info("generate_quality_patches: patch written → %s", patch_name)
                if notify_fn:
                    await notify_fn(
                        f"📝 *改进建议已保存*\n"
                        f"文件: `{patch_name}`\n"
                        f"位置: `.pending_patches/`\n"
                        f"使用 /code_health 查看详情"
                    )
            except Exception as exc:
                logger.warning("generate_quality_patches: write failed for %s: %s", patch_name, exc)

    return written


def format_code_health() -> str:
    """Format /code_health report: quality scores + pending patches count."""
    lines = ["🏥 *代码质量报告*\n"]

    if not _QUALITY_REPORT.exists():
        lines.append("⚠️ 尚未运行质量分析。请稍候自动运行或每日UTC 02:00定时执行。")
        return "\n".join(lines)

    try:
        report = json.loads(_QUALITY_REPORT.read_text(encoding="utf-8"))
    except Exception as exc:
        lines.append(f"❌ 报告读取失败: {exc}")
        return "\n".join(lines)

    gen_at = report.get("generated_at", "")[:19].replace("T", " ")
    file_count = report.get("file_count", 0)
    avg_score = report.get("avg_score", 0)
    low_quality = report.get("low_quality_files", [])
    all_files = report.get("files", [])

    # Count pending patches
    pending_count = 0
    if _PENDING_PATCHES_DIR.exists():
        pending_count = len(list(_PENDING_PATCHES_DIR.glob("*.py")))

    lines.append(f"📅 分析时间: {gen_at}")
    lines.append(f"📁 扫描文件: {file_count} 个")
    lines.append(f"📊 平均质量分: {avg_score}/100")
    lines.append(f"📝 待处理补丁: {pending_count} 个")

    if avg_score >= 80:
        lines.append("✅ 整体代码健康状况: 优秀")
    elif avg_score >= 60:
        lines.append("🟡 整体代码健康状况: 良好")
    else:
        lines.append("🔴 整体代码健康状况: 需要改进")

    # Low quality files
    if low_quality:
        lines.append(f"\n⚠️ *低质量文件* (分数<60, 共{len(low_quality)}个):")
        for f in low_quality[:10]:
            score = f["score"]
            name = f["file"]
            issues = []
            if f.get("todo_count"):
                issues.append(f"TODO×{f['todo_count']}")
            if f.get("bare_excepts"):
                issues.append(f"裸except×{f['bare_excepts']}")
            if f.get("avg_func_lines", 0) > 30:
                issues.append(f"函数均{f['avg_func_lines']}行")
            if f.get("dup_blocks"):
                issues.append(f"重复块×{f['dup_blocks']}")
            issue_str = " ".join(issues) if issues else ""
            lines.append(f"  🔴 `{name}` — {score}/100 {issue_str}")
    else:
        lines.append("\n✅ 所有文件质量分均≥60")

    # Top files by score
    good_files = [f for f in all_files if f["score"] >= 80]
    if good_files:
        lines.append(f"\n🏆 *高质量文件* (分数≥80, 共{len(good_files)}个):")
        for f in good_files[-5:][::-1]:
            lines.append(f"  ✅ `{f['file']}` — {f['score']}/100")

    # Pending patches detail
    if pending_count > 0:
        lines.append(f"\n📋 *待确认补丁* ({pending_count} 个):")
        for patch in sorted(_PENDING_PATCHES_DIR.glob("*.meta.json"))[-5:]:
            try:
                meta = json.loads(patch.read_text(encoding="utf-8"))
                src = meta.get("source_file", "?")
                sc = meta.get("score", "?")
                gen = meta.get("generated_at", "")[:19].replace("T", " ")
                lines.append(f"  📝 `{src}` (原始分:{sc}) — {gen}")
            except Exception:
                pass

    result = "\n".join(lines)
    # Truncate for Telegram's 4096 char limit
    if len(result) > 4000:
        result = result[:4000] + "\n... (truncated)"
    return result


class CodeQualityScheduler:
    """
    Runs daily quality analysis at UTC 02:00.
    Calls analyze_code_quality() + generate_quality_patches() for low-score files.
    """

    def __init__(self) -> None:
        self._task: asyncio.Task | None = None
        self._running = False
        self._notify_fn = None
        self._last_run: float = 0.0

    def set_notify_fn(self, fn) -> None:
        self._notify_fn = fn

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._schedule_loop(), name="code_quality_scheduler")
        logger.info("CodeQualityScheduler started (daily UTC 02:00)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def run_now(self) -> dict:
        """Trigger quality analysis immediately."""
        return await self._do_quality_run()

    async def _schedule_loop(self) -> None:
        while self._running:
            try:
                wait = self._seconds_until_utc_0200()
                wait = max(wait, 60)
                logger.debug("CodeQualityScheduler: next run in %.0fs", wait)
                try:
                    await asyncio.sleep(wait)
                except asyncio.CancelledError:
                    break
                if self._running:
                    await self._do_quality_run()
            except Exception as exc:
                logger.error("CodeQualityScheduler error: %s", exc, exc_info=True)

    def _seconds_until_utc_0200(self) -> float:
        """Return seconds until next UTC 02:00."""
        from datetime import timedelta
        now = datetime.utcnow()
        target = now.replace(hour=2, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target + timedelta(days=1)
        return max((target - now).total_seconds(), 1.0)

    async def _do_quality_run(self) -> dict:
        self._last_run = time.time()
        logger.info("CodeQualityScheduler: running daily quality analysis")

        if self._notify_fn:
            await self._notify_fn("🔬 *每日代码全量检查开始*\n正在扫描语法错误和代码质量...")

        # 1. Daily syntax scan + auto-repair (Task 27)
        syntax_results = await proactive_repair.run_scan_now()
        syntax_errs = len(syntax_results.get("syntax_errors", []))
        import_errs = len(syntax_results.get("import_errors", []))
        fixed = syntax_results.get("fixed", [])
        installed = syntax_results.get("installed", [])
        syntax_summary = (
            f"✅ 语法扫描完成: 无错误" if (syntax_errs + import_errs == 0)
            else f"⚠️ 发现 {syntax_errs} 个语法错误, {import_errs} 个导入错误"
              + (f"\n🔧 已自动修复: {', '.join(fixed)}" if fixed else "")
              + (f"\n📦 已安装依赖: {', '.join(installed)}" if installed else "")
        )
        if self._notify_fn:
            await self._notify_fn(f"🔍 *语法扫描结果*\n{syntax_summary}")

        # 2. Daily code quality analysis
        report = analyze_code_quality()
        low_count = len(report.get("low_quality_files", []))
        avg = report.get("avg_score", 0)
        fcount = report.get("file_count", 0)

        if self._notify_fn:
            await self._notify_fn(
                f"📊 *质量分析完成*\n"
                f"扫描: {fcount}个文件\n"
                f"平均分: {avg}/100\n"
                f"低质量文件(<60分): {low_count}个"
            )

        patches_written: list[str] = []
        if low_count > 0:
            patches_written = await generate_quality_patches(self._notify_fn)

        return {"report": report, "patches_written": patches_written}


# Module-level singleton
code_quality_scheduler = CodeQualityScheduler()
