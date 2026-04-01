"""
Breakpoint snapshots for self_repair / code evolution.

Before LLM-applied writes, originals are copied under the OS temp area
(/tmp/claude_tg_bot_repair on Unix when writable, else tempfile.gettempdir()).
Failed py_compile or pytest triggers immediate rollback + human takeover signal.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# file_path -> unix timestamp when cooldown ends
_cooldown_until: dict[str, float] = {}


def get_rollback_cooldown_sec() -> int:
    try:
        return max(60, int(os.environ.get("SELF_REPAIR_ROLLBACK_COOLDOWN_SEC", "3600")))
    except ValueError:
        return 3600


def pytest_enabled() -> bool:
    return os.environ.get("SELF_REPAIR_RUN_PYTEST", "1").lower() not in ("0", "false", "no")


def get_pytest_timeout_sec() -> int:
    try:
        return max(30, int(os.environ.get("SELF_REPAIR_PYTEST_TIMEOUT", "180")))
    except ValueError:
        return 180


def get_snapshot_root() -> Path:
    override = os.environ.get("SELF_REPAIR_SNAPSHOT_DIR", "").strip()
    if override:
        root = Path(override)
    elif os.name != "nt":
        root = Path("/tmp") / "claude_tg_bot_repair"
        try:
            root.mkdir(parents=True, exist_ok=True)
            test = root / ".write_test"
            test.write_text("ok", encoding="utf-8")
            test.unlink(missing_ok=True)
            return root
        except OSError:
            root = Path(tempfile.gettempdir()) / "claude_tg_bot_repair"
    else:
        root = Path(tempfile.gettempdir()) / "claude_tg_bot_repair"
    root.mkdir(parents=True, exist_ok=True)
    return root


def is_repair_cooldown(file_path: str) -> bool:
    deadline = _cooldown_until.get(str(Path(file_path).resolve()))
    return deadline is not None and time.time() < deadline


def record_rollback_cooldown(file_path: str) -> None:
    _cooldown_until[str(Path(file_path).resolve())] = time.time() + get_rollback_cooldown_sec()


def clear_repair_cooldown(file_path: str) -> None:
    _cooldown_until.pop(str(Path(file_path).resolve()), None)


def create_snapshot_for_path(target: Path, content: Optional[str] = None) -> Optional[Path]:
    """
    Write a copy of `target` (or `content`) into the snapshot root.
    Returns path to the .py snapshot file, or None on failure.
    """
    try:
        resolved = target.resolve()
        data = content if content is not None else resolved.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        logger.warning("repair_snapshot: cannot read %s: %s", target, e)
        return None

    root = get_snapshot_root()
    root.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    safe_name = resolved.name.replace("/", "_")[:120]
    h = abs(hash(str(resolved))) % 1_000_000
    snap = root / f"{ts}_{safe_name}_{h}.py"
    meta = root / f"{snap.name}.meta.json"
    try:
        snap.write_text(data, encoding="utf-8")
        meta.write_text(
            json.dumps(
                {
                    "original": str(resolved),
                    "created_ts": ts,
                    "size": len(data.encode("utf-8")),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        logger.info("repair_snapshot: saved %s → %s", resolved.name, snap)
        return snap
    except OSError as e:
        logger.error("repair_snapshot: write failed: %s", e)
        return None


def read_snapshot_original_path(snap: Path) -> Optional[Path]:
    meta = snap.parent / f"{snap.name}.meta.json"
    try:
        d = json.loads(meta.read_text(encoding="utf-8"))
        p = d.get("original")
        return Path(p) if p else None
    except Exception:
        return None


def restore_snapshot_to_target(snap: Path, target: Optional[Path] = None) -> tuple[bool, str]:
    """Copy snapshot bytes back to original path (from meta if target omitted)."""
    try:
        text = snap.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return False, f"read snapshot: {e}"

    dest = target or read_snapshot_original_path(snap)
    if not dest:
        return False, "no target path in meta"
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(dest) + ".rollback.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(dest))
        logger.warning("repair_snapshot: rolled back %s from %s", dest, snap.name)
        return True, ""
    except OSError as e:
        return False, str(e)


def verify_py_compile_file(abs_path: str, timeout: float = 60.0) -> tuple[bool, str]:
    try:
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", abs_path],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if r.returncode != 0:
            msg = (r.stderr or r.stdout or "py_compile failed").strip()
            return False, msg[:1200]
        return True, ""
    except subprocess.TimeoutExpired:
        return False, "py_compile timeout"
    except Exception as e:
        return False, str(e)[:500]


def run_pytest_sandbox(bot_dir: Path) -> tuple[bool, str]:
    if not pytest_enabled():
        return True, "(pytest skipped)"
    to = get_pytest_timeout_sec()
    tests_dir = bot_dir / "tests"
    if not tests_dir.is_dir():
        return True, "(no tests/ dir)"
    try:
        r = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                str(tests_dir),
                "-q",
                "--tb=line",
                "-x",
            ],
            capture_output=True,
            text=True,
            timeout=to,
            cwd=str(bot_dir),
        )
        out = (r.stdout or "") + "\n" + (r.stderr or "")
        if r.returncode != 0:
            return False, out.strip()[-2000:] or f"pytest exit {r.returncode}"
        return True, ""
    except subprocess.TimeoutExpired:
        return False, f"pytest timed out after {to}s"
    except Exception as e:
        return False, str(e)[:500]


def verify_after_repair(file_path: Path, bot_dir: Path) -> tuple[bool, str]:
    ok, msg = verify_py_compile_file(str(file_path))
    if not ok:
        return False, f"compile: {msg}"
    ok2, msg2 = run_pytest_sandbox(bot_dir)
    if not ok2:
        return False, f"pytest: {msg2}"
    return True, ""


def human_takeover_message(
    *,
    file_name: str,
    reason: str,
    snapshot_name: str,
) -> str:
    return (
        "🚨 *自我修复已回滚 — 需人工接管*\n"
        f"文件: `{file_name}`\n"
        f"验证失败: {reason[:500]}\n"
        f"快照文件: `{snapshot_name}`\n"
        f"该路径已进入 {get_rollback_cooldown_sec() // 60} 分钟自动修复冷却，请勿让机器人连环改写。"
    )


def snapshot_dir_status() -> tuple[str, int]:
    """For diagnose.py: root path and count of .py snapshots."""
    root = get_snapshot_root()
    try:
        n = len(list(root.glob("*.py")))
    except OSError:
        n = -1
    return str(root), n
