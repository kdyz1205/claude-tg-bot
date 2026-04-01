"""
Auto development loop: Claude → temp file → py_compile (+ optional pytest) → retry → commit.
"""

from __future__ import annotations

import ast
import asyncio
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

BOT_DIR = Path(__file__).resolve().parent.parent
HARNESS_DIR = BOT_DIR / "harness"
STAGING_DIR = BOT_DIR / "_autodev_staging"
MAX_RETRIES = 5

CODE_FENCE = re.compile(r"```(?:python)?\s*([\s\S]*?)```", re.I)


@dataclass
class AutoDevResult:
    success: bool
    target_path: str
    attempts: int
    last_error: str = ""
    messages: List[str] = field(default_factory=list)


def _safe_rel_path(raw: str) -> Path:
    p = Path(raw.strip().replace("\\", "/"))
    if p.is_absolute() or ".." in p.parts:
        raise ValueError("Path must be relative and cannot contain '..'")
    return p


def _extract_python(response_text: str) -> str:
    m = CODE_FENCE.search(response_text)
    if m:
        return m.group(1).strip()
    t = response_text.strip()
    if t.startswith("def ") or t.startswith("import ") or t.startswith('"""') or t.startswith("class "):
        return t
    raise ValueError("No Python code block found in model response")


def _syntax_ok(src: str) -> tuple[bool, str]:
    try:
        ast.parse(src)
        return True, ""
    except SyntaxError as e:
        return False, f"{e.msg} (line {e.lineno})"


async def _call_claude(system: str, user: str) -> str:
    import config

    if not getattr(config, "ANTHROPIC_API_KEY", None):
        raise RuntimeError("ANTHROPIC_API_KEY not set")
    import anthropic

    client = anthropic.AsyncAnthropic(
        api_key=config.ANTHROPIC_API_KEY,
        timeout=120.0,
    )
    model = getattr(config, "CLAUDE_MODEL", "claude-sonnet-4-20250514")
    resp = await client.messages.create(
        model=model,
        max_tokens=8192,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    parts = [
        b.text
        for b in resp.content
        if getattr(b, "type", None) == "text" and getattr(b, "text", None)
    ]
    return "\n".join(parts).strip()


def _harness_validate(py_src: str) -> tuple[bool, str]:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    probe = HARNESS_DIR / "_auto_dev_probe.py"
    try:
        probe.write_text(py_src, encoding="utf-8")
    except OSError as e:
        return False, f"write probe failed: {e}"
    try:
        r = subprocess.run(
            [sys.executable, "-m", "py_compile", str(probe)],
            cwd=str(HARNESS_DIR),
            capture_output=True,
            text=True,
            timeout=90,
        )
        if r.returncode != 0:
            return False, (r.stderr or r.stdout or "py_compile failed")[:4000]
        tests_dir = HARNESS_DIR / "tests"
        if tests_dir.is_dir():
            r2 = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    str(tests_dir),
                    "-q",
                    "--tb=line",
                    "-x",
                ],
                cwd=str(BOT_DIR),
                capture_output=True,
                text=True,
                timeout=180,
            )
            if r2.returncode != 0:
                return False, (r2.stdout + r2.stderr)[:4000]
        return True, ""
    finally:
        try:
            probe.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            if probe.with_suffix(".pyc").exists():
                probe.with_suffix(".pyc").unlink()
        except OSError:
            pass
        for c in HARNESS_DIR.glob("__pycache__/_auto_dev_probe.*"):
            try:
                c.unlink()
            except OSError:
                pass


class AutoDevOrchestrator:
    """Generate code via Claude, validate, then atomically write into repo."""

    def __init__(self, repo_root: Optional[Path] = None) -> None:
        self.repo_root = repo_root or BOT_DIR

    async def run(self, task_goal: str, target_rel_path: str) -> AutoDevResult:
        rel = _safe_rel_path(target_rel_path)
        dest = (self.repo_root / rel).resolve()
        try:
            dest.relative_to(self.repo_root.resolve())
        except ValueError:
            return AutoDevResult(False, str(rel), 0, "Path escapes repo root")

        system = (
            "You are an expert Python engineer. Output exactly one markdown "
            "fenced python code block. No prose outside the fence. "
            "The code must be syntactically valid and runnable in isolation "
            "unless the user asked for a library module."
        )
        feedback = ""
        messages: List[str] = []
        for attempt in range(1, MAX_RETRIES + 1):
            user = f"Task:\n{task_goal}\n\nTarget file (relative): {rel.as_posix()}\n"
            if feedback:
                user += f"\nPrevious validation error — fix it:\n{feedback}\n"
            try:
                raw = await _call_claude(system, user)
            except Exception as e:
                messages.append(f"attempt {attempt}: API {e}")
                feedback = str(e)
                await asyncio.sleep(2 * attempt)
                continue
            try:
                code = _extract_python(raw)
            except ValueError as e:
                feedback = str(e)
                messages.append(f"attempt {attempt}: {feedback}")
                continue
            ok_ast, ast_err = _syntax_ok(code)
            if not ok_ast:
                feedback = ast_err
                messages.append(f"attempt {attempt}: ast {ast_err}")
                continue
            ok_h, h_err = await asyncio.to_thread(_harness_validate, code)
            if not ok_h:
                feedback = h_err
                messages.append(f"attempt {attempt}: harness {h_err[:500]}")
                continue

            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(dest.suffix + ".autodev.tmp")
            try:
                tmp.write_text(code, encoding="utf-8")
                tmp.replace(dest)
            except OSError as e:
                return AutoDevResult(False, str(rel), attempt, str(e), messages)
            messages.append(f"attempt {attempt}: wrote {rel}")
            return AutoDevResult(True, str(rel), attempt, "", messages)

        return AutoDevResult(False, str(rel), MAX_RETRIES, feedback or "max retries", messages)
