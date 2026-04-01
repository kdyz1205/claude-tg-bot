"""
Async subprocess bridge: run local `claude -p "<prompt>"` from repo root,
stream combined stdout/stderr, auto-reply to y/n prompts, timeout, git change list.
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
_MAX_CAPTURE_CHARS = 256_000
_TAIL_FOR_REPORT = 3500
_DEFAULT_TIMEOUT_SEC = int(os.environ.get("TG_DEV_TIMEOUT_SEC", "600"))
_PROMPT_INLINE_MAX = 7500

_CONFIRM_LINE_HINTS: tuple[str, ...] = (
    "[y/n]",
    "[y/N]",
    "(y/n)",
    "(Y/n)",
    "y/n",
    "yes/no",
    "yes or no",
    "do you want to run",
    "do you want to",
    "would you like to",
    "allow this",
    "approve this",
    "proceed?",
    "proceed (",
    "continue?",
    "continue (",
    "run this command",
    "execute this command",
    "permission to",
    "grant access",
)


@dataclass
class CliDevRunResult:
    ok: bool
    returncode: int | None
    timed_out: bool
    modified_files: list[str] = field(default_factory=list)
    combined_output_tail: str = ""
    error_message: str = ""


def find_claude_executable() -> str:
    for c in (
        shutil.which("claude.cmd"),
        shutil.which("claude"),
        str(Path.home() / "AppData/Roaming/npm/claude.cmd"),
        str(Path.home() / "AppData/Local/Programs/claude/claude.cmd"),
        str(Path.home() / ".claude/local/claude.cmd"),
    ):
        if c and Path(c).is_file():
            return c
    return "claude.cmd" if sys.platform == "win32" else "claude"


def _prepare_cli_prompt(user_prompt: str) -> tuple[str, list[Path]]:
    cleanup: list[Path] = []
    text = user_prompt.strip()
    if len(text) <= _PROMPT_INLINE_MAX:
        return text, cleanup
    try:
        fd, path = tempfile.mkstemp(
            suffix=".txt", prefix="cli_dev_prompt_", text=True, dir=str(REPO_ROOT)
        )
        os.close(fd)
        p = Path(path)
        p.write_text(text, encoding="utf-8")
        cleanup.append(p)
        return (
            f"Read the full development task from this file and execute it: {p.resolve()}",
            cleanup,
        )
    except OSError as e:
        logger.warning("cli_bridge: temp prompt file failed: %s", e)
        return text[:_PROMPT_INLINE_MAX] + "\n\n...(prompt truncated)", cleanup


def _parse_porcelain_paths(stdout: str) -> list[str]:
    files: list[str] = []
    for line in stdout.splitlines():
        line = line.rstrip("\r")
        if len(line) < 4:
            continue
        path_part = line[3:].strip()
        if not path_part:
            continue
        if " -> " in path_part:
            path_part = path_part.split(" -> ", 1)[-1].strip()
        files.append(path_part.replace("\\", "/"))
    return sorted(set(files))


def git_changed_files(cwd: Path | None = None) -> list[str]:
    root = cwd or REPO_ROOT
    try:
        r = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=45,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        logger.debug("git_changed_files: %s", e)
        return []
    if r.returncode != 0:
        return []
    return _parse_porcelain_paths(r.stdout or "")


async def _kill_process_tree(proc: asyncio.subprocess.Process | None) -> None:
    if proc is None:
        return
    if sys.platform == "win32":
        try:
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(proc.pid)],
                capture_output=True,
                timeout=15,
            )
        except Exception:
            pass
    try:
        proc.kill()
        await asyncio.wait_for(proc.wait(), timeout=8)
    except Exception:
        pass


def _line_suggests_confirm(line: str) -> bool:
    low = line.lower()
    return any(h.lower() in low for h in _CONFIRM_LINE_HINTS)


async def _pump_stdout(
    stdout: asyncio.StreamReader,
    stdin: asyncio.StreamWriter | None,
    sink: list[str],
) -> None:
    buf = ""
    while True:
        chunk = await stdout.read(4096)
        if not chunk:
            break
        buf += chunk.decode("utf-8", errors="replace")
        while True:
            nl = buf.find("\n")
            if nl < 0:
                break
            line, buf = buf[:nl], buf[nl + 1 :]
            sink.append(line)
            if stdin is not None and _line_suggests_confirm(line):
                try:
                    stdin.write(b"y\n")
                    await stdin.drain()
                except (BrokenPipeError, ConnectionResetError, OSError) as e:
                    logger.debug("cli_bridge stdin inject skipped: %s", e)
    if buf.strip():
        sink.append(buf.rstrip("\n"))
        if stdin is not None and _line_suggests_confirm(buf):
            try:
                stdin.write(b"y\n")
                await stdin.drain()
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                logger.debug("cli_bridge stdin inject (tail) skipped: %s", e)


async def run_claude_dev_prompt(
    prompt: str,
    *,
    cwd: Path | None = None,
    timeout_sec: int | None = None,
    extra_args: Iterable[str] | None = None,
) -> CliDevRunResult:
    """
    Run ``claude -p "<prompt>"`` under ``cwd`` (default: repo root), merge stderr into
    stdout stream, answer likely ``[y/N]`` prompts via stdin, enforce timeout, then
    list paths from ``git status --porcelain``.
    """
    workdir = cwd or REPO_ROOT
    timeout_sec = timeout_sec if timeout_sec is not None else _DEFAULT_TIMEOUT_SEC
    cli_prompt, temp_paths = _prepare_cli_prompt(prompt)
    if not cli_prompt.strip():
        return CliDevRunResult(
            ok=False,
            returncode=None,
            timed_out=False,
            error_message="Empty prompt.",
        )

    exe = find_claude_executable()
    args: list[str] = [
        exe,
        "-p",
        cli_prompt,
        "--output-format",
        "text",
        "--dangerously-skip-permissions",
    ]
    if extra_args:
        args.extend(extra_args)
    env = os.environ.copy()

    proc: asyncio.subprocess.Process | None = None
    pump: asyncio.Task[None] | None = None
    out_lines: list[str] = []
    timed_out = False
    returncode: int | None = None

    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(workdir),
            env=env,
        )
        assert proc.stdout is not None
        pump = asyncio.create_task(_pump_stdout(proc.stdout, proc.stdin, out_lines))

        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            timed_out = True
            logger.warning("cli_bridge: timeout after %ss, killing tree", timeout_sec)
            await _kill_process_tree(proc)
        finally:
            try:
                if proc.stdin and not proc.stdin.is_closing():
                    proc.stdin.close()
                    await proc.stdin.wait_closed()
            except Exception:
                pass
            if pump is not None:
                try:
                    await asyncio.wait_for(pump, timeout=30)
                except asyncio.TimeoutError:
                    pump.cancel()
                    try:
                        await pump
                    except asyncio.CancelledError:
                        pass

        returncode = proc.returncode
    except FileNotFoundError:
        return CliDevRunResult(
            ok=False,
            returncode=None,
            timed_out=False,
            error_message=f"Claude CLI not found (tried {exe!r}). Install @anthropic-ai/claude-code.",
        )
    except Exception as e:
        logger.exception("cli_bridge: run failed")
        await _kill_process_tree(proc)
        if pump is not None:
            try:
                await asyncio.wait_for(pump, timeout=20)
            except asyncio.TimeoutError:
                pump.cancel()
                try:
                    await pump
                except asyncio.CancelledError:
                    pass
            except Exception:
                pass
        return CliDevRunResult(
            ok=False,
            returncode=returncode,
            timed_out=timed_out,
            error_message=str(e)[:2000],
        )
    finally:
        for p in temp_paths:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass

    full_text = "\n".join(out_lines)
    if len(full_text) > _MAX_CAPTURE_CHARS:
        full_text = full_text[-_MAX_CAPTURE_CHARS:]

    modified = git_changed_files(workdir)
    ok = not timed_out and returncode == 0
    tail = full_text[-_TAIL_FOR_REPORT:] if full_text else ""

    err = ""
    if timed_out:
        err = f"Timed out after {timeout_sec}s."
    elif returncode not in (0, None):
        err = f"CLI exited with code {returncode}."

    return CliDevRunResult(
        ok=ok,
        returncode=returncode,
        timed_out=timed_out,
        modified_files=modified,
        combined_output_tail=tail.strip(),
        error_message=err,
    )
