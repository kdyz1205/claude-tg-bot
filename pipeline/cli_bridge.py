"""
Telegram /dev bridge: run a long-form coding task on the repo.

Default: local Claude CLI via ``asyncio.create_subprocess_exec`` with a hard wall-clock
timeout (``TG_DEV_TIMEOUT_SEC``, default 600s), line-buffered stdout/stderr (capped) so
a hung TTY prompt cannot grow memory without bound; optional ``y`` stdin injection for
common confirmation prompts.

Set ``TG_DEV_USE_HTTP=1`` to use ``llm_http_client`` instead (no subprocess).
"""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
_MAX_CAPTURE_CHARS = 256_000
_TAIL_FOR_REPORT = 3500
_DEFAULT_TIMEOUT_SEC = int(os.environ.get("TG_DEV_TIMEOUT_SEC", "600"))
# Windows CreateProcess ~8191 char total line; keep -p small (same idea as claude_agent).
_PROMPT_INLINE_MAX = int(os.environ.get("CLI_DEV_PROMPT_INLINE_MAX", "1200" if os.name == "nt" else "7500"))

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


class _BoundedLineSink:
    """FIFO lines with total char cap (avoids unbounded memory on chatty CLI)."""

    __slots__ = ("lines", "max_chars", "_total")

    def __init__(self, max_chars: int = _MAX_CAPTURE_CHARS) -> None:
        self.lines: list[str] = []
        self.max_chars = max_chars
        self._total = 0

    def push(self, line: str) -> None:
        self.lines.append(line)
        self._total += len(line) + 1
        while self._total > self.max_chars and self.lines:
            old = self.lines.pop(0)
            self._total -= len(old) + 1

    def text(self) -> str:
        return "\n".join(self.lines)


async def _pump_stream_bounded(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter | None,
    sink: _BoundedLineSink,
    on_line: Optional[Callable[[str], Awaitable[None] | None]] = None,
) -> None:
    buf = ""
    while True:
        chunk = await reader.read(8192)
        if not chunk:
            break
        buf += chunk.decode("utf-8", errors="replace")
        while True:
            nl = buf.find("\n")
            if nl < 0:
                break
            line, buf = buf[:nl], buf[nl + 1 :]
            sink.push(line)
            if on_line is not None:
                try:
                    r = on_line(line)
                    if asyncio.iscoroutine(r):
                        await r
                except Exception as e:
                    logger.debug("cli_bridge on_line: %s", e)
            if writer is not None and _line_suggests_confirm(line):
                try:
                    writer.write(b"y\n")
                    await writer.drain()
                except (BrokenPipeError, ConnectionResetError, OSError) as e:
                    logger.debug("cli_bridge stdin inject: %s", e)
    if buf.strip():
        tail = buf.rstrip("\n")
        sink.push(tail)
        if on_line is not None:
            try:
                r = on_line(tail)
                if asyncio.iscoroutine(r):
                    await r
            except Exception as e:
                logger.debug("cli_bridge on_line tail: %s", e)
        if writer is not None and _line_suggests_confirm(buf):
            try:
                writer.write(b"y\n")
                await writer.drain()
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                logger.debug("cli_bridge stdin tail inject: %s", e)


def _use_http_dev() -> bool:
    v = os.environ.get("TG_DEV_USE_HTTP", "0").lower()
    return v in ("1", "true", "yes", "on")


async def _run_claude_dev_http(
    cli_prompt: str,
    temp_paths: list[Path],
    *,
    workdir: Path,
    timeout_sec: int,
) -> CliDevRunResult:
    import config as _cfg
    import llm_http_client

    system = (
        "You are a senior engineer. The repository root is:\n"
        f"{workdir.resolve()}\n\n"
        "Produce concrete, file-scoped edit instructions or unified diffs. "
        "You cannot execute shell commands from here; output only text. "
        "Be concise; user reads on mobile."
    )
    timed_out = False
    full_text = ""
    err_msg = ""
    try:
        text, e = await asyncio.wait_for(
            llm_http_client.complete_stateless(
                system_prompt=system,
                user_text=cli_prompt[:200_000],
                model_hint=getattr(_cfg, "CLAUDE_MODEL", None),
                timeout_sec=min(float(timeout_sec), 600.0),
                state_key=-8801,
            ),
            timeout=float(timeout_sec),
        )
        full_text = (text or "").strip()
        if e:
            err_msg = e[:2000]
    except asyncio.TimeoutError:
        timed_out = True
        err_msg = f"Timed out after {timeout_sec}s."
        logger.warning("cli_bridge: HTTP dev timeout after %ss", timeout_sec)
    except Exception as e:
        logger.exception("cli_bridge: HTTP run failed")
        err_msg = str(e)[:2000]
    finally:
        for p in temp_paths:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass

    if len(full_text) > _MAX_CAPTURE_CHARS:
        full_text = full_text[-_MAX_CAPTURE_CHARS:]

    modified = git_changed_files(workdir)
    ok = bool(full_text) and not timed_out and not err_msg
    tail = full_text[-_TAIL_FOR_REPORT:] if full_text else ""

    return CliDevRunResult(
        ok=ok,
        returncode=0 if ok else 1,
        timed_out=timed_out,
        modified_files=modified,
        combined_output_tail=tail.strip(),
        error_message=err_msg,
    )


async def _run_claude_dev_subprocess(
    cli_prompt: str,
    temp_paths: list[Path],
    *,
    workdir: Path,
    timeout_sec: int,
    extra_args: Iterable[str] | None,
    on_stdout_line: Optional[Callable[[str], Awaitable[None] | None]],
) -> CliDevRunResult:
    exe = find_claude_executable()
    if not Path(exe).is_file():
        logger.warning("cli_bridge: Claude executable not found at %s, using HTTP fallback", exe)
        return await _run_claude_dev_http(cli_prompt, temp_paths, workdir=workdir, timeout_sec=timeout_sec)

    args: list[str] = [
        exe,
        "-p",
        cli_prompt,
        "--output-format",
        "json",
        "--dangerously-skip-permissions",
    ]
    if extra_args:
        args.extend(list(extra_args))

    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(workdir),
            limit=2**20,
        )
    except (OSError, NotImplementedError) as e:
        for p in temp_paths:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        return CliDevRunResult(
            ok=False,
            returncode=None,
            timed_out=False,
            error_message=f"subprocess spawn failed: {e}"[:2000],
        )

    out_sink = _BoundedLineSink()
    err_sink = _BoundedLineSink()
    timed_out = False
    rc: int | None = None
    err_msg = ""

    assert proc.stdout is not None and proc.stderr is not None and proc.stdin is not None
    t_out = asyncio.create_task(
        _pump_stream_bounded(proc.stdout, proc.stdin, out_sink, on_stdout_line)
    )
    t_err = asyncio.create_task(
        _pump_stream_bounded(proc.stderr, None, err_sink, None),
    )
    try:
        rc = await asyncio.wait_for(proc.wait(), timeout=float(timeout_sec))
    except asyncio.TimeoutError:
        timed_out = True
        err_msg = f"CLI timed out after {timeout_sec}s (process killed)."
        logger.warning("cli_bridge: subprocess dev timeout after %ss", timeout_sec)
        await _kill_process_tree(proc)
    finally:
        await asyncio.gather(t_out, t_err, return_exceptions=True)
        try:
            proc.stdin.close()
            await proc.stdin.wait_closed()
        except Exception:
            pass
        for p in temp_paths:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass

    combined = ""
    if out_sink.text():
        combined += out_sink.text()
    if err_sink.text():
        combined += "\n--- stderr ---\n" + err_sink.text()

    stdout_text = out_sink.text()
    modified = git_changed_files(workdir)
    ok = rc == 0 and not timed_out and bool(stdout_text.strip())
    tail = combined[-_TAIL_FOR_REPORT:] if combined else ""
    if rc not in (0, None) and not err_msg:
        err_msg = f"CLI exit code {rc}. stderr tail: {err_sink.text()[-800:]}"

    return CliDevRunResult(
        ok=ok,
        returncode=rc,
        timed_out=timed_out,
        modified_files=modified,
        combined_output_tail=tail.strip(),
        error_message=err_msg,
    )


def _use_cli_dev_tunnel() -> bool:
    v = (os.environ.get("CLAUDE_CLI_TUNNEL_DEV") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


async def run_claude_dev_prompt(
    prompt: str,
    *,
    cwd: Path | None = None,
    timeout_sec: int | None = None,
    extra_args: Iterable[str] | None = None,
    on_stdout_line: Optional[Callable[[str], Awaitable[None] | None]] = None,
) -> CliDevRunResult:
    """
    Run a dev task: by default **local Claude CLI** via ``create_subprocess_exec`` with
    ``timeout_sec`` (default 600). Optional ``on_stdout_line`` receives each stdout line
    for Telegram streaming. Set ``TG_DEV_USE_HTTP=1`` to use HTTP LLM instead.

    When ``CLAUDE_CLI_TUNNEL_DEV`` is on (default), the subprocess run is scheduled on
    ``claude_cli_tunnel.PersistentClaudeCLI`` dev lane (chat-priority dual queue).
    """
    workdir = cwd or REPO_ROOT
    timeout_sec = int(timeout_sec if timeout_sec is not None else _DEFAULT_TIMEOUT_SEC)
    timeout_sec = max(30, min(timeout_sec, 3600))
    cli_prompt, temp_paths = _prepare_cli_prompt(prompt)
    if not cli_prompt.strip():
        return CliDevRunResult(
            ok=False,
            returncode=None,
            timed_out=False,
            error_message="Empty prompt.",
        )

    async def _job() -> CliDevRunResult:
        if _use_http_dev():
            return await _run_claude_dev_http(
                cli_prompt, temp_paths, workdir=workdir, timeout_sec=timeout_sec
            )
        return await _run_claude_dev_subprocess(
            cli_prompt,
            temp_paths,
            workdir=workdir,
            timeout_sec=timeout_sec,
            extra_args=extra_args,
            on_stdout_line=on_stdout_line,
        )

    if _use_cli_dev_tunnel():
        from claude_cli_tunnel import PersistentClaudeCLI

        return await PersistentClaudeCLI.instance().run(_job, lane="dev")
    return await _job()
