"""
Telegram-facing helpers wrapping ``pipeline.cli_bridge`` (formatting + streaming).

``run_dev_prompt`` runs the local Claude CLI via ``asyncio.create_subprocess_exec`` by
default (see ``TG_DEV_USE_HTTP``). Pass ``on_stdout_line`` for live TG chunks.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Optional

from pipeline.cli_bridge import (
    CliDevRunResult as TgDevRunResult,
    find_claude_executable,
    git_changed_files,
    run_claude_dev_prompt as run_dev_prompt,
)

__all__ = [
    "TgDevRunResult",
    "find_claude_executable",
    "format_telegram_report",
    "git_changed_files",
    "run_dev_prompt",
    "run_dev_prompt_with_stream",
]


async def run_dev_prompt_with_stream(
    prompt: str,
    send_text_chunk: Callable[[str], Awaitable[None]],
    *,
    cwd: Optional[Path] = None,
    timeout_sec: Optional[int] = None,
    min_interval_sec: float = 2.0,
) -> TgDevRunResult:
    """
    Run dev CLI and periodically forward buffered stdout lines to Telegram (rate-limited).
    """
    import time as _time

    last_send = 0.0
    buf: list[str] = []
    max_buf_lines = 40

    async def on_line(line: str) -> None:
        nonlocal last_send
        buf.append(line)
        if len(buf) > max_buf_lines:
            del buf[: len(buf) - max_buf_lines]
        now = _time.monotonic()
        if now - last_send < min_interval_sec:
            return
        last_send = now
        text = "\n".join(buf[-max_buf_lines:])
        if len(text) > 3500:
            text = text[-3500:]
        try:
            await send_text_chunk(f"📟 `{_time.strftime('%H:%M:%S')}`\n{text}")
        except Exception:
            pass

    return await run_dev_prompt(
        prompt,
        cwd=cwd,
        timeout_sec=timeout_sec,
        on_stdout_line=on_line,
    )


def format_telegram_report(result: TgDevRunResult) -> str:
    """Human-readable report for Telegram (keep under ~4000 chars)."""
    lines: list[str] = []
    if result.ok and result.modified_files:
        lines.append("✅ 开发任务完成，以下文件已被修改：")
    elif result.ok:
        lines.append("✅ 开发任务完成。未检测到工作区文件变更（或当前目录非 Git 仓库）。")
    elif result.timed_out:
        lines.append("⏱️ 开发任务超时，已终止子进程。当前 Git 可见变更：")
    elif result.returncode not in (0, None):
        lines.append("⚠️ CLI 已结束但退出码非零。Git 可见变更：")
    else:
        lines.append("ℹ️ 开发任务已结束。Git 可见变更：")

    if result.modified_files:
        for f in result.modified_files[:80]:
            lines.append(f"• `{f}`")
        if len(result.modified_files) > 80:
            lines.append(f"… 另有 {len(result.modified_files) - 80} 个路径未列出")
    elif not result.ok:
        lines.append("（工作区无变更记录，或未在 Git 仓库内运行）")

    if result.error_message:
        lines.append("")
        lines.append(result.error_message)

    if result.combined_output_tail:
        lines.append("")
        lines.append("—— 输出摘要 ——")
        lines.append(result.combined_output_tail[:2800])

    text = "\n".join(lines)
    if len(text) > 4090:
        text = text[:4087] + "..."
    return text
