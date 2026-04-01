"""
Claude Code CLI ``--output-format stream-json`` helpers (NDJSON on stdout).

Used for low-latency, line-by-line consumption; final text still matches ``json`` mode
``result`` semantics. Duplex stdin via ``--input-format stream-json`` is not wired here yet
(see CLI ``--replay-user-messages`` for future tunneling).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class StreamJsonAccum:
    """Aggregated state from one ``claude -p`` stream-json run."""

    session_id: str | None = None
    result_text: str = ""
    last_assistant_text: str = ""
    is_error: bool = False
    hit_rate_limit: bool = False
    raw_event_count: int = 0


def feed_stream_json_event(obj: dict, acc: StreamJsonAccum) -> None:
    acc.raw_event_count += 1
    sid = obj.get("session_id")
    if isinstance(sid, str) and sid.strip():
        acc.session_id = sid.strip()

    t = obj.get("type")
    if t == "rate_limit_event":
        acc.hit_rate_limit = True
        acc.is_error = True

    if t == "assistant":
        msg = obj.get("message") or {}
        parts: list[str] = []
        for block in msg.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(str(block.get("text") or ""))
        s = "".join(parts).strip()
        if s:
            acc.last_assistant_text = s
        if obj.get("error"):
            acc.is_error = True

    if t == "result":
        r = obj.get("result")
        if r is not None:
            acc.result_text = str(r).strip()
        if obj.get("is_error"):
            acc.is_error = True


def stream_accum_final_text(acc: StreamJsonAccum) -> str:
    if acc.result_text:
        return acc.result_text
    return acc.last_assistant_text


async def pump_stdout_ndjson(
    reader: asyncio.StreamReader,
    accum: StreamJsonAccum,
    on_event: Callable[[dict], Awaitable[None] | None] | None = None,
) -> None:
    """Read stdout until EOF; each non-empty line is one JSON object."""
    buf = ""
    while True:
        chunk = await reader.read(65_536)
        if not chunk:
            break
        buf += chunk.decode("utf-8", errors="replace")
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                logger.debug("claude_stream_json: skip non-JSON line: %s", line[:120])
                continue
            if not isinstance(obj, dict):
                continue
            feed_stream_json_event(obj, accum)
            if on_event is not None:
                try:
                    r = on_event(obj)
                    if asyncio.iscoroutine(r):
                        await r
                except Exception as e:
                    logger.debug("claude_stream_json on_event: %s", e)

    tail = buf.strip()
    if tail:
        try:
            obj = json.loads(tail)
            if isinstance(obj, dict):
                feed_stream_json_event(obj, accum)
                if on_event is not None:
                    r = on_event(obj)
                    if asyncio.iscoroutine(r):
                        await r
        except json.JSONDecodeError:
            logger.debug("claude_stream_json: trailing buffer not JSON: %s", tail[:120])


async def drain_stderr_tail(reader: asyncio.StreamReader, max_chars: int = 4000) -> str:
    chunks: list[bytes] = []
    while True:
        b = await reader.read(65_536)
        if not b:
            break
        chunks.append(b)
    raw = b"".join(chunks).decode("utf-8", errors="replace")
    return raw[-max_chars:] if len(raw) > max_chars else raw
