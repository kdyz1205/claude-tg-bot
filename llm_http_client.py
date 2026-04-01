"""
Async HTTP LLM client (aiohttp only) — Ollama, Anthropic Messages API, or OpenAI Chat Completions.

Replaces Claude Code CLI subprocess usage for text completion with bounded concurrency and timeouts.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

import aiohttp

import config

logger = logging.getLogger(__name__)

_histories: dict[int, list[dict[str, str]]] = {}
_hist_lock = asyncio.Lock()
_http_sem: asyncio.Semaphore | None = None


def _sem() -> asyncio.Semaphore:
    global _http_sem
    if _http_sem is None:
        _http_sem = asyncio.Semaphore(max(1, int(getattr(config, "LLM_HTTP_MAX_CONCURRENT", 8))))
    return _http_sem


def concurrency_snapshot() -> dict[str, int]:
    sem = _sem()
    max_c = max(1, int(getattr(config, "LLM_HTTP_MAX_CONCURRENT", 8)))
    try:
        avail = int(getattr(sem, "_value", max_c))
    except (TypeError, ValueError):
        avail = max_c
    return {"max": max_c, "available": avail, "in_use": max(0, max_c - avail)}


def resolve_backend() -> str:
    b = getattr(config, "LLM_HTTP_BACKEND", "auto") or "auto"
    if b == "auto":
        if getattr(config, "ANTHROPIC_API_KEY", None):
            return "anthropic"
        if getattr(config, "OPENAI_API_KEY", None):
            return "openai"
        return "ollama"
    return b


def effective_model(backend: str, model_hint: str | None) -> str:
    hint = (model_hint or "").strip()
    if backend == "ollama":
        return getattr(config, "OLLAMA_MODEL", "llama3.2") or "llama3.2"
    if backend == "openai":
        return hint or getattr(config, "OPENAI_MODEL", "gpt-4o")
    return hint or getattr(config, "CLAUDE_MODEL", "claude-sonnet-4-20250514")


async def clear_history(chat_id: int) -> None:
    async with _hist_lock:
        _histories.pop(chat_id, None)


def clear_history_sync(chat_id: int) -> None:
    """Best-effort sync drop (e.g. from sync bot helpers)."""
    _histories.pop(chat_id, None)


def _truncate(h: list[dict[str, str]]) -> None:
    max_msgs = int(getattr(config, "MAX_HTTP_LLM_HISTORY_MSGS", 40))
    while len(h) > max_msgs:
        h.pop(0)


async def _post_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: dict[str, str],
    payload: dict[str, Any],
    timeout: aiohttp.ClientTimeout,
) -> tuple[dict[str, Any] | None, str]:
    try:
        async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
            raw = await resp.text()
            if resp.status >= 400:
                return None, f"HTTP {resp.status}: {raw[:800]}"
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                return None, f"JSONDecodeError: {raw[:400]}"
            if not isinstance(data, dict):
                return None, "response is not a JSON object"
            return data, ""
    except asyncio.TimeoutError:
        return None, "timeout"
    except aiohttp.ClientError as e:
        return None, str(e)[:600]


def _anthropic_extract_text(data: dict[str, Any]) -> str:
    parts: list[str] = []
    for block in data.get("content") or []:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            parts.append(str(block.get("text") or ""))
    return "\n".join(parts).strip()


def _openai_extract_text(data: dict[str, Any]) -> str:
    ch = data.get("choices") or []
    if not ch or not isinstance(ch[0], dict):
        return ""
    msg = ch[0].get("message") or {}
    return str(msg.get("content") or "").strip()


def _ollama_extract_text(data: dict[str, Any]) -> str:
    m = data.get("message") or {}
    return str(m.get("content") or "").strip()


async def complete_turn(
    *,
    chat_id: int,
    system_prompt: str,
    user_text: str,
    model_hint: str | None,
    timeout_sec: float,
) -> tuple[str, str | None, str]:
    """
    One user/assistant turn with optional multi-turn memory (chat_id >= 0).

    Returns:
        (assistant_text, session_id_or_none, stderr_equivalent_for_legacy_checks)
    """
    backend = resolve_backend()
    model = effective_model(backend, model_hint)
    user_text = (user_text or "").strip()
    if not user_text:
        return "", None, "empty user message"

    timeout = aiohttp.ClientTimeout(
        total=max(5.0, float(timeout_sec)),
        connect=min(30.0, float(timeout_sec)),
        sock_read=min(120.0, float(timeout_sec)),
    )

    async with _hist_lock:
        prior = list(_histories.get(chat_id, []))

    messages_for_api: list[dict[str, str]] = prior + [{"role": "user", "content": user_text}]

    async with _sem():
        connector = aiohttp.TCPConnector(limit=32, limit_per_host=16)
        async with aiohttp.ClientSession(connector=connector) as session:
            err = ""
            data: dict[str, Any] | None = None

            if backend == "anthropic":
                key = getattr(config, "ANTHROPIC_API_KEY", "") or ""
                if not key:
                    return "", None, "ANTHROPIC_API_KEY not set"
                base = getattr(config, "ANTHROPIC_API_BASE", "https://api.anthropic.com").rstrip("/")
                url = f"{base}/v1/messages"
                hdrs = {
                    "x-api-key": key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                }
                payload = {
                    "model": model,
                    "max_tokens": min(8192, int(getattr(config, "ANTHROPIC_MAX_TOKENS", 8192))),
                    "system": system_prompt[:240_000],
                    "messages": [{"role": m["role"], "content": m["content"]} for m in messages_for_api],
                }
                data, err = await _post_json(session, url, headers=hdrs, payload=payload, timeout=timeout)
                text = _anthropic_extract_text(data) if data else ""

            elif backend == "openai":
                key = getattr(config, "OPENAI_API_KEY", "") or ""
                if not key:
                    return "", None, "OPENAI_API_KEY not set"
                base = getattr(config, "OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
                url = f"{base}/chat/completions"
                hdrs = {"authorization": f"Bearer {key}", "content-type": "application/json"}
                oa_msgs: list[dict[str, str]] = [
                    {"role": "system", "content": system_prompt[:240_000]},
                ]
                oa_msgs.extend(messages_for_api)
                payload = {
                    "model": model,
                    "messages": oa_msgs,
                    "temperature": 0.2,
                }
                data, err = await _post_json(session, url, headers=hdrs, payload=payload, timeout=timeout)
                text = _openai_extract_text(data) if data else ""

            else:
                base = getattr(config, "OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
                url = f"{base}/api/chat"
                hdrs = {"content-type": "application/json"}
                ol_msgs: list[dict[str, str]] = [
                    {"role": "system", "content": system_prompt[:120_000]},
                ]
                ol_msgs.extend(messages_for_api)
                payload = {"model": model, "messages": ol_msgs, "stream": False}
                data, err = await _post_json(session, url, headers=hdrs, payload=payload, timeout=timeout)
                text = _ollama_extract_text(data) if data else ""

    if err:
        logger.warning("llm_http_client %s error: %s", backend, err[:300])
        el = err.lower()
        if "401" in err or "403" in err or "invalid" in el and "key" in el:
            return "", None, f"auth failed: {err}"
        if "429" in err or "rate" in el:
            return "", None, f"rate limit: {err}"
        return "", None, err

    async with _hist_lock:
        h = _histories.setdefault(chat_id, [])
        h.append({"role": "user", "content": user_text})
        h.append({"role": "assistant", "content": text})
        _truncate(h)

    sid = f"http:{chat_id}" if chat_id >= 0 else None
    return text, sid, ""


async def complete_stateless(
    *,
    system_prompt: str,
    user_text: str,
    model_hint: str | None,
    timeout_sec: float,
    state_key: int = -1,
) -> tuple[str, str]:
    """Single-shot completion (no cross-call memory). Returns (text, error)."""
    text, _, err = await complete_turn(
        chat_id=state_key,
        system_prompt=system_prompt,
        user_text=user_text,
        model_hint=model_hint,
        timeout_sec=timeout_sec,
    )
    await clear_history(state_key)
    return text, err
