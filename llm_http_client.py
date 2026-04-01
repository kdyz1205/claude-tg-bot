"""
Async HTTP LLM client (aiohttp only) — Ollama, Anthropic Messages API, or OpenAI Chat Completions.

Replaces Claude Code CLI subprocess usage for text completion with bounded concurrency and timeouts.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
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


def _usage_tokens(data: dict[str, Any] | None, backend: str) -> int:
    if not data or not isinstance(data, dict):
        return 0
    if backend == "anthropic":
        u = data.get("usage") or {}
        if not isinstance(u, dict):
            return 0
        return int(u.get("input_tokens", 0) or 0) + int(u.get("output_tokens", 0) or 0)
    if backend == "openai":
        u = data.get("usage") or {}
        if not isinstance(u, dict):
            return 0
        t = int(u.get("total_tokens", 0) or 0)
        if t:
            return t
        return int(u.get("prompt_tokens", 0) or 0) + int(u.get("completion_tokens", 0) or 0)
    return 0


def _estimate_prompt_tokens(system_prompt: str, messages_for_api: list[dict[str, str]]) -> int:
    n = len(system_prompt or "")
    for m in messages_for_api:
        n += len(str(m.get("content") or ""))
    return max(512, n // 4 + 1024)


def _overload_retryable(err: str) -> bool:
    el = (err or "").lower()
    return any(
        x in el
        for x in (
            "529",
            "503",
            "502",
            "overloaded",
            "capacity",
            "temporarily unavailable",
            "try again",
        )
    )


def _model_fallback_chain(backend: str, primary: str) -> list[str]:
    primary = (primary or "").strip()
    chain: list[str] = []
    if primary:
        chain.append(primary)
    if backend == "anthropic":
        for m in getattr(config, "LLM_HTTP_FALLBACK_MODELS", []) or []:
            ms = (m or "").strip()
            if ms and ms not in chain:
                chain.append(ms)
    elif backend == "openai":
        for m in (
            getattr(config, "TASK_TIER_FAST_OPENAI", None),
            "gpt-4o-mini",
            "gpt-3.5-turbo",
        ):
            ms = (m or "").strip()
            if ms and ms not in chain:
                chain.append(ms)
    else:
        om = (getattr(config, "OLLAMA_MODEL", "") or "").strip()
        if om and om not in chain:
            chain.append(om)
    return chain[:6]


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
    model_primary = effective_model(backend, model_hint)
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

    try:
        from tracker.quota import http_llm_preflight, http_llm_record_usage
    except ImportError:
        def http_llm_preflight(_n: int) -> tuple[bool, str]:
            return True, ""

        def http_llm_record_usage(_n: int) -> None:
            return

    ok_budget, budget_msg = http_llm_preflight(
        _estimate_prompt_tokens(system_prompt, messages_for_api)
    )
    if not ok_budget:
        return "", None, budget_msg

    models = _model_fallback_chain(backend, model_primary)
    err = ""
    data: dict[str, Any] | None = None
    text = ""

    async with _sem():
        connector = aiohttp.TCPConnector(limit=32, limit_per_host=16)
        async with aiohttp.ClientSession(connector=connector) as session:
            for attempt, model in enumerate(models):
                err = ""
                data = None

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

                if not err:
                    ut = _usage_tokens(data, backend)
                    if ut > 0:
                        http_llm_record_usage(ut)
                    if attempt > 0:
                        logger.info(
                            "llm_http_client: recovered on fallback model %s (backend=%s)",
                            model,
                            backend,
                        )
                    break

                if attempt + 1 < len(models) and _overload_retryable(err):
                    logger.warning(
                        "llm_http_client overload (%s), retry model %s → next",
                        err[:120],
                        model,
                    )
                    continue

                logger.warning("llm_http_client %s error: %s", backend, err[:300])
                el = err.lower()
                if "401" in err or "403" in err or "invalid" in el and "key" in el:
                    return "", None, f"auth failed: {err}"
                if "429" in err or "rate" in el:
                    return "", None, f"rate limit: {err}"
                return "", None, err

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


_JSON_FENCE = re.compile(r"```json\s*([\s\S]*?)```", re.I)


def extract_json_object_from_llm_text(raw: str | None) -> dict[str, Any] | None:
    """First JSON object from fenced block or brace scan; never raises."""
    if not raw or not str(raw).strip():
        return None
    s = str(raw)
    m = _JSON_FENCE.search(s)
    if m:
        try:
            obj = json.loads(m.group(1).strip())
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, ValueError, TypeError):
            pass
    decoder = json.JSONDecoder()
    start = 0
    while True:
        i = s.find("{", start)
        if i < 0:
            break
        try:
            obj, _ = decoder.raw_decode(s, i)
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
        start = i + 1
    return None


def strategy_json_safe_default(*, reason: str = "llm_unavailable", detail: str | None = None) -> dict[str, Any]:
    """Default strategy / signal payload when LLM fails — never execute on this alone."""
    out: dict[str, Any] = {
        "ok": False,
        "action": "hold",
        "confidence": 0.0,
        "reason": reason[:200],
        "raw": None,
    }
    if detail:
        out["detail"] = detail[:500]
    return out


async def complete_strategy_json(
    *,
    system_prompt: str,
    user_text: str,
    model_hint: str | None = None,
    timeout_sec: float = 30.0,
    state_key: int = -911,
) -> dict[str, Any]:
    """
    One-shot LLM call expecting a single JSON object (strategy / scoring).
    Hard cap ``timeout_sec`` default 30s; malformed JSON or network errors → safe dict, never raises.
    """
    try:
        t_sec = max(5.0, min(120.0, float(timeout_sec)))
    except (TypeError, ValueError):
        t_sec = 30.0

    try:
        text, err = await complete_stateless(
            system_prompt=(system_prompt or "Reply with one JSON object only.")[:120_000],
            user_text=(user_text or "")[:120_000],
            model_hint=model_hint,
            timeout_sec=t_sec,
            state_key=state_key,
        )
    except asyncio.TimeoutError:
        return strategy_json_safe_default(reason="timeout")
    except Exception as e:
        logger.exception("complete_strategy_json transport: %s", e)
        return strategy_json_safe_default(reason="exception", detail=str(e)[:300])

    if err:
        return strategy_json_safe_default(reason="http_error", detail=err[:500])

    parsed = extract_json_object_from_llm_text(text)
    if not parsed:
        return strategy_json_safe_default(reason="parse_error", detail=(text or "")[:200])

    try:
        conf = float(parsed.get("confidence", 0) or 0)
    except (TypeError, ValueError):
        conf = 0.0
    conf = max(0.0, min(1.0, conf))

    action = str(parsed.get("action", "hold") or "hold")[:64]
    why = str(parsed.get("reason", "") or "")[:500]

    return {
        "ok": True,
        "action": action,
        "confidence": conf,
        "reason": why,
        "raw": parsed,
    }
