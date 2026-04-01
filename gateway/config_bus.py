"""
Config Bus — 白名单式写入 session_commander_config.json，供 Jarvis 语义层驱动「实权」配置。

与 session_commander 共用目录锁，避免与队列读写冲突。
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_SAFE_BOOL_KEYS = frozenset({"jarvis_auto_consume", "dry_run"})
_SAFE_STR_KEYS = frozenset({"god_active_skill", "jarvis_drain_target_session"})
_SAFE_STR_MAX = 500
_ACTIVE_SKILLS_KEY = "active_skills"
_ACTIVE_SKILLS_MAX_ITEMS = 32
_ACTIVE_SKILL_ID_MAX = 200


def _normalize_active_skills(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        s = value.strip()
        return [s[:_ACTIVE_SKILL_ID_MAX]] if s else []
    if isinstance(value, list):
        out: list[str] = []
        for x in value[:_ACTIVE_SKILLS_MAX_ITEMS]:
            s = str(x or "").strip()
            if s:
                out.append(s[:_ACTIVE_SKILL_ID_MAX])
        return out
    return []


def apply_safe_config_patch(patch: dict[str, Any]) -> tuple[bool, str]:
    """
    合并 ``patch`` 进 session_commander 配置并保存。
    返回 (ok, message)。
    """
    if not isinstance(patch, dict) or not patch:
        return False, "empty patch"

    allowed = _SAFE_BOOL_KEYS | _SAFE_STR_KEYS | {_ACTIVE_SKILLS_KEY}
    bad = [k for k in patch if k not in allowed]
    if bad:
        return False, f"keys not allowed: {bad}"

    from session_commander import _jarvis_queue_lock, load_config, save_config

    with _jarvis_queue_lock():
        cfg = load_config()
        for k, v in patch.items():
            if k in _SAFE_BOOL_KEYS:
                cfg[k] = bool(v)
            elif k in _SAFE_STR_KEYS:
                cfg[k] = str(v)[:_SAFE_STR_MAX] if v is not None else ""
            elif k == _ACTIVE_SKILLS_KEY:
                cfg[k] = _normalize_active_skills(v)
        save_config(cfg)

    try:
        import session_commander as sc

        sc.CFG = load_config()
    except Exception:
        pass

    try:
        from pipeline.god_orchestrator import GOD_ORCHESTRATOR

        GOD_ORCHESTRATOR.reload_skills()
    except Exception:
        logger.debug("config_bus god reload_skills skipped", exc_info=True)

    logger.info("Config Bus applied: %s", patch)
    return True, "ok"


def append_lab_nudge_to_queue(prompt: str, *, source: str = "jarvis_lab_nudge") -> tuple[bool, str]:
    """将「炼丹/进化」类请求写入 jarvis_pending_commands，交给桌面 Claude 执行。"""
    from session_commander import append_jarvis_pending_command

    try:
        append_jarvis_pending_command(
            prompt=(prompt or "")[:8000],
            source=source,
            sub_intent=None,
            extra={"kind": "evolver_lab_request"},
        )
        return True, "queued"
    except Exception as e:
        logger.exception("append_lab_nudge_to_queue: %s", e)
        return False, str(e)[:200]
