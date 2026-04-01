"""
BaseSkill — unified async contract for Python skills (hot-reload friendly).

New skills should subclass BaseSkill, set ``skill_id`` / ``default_timeout_sec``,
implement ``_execute``, and expose ``SKILL_CLASS = MySkill`` for the loader.
Legacy modules may keep ``async def run_skill``; ``skill_runtime`` wraps it
with ``asyncio.wait_for``.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

logger = logging.getLogger(__name__)


class SkillTimeoutError(asyncio.TimeoutError):
    def __init__(self, skill_id: str, timeout_sec: float):
        self.skill_id = skill_id
        self.timeout_sec = timeout_sec
        super().__init__(f"Skill {skill_id!r} exceeded {timeout_sec}s")


class BaseSkill(ABC):
    """Async skill with enforced timeout and optional teardown for unload."""

    skill_id: str = "unnamed_skill"
    default_timeout_sec: float = 120.0

    async def run(self, payload: dict[str, Any] | None = None, *, timeout_sec: float | None = None) -> Any:
        """
        Run skill logic under ``asyncio.wait_for``. Subclasses implement ``_execute`` only.
        """
        payload = payload or {}
        limit = self.default_timeout_sec if timeout_sec is None else float(timeout_sec)
        try:
            return await asyncio.wait_for(self._execute(payload), timeout=limit)
        except asyncio.TimeoutError as e:
            raise SkillTimeoutError(self.skill_id, limit) from e

    @abstractmethod
    async def _execute(self, payload: dict[str, Any]) -> Any:
        """Core async implementation (no timeout wrapper here)."""
        ...

    async def aclose(self) -> None:
        """Hot-unload hook: close clients, flush files, cancel background tasks."""
        await self.cleanup()

    async def cleanup(self) -> None:
        """Override to release resources before module unload."""
        return None
