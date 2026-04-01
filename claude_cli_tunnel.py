"""
claude_cli_tunnel — Persistent asyncio worker + dual queues for local ``claude`` CLI.

Claude Code's supported non-interactive path remains ``claude -p`` (one subprocess per
turn; optional ``stream-json`` via ``JARVIS_CHAT_STREAM_JSON`` in ``claude_agent``).
This module provides a **serialized tunnel**: all gateway CHAT and dev jobs
enqueue here so callers await a Future without spawning directly, chat lane **strictly
preferred** over long dev jobs when both are pending.

Self-healing: worker loop never exits; per-job exceptions complete the Future and
apply brief backoff after repeated failures.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Literal, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
Lane = Literal["chat", "dev"]


class PersistentClaudeCLI:
    """
    Singleton queue + worker. ``run(factory, lane=...)`` schedules ``await factory()``
    on the worker and returns its result.
    """

    _inst: PersistentClaudeCLI | None = None

    def __init__(self) -> None:
        self._chat_q: asyncio.Queue[tuple[Callable[[], Awaitable[Any]], asyncio.Future[Any]]] = (
            asyncio.Queue()
        )
        self._dev_q: asyncio.Queue[tuple[Callable[[], Awaitable[Any]], asyncio.Future[Any]]] = (
            asyncio.Queue()
        )
        self._worker: asyncio.Task[None] | None = None
        self._consecutive_failures = 0

    @classmethod
    def instance(cls) -> PersistentClaudeCLI:
        if cls._inst is None:
            cls._inst = PersistentClaudeCLI()
        return cls._inst

    def ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(
                self._worker_loop(), name="persistent_claude_cli_tunnel"
            )
            logger.info("PersistentClaudeCLI worker started")

    async def _next_job(
        self,
    ) -> tuple[Callable[[], Awaitable[Any]], asyncio.Future[Any], Lane]:
        """Prefer chat; otherwise race chat-get vs dev-get so chat can preempt dev wait."""
        try:
            item = self._chat_q.get_nowait()
            return item[0], item[1], "chat"
        except asyncio.QueueEmpty:
            pass

        t_chat = asyncio.create_task(self._chat_q.get())
        t_dev = asyncio.create_task(self._dev_q.get())
        try:
            done, pending = await asyncio.wait(
                {t_chat, t_dev}, return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
            finished = next(iter(done))
            factory, fut = finished.result()
            lane: Lane = "chat" if finished is t_chat else "dev"
            return factory, fut, lane
        except Exception:
            for t in (t_chat, t_dev):
                if not t.done():
                    t.cancel()
                    try:
                        await t
                    except asyncio.CancelledError:
                        pass
            raise

    async def _worker_loop(self) -> None:
        while True:
            try:
                factory, fut, lane = await self._next_job()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("PersistentClaudeCLI _next_job: %s", e)
                await asyncio.sleep(0.5)
                continue

            try:
                result = await factory()
                self._consecutive_failures = 0
                if not fut.done():
                    fut.set_result(result)
            except asyncio.CancelledError:
                if not fut.done():
                    fut.cancel()
                raise
            except Exception as e:
                self._consecutive_failures += 1
                logger.exception(
                    "PersistentClaudeCLI job failed lane=%s streak=%s",
                    lane,
                    self._consecutive_failures,
                )
                if not fut.done():
                    fut.set_exception(e)
                if self._consecutive_failures >= 5:
                    await asyncio.sleep(1.5)
                    self._consecutive_failures = 0

    async def run(self, factory: Callable[[], Awaitable[T]], *, lane: Lane) -> T:
        self.ensure_worker()
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[T] = loop.create_future()
        payload = (factory, fut)
        if lane == "chat":
            await self._chat_q.put(payload)
        else:
            await self._dev_q.put(payload)
        return await fut
