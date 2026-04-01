"""
claude_cli_tunnel — Persistent asyncio worker + dual queues for local ``claude`` CLI.

Claude Code's supported non-interactive path remains ``claude -p`` (one subprocess per
turn; optional ``stream-json`` via ``JARVIS_CHAT_STREAM_JSON`` in ``claude_agent``).
This module provides a **serialized tunnel**: all gateway CHAT and dev jobs
enqueue here so callers await a Future without spawning directly, chat lane **strictly
preferred** over long dev jobs when both are pending.

**PersistentClaudePipe** (opt-in via ``CLAUDE_CLI_PIPE_WORKER``, default on): keeps a
child ``python -u -m claude_tunnel_worker`` alive; parent sends one JSON line per turn
(path to prompt file). Watchdog restarts the child quickly if it exits.

Self-healing: worker loop never exits; per-job exceptions complete the Future and
apply brief backoff after repeated failures.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import tempfile
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Literal, TypeVar

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent


def pipe_worker_enabled() -> bool:
    return os.environ.get("CLAUDE_CLI_PIPE_WORKER", "1").strip().lower() not in (
        "0",
        "false",
        "no",
        "off",
    )

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


class PersistentClaudePipe:
    """
    Long-lived child process speaking JSONL on stdin/stdout. Each turn still invokes
    ``claude -p`` inside the child (Claude Code's model); the child serializes turns and
    avoids parent process spawn overhead for parser/teardown. Watchdog restarts dead children.
    """

    _inst: PersistentClaudePipe | None = None

    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._seq = 0
        self._rid_lock = asyncio.Lock()
        self._io_lock = asyncio.Lock()
        self._reader_task: asyncio.Task[None] | None = None
        self._watchdog_task: asyncio.Task[None] | None = None
        self._started = False
        self._last_restart = 0.0

    @classmethod
    def instance(cls) -> PersistentClaudePipe:
        if cls._inst is None:
            cls._inst = PersistentClaudePipe()
        return cls._inst

    def _spawn(self) -> None:
        import sys

        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        cmd = [sys.executable, "-u", "-m", "claude_tunnel_worker"]
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            cwd=str(REPO_ROOT),
            env=env,
        )
        logger.info("PersistentClaudePipe child started pid=%s", self._proc.pid)

    def _terminate(self) -> None:
        if self._proc is None:
            return
        try:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=1.5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        except Exception as e:
            logger.warning("PersistentClaudePipe terminate: %s", e)
        self._proc = None

    def _fail_all_pending(self, msg: str) -> None:
        for rid, fut in list(self._pending.items()):
            if not fut.done():
                fut.set_exception(RuntimeError(msg))
        self._pending.clear()

    async def _restart(self, *, reason: str) -> None:
        now = time.time()
        if now - self._last_restart < 0.35:
            await asyncio.sleep(0.35 - (now - self._last_restart))
        self._last_restart = time.time()
        logger.warning("PersistentClaudePipe restart (%s)", reason)
        self._fail_all_pending("claude pipe worker restarted")
        self._terminate()
        await asyncio.sleep(0.12)
        self._spawn()

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(0.2)
            try:
                if self._proc is not None and self._proc.poll() is not None:
                    await self._restart(reason="child_exited")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("PersistentClaudePipe watchdog: %s", e)

    async def _reader_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            proc = self._proc
            if proc is None or proc.stdout is None:
                await asyncio.sleep(0.05)
                continue
            try:
                line = await loop.run_in_executor(None, proc.stdout.readline)
            except Exception as e:
                logger.warning("PersistentClaudePipe readline: %s", e)
                await asyncio.sleep(0.1)
                continue
            if not line:
                await asyncio.sleep(0.02)
                continue
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                logger.warning("PersistentClaudePipe bad json: %s", line[:240])
                continue
            if data.get("op") == "pong":
                continue
            rid = data.get("id")
            if rid is None:
                continue
            try:
                rid_i = int(rid)
            except (TypeError, ValueError):
                continue
            fut = self._pending.pop(rid_i, None)
            if fut is None or fut.done():
                continue
            if data.get("ok"):
                fut.set_result(data)
            else:
                fut.set_exception(RuntimeError(str(data.get("error") or "worker_turn_failed")))

    async def ensure_started(self) -> None:
        if self._started:
            return
        self._started = True
        self._spawn()
        self._reader_task = asyncio.create_task(
            self._reader_loop(), name="claude_pipe_reader"
        )
        self._watchdog_task = asyncio.create_task(
            self._watchdog(), name="claude_pipe_watchdog"
        )

    async def request_turn(
        self,
        *,
        combined: str,
        resume: str | None,
        timeout_sec: float,
        wall_cap_sec: float,
    ) -> tuple[str, str, str | None]:
        await self.ensure_started()
        async with self._io_lock:
            async with self._rid_lock:
                self._seq += 1
                rid = self._seq
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[dict[str, Any]] = loop.create_future()
            self._pending[rid] = fut

            fd, tmp = tempfile.mkstemp(
                suffix=".txt", prefix="gw_pipe_", text=True, dir=str(REPO_ROOT)
            )
            os.close(fd)
            pth = Path(tmp)
            try:
                pth.write_text(combined, encoding="utf-8")
                payload = {
                    "id": rid,
                    "prompt_path": str(pth.resolve()),
                    "resume": resume or "",
                    "timeout_sec": float(timeout_sec),
                    "wall_cap_sec": float(wall_cap_sec),
                }
                line = json.dumps(payload, ensure_ascii=False) + "\n"
                proc = self._proc
                if proc is None or proc.poll() is not None or proc.stdin is None:
                    await self._restart(reason="stdin_stale")
                    proc = self._proc
                if proc is None or proc.stdin is None:
                    raise RuntimeError("claude pipe: no child process")
                await asyncio.to_thread(proc.stdin.write, line)
                await asyncio.to_thread(proc.stdin.flush)
            except Exception:
                self._pending.pop(rid, None)
                try:
                    pth.unlink()
                except OSError:
                    pass
                raise

            try:
                data = await asyncio.wait_for(fut, timeout=float(timeout_sec) + 60.0)
            except asyncio.TimeoutError:
                self._pending.pop(rid, None)
                await self._restart(reason="turn_wait_timeout")
                raise
            finally:
                try:
                    pth.unlink()
                except OSError:
                    pass

        text = str(data.get("text") or "")
        err = str(data.get("err") or "")
        sid = data.get("sid")
        sid_s: str | None = str(sid) if sid else None
        return text, err, sid_s
