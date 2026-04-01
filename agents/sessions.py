"""
agents/sessions.py — Multi-session coordinator.

Named sessions use HTTP LLM turns (``llm_http_client``) with stable virtual chat_ids — no Claude CLI subprocess.

Usage:
    mgr = SessionManager()
    mgr.create("smartmoney", project_dir="C:/Users/alexl/Desktop/crypto-analysis-")
    mgr.create("tgbot", project_dir="C:/Users/alexl/Desktop/claude tg bot")

    # Route message to the right session
    result = await mgr.send("smartmoney", "修复 login 页面的 bug")

    # Or auto-route based on content
    result = await mgr.auto_route("继续修 crypto 的 bug")
"""
import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

import llm_http_client

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SYSTEM_PROMPT_FILE = os.path.join(BOT_DIR, ".system_prompt.txt")


@dataclass
class Session:
    """One Claude CLI session tied to a project."""
    name: str
    project_dir: str
    session_id: str | None = None
    model: str = "claude-sonnet-4-6"
    busy: bool = False
    last_used: float = 0
    history: list = field(default_factory=list)  # last N interactions

    def _virtual_chat_id(self) -> int:
        h = abs(hash(self.name)) % 90_000_000
        return 8_000_000 + h

    async def send(self, message: str, timeout: int = 300) -> str:
        """Send a message via HTTP LLM with per-session history."""
        self.busy = True
        self.last_used = time.time()
        try:
            system = ""
            try:
                if os.path.isfile(SYSTEM_PROMPT_FILE):

                    def _read_sys() -> str:
                        with open(SYSTEM_PROMPT_FILE, encoding="utf-8") as f:
                            return f.read()

                    system = await asyncio.to_thread(_read_sys)
            except OSError:
                pass
            system = (
                (system or "")
                + f"\n\n## Session\nProject directory: {self.project_dir}\n"
                + (f"Session label: {self.session_id}\n" if self.session_id else "")
            )
            text, sid, err = await llm_http_client.complete_turn(
                chat_id=self._virtual_chat_id(),
                system_prompt=system[:240_000],
                user_text=message[:200_000],
                model_hint=self.model,
                timeout_sec=float(timeout),
            )
            if err:
                return f"Session '{self.name}' HTTP error: {err}"
            result = (text or "").strip() or "Done."
            self.session_id = sid or self.session_id
            self.history.append({
                "time": time.time(),
                "message": message[:100],
                "response": result[:200],
            })
            self.history = self.history[-20:]
            return result
        except asyncio.TimeoutError:
            return f"Session '{self.name}' timed out ({timeout}s)"
        except Exception as e:
            return f"Session '{self.name}' error: {e}"
        finally:
            self.busy = False


class SessionManager:
    """Manages multiple named sessions."""

    def __init__(self):
        self.sessions: dict[str, Session] = {}

    def create(
        self,
        name: str,
        project_dir: str,
        model: str = "claude-sonnet-4-6",
    ) -> Session:
        """Create or get a named session."""
        if name in self.sessions:
            return self.sessions[name]
        session = Session(name=name, project_dir=project_dir, model=model)
        self.sessions[name] = session
        logger.info(f"Session created: {name} → {project_dir}")
        return session

    def get(self, name: str) -> Session | None:
        return self.sessions.get(name)

    def list_sessions(self) -> list[dict]:
        """List all sessions with status."""
        return [
            {
                "name": s.name,
                "project_dir": s.project_dir,
                "busy": s.busy,
                "has_context": s.session_id is not None,
                "interactions": len(s.history),
                "last_used": s.last_used,
            }
            for s in self.sessions.values()
        ]

    async def send(self, name: str, message: str, timeout: int = 300) -> str:
        """Send message to a specific session."""
        session = self.sessions.get(name)
        if not session:
            return f"Session '{name}' not found. Available: {list(self.sessions.keys())}"
        if session.busy:
            return f"Session '{name}' is busy. Please wait."
        return await session.send(message, timeout=timeout)

    async def send_parallel(self, tasks: dict[str, str], timeout: int = 300) -> dict[str, str]:
        """Send messages to multiple sessions in parallel.

        Args:
            tasks: {session_name: message}
        Returns:
            {session_name: response}
        """
        async def _run(name, msg):
            return name, await self.send(name, msg, timeout=timeout)

        task_names = list(tasks.keys())
        results = await asyncio.gather(
            *[_run(name, msg) for name, msg in tasks.items()],
            return_exceptions=True,
        )

        output = {}
        for i, item in enumerate(results):
            if isinstance(item, Exception):
                output[task_names[i]] = str(item)
            else:
                name, result = item
                output[name] = result
        return output

    async def broadcast(self, message: str) -> dict[str, str]:
        """Send the same message to ALL sessions."""
        tasks = {name: message for name in self.sessions}
        return await self.send_parallel(tasks)
