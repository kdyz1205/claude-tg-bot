"""
agents/sessions.py — Multi-session coordinator.

Manages multiple Claude CLI sessions, each working on a different project.
The bot routes user messages to the right session based on context.

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
import json
import logging
import os
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

CLAUDE_CMD = os.path.join(
    os.path.expanduser("~"), "AppData", "Roaming", "npm", "claude.cmd"
)
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

    async def send(self, message: str, timeout: int = 300) -> str:
        """Send a message to this session. Returns response."""
        self.busy = True
        self.last_used = time.time()

        args = [
            CLAUDE_CMD,
            "-p", message,
            "--output-format", "json",
            "--dangerously-skip-permissions",
            "--model", self.model,
            "--append-system-prompt-file", SYSTEM_PROMPT_FILE,
        ]
        if self.session_id:
            args.extend(["--resume", self.session_id])

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=self.project_dir,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            if proc:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
            return f"Session '{self.name}' timed out ({timeout}s)"
        except Exception as e:
            return f"Session '{self.name}' error: {e}"
        finally:
            self.busy = False

        raw = stdout.decode("utf-8", errors="replace").strip()
        if not raw:
            return "No output"

        try:
            data = json.loads(raw)
            result = data.get("result", "").strip() or "Done."
            self.session_id = data.get("session_id", self.session_id)
            self.history.append({
                "time": time.time(),
                "message": message[:100],
                "response": result[:200],
            })
            # Keep last 20 interactions
            self.history = self.history[-20:]
            return result
        except json.JSONDecodeError:
            return raw[:1000]


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
