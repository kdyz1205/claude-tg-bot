"""
Session Store — remembers conversations so they can be resumed.

When a platform hits its limit mid-task, the session store saves:
- Which platform, which conversation URL
- What was the original task
- How far we got (partial output)
- What's left to do

When the platform comes back online, the orchestrator can:
1. Reopen that browser tab
2. Navigate to the saved conversation URL
3. Continue the task from where it left off
"""

from __future__ import annotations

import json
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from enum import Enum

logger = logging.getLogger(__name__)


class SessionStatus(Enum):
    ACTIVE = "active"           # Currently running
    PAUSED = "paused"           # Hit rate limit, waiting to resume
    COMPLETED = "completed"     # Finished successfully
    FAILED = "failed"           # Failed, needs retry
    RESUMABLE = "resumable"     # Platform back online, can resume


@dataclass
class SessionState:
    """State of a single conversation session."""
    session_id: str
    platform: str
    task: str                           # Original task
    conversation_url: str = ""          # Browser URL to reopen
    status: SessionStatus = SessionStatus.ACTIVE

    # Progress tracking
    messages_sent: int = 0
    partial_output: str = ""
    code_blocks: list[str] = field(default_factory=list)
    continuation_prompt: str = ""       # What to send when resuming

    # Timing
    created_at: float = field(default_factory=time.time)
    paused_at: float = 0
    resumed_at: float = 0
    completed_at: float = 0

    # Context for resumption
    metadata: dict[str, Any] = field(default_factory=dict)

    def pause(self, reason: str = "rate_limited"):
        """Mark session as paused (e.g., hit rate limit)."""
        self.status = SessionStatus.PAUSED
        self.paused_at = time.time()
        self.metadata["pause_reason"] = reason
        self.continuation_prompt = self._build_continuation()

    def mark_resumable(self):
        """Mark as ready to resume (platform back online)."""
        if self.status == SessionStatus.PAUSED:
            self.status = SessionStatus.RESUMABLE

    def resume(self):
        """Mark as actively running again."""
        self.status = SessionStatus.ACTIVE
        self.resumed_at = time.time()

    def complete(self, output: str = "", code_blocks: list[str] | None = None):
        """Mark as completed."""
        self.status = SessionStatus.COMPLETED
        self.completed_at = time.time()
        if output:
            self.partial_output = output
        if code_blocks:
            self.code_blocks = code_blocks

    def fail(self, error: str = ""):
        """Mark as failed."""
        self.status = SessionStatus.FAILED
        self.metadata["error"] = error

    def _build_continuation(self) -> str:
        """Build a prompt for resuming the conversation."""
        if self.partial_output:
            return (
                f"Continue from where we left off. "
                f"The original task was: {self.task}\n\n"
                f"Here's what was done so far:\n{self.partial_output[:500]}\n\n"
                f"Please continue and complete the remaining work."
            )
        return f"Continue working on: {self.task}"

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "platform": self.platform,
            "task": self.task,
            "conversation_url": self.conversation_url,
            "status": self.status.value,
            "messages_sent": self.messages_sent,
            "partial_output": self.partial_output[:2000],  # Truncate for storage
            "code_blocks": self.code_blocks,
            "continuation_prompt": self.continuation_prompt,
            "created_at": self.created_at,
            "paused_at": self.paused_at,
            "resumed_at": self.resumed_at,
            "completed_at": self.completed_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> SessionState:
        data["status"] = SessionStatus(data.get("status", "active"))
        return cls(**data)


class SessionStore:
    """
    Manages all conversation sessions across platforms.

    Persists to disk so sessions survive restarts.
    The orchestrator checks this on startup to find resumable sessions.
    """

    def __init__(self, state_file: str = ".harness_state/sessions.json"):
        self.state_file = Path(state_file)
        self.sessions: dict[str, SessionState] = {}
        self._load()

    def create(self, platform: str, task: str, conversation_url: str = "") -> SessionState:
        """Create a new session."""
        session_id = f"{platform}_{int(time.time())}_{len(self.sessions)}"
        session = SessionState(
            session_id=session_id,
            platform=platform,
            task=task,
            conversation_url=conversation_url,
        )
        self.sessions[session_id] = session
        self._save()
        logger.info(f"Session created: {session_id} [{platform}]")
        return session

    def get(self, session_id: str) -> SessionState | None:
        return self.sessions.get(session_id)

    def update(self, session: SessionState):
        """Update a session's state."""
        self.sessions[session.session_id] = session
        self._save()

    def get_paused(self) -> list[SessionState]:
        """Get all paused sessions (waiting to resume)."""
        return [s for s in self.sessions.values() if s.status == SessionStatus.PAUSED]

    def get_resumable(self) -> list[SessionState]:
        """Get all sessions ready to resume."""
        return [s for s in self.sessions.values() if s.status == SessionStatus.RESUMABLE]

    def get_by_platform(self, platform: str) -> list[SessionState]:
        """Get all sessions for a specific platform."""
        return [s for s in self.sessions.values() if s.platform == platform]

    def get_active(self) -> list[SessionState]:
        """Get all currently active sessions."""
        return [s for s in self.sessions.values() if s.status == SessionStatus.ACTIVE]

    def mark_platform_available(self, platform: str):
        """
        When a platform comes back online, mark all its paused sessions as resumable.
        Called by the quota tracker when a cooldown expires.
        """
        for session in self.get_by_platform(platform):
            if session.status == SessionStatus.PAUSED:
                session.mark_resumable()
                logger.info(f"Session {session.session_id} now resumable")
        self._save()

    def cleanup_old(self, max_age_hours: int = 48):
        """Remove completed/failed sessions older than max_age."""
        cutoff = time.time() - (max_age_hours * 3600)
        to_remove = [
            sid for sid, s in self.sessions.items()
            if s.status in (SessionStatus.COMPLETED, SessionStatus.FAILED)
            and s.created_at < cutoff
        ]
        for sid in to_remove:
            del self.sessions[sid]
        if to_remove:
            self._save()
            logger.info(f"Cleaned up {len(to_remove)} old sessions")

    def status_report(self) -> str:
        """Human-readable status of all sessions."""
        if not self.sessions:
            return "No active sessions."

        lines = ["Sessions:"]
        lines.append("-" * 60)

        for sid, session in sorted(self.sessions.items(), key=lambda x: -x[1].created_at):
            age_min = int((time.time() - session.created_at) / 60)
            line = (
                f"  {sid[:30]:30s} [{session.platform:12s}] "
                f"{session.status.value:10s} {age_min:4d}m ago  "
                f"msgs:{session.messages_sent}"
            )
            lines.append(line)

        summary = {
            "active": len(self.get_active()),
            "paused": len(self.get_paused()),
            "resumable": len(self.get_resumable()),
        }
        lines.append(f"\nActive: {summary['active']}  Paused: {summary['paused']}  Resumable: {summary['resumable']}")

        return "\n".join(lines)

    def _save(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        data = {sid: s.to_dict() for sid, s in self.sessions.items()}
        self.state_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _load(self):
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text(encoding="utf-8"))
            self.sessions = {
                sid: SessionState.from_dict(s) for sid, s in data.items()
            }
            logger.info(f"Loaded {len(self.sessions)} sessions from disk")
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to load sessions: {e}")
