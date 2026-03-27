"""
Protocol Layer — defines the universal message format for agent communication.

Every agent speaks the same language: Handoff objects.
A Handoff contains the source agent, target agent, payload (code/text/data),
and metadata for routing and convergence tracking.
"""

from __future__ import annotations

import json
import hashlib
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from pathlib import Path
from typing import Any


class AgentRole(Enum):
    CODER = "coder"
    REVIEWER = "reviewer"
    INTEGRATOR = "integrator"
    PLANNER = "planner"
    TESTER = "tester"
    CUSTOM = "custom"


class MessageType(Enum):
    HANDOFF = "handoff"           # Pass work to another agent
    FEEDBACK = "feedback"         # Review/critique response
    MERGE_REQUEST = "merge"       # Request to integrate code
    STATUS = "status"             # Progress update
    CONVERGENCE = "convergence"   # Signal that loop has converged


@dataclass
class FilePayload:
    """Represents a file to be handed off between agents."""
    path: str
    content: str
    language: str = ""
    insert_point: str = ""  # Where in target project to insert

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> FilePayload:
        return cls(**data)


@dataclass
class Message:
    """A single message between agents."""
    msg_type: MessageType
    sender: str
    receiver: str
    content: str
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["msg_type"] = self.msg_type.value
        return d

    @classmethod
    def from_dict(cls, data: dict) -> Message:
        data["msg_type"] = MessageType(data["msg_type"])
        return cls(**data)


@dataclass
class Handoff:
    """
    The core unit of cross-session collaboration.

    A Handoff packages everything needed for another agent/session to
    pick up work: files, context, instructions, and convergence state.
    """
    handoff_id: str = ""
    source_agent: str = ""
    target_agent: str = ""
    source_session: str = ""
    target_session: str = ""

    # What's being handed off
    files: list[FilePayload] = field(default_factory=list)
    instructions: str = ""
    context: str = ""

    # Convergence tracking
    iteration: int = 0
    max_iterations: int = 10
    convergence_score: float = 0.0
    convergence_threshold: float = 0.9
    is_converged: bool = False

    # Messages exchanged in this handoff chain
    messages: list[Message] = field(default_factory=list)

    # Metadata
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.handoff_id:
            self.handoff_id = self._generate_id()

    def _generate_id(self) -> str:
        raw = f"{self.source_agent}-{self.target_agent}-{time.time()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:12]

    def add_file(self, path: str, content: str, language: str = "", insert_point: str = ""):
        self.files.append(FilePayload(path=path, content=content, language=language, insert_point=insert_point))
        self.updated_at = time.time()

    def add_message(self, msg: Message):
        self.messages.append(msg)
        self.updated_at = time.time()

    def check_convergence(self) -> bool:
        if self.convergence_score >= self.convergence_threshold:
            self.is_converged = True
        if self.iteration >= self.max_iterations:
            self.is_converged = True
        return self.is_converged

    def to_json(self, indent: int = 2) -> str:
        d = asdict(self)
        # Convert enums in messages
        for msg in d.get("messages", []):
            if isinstance(msg.get("msg_type"), MessageType):
                msg["msg_type"] = msg["msg_type"].value
        return json.dumps(d, indent=indent, ensure_ascii=False)

    def save(self, path: str | Path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.to_json(), encoding="utf-8")

    @classmethod
    def load(cls, path: str | Path) -> Handoff:
        path = Path(path)
        data = json.loads(path.read_text(encoding="utf-8"))
        # Reconstruct nested objects
        data["files"] = [FilePayload.from_dict(f) for f in data.get("files", [])]
        data["messages"] = [Message.from_dict(m) for m in data.get("messages", [])]
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str) -> Handoff:
        data = json.loads(json_str)
        data["files"] = [FilePayload.from_dict(f) for f in data.get("files", [])]
        data["messages"] = [Message.from_dict(m) for m in data.get("messages", [])]
        return cls(**data)
