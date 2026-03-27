"""
File Channel — agents communicate via local filesystem.

Level: Basic (same machine, fastest)
Use when: All agents run on the same machine or share a mounted filesystem.

Structure:
    .handoffs/
    ├── {agent_name}/
    │   ├── inbox/          # Incoming handoffs
    │   └── outbox/         # Sent handoffs
    └── shared/             # Broadcast messages
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterator

from harness.protocol import Handoff


class FileChannel:
    """File-based communication channel."""

    def __init__(self, base_dir: str = ".handoffs", **kwargs):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _inbox(self, agent_name: str) -> Path:
        p = self.base_dir / agent_name / "inbox"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def _outbox(self, agent_name: str) -> Path:
        p = self.base_dir / agent_name / "outbox"
        p.mkdir(parents=True, exist_ok=True)
        return p

    def send(self, handoff: Handoff):
        """Send a handoff to the target agent's inbox."""
        target = handoff.target_agent
        source = handoff.source_agent

        # Write to target's inbox
        filename = f"{handoff.handoff_id}_{int(time.time())}.json"
        target_path = self._inbox(target) / filename
        handoff.save(target_path)

        # Copy to source's outbox
        source_path = self._outbox(source) / filename
        handoff.save(source_path)

    def receive(self, agent_name: str) -> Handoff | None:
        """Receive the oldest unprocessed handoff from inbox."""
        inbox = self._inbox(agent_name)
        files = sorted(inbox.glob("*.json"))
        if not files:
            return None

        # Read and remove the oldest
        handoff = Handoff.load(files[0])
        files[0].unlink()
        return handoff

    def receive_all(self, agent_name: str) -> list[Handoff]:
        """Receive all pending handoffs."""
        inbox = self._inbox(agent_name)
        handoffs = []
        for f in sorted(inbox.glob("*.json")):
            handoffs.append(Handoff.load(f))
            f.unlink()
        return handoffs

    def peek(self, agent_name: str) -> list[Handoff]:
        """View pending handoffs without consuming them."""
        inbox = self._inbox(agent_name)
        return [Handoff.load(f) for f in sorted(inbox.glob("*.json"))]

    def broadcast(self, handoff: Handoff):
        """Send a handoff to the shared broadcast directory."""
        shared = self.base_dir / "shared"
        shared.mkdir(parents=True, exist_ok=True)
        filename = f"{handoff.handoff_id}_{int(time.time())}.json"
        handoff.save(shared / filename)

    def list_agents(self) -> list[str]:
        """List all agents that have directories."""
        return [d.name for d in self.base_dir.iterdir() if d.is_dir() and d.name != "shared"]
