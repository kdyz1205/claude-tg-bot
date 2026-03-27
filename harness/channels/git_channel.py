"""
Git Channel — agents communicate via git branches.

Level: Medium (cross-machine, versioned, persistent)
Use when: Agents run in different sessions/machines but share a git remote.

Protocol:
    - Each agent has a branch: agent/{agent_name}
    - Handoffs are committed as .handoffs/{handoff_id}.json
    - Target agent fetches from source's branch
    - Merge = integration complete
"""

from __future__ import annotations

import subprocess
import json
import time
from pathlib import Path

from harness.protocol import Handoff


class GitChannel:
    """Git-based communication channel for cross-session collaboration."""

    def __init__(self, repo_dir: str = ".", remote: str = "origin", branch_prefix: str = "agent", **kwargs):
        self.repo_dir = Path(repo_dir)
        self.remote = remote
        self.branch_prefix = branch_prefix
        self.handoff_dir = self.repo_dir / ".handoffs"
        self.handoff_dir.mkdir(parents=True, exist_ok=True)

    def _run_git(self, *args, check: bool = True) -> subprocess.CompletedProcess:
        """Run a git command in the repo directory."""
        result = subprocess.run(
            ["git"] + list(args),
            cwd=self.repo_dir,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"Git command failed: git {' '.join(args)}\n{result.stderr}")
        return result

    def _agent_branch(self, agent_name: str) -> str:
        return f"{self.branch_prefix}/{agent_name}"

    def _ensure_branch(self, agent_name: str):
        """Create agent branch if it doesn't exist."""
        branch = self._agent_branch(agent_name)
        result = self._run_git("branch", "--list", branch, check=False)
        if not result.stdout.strip():
            self._run_git("checkout", "-b", branch, check=False)
            self._run_git("checkout", "-", check=False)

    def send(self, handoff: Handoff):
        """Commit a handoff to the source agent's branch."""
        source = handoff.source_agent
        self._ensure_branch(source)

        # Save handoff file
        filename = f"{handoff.handoff_id}.json"
        filepath = self.handoff_dir / filename
        handoff.save(filepath)

        # Commit on source's branch
        current_branch = self._run_git("branch", "--show-current").stdout.strip()
        source_branch = self._agent_branch(source)

        self._run_git("checkout", source_branch)
        self._run_git("add", str(filepath))
        self._run_git(
            "commit", "-m",
            f"handoff: {source} -> {handoff.target_agent} [{handoff.handoff_id}]"
        )

        # Push to remote (with retry)
        for attempt in range(4):
            result = self._run_git("push", "-u", self.remote, source_branch, check=False)
            if result.returncode == 0:
                break
            time.sleep(2 ** (attempt + 1))

        # Return to original branch
        self._run_git("checkout", current_branch)

    def receive(self, agent_name: str) -> Handoff | None:
        """Fetch and read the latest handoff addressed to this agent."""
        # Fetch all agent branches
        self._run_git("fetch", self.remote, check=False)

        # Look for handoff files addressed to this agent
        for f in sorted(self.handoff_dir.glob("*.json"), reverse=True):
            try:
                handoff = Handoff.load(f)
                if handoff.target_agent == agent_name:
                    return handoff
            except (json.JSONDecodeError, KeyError):
                continue
        return None

    def pull_from_agent(self, source_agent: str, target_agent: str) -> Handoff | None:
        """Pull the latest handoff from a specific source agent."""
        source_branch = self._agent_branch(source_agent)

        # Fetch the source branch
        self._run_git("fetch", self.remote, source_branch, check=False)

        # Check out the handoff files from source branch
        result = self._run_git(
            "checkout", f"{self.remote}/{source_branch}", "--",
            str(self.handoff_dir),
            check=False,
        )

        if result.returncode != 0:
            return None

        # Find handoffs addressed to target
        for f in sorted(self.handoff_dir.glob("*.json"), reverse=True):
            try:
                handoff = Handoff.load(f)
                if handoff.target_agent == target_agent:
                    return handoff
            except (json.JSONDecodeError, KeyError):
                continue

        return None

    def merge_agent_work(self, source_agent: str, target_branch: str = "main"):
        """Merge an agent's branch into the target branch."""
        source_branch = self._agent_branch(source_agent)
        self._run_git("checkout", target_branch)
        self._run_git("merge", source_branch, "--no-edit",
                       "-m", f"Merge {source_agent} work into {target_branch}")

    def list_agent_branches(self) -> list[str]:
        """List all agent branches."""
        result = self._run_git("branch", "-a")
        branches = result.stdout.strip().split("\n")
        return [b.strip().lstrip("* ") for b in branches if self.branch_prefix in b]
