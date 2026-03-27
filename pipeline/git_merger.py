"""
Git Merger — handles merging code from multiple agents/sessions into one branch.

This is the "integrator" that takes output from different browser agents
(each pushed to their own branch) and merges them into a unified result.
"""

from __future__ import annotations

import subprocess
import logging
import time
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MergeResult:
    success: bool
    target_branch: str
    merged_branches: list[str]
    conflicts: list[str]
    message: str


class GitMerger:
    """Handles multi-branch merging for agent collaboration."""

    def __init__(self, repo_dir: str = ".", remote: str = "origin"):
        self.repo_dir = Path(repo_dir)
        self.remote = remote

    def _git(self, *args, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git"] + list(args),
            cwd=self.repo_dir,
            capture_output=True,
            text=True,
            check=check,
        )

    def fetch_all(self):
        """Fetch all remote branches."""
        for attempt in range(4):
            r = self._git("fetch", self.remote, "--prune", check=False)
            if r.returncode == 0:
                return
            time.sleep(2 ** (attempt + 1))
        raise RuntimeError("Failed to fetch from remote after 4 attempts")

    def list_agent_branches(self, prefix: str = "agent/") -> list[str]:
        """List all agent branches."""
        result = self._git("branch", "-r")
        branches = []
        for line in result.stdout.strip().split("\n"):
            branch = line.strip().lstrip("* ")
            if prefix in branch:
                branches.append(branch)
        return branches

    def merge_branches(
        self,
        branches: list[str],
        target_branch: str = "main",
        strategy: str = "recursive",
    ) -> MergeResult:
        """
        Merge multiple branches into the target branch.

        Strategy:
        - "recursive": Standard merge, stops on conflict
        - "ours": Accept our changes on conflict
        - "theirs": Accept their changes on conflict
        """
        conflicts = []
        merged = []

        # Checkout target
        self._git("checkout", target_branch, check=False)

        for branch in branches:
            # Clean branch name (remove remote prefix)
            local_branch = branch.replace(f"{self.remote}/", "")

            try:
                if strategy in ("ours", "theirs"):
                    self._git("merge", branch, "-X", strategy, "--no-edit")
                else:
                    self._git("merge", branch, "--no-edit")
                merged.append(local_branch)
                logger.info(f"Merged {local_branch} into {target_branch}")

            except subprocess.CalledProcessError:
                # Merge conflict
                conflicts.append(local_branch)
                logger.warning(f"Conflict merging {local_branch}")
                self._git("merge", "--abort", check=False)

        success = len(conflicts) == 0

        if merged:
            # Push the merged result
            for attempt in range(4):
                r = self._git("push", "-u", self.remote, target_branch, check=False)
                if r.returncode == 0:
                    break
                time.sleep(2 ** (attempt + 1))

        return MergeResult(
            success=success,
            target_branch=target_branch,
            merged_branches=merged,
            conflicts=conflicts,
            message=f"Merged {len(merged)} branches. {len(conflicts)} conflicts."
        )

    def create_integration_branch(self, name: str = "") -> str:
        """Create a fresh integration branch for merging agent work."""
        branch_name = name or f"integration/{int(time.time())}"
        self._git("checkout", "-b", branch_name, check=False)
        return branch_name

    def auto_merge_agents(self, target: str = "main") -> MergeResult:
        """
        Automatically fetch and merge all agent branches into target.
        One-call convenience method.
        """
        self.fetch_all()
        branches = self.list_agent_branches()
        if not branches:
            return MergeResult(
                success=True,
                target_branch=target,
                merged_branches=[],
                conflicts=[],
                message="No agent branches found to merge.",
            )

        integration_branch = self.create_integration_branch()
        result = self.merge_branches(branches, integration_branch)
        return result
