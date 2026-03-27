"""
Orchestrator — the complete pipeline that ties everything together.

Flow:
    User (Telegram) → Dispatcher (classify + split) → Browser Agents (execute) → Git (merge) → Result

This is the "brain" of the harness. It:
1. Receives a task from the gateway (Telegram)
2. Dispatches to the right AI platform(s)
3. Executes via browser automation
4. Collects results
5. If multi-agent: merges via git
6. Returns the final result
"""

from __future__ import annotations

import asyncio
import logging
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from dispatcher import Dispatcher, TaskRoute, Difficulty
from browser_agents import get_browser_agent, BrowserConfig, AgentResult
from tracker import QuotaTracker, SessionStore

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Final result from the full pipeline."""
    success: bool
    task: str
    difficulty: str
    platforms_used: list[str]
    agent_results: list[AgentResult]
    merged: bool = False
    git_branch: str = ""
    total_duration: float = 0
    summary: str = ""


class Orchestrator:
    """
    The master orchestrator.

    Usage:
        orch = Orchestrator(repo_dir="/path/to/project")
        result = await orch.execute("帮我写一个 React 登录页面")
    """

    def __init__(
        self,
        repo_dir: str = ".",
        browser_config: BrowserConfig | None = None,
        dispatcher: Dispatcher | None = None,
        git_remote: str = "origin",
        quota_tracker: QuotaTracker | None = None,
        session_store: SessionStore | None = None,
    ):
        self.repo_dir = Path(repo_dir)
        self.browser_config = browser_config or BrowserConfig()
        self.quota = quota_tracker or QuotaTracker()
        self.sessions = session_store or SessionStore()
        self.dispatcher = dispatcher or Dispatcher(quota_tracker=self.quota)
        self.git_remote = git_remote

    async def execute(self, task: str, context: str = "") -> PipelineResult:
        """
        Full pipeline execution with adaptive quota management.

        1. Check for resumable sessions first
        2. Dispatch (classify difficulty, split if needed, quota-aware)
        3. If all platforms exhausted → queue task, report wait time
        4. Execute on browser agent(s)
        5. Record usage + manage sessions
        6. Git merge if multi-agent
        7. Return result
        """
        start = time.time()

        # Step 0: Check for resumable sessions for this task
        resumable = self._find_resumable_session(task)
        if resumable:
            logger.info(f"Resuming session {resumable.session_id} on {resumable.platform}")
            result = await self._resume_session(resumable)
            return PipelineResult(
                success=result.success,
                task=task,
                difficulty="RESUMED",
                platforms_used=[result.platform],
                agent_results=[result],
                total_duration=time.time() - start,
                summary=f"Resumed session on {resumable.platform}\n" + (result.output[:200] if result.output else ""),
            )

        # Step 1: Dispatch (quota-aware)
        route = self.dispatcher.dispatch(task, context)
        logger.info(f"Dispatched: {route.difficulty.name} → {route.platform}")

        # Step 1.5: If all platforms exhausted, queue and wait
        if route.metadata.get("all_exhausted"):
            wait_min = int(route.metadata["wait_seconds"] / 60)
            logger.warning(f"All platforms exhausted. Next available in {wait_min}m")
            # Create a paused session so we can resume later
            session = self.sessions.create(route.platform, task)
            session.pause("all_platforms_exhausted")
            self.sessions.update(session)
            return PipelineResult(
                success=False,
                task=task,
                difficulty=route.difficulty.name,
                platforms_used=[],
                agent_results=[],
                total_duration=time.time() - start,
                summary=f"All platforms exhausted. Task queued. Next available in {wait_min}m.\n"
                        f"Session saved: {session.session_id}",
            )

        # Step 2: Execute
        if route.metadata.get("needs_multi_agent"):
            agent_results = await self._execute_multi_agent(route)
        else:
            agent_results = [await self._execute_single_agent(route)]

        # Step 3: Record usage for each platform used
        for result in agent_results:
            if result.platform:
                self.quota.record_usage(
                    result.platform,
                    was_rate_limited=("rate limit" in result.error.lower() if result.error else False),
                )

        # Step 4: Git merge (if multi-agent)
        merged = False
        git_branch = ""
        if route.metadata.get("needs_git_merge") and len(agent_results) > 1:
            git_branch = self._git_merge(route, agent_results)
            merged = True

        # Step 5: Build result
        success = any(r.success for r in agent_results)
        summary = self._build_summary(route, agent_results, merged)

        return PipelineResult(
            success=success,
            task=task,
            difficulty=route.difficulty.name,
            platforms_used=[r.platform for r in agent_results],
            agent_results=agent_results,
            merged=merged,
            git_branch=git_branch,
            total_duration=time.time() - start,
            summary=summary,
        )

    def _find_resumable_session(self, task: str) -> "SessionState | None":
        """Check if there's a resumable session for this task."""
        from tracker import SessionStore
        # Check paused sessions — mark as resumable if platform is back
        for session in self.sessions.get_paused():
            if self.quota.is_available(session.platform):
                session.mark_resumable()
                self.sessions.update(session)

        # Find a resumable session matching this task
        for session in self.sessions.get_resumable():
            if session.task == task:
                return session
        return None

    async def _resume_session(self, session) -> AgentResult:
        """Resume a paused browser session."""
        session.resume()
        self.sessions.update(session)

        agent = get_browser_agent(session.platform, self.browser_config)
        prompt = session.continuation_prompt or session.task

        result = await agent.execute(prompt)

        # Update session
        if result.success:
            session.complete(result.output, result.code_blocks)
        else:
            session.fail(result.error)
        self.sessions.update(session)

        # Record usage
        self.quota.record_usage(
            session.platform,
            was_rate_limited=("rate limit" in result.error.lower() if result.error else False),
        )

        return result

    async def _execute_single_agent(self, route: TaskRoute) -> AgentResult:
        """Execute task on a single browser agent, with session tracking."""
        # Create session
        session = self.sessions.create(route.platform, route.task)

        agent = get_browser_agent(route.platform, self.browser_config)
        result = await agent.execute(route.task)

        # Update session based on result
        if result.success:
            session.complete(result.output, result.code_blocks)
        elif "rate limit" in result.error.lower():
            session.partial_output = result.output
            session.pause("rate_limited")
        else:
            session.fail(result.error)

        session.messages_sent += 1
        self.sessions.update(session)
        return result

    async def _execute_multi_agent(self, route: TaskRoute) -> list[AgentResult]:
        """Execute subtasks on multiple browser agents in parallel."""
        platforms = route.metadata.get("all_platforms", [route.platform])
        subtasks = route.subtasks

        # Pair subtasks with platforms
        tasks = []
        for i, subtask in enumerate(subtasks):
            platform = platforms[i % len(platforms)]
            agent = get_browser_agent(platform, self.browser_config)
            tasks.append(agent.execute(subtask))

        # Execute all in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        agent_results = []
        for r in results:
            if isinstance(r, Exception):
                agent_results.append(AgentResult(
                    success=False,
                    output="",
                    error=str(r),
                ))
            else:
                agent_results.append(r)

        return agent_results

    def _git_merge(self, route: TaskRoute, results: list[AgentResult]) -> str:
        """Merge multi-agent results via git."""
        branch_name = f"harness/multi-{int(time.time())}"

        try:
            # Create a new branch
            self._run_git("checkout", "-b", branch_name)

            # Write each agent's code output to files
            for i, result in enumerate(results):
                if not result.success or not result.code_blocks:
                    continue

                for j, code in enumerate(result.code_blocks):
                    filename = f"output_{result.platform}_{i}_{j}.py"
                    filepath = self.repo_dir / filename
                    filepath.write_text(code, encoding="utf-8")
                    self._run_git("add", str(filepath))

            # Commit
            self._run_git(
                "commit", "-m",
                f"harness: multi-agent output for '{route.task[:50]}...'"
            )

            # Push
            for attempt in range(4):
                r = self._run_git("push", "-u", self.git_remote, branch_name, check=False)
                if r.returncode == 0:
                    break
                time.sleep(2 ** (attempt + 1))

            return branch_name

        except Exception as e:
            logger.error(f"Git merge failed: {e}")
            return ""

    def _run_git(self, *args, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["git"] + list(args),
            cwd=self.repo_dir,
            capture_output=True,
            text=True,
            check=check,
        )

    def _build_summary(
        self, route: TaskRoute, results: list[AgentResult], merged: bool
    ) -> str:
        """Build a human-readable summary for Telegram response."""
        lines = [
            f"**Task completed**",
            f"Difficulty: Level {route.difficulty} ({route.difficulty.name})",
            f"Platforms: {', '.join(r.platform for r in results)}",
        ]

        for i, r in enumerate(results):
            status = "OK" if r.success else f"FAIL ({r.error[:50]})"
            lines.append(f"Agent {i+1} [{r.platform}]: {status} ({r.duration_seconds:.1f}s)")
            if r.code_blocks:
                lines.append(f"  Code blocks: {len(r.code_blocks)}")

        if merged:
            lines.append(f"Git merge: branch created")

        total_time = sum(r.duration_seconds for r in results)
        lines.append(f"Total time: {total_time:.1f}s")

        return "\n".join(lines)
