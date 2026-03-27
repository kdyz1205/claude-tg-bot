"""
Task Dispatcher — routes tasks to the right AI based on difficulty level.

The key insight: DON'T burn expensive API tokens for dispatch.
Use a rule-based engine first. Upgrade to local LLM (Ollama) later if needed.

Difficulty Levels:
    Level 1 (Simple Q&A)      → GPT web / Grok web        (free)
    Level 2 (Moderate code)    → Claude web                 (free with subscription)
    Level 3 (Heavy code)       → Claude Code web session    (free with subscription)
    Level 4 (Multi-file heavy) → Multiple Claude Code sessions → Git merge
    Level 5 (Architecture)     → Codex / Claude Code        (dispatch to multiple)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any


class Difficulty(IntEnum):
    SIMPLE = 1       # Quick Q&A, simple lookup
    MODERATE = 2     # Single-file code, moderate logic
    HEAVY = 3        # Multi-function, complex logic
    MULTI_FILE = 4   # Multiple files, needs coordination
    ARCHITECTURE = 5 # Full system design + implementation


@dataclass
class TaskRoute:
    """Where to send a task."""
    platform: str           # "gpt", "grok", "claude_web", "claude_code", "codex"
    difficulty: Difficulty
    task: str               # The original task text
    subtasks: list[str] = field(default_factory=list)  # For multi-agent split
    context: str = ""       # Additional context
    metadata: dict[str, Any] = field(default_factory=dict)


# Keyword-based difficulty classification rules
DIFFICULTY_RULES: list[tuple[list[str], Difficulty]] = [
    # Level 5 — Architecture keywords
    (["architecture", "system design", "full stack", "from scratch", "entire",
      "microservice", "infrastructure", "deploy", "重构整个", "完整系统", "从零"],
     Difficulty.ARCHITECTURE),

    # Level 4 — Multi-file keywords
    (["multiple files", "several components", "frontend and backend",
      "前端后端", "多个文件", "多个组件", "integrate", "merge", "combine",
      "cross-session", "两个session", "coordination"],
     Difficulty.MULTI_FILE),

    # Level 3 — Heavy code keywords
    (["complex", "refactor", "optimize", "algorithm", "database schema",
      "api endpoint", "authentication", "写一个完整", "复杂", "重构",
      "performance", "security", "test suite"],
     Difficulty.HEAVY),

    # Level 2 — Moderate code keywords
    (["write", "create", "implement", "function", "component", "class",
      "写一个", "做一个", "帮我写", "code", "script", "fix bug"],
     Difficulty.MODERATE),

    # Level 1 — Simple (default)
    (["what is", "how to", "explain", "什么是", "怎么", "为什么",
      "translate", "summarize", "list", "compare"],
     Difficulty.SIMPLE),
]

# Platform assignment by difficulty
PLATFORM_MAP: dict[Difficulty, list[str]] = {
    Difficulty.SIMPLE: ["gpt", "grok"],
    Difficulty.MODERATE: ["claude_web", "gpt"],
    Difficulty.HEAVY: ["claude_code"],
    Difficulty.MULTI_FILE: ["claude_code", "claude_code"],  # Two sessions
    Difficulty.ARCHITECTURE: ["claude_code", "codex"],
}


class Dispatcher:
    """
    Routes tasks to the appropriate AI platform based on difficulty.
    Integrates with QuotaTracker for adaptive routing.

    Usage:
        dispatcher = Dispatcher()
        route = dispatcher.dispatch("帮我写一个 React 登录页面")
        # route.platform = "claude_web"
        # route.difficulty = Difficulty.MODERATE

        # With quota awareness:
        from tracker import QuotaTracker
        dispatcher = Dispatcher(quota_tracker=QuotaTracker())
        route = dispatcher.dispatch("...")
        # If claude_web is exhausted, auto-falls back to gpt
    """

    def __init__(
        self,
        rules: list | None = None,
        platform_map: dict | None = None,
        quota_tracker=None,
    ):
        self.rules = rules or DIFFICULTY_RULES
        self.platform_map = platform_map or PLATFORM_MAP
        self.quota = quota_tracker  # Optional: tracker.QuotaTracker instance

    def classify_difficulty(self, task: str) -> Difficulty:
        """Classify task difficulty using keyword matching."""
        task_lower = task.lower()

        # Check rules from hardest to easiest (higher difficulty first)
        for keywords, difficulty in self.rules:
            for kw in keywords:
                if kw in task_lower:
                    return difficulty

        # Default to moderate (safe bet)
        return Difficulty.MODERATE

    def estimate_file_count(self, task: str) -> int:
        """Estimate how many files this task might touch."""
        task_lower = task.lower()
        indicators = {
            "full stack": 10, "fullstack": 10, "完整": 8,
            "frontend and backend": 6, "前端后端": 6,
            "multiple": 4, "several": 4, "多个": 4,
            "component": 2, "page": 2, "组件": 2,
        }
        for kw, count in indicators.items():
            if kw in task_lower:
                return count
        return 1

    def split_task(self, task: str, difficulty: Difficulty) -> list[str]:
        """
        Split a complex task into subtasks for multiple agents.
        Only splits for Level 4+ tasks.
        """
        if difficulty < Difficulty.MULTI_FILE:
            return [task]

        # Simple split strategy: identify distinct components
        subtasks = []

        # Look for explicit list items
        lines = task.split("\n")
        numbered = [l.strip() for l in lines if re.match(r"^\d+[\.\)]\s", l.strip())]
        if numbered:
            return numbered

        # Look for "and" / "以及" / "还有" separators
        parts = re.split(r"\band\b|以及|还有|同时|,\s*(?=\w)", task)
        if len(parts) > 1:
            return [p.strip() for p in parts if p.strip()]

        # Fallback: frontend/backend split
        if any(kw in task.lower() for kw in ["frontend", "backend", "前端", "后端", "full stack"]):
            subtasks.append(f"Frontend part: {task}")
            subtasks.append(f"Backend part: {task}")
            return subtasks

        return [task]

    def _apply_quota_fallback(self, platforms: list[str]) -> list[str]:
        """
        If quota tracker is available, replace exhausted platforms with fallbacks.
        This is where adaptive routing happens.
        """
        if self.quota is None:
            return platforms

        resolved = []
        for p in platforms:
            if self.quota.is_available(p):
                resolved.append(p)
            else:
                # Platform exhausted — find a fallback
                fallback = self.quota.get_best_platform(preferred=None)
                if fallback:
                    resolved.append(fallback)
                else:
                    # Everything exhausted — still add it, orchestrator will queue
                    resolved.append(p)
        return resolved

    def dispatch(self, task: str, context: str = "") -> TaskRoute:
        """
        Main dispatch function.
        Analyzes the task → classifies difficulty → routes to platform(s).
        If quota tracker is set, applies adaptive fallbacks.
        """
        difficulty = self.classify_difficulty(task)
        platforms = self.platform_map.get(difficulty, ["claude_web"])

        # Adaptive: replace exhausted platforms with available ones
        platforms = self._apply_quota_fallback(platforms)

        subtasks = self.split_task(task, difficulty)

        # Calculate wait time if all platforms are exhausted
        wait_seconds = 0
        if self.quota and not any(self.quota.is_available(p) for p in platforms):
            wait_seconds = min(
                self.quota.time_until_available(p) for p in platforms
            )

        route = TaskRoute(
            platform=platforms[0],
            difficulty=difficulty,
            task=task,
            subtasks=subtasks,
            context=context,
            metadata={
                "all_platforms": platforms,
                "estimated_files": self.estimate_file_count(task),
                "needs_multi_agent": difficulty >= Difficulty.MULTI_FILE,
                "needs_git_merge": difficulty >= Difficulty.MULTI_FILE,
                "all_exhausted": wait_seconds > 0,
                "wait_seconds": wait_seconds,
            },
        )

        return route

    def dispatch_report(self, task: str) -> str:
        """Human-readable dispatch report."""
        route = self.dispatch(task)
        lines = [
            f"Task: {route.task[:80]}...",
            f"Difficulty: Level {route.difficulty} ({route.difficulty.name})",
            f"Primary Platform: {route.platform}",
            f"Estimated Files: {route.metadata['estimated_files']}",
        ]
        if route.metadata["needs_multi_agent"]:
            lines.append(f"Multi-Agent: Yes → {route.metadata['all_platforms']}")
            lines.append("Subtasks:")
            for i, st in enumerate(route.subtasks, 1):
                lines.append(f"  {i}. {st[:100]}")
        if route.metadata.get("all_exhausted"):
            wait_min = int(route.metadata["wait_seconds"] / 60)
            lines.append(f"WARNING: All platforms exhausted. Next available in {wait_min}m")
        if self.quota:
            lines.append("")
            lines.append(self.quota.status_report())
        return "\n".join(lines)
