"""
agents/autonomy.py — Self-directed autonomous agent with observe-act-verify loops.

This is the "consciousness" layer that makes the bot self-evolving:
1. Observe: Screenshot + accessibility tree + system state
2. Plan: Decide what to do next based on goals + context
3. Act: Execute via tools/CLI/sessions
4. Verify: Check if the action succeeded
5. Learn: Record outcome, update patterns, adjust strategy

The bot can:
- Set its own goals based on user intent
- Break goals into sub-tasks
- Self-correct when things fail
- Learn from successes and failures
- Improve its own code
- Control multiple Claude Code sessions in parallel
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@dataclass
class Goal:
    """A goal the bot is pursuing."""
    description: str
    priority: int = 1  # 1=highest
    sub_goals: list = field(default_factory=list)
    status: str = "pending"  # pending, active, completed, failed, paused
    created_at: float = field(default_factory=time.time)
    attempts: int = 0
    max_attempts: int = 5
    last_error: str = ""
    context: dict = field(default_factory=dict)


@dataclass
class Observation:
    """What the bot observes about the current state."""
    timestamp: float = field(default_factory=time.time)
    active_window: str = ""
    screen_state: str = ""  # brief description
    system_health: dict = field(default_factory=dict)
    pending_goals: int = 0
    last_action_result: str = ""


class AutonomyEngine:
    """
    Self-directed agent that pursues goals autonomously.

    Implements the observe-act-verify loop with learning:
    - Goals persist across sessions (JSON file)
    - Each cycle: observe → plan → act → verify → learn
    - Failures trigger self-healing strategies
    - Successes get extracted as reusable patterns
    """

    _STATE_FILE = os.path.join(BOT_DIR, ".autonomy_state.json")
    _MAX_GOALS = 50
    _MAX_HISTORY = 200

    def __init__(self):
        self.goals: list[Goal] = []
        self.history: list[dict] = []  # action history
        self._running = False
        self._loop_task: asyncio.Task | None = None
        self._send_fn: Callable | None = None  # async fn to send status to user
        self._load_state()

    # ── State persistence ──

    def _load_state(self):
        try:
            if os.path.exists(self._STATE_FILE):
                with open(self._STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.goals = [Goal(**g) for g in data.get("goals", [])]
                self.history = data.get("history", [])[-self._MAX_HISTORY:]
        except Exception as e:
            logger.warning(f"Autonomy: failed to load state: {e}")

    def _save_state(self):
        try:
            data = {
                "goals": [
                    {k: v for k, v in g.__dict__.items() if not k.startswith("_")}
                    for g in self.goals[-self._MAX_GOALS:]
                ],
                "history": self.history[-self._MAX_HISTORY:],
                "saved_at": time.time(),
            }
            tmp = self._STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=1)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._STATE_FILE)
        except Exception as e:
            logger.warning(f"Autonomy: failed to save state: {e}")
            try:
                os.unlink(self._STATE_FILE + ".tmp")
            except OSError:
                pass

    # ── Goal management ──

    def add_goal(self, description: str, priority: int = 1, context: dict = None) -> Goal:
        """Add a new goal for the bot to pursue."""
        goal = Goal(description=description, priority=priority, context=context or {})
        self.goals.append(goal)
        self.goals.sort(key=lambda g: (g.status != "active", g.priority))
        self._save_state()
        logger.info(f"Autonomy: new goal added — {description}")
        return goal

    def complete_goal(self, goal: Goal, result: str = ""):
        """Mark a goal as completed."""
        goal.status = "completed"
        self.history.append({
            "type": "goal_completed",
            "goal": goal.description,
            "time": time.time(),
            "attempts": goal.attempts,
            "result": result[:500],
        })
        self._save_state()

    def fail_goal(self, goal: Goal, error: str):
        """Mark a goal as failed after max attempts."""
        goal.status = "failed"
        goal.last_error = error[:500]
        self.history.append({
            "type": "goal_failed",
            "goal": goal.description,
            "time": time.time(),
            "error": error[:500],
            "attempts": goal.attempts,
        })
        self._save_state()

    def get_active_goals(self) -> list[Goal]:
        """Get goals that are pending or active."""
        return [g for g in self.goals if g.status in ("pending", "active")]

    def get_status_summary(self) -> str:
        """Brief summary of autonomy state."""
        active = sum(1 for g in self.goals if g.status == "active")
        pending = sum(1 for g in self.goals if g.status == "pending")
        completed = sum(1 for g in self.goals if g.status == "completed")
        failed = sum(1 for g in self.goals if g.status == "failed")
        return (
            f"Goals: {active} active, {pending} pending, "
            f"{completed} done, {failed} failed | "
            f"History: {len(self.history)} actions"
        )

    # ── Observe-Act-Verify loop ──

    async def observe(self) -> Observation:
        """Observe current system state."""
        obs = Observation()
        obs.pending_goals = len(self.get_active_goals())

        # Get active window
        try:
            import pyautogui
            win = pyautogui.getActiveWindow()
            if win:
                obs.active_window = win.title
        except Exception:
            pass

        # System health
        try:
            from self_monitor import self_monitor
            obs.system_health = self_monitor.get_health_summary()
        except Exception:
            pass

        # Last action result
        if self.history:
            last = self.history[-1]
            obs.last_action_result = last.get("result", last.get("error", ""))[:200]

        return obs

    async def plan_next_action(self, goal: Goal, observation: Observation) -> dict:
        """Use Claude to plan the next action for a goal.

        Returns dict with: action, tool, params, reasoning
        """
        from agents.loop import _cli_call

        context_str = json.dumps(goal.context, ensure_ascii=False)[:500] if goal.context else ""
        prompt = (
            f"Goal: {goal.description}\n"
            f"Attempt: {goal.attempts + 1}/{goal.max_attempts}\n"
            f"Active window: {observation.active_window}\n"
            f"Last result: {observation.last_action_result[:200]}\n"
            f"Context: {context_str}\n"
            f"Previous error: {goal.last_error}\n\n"
            f"What is the single next action to take? "
            f"Reply in JSON: {{\"action\": \"...\", \"tool\": \"...\", \"params\": {{...}}, \"reasoning\": \"...\"}}"
        )

        try:
            response, _ = await _cli_call(
                prompt,
                model="claude-haiku-4-5-20251001",  # Fast planning
                timeout=30,
                cwd=goal.context.get("project_dir", BOT_DIR),
            )
            # Parse JSON from response
            import re
            json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
        except Exception as e:
            logger.warning(f"Autonomy: planning failed: {e}")

        return {"action": "skip", "reasoning": "Planning failed"}

    async def execute_action(self, action: dict, goal: Goal) -> tuple[bool, str]:
        """Execute a planned action. Returns (success, result)."""
        tool_name = action.get("tool", "")
        params = action.get("params", {})

        if not tool_name or action.get("action") == "skip":
            return False, "No action to execute"

        try:
            from tools import execute_tool
            result, _ = await execute_tool(tool_name, params)
            success = "error" not in result.lower()[:100]
            return success, result[:1000]
        except Exception as e:
            return False, str(e)[:500]

    async def verify_action(self, action: dict, result: str, goal: Goal) -> bool:
        """Verify if the action achieved what was intended."""
        # Simple heuristic verification
        if "error" in result.lower()[:200]:
            return False
        if "failed" in result.lower()[:200]:
            return False
        if "not found" in result.lower()[:200]:
            return False
        if result.strip() == "(no output)":
            return False
        return True

    async def run_one_cycle(self, goal: Goal) -> bool:
        """Run one observe-act-verify cycle for a goal. Returns True if goal completed."""
        goal.status = "active"
        goal.attempts += 1

        # 1. Observe
        obs = await self.observe()

        # 2. Plan
        action = await self.plan_next_action(goal, obs)
        logger.info(f"Autonomy: planned action for '{goal.description[:50]}': {action.get('action', '?')}")

        # 3. Act
        success, result = await self.execute_action(action, goal)

        # 4. Verify
        verified = await self.verify_action(action, result, goal) if success else False

        # 5. Learn
        self.history.append({
            "type": "action",
            "goal": goal.description[:100],
            "action": action.get("action", "?"),
            "tool": action.get("tool", ""),
            "success": verified,
            "result": result[:300],
            "time": time.time(),
        })

        if not verified:
            goal.last_error = result[:500]
            if goal.attempts >= goal.max_attempts:
                self.fail_goal(goal, f"Max attempts reached. Last: {result[:200]}")
                return False
        else:
            # Check if goal is complete (simple check — could use Claude for complex goals)
            if "done" in result.lower() or "complete" in result.lower() or "success" in result.lower():
                self.complete_goal(goal, result[:500])
                return True

        self._save_state()
        return False

    # ── Continuous loop ──

    async def _continuous_loop(self, interval: float = 10.0):
        """Background loop that pursues goals autonomously."""
        logger.info("Autonomy: continuous loop started")
        while self._running:
            try:
                active_goals = self.get_active_goals()
                if not active_goals:
                    await asyncio.sleep(interval)
                    continue

                # Work on highest priority goal
                goal = active_goals[0]
                completed = await self.run_one_cycle(goal)

                if completed and self._send_fn:
                    try:
                        await self._send_fn(f"✅ Goal completed: {goal.description[:100]}")
                    except Exception:
                        pass

                await asyncio.sleep(max(2.0, interval))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Autonomy loop error: {e}")
                await asyncio.sleep(interval * 2)

        logger.info("Autonomy: continuous loop stopped")

    def start(self, send_fn: Callable = None, interval: float = 10.0):
        """Start the autonomous loop."""
        if self._running:
            return
        self._running = True
        self._send_fn = send_fn
        self._loop_task = asyncio.create_task(self._continuous_loop(interval))
        def _on_done(t):
            try:
                if not t.cancelled():
                    t.result()
            except Exception as e:
                logger.error(f"Autonomy loop crashed: {e}", exc_info=True)
        self._loop_task.add_done_callback(_on_done)

    def stop(self):
        """Stop the autonomous loop."""
        self._running = False
        if self._loop_task and not self._loop_task.done():
            self._loop_task.cancel()

    # ── Self-evolution ──

    async def self_evaluate(self) -> dict:
        """Evaluate the bot's own performance and suggest improvements."""
        stats = {
            "total_actions": len(self.history),
            "successes": sum(1 for h in self.history if h.get("success")),
            "failures": sum(1 for h in self.history if not h.get("success") and h.get("type") == "action"),
            "goals_completed": sum(1 for g in self.goals if g.status == "completed"),
            "goals_failed": sum(1 for g in self.goals if g.status == "failed"),
        }
        total = stats["successes"] + stats["failures"]
        stats["success_rate"] = stats["successes"] / total if total > 0 else 0

        # Identify weak areas from failure patterns
        failure_tools = {}
        for h in self.history:
            if not h.get("success") and h.get("type") == "action":
                tool = h.get("tool", "unknown")
                failure_tools[tool] = failure_tools.get(tool, 0) + 1
        stats["weak_tools"] = sorted(failure_tools.items(), key=lambda x: -x[1])[:5]

        return stats

    async def propose_self_improvement(self) -> list[str]:
        """Analyze performance and propose improvements to the bot's own code."""
        stats = await self.self_evaluate()
        proposals = []

        if stats["success_rate"] < 0.7 and stats["total_actions"] > 10:
            proposals.append(
                f"Success rate is {stats['success_rate']:.0%}. "
                f"Weak tools: {stats['weak_tools'][:3]}. "
                f"Consider improving error handling or adding retries."
            )

        if stats["goals_failed"] > stats["goals_completed"] and stats["goals_failed"] > 3:
            proposals.append(
                f"More goals failing ({stats['goals_failed']}) than completing ({stats['goals_completed']}). "
                f"May need to improve planning or break goals into smaller sub-tasks."
            )

        return proposals


# ── Module-level singleton ──

_engine: AutonomyEngine | None = None

def get_autonomy_engine() -> AutonomyEngine:
    global _engine
    if _engine is None:
        _engine = AutonomyEngine()
    return _engine
