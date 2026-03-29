"""
agents/reflexion.py — Self-critique and reflection loop (Shinn et al., 2023).

After each action/response, the bot reflects on what it did:
1. Did the action achieve the goal?
2. What could have been done better?
3. What should be remembered for next time?

This implements the Reflexion pattern from the NLAH paper:
- Generate response → Execute → Observe outcome → Reflect → Store insight → Retry if needed

The reflection memory is separate from action_memory — it stores *insights*
(why things worked/failed) rather than raw action logs.
"""

import asyncio
import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_REFLECTIONS_FILE = os.path.join(BOT_DIR, ".reflections.jsonl")
_MAX_REFLECTIONS = 500


class ReflexionEngine:
    """Self-critique engine that learns from each interaction."""

    def __init__(self):
        self._reflections: list[dict] = []
        self._insights: dict[str, list[str]] = {}  # category -> insights
        self._load()

    def _load(self):
        try:
            if os.path.exists(_REFLECTIONS_FILE):
                with open(_REFLECTIONS_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                self._reflections.append(json.loads(line))
                            except json.JSONDecodeError:
                                pass
                self._reflections = self._reflections[-_MAX_REFLECTIONS:]
                # Rebuild insights index
                for r in self._reflections:
                    cat = r.get("category", "general")
                    insight = r.get("insight", "")
                    if insight:
                        self._insights.setdefault(cat, []).append(insight)
        except Exception as e:
            logger.warning(f"Reflexion: load failed: {e}")

    def _save_reflection(self, reflection: dict):
        """Append one reflection to JSONL file."""
        try:
            with open(_REFLECTIONS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(reflection, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"Reflexion: save failed: {e}")

    def reflect_on_action(
        self,
        action: str,
        result: str,
        success: bool,
        user_feedback: str = "",
        duration_ms: float = 0,
    ) -> dict:
        """
        Reflect on an action and extract insights.

        This is a lightweight local reflection (no LLM call).
        For deeper reflection, use reflect_with_llm().
        """
        reflection = {
            "timestamp": time.time(),
            "action": action[:200],
            "result": result[:300],
            "success": success,
            "user_feedback": user_feedback[:200],
            "duration_ms": duration_ms,
            "insight": "",
            "category": "general",
        }

        # Auto-categorize
        action_lower = action.lower()
        if any(kw in action_lower for kw in ["click", "screenshot", "mouse", "keyboard", "window"]):
            reflection["category"] = "computer_control"
        elif any(kw in action_lower for kw in ["file", "code", "edit", "write", "read", "git"]):
            reflection["category"] = "coding"
        elif any(kw in action_lower for kw in ["browser", "navigate", "url", "web"]):
            reflection["category"] = "browsing"
        elif any(kw in action_lower for kw in ["search", "find", "query"]):
            reflection["category"] = "search"

        # Extract insight from outcome
        if not success:
            if "timeout" in result.lower():
                reflection["insight"] = f"Action '{action[:50]}' timed out. Consider shorter timeout or async approach."
            elif "not found" in result.lower():
                reflection["insight"] = f"Target not found for '{action[:50]}'. Verify element exists before acting."
            elif "permission" in result.lower() or "denied" in result.lower():
                reflection["insight"] = f"Permission issue on '{action[:50]}'. May need elevated access."
            elif "error" in result.lower():
                reflection["insight"] = f"Error in '{action[:50]}': {result[:100]}. Add error handling."
            else:
                reflection["insight"] = f"Failed: '{action[:50]}' → {result[:100]}"
        else:
            if duration_ms > 10000:
                reflection["insight"] = f"Slow success ({duration_ms:.0f}ms) for '{action[:50]}'. Optimize if repeated."
            elif user_feedback and any(neg in user_feedback.lower() for neg in ["不", "wrong", "no", "错"]):
                reflection["insight"] = f"User disagreed despite technical success on '{action[:50]}'. Misunderstood intent."

        # Store
        self._reflections.append(reflection)
        if reflection["insight"]:
            self._insights.setdefault(reflection["category"], []).append(reflection["insight"])
        self._save_reflection(reflection)

        return reflection

    async def reflect_with_llm(self, action: str, result: str, context: str = "") -> str:
        """Use Claude to generate a deeper reflection (more expensive, use sparingly)."""
        try:
            from agents.loop import _cli_call
            prompt = (
                f"Reflect on this action and its result. What went well? What could improve? "
                f"What insight should be remembered?\n\n"
                f"Action: {action[:300]}\n"
                f"Result: {result[:500]}\n"
                f"Context: {context[:300]}\n\n"
                f"Reply with a single insight sentence (under 100 chars)."
            )
            response, _ = await _cli_call(
                prompt, model="claude-haiku-4-5-20251001", timeout=15
            )
            return response.strip()[:200]
        except Exception as e:
            return f"Reflection failed: {e}"

    def get_relevant_insights(self, category: str, n: int = 5) -> list[str]:
        """Get the most recent insights for a category."""
        insights = self._insights.get(category, [])
        return insights[-n:] if insights else []

    def get_all_insights(self, n: int = 10) -> list[str]:
        """Get recent insights across all categories."""
        all_insights = []
        for cat_insights in self._insights.values():
            all_insights.extend(cat_insights[-3:])
        return all_insights[-n:]

    def get_failure_rate_by_category(self) -> dict[str, float]:
        """Get failure rates per category."""
        cats: dict[str, list[bool]] = {}
        for r in self._reflections[-100:]:
            cat = r.get("category", "general")
            cats.setdefault(cat, []).append(r.get("success", True))
        return {
            cat: 1 - (sum(results) / len(results)) if results else 0
            for cat, results in cats.items()
        }

    def should_retry(self, action: str, category: str = "general") -> tuple[bool, str]:
        """Check if an action should be retried based on past reflections."""
        # Look at recent reflections for this category
        recent = [r for r in self._reflections[-20:] if r.get("category") == category]
        failures = [r for r in recent if not r.get("success")]

        if len(failures) >= 3:
            # Too many recent failures in this category — suggest alternative
            return False, f"Category '{category}' has {len(failures)} recent failures. Try a different approach."

        return True, ""


# ── Singleton ──

_engine: ReflexionEngine | None = None

def get_reflexion_engine() -> ReflexionEngine:
    global _engine
    if _engine is None:
        _engine = ReflexionEngine()
    return _engine
