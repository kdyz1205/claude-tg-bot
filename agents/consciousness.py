"""
agents/consciousness.py — Self-awareness and meta-cognition layer.

This module gives the bot awareness of:
1. Its own state (health, capabilities, limitations)
2. Its performance over time (improving? degrading?)
3. When it needs to evolve (new patterns, new capabilities)
4. How to improve itself (code changes, strategy shifts)

The "consciousness" is really a meta-cognitive loop:
- Monitor own performance metrics
- Detect drift or degradation
- Propose and execute self-improvements
- Track which improvements actually helped
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class SelfAwareness:
    """
    Meta-cognitive layer that monitors the bot's own state.

    Tracks:
    - Performance trends (success rate, response time, error frequency)
    - Capability gaps (what users ask for that the bot can't do)
    - Evolution history (what changed and whether it helped)
    - Resource usage (token costs, API limits, memory)
    """

    _STATE_FILE = os.path.join(BOT_DIR, ".consciousness_state.json")
    _MAX_EVOLUTION_LOG = 100
    _MAX_CAPABILITY_GAPS = 50

    def __init__(self):
        self.performance_windows: list[dict] = []  # rolling 24h performance snapshots
        self.capability_gaps: list[dict] = []       # things users asked that bot couldn't do
        self.evolution_log: list[dict] = []         # self-improvement attempts and results
        self.identity: dict = {
            "name": "TG Remote Controller",
            "version": "4.0",
            "created": "2025",
            "purpose": "Remote PC control + autonomous coding via Telegram",
            "strengths": [],
            "weaknesses": [],
            "last_self_reflection": 0,
        }
        self._load()

    def _load(self):
        try:
            if os.path.exists(self._STATE_FILE):
                with open(self._STATE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.performance_windows = data.get("performance_windows", [])[-48:]
                self.capability_gaps = data.get("capability_gaps", [])[-self._MAX_CAPABILITY_GAPS:]
                self.evolution_log = data.get("evolution_log", [])[-self._MAX_EVOLUTION_LOG:]
                saved_identity = data.get("identity", {})
                self.identity.update(saved_identity)
        except Exception as e:
            logger.warning(f"Consciousness: load failed: {e}")

    def _save(self):
        try:
            data = {
                "performance_windows": self.performance_windows[-48:],
                "capability_gaps": self.capability_gaps[-self._MAX_CAPABILITY_GAPS:],
                "evolution_log": self.evolution_log[-self._MAX_EVOLUTION_LOG:],
                "identity": self.identity,
                "saved_at": time.time(),
            }
            tmp = self._STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=1)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self._STATE_FILE)
        except Exception as e:
            logger.warning(f"Consciousness: save failed: {e}")
            try:
                os.unlink(self._STATE_FILE + ".tmp")
            except OSError:
                pass

    # ── Performance monitoring ──

    def record_performance_snapshot(self):
        """Take a snapshot of current performance metrics."""
        snapshot = {
            "timestamp": time.time(),
            "metrics": {},
        }

        try:
            from self_monitor import self_monitor
            health = self_monitor.get_health_summary()
            snapshot["metrics"]["health"] = health.get("overall", "unknown")
            snapshot["metrics"]["consecutive_failures"] = health.get("consecutive_failures", 0)
            snapshot["metrics"]["success_rate"] = health.get("success_rate", 0)
        except Exception:
            pass

        try:
            import harness_learn
            scores = harness_learn.get_recent_scores(10)
            if scores:
                avg = sum(s.get("score", 0) for s in scores) / len(scores)
                snapshot["metrics"]["avg_score"] = round(avg, 3)
        except Exception:
            pass

        self.performance_windows.append(snapshot)
        self.performance_windows = self.performance_windows[-48:]  # Keep 48 snapshots (24h at 30min intervals)
        self._save()
        return snapshot

    def detect_performance_trend(self) -> str:
        """Detect if performance is improving, stable, or degrading."""
        windows = self.performance_windows
        if len(windows) < 4:
            return "insufficient_data"

        recent = windows[-4:]
        older = windows[-8:-4] if len(windows) >= 8 else windows[:4]

        def avg_score(w):
            scores = [s["metrics"].get("avg_score", 0) for s in w if "avg_score" in s.get("metrics", {})]
            return sum(scores) / len(scores) if scores else 0

        recent_avg = avg_score(recent)
        older_avg = avg_score(older)

        if recent_avg > older_avg + 0.05:
            return "improving"
        elif recent_avg < older_avg - 0.05:
            return "degrading"
        return "stable"

    # ── Capability gap tracking ──

    def record_capability_gap(self, user_request: str, failure_reason: str):
        """Record something the user asked that the bot couldn't do."""
        self.capability_gaps.append({
            "request": user_request[:300],
            "reason": failure_reason[:300],
            "timestamp": time.time(),
            "addressed": False,
        })
        self.capability_gaps = self.capability_gaps[-self._MAX_CAPABILITY_GAPS:]
        self._save()

    def get_top_gaps(self, n: int = 5) -> list[dict]:
        """Get the most common unaddressed capability gaps."""
        unaddressed = [g for g in self.capability_gaps if not g.get("addressed")]
        # Group by similar reasons
        reasons = {}
        for g in unaddressed:
            key = g["reason"][:100]
            reasons.setdefault(key, []).append(g)
        # Sort by frequency
        sorted_gaps = sorted(reasons.items(), key=lambda x: -len(x[1]))
        return [{"reason": k, "count": len(v), "examples": [x["request"][:100] for x in v[:3]]} for k, v in sorted_gaps[:n]]

    # ── Self-evolution ──

    def record_evolution(self, change_type: str, description: str, files_changed: list[str] = None):
        """Record a self-improvement attempt."""
        entry = {
            "type": change_type,  # "bug_fix", "new_feature", "optimization", "learning"
            "description": description[:500],
            "files": files_changed or [],
            "timestamp": time.time(),
            "outcome": "pending",  # will be updated later
        }
        self.evolution_log.append(entry)
        self.evolution_log = self.evolution_log[-self._MAX_EVOLUTION_LOG:]
        self._save()
        return len(self.evolution_log) - 1

    def record_evolution_outcome(self, index: int, success: bool, notes: str = ""):
        """Record whether a self-improvement actually helped."""
        if 0 <= index < len(self.evolution_log):
            self.evolution_log[index]["outcome"] = "success" if success else "failed"
            self.evolution_log[index]["notes"] = notes[:300]
            self._save()

    # ── Self-reflection ──

    def self_reflect(self) -> dict:
        """Generate a self-reflection report."""
        now = time.time()
        self.identity["last_self_reflection"] = now

        report = {
            "timestamp": now,
            "performance_trend": self.detect_performance_trend(),
            "top_capability_gaps": self.get_top_gaps(3),
            "recent_evolutions": self.evolution_log[-5:],
            "identity": self.identity,
        }

        # Update strengths/weaknesses based on data
        evolutions = self.evolution_log[-20:]
        successes = [e for e in evolutions if e.get("outcome") == "success"]
        failures = [e for e in evolutions if e.get("outcome") == "failed"]

        if successes:
            self.identity["strengths"] = list(set(
                e["type"] for e in successes
            ))[:5]
        if failures:
            self.identity["weaknesses"] = list(set(
                e["type"] for e in failures
            ))[:5]

        self._save()
        return report

    def get_self_description(self) -> str:
        """Generate a self-aware description for system prompts."""
        trend = self.detect_performance_trend()
        gaps = self.get_top_gaps(2)
        gap_str = "; ".join(g["reason"][:50] for g in gaps) if gaps else "none identified"

        return (
            f"I am {self.identity['name']} v{self.identity['version']}. "
            f"Performance: {trend}. "
            f"Strengths: {', '.join(self.identity.get('strengths', ['general']))[:100]}. "
            f"Known gaps: {gap_str}. "
            f"I have completed {sum(1 for e in self.evolution_log if e.get('outcome') == 'success')} "
            f"self-improvements and learned from {sum(1 for e in self.evolution_log if e.get('outcome') == 'failed')} failures."
        )


# ── Singleton ──

_awareness: SelfAwareness | None = None

def get_self_awareness() -> SelfAwareness:
    global _awareness
    if _awareness is None:
        _awareness = SelfAwareness()
    return _awareness
