"""
Quota Tracker — knows every platform's limits and current usage.

Each platform has different rules:
- Claude Pro:   ~45 Opus / ~100 Sonnet messages per 5 hours
- ChatGPT Plus: ~80 GPT-4o messages per 3 hours
- Grok:         ~30 messages per 2 hours (free tier)
- Claude Code:  Usage-based with daily caps

These numbers change. The tracker is designed to learn from actual
rate limit responses and self-adjust.
"""

from __future__ import annotations

import json
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class PlatformQuota:
    """Quota definition for a single platform."""
    platform: str
    max_messages_per_window: int    # e.g., 45 for Claude Opus
    window_seconds: int             # e.g., 18000 (5 hours)
    cooldown_seconds: int           # How long to wait after hitting limit
    priority: int = 0               # Higher = preferred when available
    is_free: bool = True            # Free tier or paid
    notes: str = ""

    @property
    def window_hours(self) -> float:
        return self.window_seconds / 3600


# Default quotas — adjust based on your actual subscription tiers
DEFAULT_QUOTAS: dict[str, PlatformQuota] = {
    "claude_web": PlatformQuota(
        platform="claude_web",
        max_messages_per_window=100,
        window_seconds=5 * 3600,     # 5 hours
        cooldown_seconds=5 * 3600,
        priority=8,
        notes="Claude Pro Sonnet. ~100 msgs / 5hr window",
    ),
    "claude_code": PlatformQuota(
        platform="claude_code",
        max_messages_per_window=50,
        window_seconds=5 * 3600,
        cooldown_seconds=5 * 3600,
        priority=10,                 # Highest priority for code tasks
        notes="Claude Code Web. Heavy usage per message",
    ),
    "gpt": PlatformQuota(
        platform="gpt",
        max_messages_per_window=80,
        window_seconds=3 * 3600,     # 3 hours
        cooldown_seconds=3 * 3600,
        priority=5,
        notes="ChatGPT Plus GPT-4o",
    ),
    "grok": PlatformQuota(
        platform="grok",
        max_messages_per_window=30,
        window_seconds=2 * 3600,     # 2 hours
        cooldown_seconds=2 * 3600,
        priority=3,
        notes="Grok free tier",
    ),
    "codex": PlatformQuota(
        platform="codex",
        max_messages_per_window=20,
        window_seconds=24 * 3600,    # Daily
        cooldown_seconds=24 * 3600,
        priority=9,
        notes="OpenAI Codex / ChatGPT Codex mode",
    ),
}


@dataclass
class UsageRecord:
    """A single usage event."""
    platform: str
    timestamp: float
    message_count: int = 1
    was_rate_limited: bool = False
    error: str = ""


class QuotaTracker:
    """
    Tracks real-time usage across all platforms.

    Features:
    - Records every message sent to each platform
    - Calculates remaining capacity in current window
    - Detects rate limits and auto-adjusts quotas
    - Tells the dispatcher which platforms are available
    - Persists state to disk so it survives restarts
    """

    def __init__(
        self,
        quotas: dict[str, PlatformQuota] | None = None,
        state_file: str = ".harness_state/quota.json",
    ):
        self.quotas = quotas or dict(DEFAULT_QUOTAS)
        self.state_file = Path(state_file)
        self.usage: dict[str, list[UsageRecord]] = {p: [] for p in self.quotas}
        self.cooldown_until: dict[str, float] = {p: 0 for p in self.quotas}

        # Load persisted state if exists
        self._load_state()

    # ── Core API ──────────────────────────────────────────────

    def record_usage(self, platform: str, was_rate_limited: bool = False):
        """Record that we sent a message to a platform."""
        if platform not in self.quotas:
            logger.warning(f"Unknown platform: {platform}")
            return

        record = UsageRecord(
            platform=platform,
            timestamp=time.time(),
            was_rate_limited=was_rate_limited,
        )
        self.usage.setdefault(platform, []).append(record)

        if was_rate_limited:
            self._handle_rate_limit(platform)

        self._cleanup_old_records()
        self._save_state()

    def is_available(self, platform: str) -> bool:
        """Is this platform available right now?"""
        if platform not in self.quotas:
            return False

        # Check cooldown
        if time.time() < self.cooldown_until.get(platform, 0):
            return False

        # Check usage within window
        remaining = self.remaining(platform)
        return remaining > 0

    def remaining(self, platform: str) -> int:
        """How many messages left in the current window?"""
        if platform not in self.quotas:
            return 0

        quota = self.quotas[platform]
        window_start = time.time() - quota.window_seconds
        recent = [
            r for r in self.usage.get(platform, [])
            if r.timestamp >= window_start
        ]
        used = sum(r.message_count for r in recent)
        return max(0, quota.max_messages_per_window - used)

    def usage_percent(self, platform: str) -> float:
        """What percentage of quota is used? 0.0 to 1.0."""
        if platform not in self.quotas:
            return 1.0
        quota = self.quotas[platform]
        remaining = self.remaining(platform)
        return 1.0 - (remaining / quota.max_messages_per_window)

    def cooldown_remaining(self, platform: str) -> float:
        """Seconds until cooldown ends. 0 if not in cooldown."""
        until = self.cooldown_until.get(platform, 0)
        return max(0, until - time.time())

    def time_until_available(self, platform: str) -> float:
        """Seconds until this platform becomes available again."""
        # If in cooldown, return cooldown time
        cd = self.cooldown_remaining(platform)
        if cd > 0:
            return cd

        # If over quota, find when oldest message in window expires
        if self.remaining(platform) <= 0:
            quota = self.quotas[platform]
            window_start = time.time() - quota.window_seconds
            records = sorted(
                [r for r in self.usage.get(platform, []) if r.timestamp >= window_start],
                key=lambda r: r.timestamp,
            )
            if records:
                # Oldest record will expire at: record.timestamp + window_seconds
                return records[0].timestamp + quota.window_seconds - time.time()

        return 0  # Available now

    # ── Adaptive Routing ──────────────────────────────────────

    def get_available_platforms(self) -> list[str]:
        """Get all available platforms, sorted by priority (highest first)."""
        available = [p for p in self.quotas if self.is_available(p)]
        return sorted(available, key=lambda p: self.quotas[p].priority, reverse=True)

    def get_best_platform(self, preferred: str | None = None) -> str | None:
        """
        Get the best available platform.
        Prefers `preferred` if it's available, otherwise falls back.
        """
        if preferred and self.is_available(preferred):
            return preferred

        available = self.get_available_platforms()
        return available[0] if available else None

    def get_fallback_chain(self, primary: str) -> list[str]:
        """
        Get the fallback chain for a platform.
        If primary is exhausted, try these in order.
        """
        all_platforms = sorted(
            self.quotas.keys(),
            key=lambda p: self.quotas[p].priority,
            reverse=True,
        )
        # Primary first, then others by priority
        chain = [primary] + [p for p in all_platforms if p != primary]
        return [p for p in chain if p in self.quotas]

    # ── Status Report ─────────────────────────────────────────

    def status_report(self) -> str:
        """Human-readable status of all platforms."""
        lines = ["Platform Status:"]
        lines.append("-" * 50)

        for name, quota in sorted(self.quotas.items(), key=lambda x: -x[1].priority):
            remaining = self.remaining(name)
            total = quota.max_messages_per_window
            pct = self.usage_percent(name)
            available = self.is_available(name)

            status = "AVAILABLE" if available else "COOLDOWN"
            bar = self._progress_bar(pct)

            line = f"  {name:15s} {bar} {remaining:3d}/{total:3d}  [{status}]"

            cd = self.cooldown_remaining(name)
            if cd > 0:
                mins = int(cd / 60)
                line += f"  (resets in {mins}m)"

            lines.append(line)

        return "\n".join(lines)

    def _progress_bar(self, pct: float, width: int = 15) -> str:
        """Simple text progress bar."""
        filled = int(pct * width)
        empty = width - filled
        if pct >= 0.9:
            return f"[{'#' * filled}{'.' * empty}] WARN"
        elif pct >= 0.7:
            return f"[{'#' * filled}{'.' * empty}]  70%"
        else:
            return f"[{'#' * filled}{'.' * empty}]  OK "

    # ── Internal ──────────────────────────────────────────────

    def _handle_rate_limit(self, platform: str):
        """Handle a rate limit response — enter cooldown."""
        quota = self.quotas[platform]
        self.cooldown_until[platform] = time.time() + quota.cooldown_seconds
        logger.warning(
            f"[{platform}] Rate limited! Cooldown for "
            f"{quota.cooldown_seconds / 3600:.1f}h"
        )

        # Adaptive: if we hit the limit before expected, reduce our estimate
        window_start = time.time() - quota.window_seconds
        actual_used = sum(
            r.message_count for r in self.usage.get(platform, [])
            if r.timestamp >= window_start
        )
        if actual_used < quota.max_messages_per_window:
            # We hit the limit sooner than expected — adjust down
            old_max = quota.max_messages_per_window
            quota.max_messages_per_window = max(1, actual_used - 1)
            logger.info(
                f"[{platform}] Adjusted quota: {old_max} → "
                f"{quota.max_messages_per_window} (adaptive)"
            )

    def _cleanup_old_records(self):
        """Remove usage records older than the longest window."""
        max_window = max(q.window_seconds for q in self.quotas.values())
        cutoff = time.time() - (max_window * 2)  # Keep 2x window for safety
        for platform in self.usage:
            self.usage[platform] = [
                r for r in self.usage[platform]
                if r.timestamp >= cutoff
            ]

    def _save_state(self):
        """Persist state to disk."""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "cooldown_until": self.cooldown_until,
            "usage": {
                platform: [
                    {
                        "timestamp": r.timestamp,
                        "message_count": r.message_count,
                        "was_rate_limited": r.was_rate_limited,
                    }
                    for r in records
                ]
                for platform, records in self.usage.items()
            },
            "adjusted_quotas": {
                name: q.max_messages_per_window
                for name, q in self.quotas.items()
            },
            "saved_at": time.time(),
        }
        self.state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _load_state(self):
        """Load persisted state from disk."""
        if not self.state_file.exists():
            return

        try:
            state = json.loads(self.state_file.read_text(encoding="utf-8"))

            # Restore cooldowns
            self.cooldown_until = {
                k: v for k, v in state.get("cooldown_until", {}).items()
                if k in self.quotas
            }

            # Restore usage records
            for platform, records in state.get("usage", {}).items():
                if platform in self.quotas:
                    self.usage[platform] = [
                        UsageRecord(
                            platform=platform,
                            timestamp=r["timestamp"],
                            message_count=r.get("message_count", 1),
                            was_rate_limited=r.get("was_rate_limited", False),
                        )
                        for r in records
                    ]

            # Restore adjusted quotas
            for name, max_msg in state.get("adjusted_quotas", {}).items():
                if name in self.quotas:
                    self.quotas[name].max_messages_per_window = max_msg

            self._cleanup_old_records()
            logger.info("Quota state restored from disk")

        except (json.JSONDecodeError, KeyError) as e:
            logger.warning(f"Failed to load quota state: {e}")
