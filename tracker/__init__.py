"""
Quota Tracker — adaptive usage management for all AI platforms.

Tracks per-platform:
- Messages sent / remaining
- Cooldown windows (when limit hit → when it resets)
- Session state (can we resume a conversation?)
- Cost tier (free vs paid capacity)

The dispatcher queries this before routing. If Claude is exhausted,
it automatically falls back to GPT. When Claude resets, it comes back.
"""

from tracker.quota import QuotaTracker, PlatformQuota, UsageRecord
from tracker.session_store import SessionStore, SessionState

__all__ = [
    "QuotaTracker", "PlatformQuota", "UsageRecord",
    "SessionStore", "SessionState",
]
