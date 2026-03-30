"""
memory_engine.py — Structured JSON memory system for the Telegram bot.

Categories:
  - user_profile: User preferences and known facts
  - shortcuts: Auto-learned command shortcuts (trigger → action)
  - patterns: Recurring command patterns with success/fail counts
  - summaries: Periodic conversation summaries

Auto-cleanup: when total entries > 500, lowest-scored entries are pruned.
"""

import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
MEMORY_FILE = os.path.join(BOT_DIR, ".bot_memory.json")
MAX_ENTRIES = 500

_lock = threading.Lock()
_memory: dict | None = None


# ── Internal helpers ────────────────────────────────────────────────────────

def _default_memory() -> dict:
    return {
        "version": 2,
        "user_profile": {
            "name": "Alex",
            "language": "中英混用，中文为主",
            "style": "简洁直接，不要问问题",
            "screen": "1920x1080",
            "projects": {
                "crypto": "C:/Users/alexl/Desktop/crypto-analysis-/",
                "tg_bot": "C:/Users/alexl/Desktop/claude tg bot/",
            },
        },
        "shortcuts": [],
        "patterns": [],
        "summaries": [],
        "last_updated": datetime.now().isoformat(),
    }


def _load() -> dict:
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and data.get("version") == 2:
                return data
        except Exception as e:
            logger.warning("memory_engine: failed to load: %s", e)
    # Try migrating from .bot_memory.md
    return _migrate_from_md()


def _migrate_from_md() -> dict:
    """Migrate existing .bot_memory.md into the new JSON format."""
    mem = _default_memory()
    md_path = os.path.join(BOT_DIR, ".bot_memory.md")
    if os.path.exists(md_path):
        try:
            with open(md_path, "r", encoding="utf-8") as f:
                content = f.read()
            # Store old md content as first summary
            if content.strip():
                mem["summaries"].append({
                    "date": datetime.now().isoformat(),
                    "text": content[:2000],
                    "source": "migrated_from_md",
                    "score": 1.0,
                })
        except Exception as e:
            logger.warning("memory_engine: migration failed: %s", e)
    return mem


def _save(mem: dict) -> None:
    tmp = MEMORY_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(mem, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, MEMORY_FILE)
    except Exception as e:
        logger.warning("memory_engine: failed to save: %s", e)


def _total_entries(mem: dict) -> int:
    return len(mem.get("shortcuts", [])) + len(mem.get("patterns", [])) + len(mem.get("summaries", []))


def _cleanup(mem: dict) -> None:
    """Remove lowest-scored entries across all lists when total > MAX_ENTRIES."""
    if _total_entries(mem) <= MAX_ENTRIES:
        return
    # Sort each list by score ascending, drop bottom entries
    for key in ("shortcuts", "patterns", "summaries"):
        lst = mem[key]
        lst.sort(key=lambda x: x.get("score", 0.5))
    # Remove lowest score items until under limit
    while _total_entries(mem) > MAX_ENTRIES:
        # Find category with lowest minimum score
        candidates = []
        for key in ("shortcuts", "patterns", "summaries"):
            if mem[key]:
                candidates.append((mem[key][0].get("score", 0.5), key))
        if not candidates:
            break
        candidates.sort()
        _, worst_key = candidates[0]
        mem[worst_key].pop(0)
    logger.info("memory_engine: cleanup done, %d entries remain", _total_entries(mem))


# ── Public API ───────────────────────────────────────────────────────────────

def get_memory() -> dict:
    """Return the in-memory dict (load once per process)."""
    global _memory
    with _lock:
        if _memory is None:
            _memory = _load()
        return _memory


def save() -> None:
    """Persist current memory to disk."""
    with _lock:
        if _memory is not None:
            _memory["last_updated"] = datetime.now().isoformat()
            _cleanup(_memory)
            _save(_memory)


def learn_pattern(text: str, success: bool, duration_ms: float = 0.0) -> None:
    """Record a command pattern outcome; auto-create shortcut for frequent successes."""
    if not text or len(text) < 2:
        return
    key = text.strip()[:120]
    mem = get_memory()
    with _lock:
        # Find existing pattern
        for p in mem["patterns"]:
            if p["text"] == key:
                if success:
                    p["success_count"] = p.get("success_count", 0) + 1
                else:
                    p["fail_count"] = p.get("fail_count", 0) + 1
                total = p["success_count"] + p.get("fail_count", 0)
                p["score"] = p["success_count"] / total if total else 0.5
                p["last_used"] = datetime.now().isoformat()
                # Promote to shortcut if frequent enough
                _maybe_promote_shortcut(mem, p)
                return
        # New pattern
        entry = {
            "text": key,
            "success_count": 1 if success else 0,
            "fail_count": 0 if success else 1,
            "score": 1.0 if success else 0.0,
            "last_used": datetime.now().isoformat(),
            "duration_ms": round(duration_ms),
        }
        mem["patterns"].append(entry)
    # Debounced save every 20 pattern updates
    _maybe_autosave()


_save_counter = 0

def _maybe_autosave() -> None:
    global _save_counter
    _save_counter += 1
    if _save_counter % 20 == 0:
        # save() acquires _lock internally, safe from any thread
        save()


def _maybe_promote_shortcut(mem: dict, pattern: dict) -> None:
    """Promote pattern to shortcut when it has 3+ successes and score >= 0.8."""
    if pattern["success_count"] < 3 or pattern.get("score", 0) < 0.8:
        return
    key = pattern["text"]
    # Already a shortcut?
    for s in mem["shortcuts"]:
        if s.get("trigger") == key:
            s["frequency"] = pattern["success_count"]
            s["score"] = pattern["score"]
            return
    mem["shortcuts"].append({
        "trigger": key,
        "action": key,  # same text — bot already knows how to handle it
        "frequency": pattern["success_count"],
        "score": pattern["score"],
        "created": datetime.now().isoformat(),
    })
    logger.info("memory_engine: promoted shortcut: %.60s", key)


def add_summary(text: str, source: str = "auto") -> None:
    """Add a conversation summary entry."""
    if not text or len(text) < 10:
        return
    mem = get_memory()
    with _lock:
        mem["summaries"].append({
            "date": datetime.now().isoformat(),
            "text": text[:1000],
            "source": source,
            "score": 0.7,
        })
    save()


def update_profile(key: str, value: Any) -> None:
    """Update a user profile field."""
    mem = get_memory()
    with _lock:
        mem["user_profile"][key] = value
    save()


def get_shortcuts() -> list[dict]:
    mem = get_memory()
    return sorted(mem["shortcuts"], key=lambda x: x.get("frequency", 0), reverse=True)


def get_patterns(top_n: int = 20) -> list[dict]:
    mem = get_memory()
    return sorted(mem["patterns"], key=lambda x: x.get("success_count", 0), reverse=True)[:top_n]


def get_summaries(limit: int = 5) -> list[dict]:
    mem = get_memory()
    return mem["summaries"][-limit:]


def format_display() -> str:
    """Return a human-readable memory overview for Telegram."""
    mem = get_memory()
    profile = mem["user_profile"]
    shortcuts = get_shortcuts()[:10]
    patterns = get_patterns(10)
    summaries = get_summaries(3)
    stats = (
        f"Shortcuts: {len(mem['shortcuts'])} | "
        f"Patterns: {len(mem['patterns'])} | "
        f"Summaries: {len(mem['summaries'])}"
    )

    lines = [
        "🧠 **Bot Memory**",
        f"Updated: {mem.get('last_updated', '?')[:16]}",
        f"Entries: {stats}",
        "",
        "**👤 Profile**",
    ]
    for k, v in profile.items():
        if isinstance(v, dict):
            lines.append(f"  {k}: {list(v.keys())}")
        else:
            lines.append(f"  {k}: {v}")

    if shortcuts:
        lines.append("\n**⚡ Top Shortcuts**")
        for s in shortcuts[:5]:
            lines.append(f"  [{s.get('frequency', 0)}x] {s['trigger'][:60]}")

    if patterns:
        lines.append("\n**📊 Top Patterns** (success/total)")
        for p in patterns[:5]:
            tot = p["success_count"] + p.get("fail_count", 0)
            lines.append(f"  {p['success_count']}/{tot} {p['text'][:60]}")

    if summaries:
        lines.append("\n**📝 Recent Summaries**")
        for s in summaries:
            lines.append(f"  [{s['date'][:10]}] {s['text'][:80]}...")

    return "\n".join(lines)


def format_stats_brief() -> str:
    """One-line stats for the panel memory button."""
    mem = get_memory()
    total = _total_entries(mem)
    shortcuts = len(mem["shortcuts"])
    patterns = len(mem["patterns"])
    return (
        f"🧠 Memory: {total} entries "
        f"({shortcuts} shortcuts, {patterns} patterns)\n"
        f"Updated: {mem.get('last_updated', '?')[:16]}"
    )


def get_context_for_prompt(max_chars: int = 400) -> str:
    """Return compact memory context for injection into agent system prompt.

    Includes top shortcuts (high-frequency) and top patterns (high success).
    Stays under max_chars to not bloat the prompt.
    """
    mem = get_memory()
    lines = []

    shortcuts = sorted(mem["shortcuts"], key=lambda x: x.get("frequency", 0), reverse=True)[:5]
    if shortcuts:
        lines.append("已学习快捷指令:")
        for s in shortcuts:
            lines.append(f"  [{s.get('frequency', 0)}x] {s['trigger'][:60]}")

    patterns = sorted(mem["patterns"], key=lambda x: x.get("success_count", 0), reverse=True)[:5]
    if patterns:
        lines.append("高频成功指令:")
        for p in patterns:
            sc = p.get("success_count", 0)
            fc = p.get("fail_count", 0)
            if sc > 0:
                lines.append(f"  {sc}/{sc+fc} {p['text'][:60]}")

    result = "\n".join(lines)
    return result[:max_chars] if result else ""
