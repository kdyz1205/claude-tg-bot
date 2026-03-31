"""
Session Learner — Learn from active Claude Code sessions' logs and behaviors.
Cross-session communication: ask questions, delegate tasks, share knowledge.

Uses Claude Code CLI --resume flag for direct session interaction (no GUI).
Reads Claude Code session transcripts (JSONL files), extracts problem-solving
patterns, builds a reusable knowledge base, and enables cross-session
communication via CLI commands.
"""

import asyncio
import json
import logging
import os
import re
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Default locations where Claude Code stores session data
_HOME = os.path.expanduser("~")
_CLAUDE_DIR = os.path.join(_HOME, ".claude")
_PROJECTS_DIR = os.path.join(_CLAUDE_DIR, "projects")
_SESSIONS_DIR = os.path.join(_CLAUDE_DIR, "sessions")

# Knowledge base persistence
_KNOWLEDGE_FILE = os.path.join(BOT_DIR, "session_knowledge.json")

# GUI countdown before any mouse/keyboard action (seconds)
_GUI_COUNTDOWN_SECONDS = 3

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_json_load(path: str, default: Any = None):
    """Load JSON/JSONL from disk, return default on failure."""
    try:
        if not os.path.exists(path):
            return default
        if path.endswith(".jsonl"):
            entries = []
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entries.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
            return entries
        else:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                return json.load(f)
    except Exception as exc:
        logger.debug("session_learner: failed to load %s: %s", path, exc)
        return default


def _atomic_save(path: str, data: Any):
    """Atomic write: tmp then move."""
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception as exc:
        logger.warning("session_learner: save failed %s: %s", path, exc)
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _truncate(s: str, maxlen: int = 300) -> str:
    if not s:
        return ""
    return s[:maxlen] + ("..." if len(s) > maxlen else "")


async def _gui_countdown(action_description: str = "taking control"):
    """Show a 3-second countdown before any GUI mouse/keyboard action.

    Gives the user time to stop moving the mouse. Shows a system notification
    on Windows via PowerShell.
    """
    for remaining in range(_GUI_COUNTDOWN_SECONDS, 0, -1):
        msg = f"Bot {action_description} in {remaining}s — hands off mouse!"
        try:
            # Use WScript.Shell Popup with auto-dismiss (1 second timeout)
            safe_msg = msg.replace("'", "''")
            subprocess.Popen(
                ["powershell", "-NoProfile", "-Command",
                 f"$null = (New-Object -ComObject WScript.Shell).Popup('{safe_msg}', 1, 'Bot Countdown', 48)"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        except Exception:
            # Fallback: just log it
            logger.info("GUI countdown: %s", msg)
        await asyncio.sleep(1)


# Tool names that indicate specific capabilities
_TOOL_CATEGORIES = {
    "file_ops": {"Read", "Write", "Edit", "Glob", "Grep"},
    "execution": {"Bash", "bash"},
    "web": {"WebSearch", "WebFetch", "web_search", "web_fetch",
            "mcp__Claude_in_Chrome__navigate", "mcp__Claude_in_Chrome__read_page"},
    "gui": {"mcp__Claude_in_Chrome__computer", "mcp__Claude_in_Chrome__javascript_tool"},
    "notebook": {"NotebookEdit"},
    "git": set(),  # detected by bash content
}


def _classify_tool(tool_name: str, tool_input: dict | None = None) -> str:
    """Map a tool name to a high-level category."""
    for cat, names in _TOOL_CATEGORIES.items():
        if tool_name in names:
            return cat
    if tool_name.startswith("mcp__"):
        return "mcp_integration"
    # Check bash content for git
    if tool_name in ("Bash", "bash") and tool_input:
        cmd = tool_input.get("command", "")
        if cmd.startswith("git ") or "git " in cmd:
            return "git"
    return "other"


# ---------------------------------------------------------------------------
# SessionLearner
# ---------------------------------------------------------------------------

class SessionLearner:
    """Learns from Claude Code session transcripts and enables cross-session
    communication and knowledge sharing.

    Session interaction uses Claude Code CLI --resume flag (no GUI needed).
    """

    def __init__(self, sessions_dir: str | None = None):
        self._projects_dir = _PROJECTS_DIR
        self._sessions_dir = _SESSIONS_DIR
        self._extra_dirs = [sessions_dir] if sessions_dir else []

        # Knowledge base: persisted to session_knowledge.json
        self._knowledge = self._load_knowledge()

    # -- persistence -------------------------------------------------------

    def _load_knowledge(self) -> dict:
        data = _safe_json_load(_KNOWLEDGE_FILE, None)
        if isinstance(data, dict):
            return data
        return {
            "patterns": [],          # list of problem-solving patterns
            "strategies": [],        # generalised reusable strategies
            "session_summaries": [], # per-session learning summaries
            "skill_gaps": [],        # identified gaps
            "stats": {
                "sessions_analyzed": 0,
                "patterns_extracted": 0,
                "last_scan": None,
            },
        }

    # Maximum entries per knowledge list to prevent unbounded growth
    _MAX_PATTERNS = 1000
    _MAX_STRATEGIES = 500
    _MAX_SUMMARIES = 200
    _MAX_SKILL_GAPS = 100

    def _save_knowledge(self):
        # Enforce caps before saving — prune oldest entries
        k = self._knowledge
        if len(k.get("patterns", [])) > self._MAX_PATTERNS:
            k["patterns"] = k["patterns"][-self._MAX_PATTERNS:]
        if len(k.get("strategies", [])) > self._MAX_STRATEGIES:
            k["strategies"] = k["strategies"][-self._MAX_STRATEGIES:]
        if len(k.get("session_summaries", [])) > self._MAX_SUMMARIES:
            k["session_summaries"] = k["session_summaries"][-self._MAX_SUMMARIES:]
        if len(k.get("skill_gaps", [])) > self._MAX_SKILL_GAPS:
            k["skill_gaps"] = k["skill_gaps"][-self._MAX_SKILL_GAPS:]
        _atomic_save(_KNOWLEDGE_FILE, self._knowledge)

    # =====================================================================
    # CLI-based session discovery and interaction
    # =====================================================================

    def list_active_sessions(self) -> list[dict]:
        """Read ~/.claude/sessions/*.json to find all running sessions.

        Returns list of dicts with keys:
          pid, session_id, cwd, started_at, kind, entrypoint, project_name
        """
        active: list[dict] = []
        if not os.path.isdir(self._sessions_dir):
            return active

        for fname in os.listdir(self._sessions_dir):
            if not fname.endswith(".json"):
                continue
            fpath = os.path.join(self._sessions_dir, fname)
            data = _safe_json_load(fpath, None)
            if not isinstance(data, dict) or not data.get("sessionId"):
                continue

            cwd = data.get("cwd", "")
            # Derive a friendly project name from the cwd
            project_name = os.path.basename(cwd) if cwd else "unknown"

            active.append({
                "pid": data.get("pid"),
                "session_id": data.get("sessionId"),
                "cwd": cwd,
                "started_at": data.get("startedAt"),
                "kind": data.get("kind", ""),
                "entrypoint": data.get("entrypoint", ""),
                "project_name": project_name,
                "metadata_file": fpath,
            })

        # Sort by startedAt descending (most recent first)
        active.sort(key=lambda s: s.get("started_at", 0), reverse=True)
        return active

    def get_session_by_name(self, name: str) -> dict | None:
        """Find a session by name or project directory substring.

        Matches against: project_name, cwd basename, or session_id prefix.
        Case-insensitive partial match.
        """
        name_lower = name.lower()
        sessions = self.list_active_sessions()

        for s in sessions:
            # Exact session_id match
            if s["session_id"] == name:
                return s
            # session_id prefix match
            if s["session_id"].startswith(name):
                return s
            # Project name match (case-insensitive, partial)
            proj = s.get("project_name", "").lower()
            if name_lower in proj:
                return s
            # CWD match
            cwd = s.get("cwd", "").lower()
            if name_lower in cwd:
                return s

        return None

    def get_session_by_project(self, project_path: str) -> dict | None:
        """Find a session by its working directory path.

        Normalizes paths for comparison (case-insensitive on Windows).
        """
        target = os.path.normpath(project_path).lower()
        sessions = self.list_active_sessions()

        for s in sessions:
            cwd = os.path.normpath(s.get("cwd", "")).lower()
            if cwd == target:
                return s

        return None

    def read_session_log(self, session_id: str, last_n: int = 50) -> list[dict]:
        """Read the last N entries from a session's JSONL log file.

        Searches across all project directories for the session log.
        """
        last_n = min(max(1, last_n), 5000)  # clamp
        fpath = self._find_session_file(session_id)
        if not fpath:
            return []

        entries = _safe_json_load(fpath, [])
        if not entries:
            return []

        return entries[-last_n:]

    # -- CLI-based session communication -----------------------------------

    async def ask_session(self, session_id: str, question: str,
                          timeout: float = 120) -> str | None:
        """Send a question to a session via CLI --resume (no GUI needed).

        Uses: claude.cmd --resume <session_id> -p "question" --output-format json

        Args:
            session_id: The session UUID to resume.
            question: The question/message to send.
            timeout: Max seconds to wait for response.

        Returns:
            The response text, or None if communication failed.
        """
        timeout = min(max(5.0, timeout), 600)

        # Determine the session's working directory from metadata
        session_cwd = None
        sessions = self.list_active_sessions()
        for s in sessions:
            if s["session_id"] == session_id:
                session_cwd = s.get("cwd")
                break

        # If not found in active sessions, try to find it from log location
        if not session_cwd:
            fpath = self._find_session_file(session_id)
            if fpath:
                # The log is under projects/<project-dir>/<session-id>.jsonl
                session_cwd = os.path.dirname(os.path.dirname(fpath))
                if not os.path.isdir(session_cwd):
                    session_cwd = None

        # Fallback to bot directory
        if not session_cwd:
            session_cwd = BOT_DIR

        logger.info("ask_session: sending to %s (cwd=%s): %s",
                     session_id[:12], session_cwd, _truncate(question, 100))

        try:
            proc = await asyncio.create_subprocess_exec(
                "claude.cmd",
                "--resume", session_id,
                "-p", question,
                "--output-format", "json",
                "--dangerously-skip-permissions",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=session_cwd,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning("ask_session: timed out after %.0fs for %s",
                           timeout, session_id[:12])
            try:
                proc.kill()
            except Exception:
                pass
            return None
        except FileNotFoundError:
            logger.error("ask_session: claude.cmd not found in PATH")
            return None
        except Exception as exc:
            logger.error("ask_session: subprocess error: %s", exc)
            return None

        if proc.returncode != 0:
            err_text = stderr.decode("utf-8", errors="replace").strip() if stderr else ""
            logger.warning("ask_session: claude.cmd returned %d: %s",
                           proc.returncode, _truncate(err_text, 200))
            # Still try to parse stdout — sometimes there's partial output
            if not stdout:
                return None

        # Parse response
        raw = stdout.decode("utf-8", errors="replace").strip() if stdout else ""
        if not raw:
            return None

        # Try JSON parse (--output-format json)
        try:
            data = json.loads(raw)
            # Claude CLI JSON output has a "result" or "content" field
            if isinstance(data, dict):
                # Try common response shapes
                for key in ("result", "content", "text", "response"):
                    if key in data:
                        val = data[key]
                        if isinstance(val, str):
                            return val
                        elif isinstance(val, list):
                            # Content blocks
                            parts = []
                            for block in val:
                                if isinstance(block, dict) and block.get("type") == "text":
                                    parts.append(block.get("text", ""))
                                elif isinstance(block, str):
                                    parts.append(block)
                            return "\n".join(parts) if parts else str(val)
                        return str(val)
                # If no known key, return the whole thing
                return json.dumps(data, ensure_ascii=False)[:5000]
            elif isinstance(data, str):
                return data
        except json.JSONDecodeError:
            # Not JSON — return raw text
            pass

        return raw[:5000] if raw else None

    async def delegate_task(self, session_id: str, task: str,
                            timeout: float = 300) -> dict:
        """Send a task to another session via CLI and get the result.

        Args:
            session_id: Target session UUID.
            task: The task description to send.
            timeout: Max seconds to wait.

        Returns:
            {status, session_id, task, response, duration_seconds}
        """
        start = time.time()

        # Prefix with clear instruction marker
        prefixed = (
            f"[DELEGATED TASK] {task}\n"
            f"When done, end your response with: TASK_COMPLETE"
        )

        response = await self.ask_session(session_id, prefixed, timeout=timeout)
        duration = round(time.time() - start, 1)

        if response is None:
            return {
                "status": "failed",
                "session_id": session_id,
                "task": task,
                "response": None,
                "duration_seconds": duration,
                "error": "Could not communicate with session via CLI",
            }

        completed = "task_complete" in response.lower() if response else False

        return {
            "status": "completed" if completed else "partial",
            "session_id": session_id,
            "task": task,
            "response": response,
            "duration_seconds": duration,
        }

    async def ask_session_by_name(self, name: str, question: str,
                                  timeout: float = 120) -> str | None:
        """Convenience: find a session by name/project, then ask it.

        Args:
            name: Session name, project dir substring, or session_id prefix.
            question: The question to send.
            timeout: Max seconds to wait.

        Returns:
            Response text or None.
        """
        session = self.get_session_by_name(name)
        if not session:
            logger.warning("ask_session_by_name: no session matching '%s'", name)
            return None
        return await self.ask_session(session["session_id"], question, timeout=timeout)

    async def delegate_task_by_name(self, name: str, task: str,
                                    timeout: float = 300) -> dict:
        """Convenience: find a session by name/project, then delegate a task.

        Args:
            name: Session name, project dir substring, or session_id prefix.
            task: The task to delegate.
            timeout: Max seconds to wait.

        Returns:
            Result dict (same shape as delegate_task).
        """
        session = self.get_session_by_name(name)
        if not session:
            return {
                "status": "failed",
                "session_id": None,
                "task": task,
                "response": None,
                "duration_seconds": 0,
                "error": f"No session matching '{name}'",
            }
        return await self.delegate_task(session["session_id"], task, timeout=timeout)

    # =====================================================================
    # 1. scan_session_logs
    # =====================================================================

    def scan_session_logs(self) -> list[dict]:
        """Find and parse all Claude Code session JSONL transcripts.

        Scans:
          ~/.claude/projects/*/  (project-scoped sessions)
          ~/.claude/sessions/    (session metadata -- maps PID to sessionId)
          Any extra directories passed to __init__

        Returns list of session summaries:
          {session_id, project, file_path, message_count, first_ts, last_ts,
           task_summary, tools_used, errors, success_signals}
        """
        summaries = []
        seen_ids: set[str] = set()

        # Gather all .jsonl files under projects/
        jsonl_files: list[tuple[str, str]] = []  # (path, project_name)

        if os.path.isdir(self._projects_dir):
            for proj in os.listdir(self._projects_dir):
                proj_path = os.path.join(self._projects_dir, proj)
                if not os.path.isdir(proj_path):
                    continue
                for fname in os.listdir(proj_path):
                    if fname.endswith(".jsonl"):
                        jsonl_files.append(
                            (os.path.join(proj_path, fname), proj)
                        )

        # Extra directories
        for d in self._extra_dirs:
            if d and os.path.isdir(d):
                for fname in os.listdir(d):
                    if fname.endswith(".jsonl"):
                        jsonl_files.append(
                            (os.path.join(d, fname), os.path.basename(d))
                        )

        # Also look for .jsonl in the bot directory itself
        # Only pick up files that look like session logs (contain UUID-like pattern)
        _SESSION_RE = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)
        for fname in os.listdir(BOT_DIR):
            if fname.endswith(".jsonl") and not fname.startswith(".") and _SESSION_RE.search(fname):
                jsonl_files.append((os.path.join(BOT_DIR, fname), "bot_dir"))

        for fpath, project in jsonl_files:
            try:
                entries = _safe_json_load(fpath, [])
                if not entries:
                    continue

                session_id = Path(fpath).stem
                if session_id in seen_ids:
                    continue
                seen_ids.add(session_id)

                summary = self._summarize_session(entries, session_id, project, fpath)
                if summary and summary.get("message_count", 0) > 0:
                    summaries.append(summary)
            except Exception as exc:
                logger.debug("session_learner: error scanning %s: %s", fpath, exc)

        # Sort by recency
        summaries.sort(key=lambda s: s.get("last_ts", ""), reverse=True)

        self._knowledge["stats"]["last_scan"] = datetime.now().isoformat()
        self._knowledge["stats"]["sessions_analyzed"] = len(summaries)
        self._save_knowledge()

        return summaries

    def _summarize_session(
        self, entries: list[dict], session_id: str, project: str, fpath: str
    ) -> dict | None:
        """Build a summary dict from raw JSONL entries."""
        messages = [
            e for e in entries
            if e.get("type") in ("user", "assistant") and e.get("message")
        ]
        if not messages:
            return None

        timestamps = [m.get("timestamp", "") for m in messages if m.get("timestamp")]
        first_ts = min(timestamps) if timestamps else ""
        last_ts = max(timestamps) if timestamps else ""

        # Extract user messages for task identification
        user_texts: list[str] = []
        for m in messages:
            if m.get("type") == "user":
                msg = m.get("message", {})
                if isinstance(msg, str):
                    user_texts.append(msg[:500])
                    continue
                content = msg.get("content", "") if isinstance(msg, dict) else ""
                if isinstance(content, str):
                    user_texts.append(content[:500])
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get("type") == "text":
                            user_texts.append(block["text"][:500])

        # Extract tools used
        tools_used: Counter = Counter()
        errors: list[str] = []
        success_signals: list[str] = []

        for m in messages:
            if m.get("type") != "assistant":
                continue
            msg = m.get("message", {})
            content = msg.get("content", [])
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "tool_use":
                    tools_used[block.get("name", "unknown")] += 1
                elif block.get("type") == "tool_result":
                    result_text = str(block.get("content", ""))[:500]
                    if block.get("is_error"):
                        errors.append(_truncate(result_text, 200))
                    elif any(kw in result_text.lower() for kw in
                             ("success", "done", "completed", "created", "saved")):
                        success_signals.append(_truncate(result_text, 200))
                elif block.get("type") == "text":
                    text = block.get("text", "")
                    if any(kw in text.lower() for kw in
                           ("error", "failed", "traceback", "exception")):
                        errors.append(_truncate(text, 200))

        # Also scan for tool_result entries at top level (Claude Code format)
        for e in entries:
            if e.get("type") == "tool_result":
                result_text = str(e.get("content", ""))[:500]
                if e.get("is_error"):
                    errors.append(_truncate(result_text, 200))

        task_summary = ""
        if user_texts:
            # First user message is usually the task
            task_summary = _truncate(user_texts[0], 300)

        return {
            "session_id": session_id,
            "project": project,
            "file_path": fpath,
            "message_count": len(messages),
            "first_ts": first_ts,
            "last_ts": last_ts,
            "task_summary": task_summary,
            "tools_used": dict(tools_used.most_common(20)),
            "errors": errors[:10],
            "success_signals": success_signals[:10],
            "user_message_count": len(user_texts),
        }

    # =====================================================================
    # 2. extract_problem_solving_patterns
    # =====================================================================

    def extract_problem_solving_patterns(self, session_log: list[dict]) -> list[dict]:
        """From a session's JSONL entries, extract HOW problems were solved.

        Each pattern:
          {problem_type, approach, tools_used, success, steps_taken, key_decisions}
        """
        patterns: list[dict] = []

        # Group messages into user-assistant exchanges
        exchanges = self._split_into_exchanges(session_log)

        for exchange in exchanges:
            user_msg = exchange.get("user_text", "")
            assistant_blocks = exchange.get("assistant_blocks", [])
            tools = exchange.get("tools_used", [])
            had_error = exchange.get("had_error", False)
            recovered = exchange.get("recovered_from_error", False)

            # Classify the problem type
            problem_type = self._classify_problem(user_msg)

            # Determine approach from tools and text
            approach = self._identify_approach(tools, assistant_blocks)

            # Identify key decisions
            key_decisions = self._extract_decisions(assistant_blocks)

            success = not had_error or recovered

            if problem_type != "unknown" or len(tools) > 0:
                patterns.append({
                    "problem_type": problem_type,
                    "approach": approach,
                    "tools_used": list(set(tools)),
                    "success": success,
                    "steps_taken": len(tools),
                    "key_decisions": key_decisions,
                    "user_prompt_preview": _truncate(user_msg, 150),
                    "had_error": had_error,
                    "error_recovery": recovered,
                })

        return patterns

    def _split_into_exchanges(self, entries: list[dict]) -> list[dict]:
        """Split JSONL entries into user-assistant exchange pairs."""
        exchanges: list[dict] = []
        current: dict | None = None

        for entry in entries:
            msg_type = entry.get("type")
            message = entry.get("message", {})

            if msg_type == "user":
                # Start new exchange
                if current:
                    exchanges.append(current)
                content = message.get("content", "")
                if isinstance(content, list):
                    text_parts = [
                        b.get("text", "") for b in content
                        if isinstance(b, dict) and b.get("type") == "text"
                    ]
                    content = " ".join(text_parts)
                current = {
                    "user_text": str(content)[:1000],
                    "assistant_blocks": [],
                    "tools_used": [],
                    "had_error": False,
                    "recovered_from_error": False,
                }

            elif msg_type == "assistant" and current is not None:
                content = message.get("content", [])
                if isinstance(content, list):
                    had_error_this_turn = False
                    for block in content:
                        if not isinstance(block, dict):
                            continue
                        current["assistant_blocks"].append(block)
                        if block.get("type") == "tool_use":
                            current["tools_used"].append(block.get("name", ""))
                        if block.get("type") == "tool_result" and block.get("is_error"):
                            had_error_this_turn = True

                    if had_error_this_turn:
                        current["had_error"] = True
                    elif current["had_error"] and not had_error_this_turn:
                        # Assistant continued working after error without new errors
                        current["recovered_from_error"] = True

            elif msg_type == "tool_result":
                # Top-level tool results (Claude Code format)
                if current is not None:
                    if entry.get("is_error"):
                        current["had_error"] = True
                    elif current["had_error"]:
                        # Non-error result after error = recovery
                        current["recovered_from_error"] = True

        if current:
            exchanges.append(current)

        return exchanges

    def _classify_problem(self, user_text: str) -> str:
        """Classify a user message into a problem type."""
        text = user_text.lower()
        classifiers = {
            "debugging": [r"fix", r"bug", r"error", r"broken", r"not work",
                          r"crash", r"fail", r"debug", r"issue"],
            "implementation": [r"create", r"write", r"implement", r"build",
                               r"add", r"make", r"develop", r"new feature"],
            "refactoring": [r"refactor", r"clean", r"simplify", r"reorganize",
                            r"restructure", r"improve code"],
            "analysis": [r"analyze", r"explain", r"review", r"understand",
                         r"what does", r"how does", r"look at"],
            "deployment": [r"deploy", r"start", r"run", r"launch", r"restart",
                           r"install", r"setup"],
            "research": [r"search", r"find", r"look up", r"check", r"browse",
                         r"investigate"],
            "gui_interaction": [r"click", r"screenshot", r"screen", r"window",
                                r"browser", r"open"],
            "file_management": [r"read file", r"edit file", r"move", r"copy",
                                r"delete", r"rename"],
            "configuration": [r"config", r"setting", r"environment", r"env",
                              r".env", r"api key"],
        }
        scores: dict[str, int] = {}
        for category, kw_patterns in classifiers.items():
            score = sum(1 for p in kw_patterns if re.search(p, text))
            if score > 0:
                scores[category] = score

        if scores:
            return max(scores, key=scores.get)
        return "unknown"

    def _identify_approach(
        self, tools: list[str], blocks: list[dict]
    ) -> str:
        """Identify the high-level approach from tool usage."""
        categories = Counter(_classify_tool(t) for t in tools if t)
        if not categories:
            return "conversational"

        top = categories.most_common(3)
        parts = [cat for cat, _ in top]

        if "execution" in parts and "file_ops" in parts:
            return "read-then-execute"
        if "execution" in parts and "web" in parts:
            return "web-assisted-execution"
        if "file_ops" in parts and len(parts) == 1:
            return "file-analysis"
        if "execution" in parts and len(parts) == 1:
            return "direct-execution"
        if "web" in parts:
            return "web-research"
        if "gui" in parts:
            return "gui-automation"
        return "+".join(parts)

    def _extract_decisions(self, blocks: list[dict]) -> list[str]:
        """Extract key decisions from assistant response blocks."""
        decisions: list[str] = []
        prev_tool = None

        for block in blocks:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "text":
                text = block.get("text", "")
                # Look for reasoning indicators
                for pattern in [
                    r"(?:instead|rather than|better to|let me try|alternative)",
                    r"(?:first.*then|step \d|approach:)",
                    r"(?:the issue is|root cause|because|the problem)",
                ]:
                    match = re.search(pattern, text.lower())
                    if match:
                        start = max(0, match.start() - 30)
                        end = min(len(text), match.end() + 100)
                        snippet = text[start:end].strip()
                        decisions.append(_truncate(snippet, 150))
                        break

            elif block.get("type") == "tool_use":
                tool = block.get("name", "")
                if prev_tool and tool != prev_tool:
                    decisions.append(f"switched from {prev_tool} to {tool}")
                prev_tool = tool

        return decisions[:5]

    # =====================================================================
    # 3. learn_from_session
    # =====================================================================

    def learn_from_session(self, session_id: str) -> dict:
        """Deep analysis of one session: what worked, what can be reused.

        Returns:
          {session_id, task, approaches_tried, successful_approaches,
           failed_approaches, reusable_strategies, patterns_found}
        """
        # Find the session file
        fpath = self._find_session_file(session_id)
        if not fpath:
            return {"error": f"Session {session_id} not found"}

        entries = _safe_json_load(fpath, [])
        if not entries:
            return {"error": f"Session {session_id} is empty"}

        # Extract patterns
        patterns = self.extract_problem_solving_patterns(entries)

        # Identify what the session was trying to do
        user_msgs = []
        for e in entries:
            if e.get("type") == "user":
                msg = e.get("message", {})
                content = msg.get("content", "")
                if isinstance(content, str) and content.strip():
                    user_msgs.append(content[:500])

        task = user_msgs[0] if user_msgs else "Unknown task"

        # Separate successful and failed approaches
        successful = [p for p in patterns if p["success"]]
        failed = [p for p in patterns if not p["success"]]

        # Generate reusable strategies
        strategies = self._generalize_strategies(patterns)

        # Store in knowledge base
        learning = {
            "session_id": session_id,
            "task": _truncate(task, 300),
            "learned_at": datetime.now().isoformat(),
            "approaches_tried": len(patterns),
            "successful_approaches": [
                {
                    "problem_type": p["problem_type"],
                    "approach": p["approach"],
                    "tools": p["tools_used"],
                    "steps": p["steps_taken"],
                }
                for p in successful[:10]
            ],
            "failed_approaches": [
                {
                    "problem_type": p["problem_type"],
                    "approach": p["approach"],
                    "tools": p["tools_used"],
                }
                for p in failed[:10]
            ],
            "reusable_strategies": strategies,
            "patterns_found": len(patterns),
        }

        # Update knowledge base
        self._knowledge["session_summaries"].append(learning)
        if len(self._knowledge["session_summaries"]) > 200:
            self._knowledge["session_summaries"] = \
                self._knowledge["session_summaries"][-200:]

        for s in strategies:
            self._knowledge["strategies"].append(s)
        if len(self._knowledge["strategies"]) > 500:
            self._knowledge["strategies"] = self._knowledge["strategies"][-500:]

        for p in patterns:
            self._knowledge["patterns"].append(p)
        if len(self._knowledge["patterns"]) > 2000:
            self._knowledge["patterns"] = self._knowledge["patterns"][-2000:]

        self._knowledge["stats"]["patterns_extracted"] = len(self._knowledge["patterns"])
        self._save_knowledge()

        return learning

    def _find_session_file(self, session_id: str) -> str | None:
        """Locate a session JSONL by ID across all known directories."""
        fname = session_id if session_id.endswith(".jsonl") else f"{session_id}.jsonl"

        # Search projects
        if os.path.isdir(self._projects_dir):
            for proj in os.listdir(self._projects_dir):
                candidate = os.path.join(self._projects_dir, proj, fname)
                if os.path.isfile(candidate):
                    return candidate

        # Search extra dirs
        for d in self._extra_dirs:
            if d:
                candidate = os.path.join(d, fname)
                if os.path.isfile(candidate):
                    return candidate

        # Search bot dir
        candidate = os.path.join(BOT_DIR, fname)
        if os.path.isfile(candidate):
            return candidate

        return None

    def _generalize_strategies(self, patterns: list[dict]) -> list[dict]:
        """From a set of patterns, extract generalizable strategies."""
        strategies: list[dict] = []

        # Group by problem type
        by_type: dict[str, list[dict]] = defaultdict(list)
        for p in patterns:
            by_type[p["problem_type"]].append(p)

        for ptype, type_patterns in by_type.items():
            if ptype == "unknown":
                continue

            successful = [p for p in type_patterns if p["success"]]
            failed = [p for p in type_patterns if not p["success"]]

            if not successful:
                continue

            # Most common successful approach
            approach_counts = Counter(p["approach"] for p in successful)
            best_approach = approach_counts.most_common(1)[0][0]

            # Most common tools in successful attempts
            all_tools: list[str] = []
            for p in successful:
                all_tools.extend(p["tools_used"])
            common_tools = [t for t, _ in Counter(all_tools).most_common(5)]

            # Average steps
            avg_steps = (
                sum(p["steps_taken"] for p in successful) / len(successful)
                if successful else 0
            )

            # Error recovery rate
            error_recoveries = sum(1 for p in successful if p.get("error_recovery"))

            strategies.append({
                "problem_type": ptype,
                "recommended_approach": best_approach,
                "recommended_tools": common_tools,
                "avg_steps": round(avg_steps, 1),
                "success_rate": (
                    len(successful) / len(type_patterns)
                    if type_patterns else 0
                ),
                "sample_size": len(type_patterns),
                "error_recovery_rate": (
                    error_recoveries / len(successful)
                    if successful else 0
                ),
                "avoid_approaches": list(set(
                    p["approach"] for p in failed
                    if p["approach"] not in [pp["approach"] for pp in successful]
                ))[:3],
            })

        return strategies

    # =====================================================================
    # 6. get_knowledge_base
    # =====================================================================

    def get_knowledge_base(self) -> dict:
        """Return all learned patterns organized by category."""
        patterns = self._knowledge.get("patterns", [])
        strategies = self._knowledge.get("strategies", [])

        # Organize strategies by problem type
        strategies_by_type: dict[str, list[dict]] = defaultdict(list)
        for s in strategies:
            strategies_by_type[s.get("problem_type", "other")].append(s)

        # Aggregate tool usage
        tool_counter: Counter = Counter()
        for p in patterns:
            for t in p.get("tools_used", []):
                tool_counter[t] += 1

        # Success rates by type
        type_results: dict[str, dict] = defaultdict(lambda: {"ok": 0, "total": 0})
        for p in patterns:
            ptype = p.get("problem_type", "unknown")
            type_results[ptype]["total"] += 1
            if p.get("success"):
                type_results[ptype]["ok"] += 1

        success_rates = {
            ptype: round(v["ok"] / v["total"], 2) if v["total"] else 0
            for ptype, v in type_results.items()
        }

        return {
            "strategies_by_type": dict(strategies_by_type),
            "top_tools": tool_counter.most_common(15),
            "success_rates": success_rates,
            "total_patterns": len(patterns),
            "total_sessions": len(self._knowledge.get("session_summaries", [])),
            "stats": self._knowledge.get("stats", {}),
        }

    # =====================================================================
    # 7. generate_training_curriculum
    # =====================================================================

    def generate_training_curriculum(self) -> list[dict]:
        """Identify skill gaps from session analysis and generate training tasks."""
        patterns = self._knowledge.get("patterns", [])
        if not patterns:
            return [{
                "skill": "general",
                "gap_description": "No sessions analyzed yet",
                "training_task": "Run scan_session_logs() then learn_from_session() on recent sessions",
                "priority": "high",
                "evidence": "Empty knowledge base",
            }]

        curriculum: list[dict] = []

        # 1. Find problem types with low success rates
        type_results: dict[str, dict] = defaultdict(lambda: {"ok": 0, "fail": 0})
        for p in patterns:
            ptype = p.get("problem_type", "unknown")
            if p.get("success"):
                type_results[ptype]["ok"] += 1
            else:
                type_results[ptype]["fail"] += 1

        for ptype, counts in type_results.items():
            total = counts["ok"] + counts["fail"]
            if total < 2:
                continue
            rate = counts["ok"] / total
            if rate < 0.7:
                curriculum.append({
                    "skill": ptype,
                    "gap_description": f"Low success rate ({rate:.0%}) for {ptype} tasks",
                    "training_task": f"Practice {ptype}: attempt 3 tasks of this type with focus on error recovery",
                    "priority": "high" if rate < 0.5 else "medium",
                    "evidence": f"{counts['fail']} failures out of {total} attempts",
                })

        # 2. Find tools never used (potential blind spots)
        all_tools_used = set()
        for p in patterns:
            all_tools_used.update(p.get("tools_used", []))

        expected_tools = {"Bash", "Read", "Write", "Edit", "Grep", "Glob", "WebSearch", "WebFetch"}
        unused = expected_tools - all_tools_used
        if unused:
            curriculum.append({
                "skill": "tool_breadth",
                "gap_description": f"Tools never used: {', '.join(sorted(unused))}",
                "training_task": f"Try using {', '.join(sorted(unused))} in upcoming tasks",
                "priority": "low",
                "evidence": f"Out of {len(expected_tools)} expected tools, {len(unused)} never appeared in sessions",
            })

        # 3. Find recurring error types
        error_types: Counter = Counter()
        for p in patterns:
            if p.get("had_error") and not p.get("error_recovery"):
                error_types[p.get("problem_type", "unknown")] += 1

        for etype, count in error_types.most_common(5):
            if count >= 3:
                curriculum.append({
                    "skill": f"error_recovery_{etype}",
                    "gap_description": f"Unrecovered errors in {etype} tasks ({count}x)",
                    "training_task": f"When {etype} errors occur, practice: 1) read error carefully, 2) try alternative approach, 3) verify fix",
                    "priority": "high",
                    "evidence": f"{count} unrecovered errors",
                })

        # 4. Check for tasks that take too many steps
        long_tasks = [p for p in patterns if p.get("steps_taken", 0) > 15]
        if len(long_tasks) >= 3:
            avg_steps = sum(p["steps_taken"] for p in long_tasks) / len(long_tasks) if long_tasks else 0
            curriculum.append({
                "skill": "task_efficiency",
                "gap_description": f"{len(long_tasks)} tasks took >15 steps (avg {avg_steps:.0f})",
                "training_task": "Practice decomposing complex tasks into smaller sub-tasks before starting",
                "priority": "medium",
                "evidence": f"{len(long_tasks)} long tasks detected",
            })

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        curriculum.sort(key=lambda x: priority_order.get(x["priority"], 9))

        # Store gaps
        self._knowledge["skill_gaps"] = curriculum
        self._save_knowledge()

        return curriculum

    # =====================================================================
    # 8. get_session_summary
    # =====================================================================

    def get_session_summary(self) -> str:
        """Human-readable summary of everything learned from all sessions."""
        kb = self.get_knowledge_base()
        summaries = self._knowledge.get("session_summaries", [])
        strategies = self._knowledge.get("strategies", [])

        lines: list[str] = []
        lines.append("=" * 60)
        lines.append("SESSION LEARNER -- KNOWLEDGE SUMMARY")
        lines.append("=" * 60)

        # Stats
        stats = kb.get("stats", {})
        lines.append(f"\nSessions analyzed: {kb['total_sessions']}")
        lines.append(f"Patterns extracted: {kb['total_patterns']}")
        lines.append(f"Strategies learned: {len(strategies)}")
        lines.append(f"Last scan: {stats.get('last_scan', 'never')}")

        # Active sessions (live from metadata)
        active = self.list_active_sessions()
        if active:
            lines.append(f"\n--- Active Sessions ({len(active)}) ---")
            for s in active:
                pid = s.get("pid", "?")
                sid = s.get("session_id", "?")[:12]
                proj = s.get("project_name", "?")
                lines.append(f"  PID {pid}: {sid}... in {proj}")

        # Top tools
        if kb["top_tools"]:
            lines.append("\n--- Most Used Tools ---")
            for tool, count in kb["top_tools"][:10]:
                lines.append(f"  {tool}: {count}x")

        # Success rates
        if kb["success_rates"]:
            lines.append("\n--- Success Rates by Problem Type ---")
            for ptype, rate in sorted(
                kb["success_rates"].items(), key=lambda x: x[1]
            ):
                bar = "#" * int(rate * 20) + "." * (20 - int(rate * 20))
                lines.append(f"  {ptype:25s} [{bar}] {rate:.0%}")

        # Key strategies
        if strategies:
            lines.append("\n--- Learned Strategies ---")
            best: dict[str, dict] = {}
            for s in strategies:
                pt = s["problem_type"]
                if pt not in best or s.get("sample_size", 0) > best[pt].get("sample_size", 0):
                    best[pt] = s

            for pt, s in sorted(best.items()):
                lines.append(f"\n  [{pt}]")
                lines.append(f"    Best approach: {s['recommended_approach']}")
                lines.append(f"    Key tools: {', '.join(s['recommended_tools'][:5])}")
                lines.append(f"    Avg steps: {s['avg_steps']}")
                lines.append(f"    Success rate: {s['success_rate']:.0%} (n={s['sample_size']})")
                if s.get("avoid_approaches"):
                    lines.append(f"    Avoid: {', '.join(s['avoid_approaches'])}")

        # Recent sessions
        if summaries:
            lines.append("\n--- Recent Session Learnings ---")
            for s in summaries[-5:]:
                lines.append(f"\n  Session: {s['session_id'][:12]}...")
                lines.append(f"    Task: {_truncate(s.get('task', '?'), 80)}")
                lines.append(f"    Approaches: {s.get('approaches_tried', 0)} tried, "
                             f"{len(s.get('successful_approaches', []))} succeeded, "
                             f"{len(s.get('failed_approaches', []))} failed")
                strats = s.get("reusable_strategies", [])
                if strats:
                    lines.append(f"    Strategies: {len(strats)} extracted")

        # Skill gaps
        gaps = self._knowledge.get("skill_gaps", [])
        if gaps:
            lines.append("\n--- Skill Gaps ---")
            for g in gaps[:5]:
                lines.append(f"  [{g['priority'].upper()}] {g['skill']}: {g['gap_description']}")

        lines.append("\n" + "=" * 60)
        return "\n".join(lines)

    # =====================================================================
    # Convenience: learn from all recent sessions
    # =====================================================================

    def learn_from_all_recent(self, max_sessions: int = 20) -> dict:
        """Scan logs, pick the most recent sessions, and learn from each."""
        max_sessions = min(max_sessions, 100)
        summaries = self.scan_session_logs()
        learned = 0
        errors = 0

        # Temporarily disable per-session saves; single save at the end
        _orig_save = self._save_knowledge
        self._save_knowledge = lambda: None  # type: ignore[assignment]

        try:
            for s in summaries[:max_sessions]:
                sid = s.get("session_id", "")
                if not sid:
                    continue
                try:
                    result = self.learn_from_session(sid)
                    if "error" not in result:
                        learned += 1
                except Exception as exc:
                    logger.debug("session_learner: learn error for %s: %s", sid, exc)
                    errors += 1
        finally:
            self._save_knowledge = _orig_save  # type: ignore[method-assign]

        # Single save after all learning
        self._save_knowledge()

        # Generate curriculum after learning
        curriculum = self.generate_training_curriculum()

        return {
            "sessions_scanned": len(summaries),
            "sessions_learned": learned,
            "errors": errors,
            "curriculum_items": len(curriculum),
            "knowledge_summary": self.get_session_summary(),
        }

    # =====================================================================
    # Convenience: get active session IDs from metadata (legacy alias)
    # =====================================================================

    def get_active_sessions(self) -> list[dict]:
        """Alias for list_active_sessions() (backward compatibility)."""
        return self.list_active_sessions()


# ---------------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------------

_instance: SessionLearner | None = None
_instance_lock = __import__("threading").Lock()


def get_learner() -> SessionLearner:
    """Get or create the global SessionLearner instance (thread-safe)."""
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = SessionLearner()
    return _instance
