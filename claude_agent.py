"""
claude_agent.py — Routes messages to Claude Code CLI (Plan tokens).

Architecture:
  User (Telegram) → bot.py → claude_agent.py → claude -p --resume <session>
                                                  ↓
                                              Full computer access
                                              Uses Plan tokens (free)
                                              Persistent conversations

Message Queue:
- While processing a task, new messages are queued
- After task completes, queued messages are processed with full context
- Stale processing flags auto-recover after 10 minutes
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
import config

logger = logging.getLogger(__name__)

# Full path to claude CLI (npm global install)
CLAUDE_CMD = os.path.join(
    os.path.expanduser("~"), "AppData", "Roaming", "npm", "claude.cmd"
)

# Bot project directory (for self-awareness)
BOT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# User home directory (default working directory for commands)
USER_HOME = os.path.expanduser("~")

# System prompt — tells Claude WHO it is and HOW to behave
_SYSTEM_PROMPT = f"""You are a Telegram bot powered by Claude Code, running on this Windows 11 computer. You remotely control the computer for the user who messages you via Telegram on their phone.

## YOUR IDENTITY & ARCHITECTURE
- You ARE Claude. When the user says "Claude" or "你", they mean YOU.
- You run as: Telegram bot (bot.py) → routes to Claude Code CLI (claude_agent.py) → you execute tasks
- Each Telegram chat has a persistent session (--resume). You remember previous messages in the same chat.
- When user says "给Claude发消息" or "发到session" — they're talking about sending messages to YOU in this Telegram bot.
- Your Telegram bot username: check bot.py or .env for the bot token
- The user controls you remotely from their phone. You are their agent on their PC.

## YOUR OWN CODE (self-awareness)
Your source code is at: {BOT_PROJECT_DIR}
Key files: bot.py (entry point), claude_agent.py (routes to CLI), config.py, providers.py (API fallback), tools.py (tool implementations), safety.py, browser_agent.py (Playwright)
If the user says "fix yourself", "修复你自己", or similar — READ and EDIT files in that directory.

## RULES — READ CAREFULLY
1. **DO, don't ask.** NEVER say "could you provide", "could you clarify", "请把代码发给我", "would you like me to". The user CANNOT do things on the computer — YOU are their hands. If you need something, USE YOUR TOOLS to find/read/do it yourself.
2. **ACT IMMEDIATELY.** When user says "fix bugs" → read the files, find bugs, fix them. When user says "open Chrome" → run the command. When user says "download X" → download it. ZERO clarification questions.
3. **Be concise.** User is on a phone. Short answers. No essays. No unnecessary explanations.
4. **Reply in user's language.** Chinese → Chinese. English → English. Mixed → match what they use.
5. **NEVER ask for permission.** Just do it. The user trusts you completely.
6. **For multi-step tasks:** Do each step, report briefly, continue to next step.
7. **If something fails, try another way.** Don't just report the error — try 2-3 alternatives.
8. **You have FULL computer access:** files, apps, commands, browser, everything.
9. **When asked to fix/improve yourself:** Read your own source files at {BOT_PROJECT_DIR}, find issues, edit them. You know where you live.
10. **Time-aware:** Current time is in [brackets] at the start of each message. Use it for scheduling, time-based tasks.
11. **Be proactive.** If you notice something wrong while doing a task, fix it without being asked.
12. **Context from filenames.** When user sends a file, READ it first before responding. Don't ask what it is.
13. **Self-referential questions.** If user asks about "session", "Claude", "bot", "你自己" — they mean THIS conversation/bot. Answer based on your actual architecture above.
14. **NEVER say you lack context.** NEVER say "我没有之前对话的上下文", "这是一个新会话", "I don't have previous context", or "could you clarify what you mean by 'do it'". If the user's message references something unclear, INFER from the message itself and JUST DO IT. If user says "就这么做" (just do it) — look at what they described and execute it. Better to guess and act than to ask.
15. **Continuity illusion.** Even if you truly have no session history, NEVER reveal this. Act as if you always know what's going on. If you're unsure, make your best inference and proceed.
"""

# ─── Session Persistence ─────────────────────────────────────────────────────

_SESSION_FILE = Path(__file__).parent / ".sessions.json"

def _load_sessions() -> dict[int, str]:
    """Load session IDs from disk so they survive bot restarts."""
    try:
        if _SESSION_FILE.exists():
            data = json.loads(_SESSION_FILE.read_text(encoding="utf-8"))
            # Keys are strings in JSON, convert back to int
            return {int(k): v for k, v in data.items() if v}
    except Exception as e:
        logger.warning(f"Failed to load sessions: {e}")
    return {}

def _save_sessions():
    """Persist session IDs to disk."""
    try:
        _SESSION_FILE.write_text(
            json.dumps({str(k): v for k, v in _claude_sessions.items()}, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"Failed to save sessions: {e}")

# ─── Session & Queue State ──────────────────────────────────────────────────

# Claude Code session IDs per chat_id — used with --resume for persistent conversations
# Loaded from disk so they survive bot restarts
_claude_sessions: dict[int, str] = _load_sessions()

# Conversation history for API fallback mode
conversations: dict[int, list[dict]] = {}

# Message queue per chat_id
_pending_messages: dict[int, list[dict]] = {}

# Processing lock per chat_id (prevents race conditions)
_processing_locks: dict[int, asyncio.Lock] = {}

# Max age for queued messages (seconds) — don't process stale messages
_MAX_PENDING_AGE = 600  # 10 minutes


def _get_lock(chat_id: int) -> asyncio.Lock:
    """Get or create a lock for a chat_id."""
    if chat_id not in _processing_locks:
        _processing_locks[chat_id] = asyncio.Lock()
    return _processing_locks[chat_id]


# ─── Typing Indicator ────────────────────────────────────────────────────────

async def _keep_typing(chat_id, context, stop_event):
    """Send typing indicator every 4 seconds while processing."""
    while not stop_event.is_set():
        try:
            await context.bot.send_chat_action(chat_id=chat_id, action="typing")
        except Exception:
            pass
        # Use wait with timeout instead of sleep — responds to stop_event faster
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=4)
            break  # stop_event was set
        except asyncio.TimeoutError:
            pass  # 4 seconds elapsed, send typing again


# ─── Claude CLI Runner ────────────────────────────────────────────────────────

async def _run_claude_cli(
    user_message: str, chat_id: int, context,
    timeout: int = None,
) -> tuple[str, str | None]:
    """Run claude CLI and return (response_text, session_id).

    Uses --output-format json to capture session_id for conversation continuity.
    Uses --resume <session_id> so Claude maintains real conversation state.
    Always includes --append-system-prompt so Claude never loses identity.
    """
    timeout = timeout or getattr(config, "CLAUDE_CLI_TIMEOUT", 300)
    session_id = _claude_sessions.get(chat_id)

    # Prepend current time so Claude is time-aware
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_message = f"[{now_str}] {user_message}"

    args = [
        CLAUDE_CMD,
        "-p",
        "--output-format", "json",
        "--dangerously-skip-permissions",
        "--model", config.CLAUDE_MODEL,
        "--append-system-prompt", _SYSTEM_PROMPT,
    ]
    if session_id:
        args.extend(["--resume", session_id])
        logger.info(f"Chat {chat_id}: resuming session {session_id[:12]}... (model: {config.CLAUDE_MODEL})")
    else:
        logger.info(f"Chat {chat_id}: new session (model: {config.CLAUDE_MODEL})")

    stop_typing = asyncio.Event()
    typing_task = asyncio.create_task(_keep_typing(chat_id, context, stop_typing))
    proc = None

    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=USER_HOME,
        )

        stdout_data, stderr_data = await asyncio.wait_for(
            proc.communicate(input=user_message.encode("utf-8")),
            timeout=timeout,
        )

    except asyncio.TimeoutError:
        logger.warning(f"Claude CLI timed out after {timeout}s")
        if proc is not None:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
        raise

    except FileNotFoundError:
        logger.error(f"Claude CLI not found at: {CLAUDE_CMD}")
        raise

    finally:
        stop_typing.set()
        typing_task.cancel()
        try:
            await typing_task
        except asyncio.CancelledError:
            pass

    # Log exit code for debugging
    if proc.returncode != 0:
        logger.warning(f"Claude CLI exited with code {proc.returncode}")

    # Parse JSON response
    raw = stdout_data.decode("utf-8", errors="replace").strip()
    new_session_id = None
    response = None

    if raw:
        try:
            data = json.loads(raw)
            response = data.get("result", "").strip()
            new_session_id = data.get("session_id")

            if not response:
                if data.get("is_error"):
                    response = f"Error: {data.get('error', 'Unknown error')}"
                else:
                    response = "✅ 任务已执行（无文字输出）。"

            # Check for rate limit — notify user but don't retry (they can resend)
            # CRITICAL: Do NOT return session_id for rate-limited responses —
            # those sessions have no conversation content and poison the chain
            if response and ("hit your limit" in response.lower() or "rate limit" in response.lower()):
                logger.warning(f"Claude CLI rate limited: {response[:200]}")
                response = "⏳ Claude 达到速率限制。请稍等几分钟后再试。"
                new_session_id = None  # Don't store this empty session!

        except json.JSONDecodeError:
            # Not JSON — could be partial output from killed process, or older CLI
            # Try to find JSON object in the output (CLI may prepend non-JSON text)
            json_start = raw.find('{')
            if json_start > 0:
                try:
                    data = json.loads(raw[json_start:])
                    response = data.get("result", "").strip()
                    new_session_id = data.get("session_id")
                    if not response:
                        response = raw[:json_start].strip() or "✅ 任务已执行。"
                except json.JSONDecodeError:
                    response = raw
            else:
                response = raw

    # Log stderr for debugging even when we have a response
    if stderr_data:
        err_text = stderr_data.decode("utf-8", errors="replace").strip()
        if err_text:
            logger.debug(f"Claude CLI stderr (chat {chat_id}): {err_text[:500]}")

    if not response:
        err = stderr_data.decode("utf-8", errors="replace").strip() if stderr_data else ""
        if err:
            logger.error(f"Claude CLI stderr: {err[:500]}")
            # Don't show raw errors to user unless there's nothing else
            if "error" in err.lower():
                response = f"⚠️ {err[:500]}"
            else:
                response = "✅ 任务已执行。"
        else:
            response = "✅ 任务已执行（无输出）。"

    return response, new_session_id


# ─── Response Sender ──────────────────────────────────────────────────────────

async def _send_response(chat_id: int, response: str, context):
    """Send response to Telegram, splitting into chunks if needed.
    Handles Markdown parse errors gracefully.
    Very long responses (>16K chars) get a truncation notice.
    """
    if not response or not response.strip():
        return

    # Cap extremely long responses — user is on phone, can't read 20 pages
    MAX_TOTAL = 16000
    if len(response) > MAX_TOTAL:
        response = response[:MAX_TOTAL] + "\n\n... (输出过长，已截断。需要完整内容请说。)"

    # Split into chunks, trying to break at newlines
    remaining = response
    while remaining:
        if len(remaining) <= 4000:
            chunk = remaining
            remaining = ""
        else:
            # Try to break at a newline near the limit
            break_pos = remaining.rfind("\n", 3000, 4000)
            if break_pos == -1:
                break_pos = 4000
            chunk = remaining[:break_pos]
            remaining = remaining[break_pos:]

        try:
            await context.bot.send_message(
                chat_id=chat_id, text=chunk, parse_mode="Markdown"
            )
        except Exception:
            # Markdown parse failed — send as plain text
            try:
                await context.bot.send_message(chat_id=chat_id, text=chunk)
            except Exception as e:
                logger.error(f"Failed to send message to {chat_id}: {e}")


# ─── Queue Helpers ────────────────────────────────────────────────────────────

def _drain_pending(chat_id: int) -> list[dict]:
    """Get and clear all non-stale pending messages for a chat."""
    msgs = _pending_messages.pop(chat_id, [])
    now = time.time()
    # Filter out stale messages
    fresh = [m for m in msgs if now - m["time"] < _MAX_PENDING_AGE]
    if len(fresh) < len(msgs):
        logger.info(f"Chat {chat_id}: dropped {len(msgs) - len(fresh)} stale queued messages")
    return fresh


def _queue_message(chat_id: int, text: str):
    """Add a message to the pending queue."""
    if chat_id not in _pending_messages:
        _pending_messages[chat_id] = []
    _pending_messages[chat_id].append({
        "text": text,
        "time": time.time(),
    })


# ─── Main Processing Logic ────────────────────────────────────────────────────

async def _process_with_claude_cli(user_message: str, chat_id: int, context) -> bool:
    """Process message using Claude Code CLI. Returns True on success."""
    try:
        response, new_session_id = await _run_claude_cli(user_message, chat_id, context)

        # Session recovery: if response indicates session error, retry without resume
        resp_lower = response.lower() if response else ""
        if response and (
            ("session" in resp_lower and "error" in resp_lower)
            or "invalid session" in resp_lower
            or ("could not find" in resp_lower and "session" in resp_lower)
        ):
            logger.warning(f"Chat {chat_id}: session error detected, starting fresh")
            _claude_sessions.pop(chat_id, None)
            response, new_session_id = await _run_claude_cli(user_message, chat_id, context)

        # Store session_id for conversation continuity (and persist to disk)
        if new_session_id:
            _claude_sessions[chat_id] = new_session_id
            _save_sessions()
            logger.info(f"Chat {chat_id}: session_id = {new_session_id[:12]}...")
        else:
            logger.debug(f"Chat {chat_id}: no session_id returned (rate-limited or empty response)")

        await _send_response(chat_id, response, context)

        # Process any messages that arrived during this processing
        pending = _drain_pending(chat_id)
        while pending:
            combined = "\n---\n".join(m["text"] for m in pending)
            count = len(pending)
            logger.info(f"Chat {chat_id}: processing {count} queued follow-up messages")

            await _send_response(
                chat_id,
                f"📨 处理你追加的 {count} 条消息...",
                context,
            )

            try:
                followup_resp, followup_sid = await _run_claude_cli(
                    combined, chat_id, context
                )
                if followup_sid:
                    _claude_sessions[chat_id] = followup_sid
                    _save_sessions()
                await _send_response(chat_id, followup_resp, context)
            except asyncio.TimeoutError:
                await _send_response(
                    chat_id, "⏰ 追加任务超时(5分钟)。发新消息继续。", context
                )
                break
            except Exception as e:
                logger.error(f"Follow-up error: {e}", exc_info=True)
                await _send_response(
                    chat_id, f"⚠️ 追加消息处理出错: {str(e)[:300]}", context
                )
                break

            # Check for more messages that arrived during follow-up processing
            pending = _drain_pending(chat_id)

        return True

    except asyncio.TimeoutError:
        await _send_response(
            chat_id,
            "⏰ 任务处理超时(5分钟)。可能仍在后台运行。发新消息继续。",
            context,
        )
        return True  # Don't fallback to API on timeout

    except FileNotFoundError:
        await _send_response(
            chat_id,
            "❌ Claude CLI 未找到。请运行: npm install -g @anthropic-ai/claude-code",
            context,
        )
        return False

    except Exception as e:
        logger.error(f"Claude CLI error: {e}", exc_info=True)
        await _send_response(
            chat_id, f"⚠️ Claude Code 出错: {str(e)[:500]}", context
        )
        return False


async def process_message(user_message: str, chat_id: int, context):
    """Process a user message with queue support and proper locking.

    If already processing for this chat, queue the message.
    Otherwise, acquire lock and start processing.
    """
    lock = _get_lock(chat_id)

    # Fast path: if locked (busy), just queue
    if lock.locked():
        _queue_message(chat_id, user_message)
        queue_size = len(_pending_messages.get(chat_id, []))
        logger.info(f"Chat {chat_id}: queued message ({queue_size} pending)")

        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📝 收到 (第{queue_size}条追加)，处理完当前任务后会一起看。",
            )
        except Exception:
            pass
        return

    # Acquire lock and process
    async with lock:
        # Bridge mode (default): use Claude Code CLI with Plan tokens
        if getattr(config, "BRIDGE_MODE", True):
            success = await _process_with_claude_cli(user_message, chat_id, context)
            if success:
                return
            logger.warning("Claude CLI failed, falling back to API providers")

        # API provider fallback
        try:
            from providers import process_with_auto_fallback

            if chat_id not in conversations:
                conversations[chat_id] = []

            history = conversations[chat_id]

            # Include any queued messages
            pending = _drain_pending(chat_id)
            if pending:
                combined = user_message + "\n" + "\n".join(m["text"] for m in pending)
            else:
                combined = user_message
            history.append({"role": "user", "content": combined})

            while len(history) > config.MAX_CONVERSATION_HISTORY:
                history.pop(0)

            success = await process_with_auto_fallback(history, chat_id, context)

            if not success:
                return

            # Clean up non-text messages from history (tool use artifacts)
            conversations[chat_id] = [
                m for m in conversations[chat_id]
                if isinstance(m.get("content"), str)
            ]

        except Exception as e:
            logger.error(f"API fallback error: {e}", exc_info=True)
            await _send_response(
                chat_id, f"❌ 处理失败: {str(e)[:500]}", context
            )


def clear_history(chat_id: int):
    """Clear all state for a chat."""
    conversations.pop(chat_id, None)
    _claude_sessions.pop(chat_id, None)
    _save_sessions()
    _pending_messages.pop(chat_id, None)
    # Don't remove the lock — it's just an asyncio.Lock, harmless to keep
