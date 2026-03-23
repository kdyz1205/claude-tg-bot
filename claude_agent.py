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
_SYSTEM_PROMPT = f"""You are a Telegram bot running on this Windows 11 computer. You remotely control the computer for the user who messages you via Telegram on their phone.

## YOUR OWN CODE (self-awareness)
Your source code is at: {BOT_PROJECT_DIR}
Key files: bot.py, claude_agent.py, config.py, providers.py, tools.py, safety.py
If the user says "fix yourself", "修复你自己", or similar — READ and EDIT files in that directory.

## RULES — READ CAREFULLY
1. **DO, don't ask.** NEVER say "could you provide", "could you clarify", "请把代码发给我". The user CANNOT do things on the computer — YOU are their hands. If you need something, USE YOUR TOOLS to find/read/do it yourself.
2. **ACT IMMEDIATELY.** When user says "fix bugs" → read the files, find bugs, fix them. When user says "open Chrome" → run the command. When user says "download X" → download it.
3. **Be concise.** User is on a phone. Short answers. No essays.
4. **Reply in user's language.** Chinese → Chinese. English → English.
5. **NEVER ask for permission.** Just do it.
6. **For multi-step tasks:** Do each step, report briefly, continue.
7. **If something fails, try another way.** Don't just report the error.
8. **You have FULL computer access:** files, apps, commands, browser, everything.
9. **When asked to fix/improve yourself:** Read your own source files at {BOT_PROJECT_DIR}, find issues, edit them. You know where you live.
"""

# ─── Session & Queue State ──────────────────────────────────────────────────

# Claude Code session IDs per chat_id — used with --resume for persistent conversations
_claude_sessions: dict[int, str] = {}

# Conversation history for API fallback mode
conversations: dict[int, list[dict]] = {}

# Message queue per chat_id
_pending_messages: dict[int, list[dict]] = {}

# Processing lock per chat_id (prevents race conditions)
_processing_locks: dict[int, asyncio.Lock] = {}

# Whether currently processing (inside the lock)
_is_busy: dict[int, bool] = {}

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
        await asyncio.sleep(4)


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

    args = [
        CLAUDE_CMD,
        "-p",
        "--output-format", "json",
        "--dangerously-skip-permissions",
        "--append-system-prompt", _SYSTEM_PROMPT,
    ]
    if session_id:
        args.extend(["--resume", session_id])
        logger.info(f"Chat {chat_id}: resuming session {session_id[:12]}...")
    else:
        logger.info(f"Chat {chat_id}: new session")

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

            # Check for rate limit
            if "hit your limit" in response.lower() or "rate limit" in response.lower():
                logger.warning(f"Claude CLI rate limited: {response[:200]}")

        except json.JSONDecodeError:
            # Not JSON — treat as plain text (older CLI versions)
            response = raw

    if not response:
        err = stderr_data.decode("utf-8", errors="replace").strip() if stderr_data else ""
        if err:
            logger.error(f"Claude CLI stderr: {err[:500]}")
            # Don't show raw errors to user unless there's nothing else
            if "error" in err.lower() or "Error" in err:
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
    """
    if not response or not response.strip():
        return

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

        # Store session_id for conversation continuity
        if new_session_id:
            _claude_sessions[chat_id] = new_session_id
            logger.info(f"Chat {chat_id}: session_id = {new_session_id[:12]}...")
        else:
            logger.warning(f"Chat {chat_id}: no session_id returned")

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
    _pending_messages.pop(chat_id, None)
    # Don't remove the lock — it's just an asyncio.Lock, harmless to keep
