"""
claude_agent.py — Smart message router for the Telegram bot.

Architecture:
  User (Telegram) → bot.py → claude_agent.py → routing decision:
    1. Simple Q&A → Gemini API (free, fast)
    2. Computer tasks → Claude Code CLI (--resume session)
    3. Rate-limited → Web AI fallback (browser automation, free)

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

# ─── Harness Integration (browser-based free AI fallback) ────────────────────
_orchestrator = None
_harness_available = False

def _init_harness():
    """Lazy-init the harness orchestrator for browser-based AI fallback."""
    global _orchestrator, _harness_available
    if _orchestrator is not None:
        return _orchestrator
    try:
        from pipeline.orchestrator import Orchestrator
        from tracker.quota import QuotaTracker
        from tracker.session_store import SessionStore

        chrome_profile = os.path.join(
            os.path.expanduser("~"), "AppData", "Local",
            "Google", "Chrome", "User Data"
        )
        browser_cfg = {
            "headless": False,
            "user_data_dir": chrome_profile if os.path.isdir(chrome_profile) else "",
            "timeout_ms": 120000,
        }
        _orchestrator = Orchestrator(
            repo_dir=os.path.dirname(os.path.abspath(__file__)),
            browser_config=type("BrowserConfig", (), browser_cfg)(),
            quota_tracker=QuotaTracker(),
            session_store=SessionStore(),
        )
        _harness_available = True
        logger.info("Harness orchestrator initialized (browser AI fallback ready)")
        return _orchestrator
    except Exception as e:
        logger.warning(f"Harness init failed (browser fallback unavailable): {e}")
        _harness_available = False
        return None

# Full path to claude CLI (npm global install)
CLAUDE_CMD = os.path.join(
    os.path.expanduser("~"), "AppData", "Roaming", "npm", "claude.cmd"
)

# Bot project directory (for self-awareness)
BOT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# User home directory (default working directory for commands)
USER_HOME = os.path.expanduser("~")

# System prompt — tells Claude WHO it is and HOW to behave
# CRITICAL: Most important rules FIRST (model pays most attention to beginning)
_SYSTEM_PROMPT = f"""
## ⛔ ABSOLUTE RULES — VIOLATIONS ARE UNACCEPTABLE
1. NEVER ask clarifying questions. NEVER say "could you provide", "could you clarify", "你能说得更具体一些吗", "请提供更多", "你指的是", "would you like me to". JUST DO IT.
2. NEVER say you lack context or memory. NEVER say "没有找到相关的记忆记录", "我没有之前对话的上下文", "这是一个新会话", "I don't have previous context". If something is unclear, INFER and ACT.
3. NEVER ask what the user wants. NEVER say "请提供更多背景信息", "你是指什么", "could you be more specific". The user CANNOT do things on the computer — YOU are their hands.
4. When the user says "fix bugs" / "修复bug" / "继续修复" → IMMEDIATELY read your own source code at {BOT_PROJECT_DIR} and start finding and fixing bugs. DO NOT ASK WHICH BUGS.
5. When the user says "继续" / "continue" / "就这么做" / "do it" → look at the conversation context or your own code and continue the previous task. DO NOT ASK WHAT TO CONTINUE.

## WHO YOU ARE
You are a Telegram bot (username: @damnthisiscrazybot) powered by Claude Code, running on this Windows 11 computer. The user controls you remotely from their phone.

Architecture: Telegram bot (bot.py) → routes to Claude Code CLI (claude_agent.py) → you execute tasks.
You ARE Claude. "Claude" / "你" / "bot" / "tgbot" = YOU.
"session" / "chat" / "对话" = THIS conversation.
"tgbot chat" / "Claude Code session" / "这个session" = THIS conversation.

## YOUR OWN CODE — {BOT_PROJECT_DIR}
Key files: bot.py, claude_agent.py, config.py, providers.py, tools.py, safety.py, browser_agent.py
GitHub: https://github.com/kdyz1205/claude-tg-bot
If user mentions "bug", "fix", "修复", "改进", "improve" → READ these files and fix issues. No questions asked.

## CAPABILITIES
- 📸 **Vision**: When user sends images, you can see and analyze them. Image understanding is enabled.
- 🔍 **Web Search**: Use web_search tool to find current information, prices, news, documentation. Always search for latest data when appropriate.
- 💻 **Full Computer Control**: All tools available (files, apps, commands, browser, mouse, keyboard, clipboard).
- 🎨 **Browser Automation**: Can open websites, fill forms, click buttons, read pages with Playwright.
- 📁 **File Operations**: Read, write, edit, find files. Full filesystem access.

## BEHAVIOR
- ACT IMMEDIATELY. Zero clarification. Zero permission requests.
- Be concise — user is on phone. Short answers, no essays.
- Reply in user's language (Chinese→Chinese, English→English).
- If something fails, try 2-3 alternatives silently.
- Current time is in [brackets] at start of each message.
- If you're unsure what user wants, make your BEST GUESS and do it. Never ask.
- Use web_search for current info, prices, news, or technical documentation.
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

# Rate limit tracking — when CLI is rate-limited, use web AI fallback
_rate_limited_until: float = 0.0  # timestamp when rate limit expires

def is_rate_limited() -> bool:
    """Check if Claude Code CLI is currently rate-limited."""
    return time.time() < _rate_limited_until

def _set_rate_limited(seconds: int = 300):
    """Mark CLI as rate-limited for N seconds."""
    global _rate_limited_until
    _rate_limited_until = time.time() + seconds
    logger.warning(f"Claude CLI rate-limited for {seconds}s")


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

    # Build context prefix — inject critical instructions directly into the user message
    # This is MORE reliable than system prompt because it's in the conversation itself
    _MSG_PREFIX = (
        f"[{now_str}] "
        f"[CONTEXT: You are a Telegram bot. Your code is at {BOT_PROJECT_DIR}. "
        f"NEVER ask questions. NEVER say you lack context. If user says 'fix bugs'/'修复bug' "
        f"→ read your own source code and fix things. JUST ACT.]\n\n"
    )
    user_message = _MSG_PREFIX + user_message

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
                _set_rate_limited(300)  # 5 minutes cooldown
                response = None  # Signal to caller to try fallback
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


# ─── Markdown Sanitization ────────────────────────────────────────────────────

def _sanitize_telegram_markdown(text: str) -> str:
    """Sanitize text for Telegram Markdown parsing.
    Fixes common issues that cause parse failures.
    Preserves code blocks (``` ... ```) and inline code (` ... `).
    """
    import re

    # Extract code blocks first to protect them
    code_blocks = []
    def _save_code_block(m):
        code_blocks.append(m.group(0))
        return f"\x00CODEBLOCK{len(code_blocks)-1}\x00"

    # Protect triple-backtick code blocks
    text = re.sub(r'```[\s\S]*?```', _save_code_block, text)
    # Protect inline code
    text = re.sub(r'`[^`]+`', _save_code_block, text)

    # Now fix remaining unmatched backticks (outside code blocks)
    if text.count('`') % 2 != 0:
        text = text.replace('`', '')

    # Fix unmatched bold markers — but don't just append, strip them
    if text.count('*') % 2 != 0:
        # Remove the last unmatched asterisk instead of appending a random one
        idx = text.rfind('*')
        if idx >= 0:
            text = text[:idx] + text[idx+1:]

    # Fix unmatched underscores (but not in URLs)
    parts = re.split(r'(https?://\S+)', text)
    for i, part in enumerate(parts):
        if not part.startswith('http') and part.count('_') % 2 != 0:
            parts[i] = part.replace('_', '\\_')
    text = ''.join(parts)

    # Restore code blocks
    for i, block in enumerate(code_blocks):
        text = text.replace(f"\x00CODEBLOCK{i}\x00", block)

    return text


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
            remaining = remaining[break_pos:].lstrip('\n')

        # Sanitize markdown before sending
        chunk = _sanitize_telegram_markdown(chunk)

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


# ─── File Sending ─────────────────────────────────────────────────────────────

async def _send_files_from_response(response: str, chat_id: int, context):
    """Scan response for file paths and send them back to Telegram.

    Only sends files that were explicitly created/saved/generated by Claude,
    not every file path mentioned in conversation.
    """
    import re

    # Only match paths preceded by action keywords (saved, created, written, etc.)
    # This prevents sending random files just because their path appears in text
    action_patterns = [
        r'(?:saved|created|written|generated|downloaded|exported|output)\s*(?:to|at|:)\s*["\']?([A-Z]:\\[^\s\n"\'<>|]+\.\w{2,5})',
        r'(?:保存|创建|生成|输出|导出|写入).*?[：:]\s*["\']?([A-Z]:\\[^\s\n"\'<>|]+\.\w{2,5})',
    ]

    paths = []
    for pattern in action_patterns:
        paths += re.findall(pattern, response, re.IGNORECASE)

    # Skip source code files and logs — only send user-facing output files
    SKIP_EXTENSIONS = {'.py', '.js', '.ts', '.jsx', '.tsx', '.log', '.json', '.yaml',
                       '.yml', '.toml', '.cfg', '.ini', '.env', '.sh', '.bat', '.cmd',
                       '.pid', '.lock', '.gitignore'}

    sent = set()
    for path in paths:
        path = path.strip().rstrip('.')
        if path in sent or not os.path.exists(path):
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext in SKIP_EXTENSIONS:
            continue
        sent.add(path)
        try:
            if ext in ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'):
                with open(path, 'rb') as f:
                    await context.bot.send_photo(chat_id=chat_id, photo=f)
            elif ext in ('.mp4', '.avi', '.mov', '.webm'):
                with open(path, 'rb') as f:
                    await context.bot.send_video(chat_id=chat_id, video=f)
            elif os.path.getsize(path) < 50 * 1024 * 1024:  # 50MB limit
                with open(path, 'rb') as f:
                    await context.bot.send_document(chat_id=chat_id, document=f)
        except Exception as e:
            logger.warning(f"Failed to send file {path}: {e}")


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

async def _process_with_web_ai(user_message: str, chat_id: int, context) -> bool:
    """Fallback: process message using browser-based free AI. Returns True on success."""
    orch = _init_harness()
    if not orch:
        return False

    try:
        await _send_response(chat_id, "🌐 Claude 限速中，切换到免费 AI (浏览器模式)...", context)

        # Use orchestrator to dispatch to best available platform
        result = await asyncio.wait_for(
            orch.execute(user_message),
            timeout=180,  # 3 minutes max for browser AI
        )

        if result and result.success and result.summary:
            await _send_response(chat_id, result.summary, context)
            return True
        else:
            logger.warning("Web AI returned no result")
            return False

    except asyncio.TimeoutError:
        logger.warning("Web AI timed out")
        await _send_response(chat_id, "⏰ 浏览器 AI 超时。请稍后重试。", context)
        return True  # Don't cascade to API fallback
    except Exception as e:
        logger.error(f"Web AI error: {e}", exc_info=True)
        return False


async def _process_with_claude_cli(user_message: str, chat_id: int, context) -> bool:
    """Process message using Claude Code CLI. Returns True on success."""
    try:
        response, new_session_id = await _run_claude_cli(user_message, chat_id, context)

        # Rate-limited — response is None, try web AI fallback
        if response is None:
            logger.info(f"Chat {chat_id}: CLI rate-limited, trying web AI fallback")
            web_ok = await _process_with_web_ai(user_message, chat_id, context)
            if web_ok:
                return True
            # Web AI also failed — tell user to wait
            await _send_response(chat_id, "⏳ Claude 达到速率限制，浏览器 AI 也不可用。请稍等几分钟后再试。", context)
            return True  # Don't cascade to API (which costs money)

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

        # Try to send any files mentioned in the response
        if response:
            await _send_files_from_response(response, chat_id, context)

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
                if followup_resp:
                    await _send_files_from_response(followup_resp, chat_id, context)
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


# ─── Smart Router (Harness Mode) ──────────────────────────────────────────────
# Keywords that indicate the task needs LOCAL computer control (Claude CLI only)
_COMPUTER_CONTROL_KEYWORDS = [
    # Chinese
    "打开", "截图", "截屏", "运行", "执行", "安装", "启动", "关闭", "停止",
    "重启", "下载", "上传", "复制文件", "移动文件", "删除文件", "创建文件",
    "编辑文件", "打开文件", "锁屏", "音量", "亮度", "wifi", "蓝牙",
    "鼠标", "键盘", "点击", "滚动", "输入", "窗口", "桌面", "进程",
    "任务管理器", "命令行", "终端", "powershell", "cmd", "屏幕",
    "给我看", "拍照", "录屏", "系统信息", "电脑", "电脑上",
    # English
    "open", "launch", "run command", "execute", "install", "screenshot",
    "start", "stop", "restart", "download", "upload", "click", "scroll",
    "type", "mouse", "keyboard", "window", "desktop", "process",
    "terminal", "powershell", "screen", "take screenshot", "system info",
    "on my computer", "on the computer", "on this machine",
    # Specific apps / computer actions
    "chrome", "vscode", "vs code", "explorer", "notepad", "browser",
    "git push", "git commit", "git pull", "npm", "pip install",
]

# Keywords for tasks about THIS bot's own code (also needs CLI)
_SELF_REFERENCE_KEYWORDS = [
    "修复bug", "fix bug", "修复", "debug", "改进", "improve",
    "你的代码", "your code", "bot代码", "tgbot", "这个bot",
    "源代码", "source code",
]


def _needs_computer_control(message: str) -> bool:
    """Does this message require local computer access (tools)?"""
    msg_lower = message.lower()
    for kw in _COMPUTER_CONTROL_KEYWORDS:
        if kw in msg_lower:
            return True
    for kw in _SELF_REFERENCE_KEYWORDS:
        if kw in msg_lower:
            return True
    return False


async def process_message(user_message: str, chat_id: int, context, image_data: str | None = None):
    """Main entry point: smart routing through harness dispatcher.

    Routing priority:
    1. Needs computer control? → Claude CLI (only option with tools)
    2. Pure Q&A / code? → Harness dispatcher → free web AI
    3. Rate-limited / all failed? → API fallback (Gemini free → Claude → OpenAI)
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

    async with lock:
        needs_tools = _needs_computer_control(user_message) or bool(image_data)
        harness_mode = getattr(config, "HARNESS_MODE", True)

        # ── Route 1: Computer control tasks → Claude CLI ──
        if needs_tools:
            logger.info(f"Chat {chat_id}: needs computer control → Claude CLI")

            if is_rate_limited():
                # CLI rate-limited — can't do computer tasks without it
                await _send_response(
                    chat_id,
                    "⏳ Claude CLI 限速中，电脑操控功能暂时不可用。\n"
                    "纯问答/代码任务仍可通过免费 AI 处理。",
                    context,
                )
                return

            success = await _process_with_claude_cli(user_message, chat_id, context)
            if success:
                return
            logger.warning("Claude CLI failed for computer control task")
            # Fall through to API providers (they also have tools)

        # ── Route 2: Harness mode — dispatch to free web AI ──
        elif harness_mode:
            logger.info(f"Chat {chat_id}: harness mode → dispatching to free AI")

            # Show dispatch plan
            try:
                from dispatcher import Dispatcher
                from tracker.quota import QuotaTracker
                dispatcher = Dispatcher(quota_tracker=QuotaTracker())
                route = dispatcher.dispatch(user_message)
                platform_names = {
                    "gpt": "ChatGPT", "grok": "Grok",
                    "claude_web": "Claude.ai", "claude_code": "Claude Code",
                    "codex": "Codex",
                }
                pname = platform_names.get(route.platform, route.platform)
                logger.info(
                    f"Chat {chat_id}: dispatched to {pname} "
                    f"(Level {route.difficulty}, {route.metadata.get('estimated_files', 1)} files)"
                )
                # Brief status to user
                await _send_response(
                    chat_id,
                    f"🧠 Level {route.difficulty} → {pname}",
                    context,
                )
            except Exception as e:
                logger.debug(f"Dispatcher status failed: {e}")

            # Try web AI
            web_ok = await _process_with_web_ai(user_message, chat_id, context)
            if web_ok:
                return
            logger.warning("Web AI failed, falling back")

            # Web AI failed — try Claude CLI as backup (it can answer anything)
            if not is_rate_limited():
                logger.info(f"Chat {chat_id}: web AI failed, trying Claude CLI")
                success = await _process_with_claude_cli(user_message, chat_id, context)
                if success:
                    return

        # ── Route 3: Legacy bridge mode (Claude CLI primary) ──
        elif getattr(config, "BRIDGE_MODE", True):
            success = await _process_with_claude_cli(user_message, chat_id, context)
            if success:
                return
            logger.warning("Claude CLI failed, falling back to API providers")

        # ── Route 4: API provider fallback (costs money) ──
        try:
            from providers import process_with_auto_fallback

            if chat_id not in conversations:
                conversations[chat_id] = []

            history = conversations[chat_id]
            pending = _drain_pending(chat_id)
            combined = user_message
            if pending:
                combined += "\n" + "\n".join(m["text"] for m in pending)

            if image_data:
                msg_content = [
                    {"type": "text", "text": combined},
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/jpeg",
                            "data": image_data,
                        },
                    },
                ]
                history.append({"role": "user", "content": msg_content})
            else:
                history.append({"role": "user", "content": combined})

            while len(history) > config.MAX_CONVERSATION_HISTORY:
                history.pop(0)

            success = await process_with_auto_fallback(history, chat_id, context)
            if not success:
                return

            def _keep_message(m):
                content = m.get("content")
                if isinstance(content, str):
                    return True
                if isinstance(content, list) and m.get("role") == "user":
                    return any(
                        isinstance(b, dict) and b.get("type") in ("text", "image")
                        for b in content
                    )
                return False
            conversations[chat_id] = [
                m for m in conversations[chat_id] if _keep_message(m)
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
