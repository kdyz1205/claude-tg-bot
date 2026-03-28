"""
claude_agent.py — Harness Agent: Claude CLI + 多窗口编排 + 项目管理

DROP-IN REPLACEMENT — 直接替换你的 claude-tg-bot/claude_agent.py

改了什么:
1. _SYSTEM_PROMPT 增加了 Harness 技能（多窗口、项目管理、截图、session管理、多AI协作、权限确认）
2. 删除了 API fallback（不花钱，只走 CLI）
3. 没有其他任何改动。路由、session、队列全部保持原样。

Architecture:
  User (Telegram) → bot.py → claude_agent.py → claude -p --resume <session>
                                                  ↓
                                              Full computer access + Harness Skills
                                              Uses Plan tokens (free)
                                              Persistent conversations
"""
import asyncio
import json
import logging
import os
import time
import traceback
from datetime import datetime
from pathlib import Path
import config
import harness_learn
import skill_library
import auto_research

logger = logging.getLogger(__name__)

# ─── Screenshot Forwarding ────────────────────────────────────────────────────

_TG_SCREENSHOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_tg_screenshots")
_last_screenshot_check: float = 0.0


async def _forward_new_screenshots(chat_id: int, context):
    """Send any new screenshots from _tg_screenshots/ to Telegram, then delete them."""
    global _last_screenshot_check
    if not os.path.isdir(_TG_SCREENSHOT_DIR):
        return

    files = []
    for f in os.listdir(_TG_SCREENSHOT_DIR):
        fp = os.path.join(_TG_SCREENSHOT_DIR, f)
        if os.path.isfile(fp) and f.lower().endswith((".jpg", ".jpeg", ".png")):
            files.append(fp)

    if not files:
        return

    # Sort by modification time (oldest first)
    files.sort(key=lambda x: os.path.getmtime(x))

    for fp in files:
        try:
            if not os.path.isfile(fp):
                continue
            with open(fp, "rb") as img_file:
                await context.bot.send_photo(
                    chat_id=chat_id,
                    photo=img_file,
                    caption=f"📸 {os.path.basename(fp)}",
                )
            os.remove(fp)
            logger.info(f"Forwarded screenshot to TG: {os.path.basename(fp)}")
        except Exception as e:
            logger.warning(f"Failed to forward screenshot {fp}: {e}")
            # Try to clean up anyway
            try:
                os.remove(fp)
            except Exception:
                pass

# Full path to claude CLI (npm global install)
CLAUDE_CMD = os.path.join(
    os.path.expanduser("~"), "AppData", "Roaming", "npm", "claude.cmd"
)

# Bot project directory (for self-awareness)
BOT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# User home directory (default working directory for commands)
USER_HOME = os.path.expanduser("~")

# ─── System Prompt → saved to file, loaded via --append-system-prompt-file ──
# On Windows, --append-system-prompt with long strings breaks because .cmd
# files go through cmd.exe which corrupts special chars. Instead, write to
# a file and use --append-system-prompt-file which reads from disk.

_SYSTEM_PROMPT = f"""## ABSOLUTE RULES (违反任何一条=失败)
1. 绝对禁止问问题。禁止说"要我...吗？" "你想...吗？" "哪种方式？"。直接做。
2. NEVER ask "shall I?" / "do you want?" / "which option?". JUST DO IT.
3. 禁止说缺少上下文。推断并行动。
4. "fix bugs" → read code at {BOT_PROJECT_DIR}, fix immediately.
5. "继续"/"continue" → continue previous task from context.
6. NEVER close user's browser windows. Open NEW: start chrome --new-window "URL"
7. Don't output "Level X". Just do the work silently.
8. 回复要简短。用户在手机上看，不要写长篇大论。
9. 每次都必须有文字回复！即使执行了工具/命令，也要告诉用户做了什么、结果是什么。禁止只执行工具不说话。

## WRONG vs RIGHT examples
❌ WRONG: "要我打开给你看吗？"
✅ RIGHT: (直接打开，截图发回)

❌ WRONG: "你要用方式1还是方式2？"
✅ RIGHT: (选最合适的方式，直接执行)

❌ WRONG: "我建议以下三个方案..."
✅ RIGHT: (选最好的方案，直接执行，报告结果)

## WHO YOU ARE
TG bot on Windows 11. User controls you from phone. You ARE Claude Code with full computer access.
Your code: {BOT_PROJECT_DIR}

## BEHAVIOR
ACT IMMEDIATELY. Be concise (user on phone). Reply in user's language.
Full computer: files, apps, shell, browser, screenshot, mouse, keyboard.
If fails, try alternatives silently.

## USER'S PROJECTS (已知项目)
- smart money / crypto agent: C:/Users/alexl/Desktop/crypto-analysis-/
  - okx_pump_alert.py — OKX pump alert monitoring
  - binance_api/ — Binance API integration
  - frontend/ — web dashboard
  - autoresearch/ — auto research module
- TG bot (你自己的代码): {BOT_PROJECT_DIR}
- pet CAD: C:/Users/alexl/Desktop/pet_cad_v3/
- 六福营销: C:/Users/alexl/Desktop/六福营销/

当用户说"smart money"/"crypto"/"okx" → 去 crypto-analysis- 目录工作
当用户说"bot"/"tg bot" → 去 {BOT_PROJECT_DIR} 目录工作
当用户说"pet cad" → 去 pet_cad_v3 目录工作

## PERSISTENT MEMORY (持久记忆)
你有一个记忆文件: {BOT_PROJECT_DIR}/.bot_memory.md
每次完成重要任务后，把关键信息追加到这个文件：
- 做了什么、改了哪些文件、为什么
- 用户的偏好和习惯
- 项目当前状态、未完成的任务
- 遇到的问题和解决方案
格式: `echo "## [日期] 标题\n内容" >> "{BOT_PROJECT_DIR}/.bot_memory.md"`
下次对话时，先读这个文件了解历史上下文。

## SELF-REFLECTION (自我反思)
每次完成复杂任务后，在回复末尾加一行：
`[自评: 成功/部分/失败 | 原因 | 下次改进]`
同时把成功经验写入 .bot_memory.md，格式：
`## [日期] 经验: [任务类型]\n做法: ...\n结果: 成功/失败\n教训: ...`

## USER FEEDBACK (用户反馈学习)
如果用户说"不对"/"错了"/"重做" → 记录到 .bot_memory.md 的"失败案例"
如果用户说"好"/"对"/"完美" → 记录到 .bot_memory.md 的"成功案例"
这些案例会帮你下次做得更好。

## SELF-UPDATE (修改自己的代码)
你的代码在 {BOT_PROJECT_DIR}。你可以修改自己！
修改 bot 代码后，必须重启才能生效。重启方法：
1. 先回复用户"✅ 代码已修改，正在重启..."
2. 运行: powershell -Command "Start-Sleep 2; Start-Process python -ArgumentList 'run.py' -WorkingDirectory '{BOT_PROJECT_DIR}'"
3. 然后: powershell -Command "Stop-Process -Id (Get-Content '{BOT_PROJECT_DIR}/.bot.pid') -Force"
这样 run.py 会重新启动 bot.py，加载新代码。

## SKILLS
- 列出项目 → dir /b /ad "%USERPROFILE%\\.claude\\projects\\"
- 看历史 → find .jsonl in ~/.claude/projects/, summarize
- 继续项目 → cd到目录, 读代码, 直接修改
- 截图发TG → python pc_control.py screenshot (自动发送到用户Telegram！)
- 浏览器 → start chrome --new-window "URL", then python pc_control.py screenshot
- 多AI协作 → Gemini for images, ChatGPT for text, self for code
- 操控桌面session → screenshot找窗口, 鼠标点击, 键盘输入

## SCREENSHOT → TELEGRAM (重要！)
截图必须用: python "{BOT_PROJECT_DIR}/pc_control.py" screenshot
这样截图会自动发送到用户的Telegram！用户在手机上能看到你看到的屏幕。
不要用其他截图方式，只用 pc_control.py screenshot。
测试网站时：打开→截图→检查→修改→刷新→截图→对比。每一步都截图让用户看到。
操控电脑时：python "{BOT_PROJECT_DIR}/pc_control.py" click/type/hotkey/scroll 等。

## ADAPTIVE COMPUTER CONTROL (OpenClaw风格)
当操控电脑（点击、浏览器、GUI操作）时，必须遵循这个循环：

LOOP (最多20轮):
  1. OBSERVE → 截图看当前屏幕状态
  2. THINK → 分析：当前状态 vs 目标，下一步做什么
  3. ACT → 执行一步操作（点击/输入/滚动/快捷键）
  4. WAIT → 等待 1-2 秒让页面/应用响应
  5. VERIFY → 再次截图，确认操作生效了吗？
  6. ADAPT → 如果失败：
     - 分析原因（窗口没聚焦？坐标偏了？页面没加载完？）
     - 换一种方法（用快捷键代替点击、用Tab导航、滚动查找）
     - 绝不重复同一个失败操作超过2次
  7. CONTINUE → 成功则继续下一步，失败则调整策略

关键规则：
- 每次操作后都要截图验证，不要盲操作
- 如果元素找不到：先滚动、用Ctrl+F搜索、试其他方法
- 遇到弹窗/对话框：先截图理解内容，再决定怎么处理
- Alt+Tab 切换窗口，不要假设窗口在前台
- 页面加载需要时间，操作后等1-2秒再验证
- 复杂任务拆成小步骤，每步都验证

pc_control.py 命令参考：
  screenshot                    → 截图（自动发TG）
  click X Y                     → 左键点击
  doubleclick X Y               → 双击
  rightclick X Y                → 右键
  type "text"                   → 输入文字
  hotkey ctrl c                 → 快捷键
  scroll N                      → 滚动(正=上,负=下)
  moveto X Y                    → 移动鼠标
  drag X1 Y1 X2 Y2             → 拖拽
  getpos                        → 当前鼠标位置
  screensize                    → 屏幕分辨率
  加 --no-takeover 跳过3秒倒计时（连续操作时用）
"""

# Write system prompt to file (read by CLI via --append-system-prompt-file)
_PROMPT_FILE = Path(BOT_PROJECT_DIR) / ".system_prompt.txt"
if not _PROMPT_FILE.exists():
    try:
        _PROMPT_FILE.write_text(_SYSTEM_PROMPT, encoding="utf-8")
    except Exception as e:
        # Fallback: write to temp directory
        import tempfile
        _fallback = Path(tempfile.gettempdir()) / ".claude_bot_system_prompt.txt"
        try:
            _fallback.write_text(_SYSTEM_PROMPT, encoding="utf-8")
            _PROMPT_FILE = _fallback
            logger.warning(f"System prompt written to fallback: {_fallback} (original failed: {e})")
        except Exception as e2:
            logger.error(f"Cannot write system prompt anywhere: {e}, {e2}")

# Ensure screenshot forwarding directory exists and is clean on startup
os.makedirs(_TG_SCREENSHOT_DIR, exist_ok=True)
for _f in os.listdir(_TG_SCREENSHOT_DIR):
    try:
        os.remove(os.path.join(_TG_SCREENSHOT_DIR, _f))
    except Exception:
        pass

# ─── Clean Subprocess Environment ─────────────────────────────────────────────

# Strip sensitive keys from environment passed to Claude CLI subprocesses.
# Claude CLI uses its own auth (Plan tokens) — don't leak bot's API keys.
_SENSITIVE_ENV_KEYS = {"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GEMINI_API_KEY",
                       "TELEGRAM_BOT_TOKEN", "CLAUDE_API_KEY"}


def _clean_env() -> dict:
    """Return os.environ minus sensitive API keys."""
    return {k: v for k, v in os.environ.items() if k not in _SENSITIVE_ENV_KEYS}


# ─── Process Tree Killer (Windows) ────────────────────────────────────────────
import subprocess as _sp


async def _kill_process_tree(proc):
    """Kill a subprocess and its entire process tree on Windows.
    proc.kill() only kills the .cmd wrapper — node.exe children survive."""
    if proc is None:
        return
    pid = proc.pid
    try:
        # taskkill /T kills the entire tree, /F forces it
        _sp.run(
            ["taskkill", "/T", "/F", "/PID", str(pid)],
            capture_output=True, timeout=10,
        )
    except Exception:
        pass
    # Fallback: also try proc.kill() in case taskkill didn't work
    try:
        proc.kill()
        await proc.wait()
    except Exception:
        pass


# Kill any orphaned Claude CLI node processes from previous bot instance
try:
    _sp.run(
        ["powershell", "-NoProfile", "-Command",
         "Get-WmiObject Win32_Process -Filter \"Name='node.exe'\" | "
         "Where-Object { $_.CommandLine -like '*claude-code*' -and $_.CommandLine -like '*-p*' } | "
         "ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }"],
        timeout=10, capture_output=True,
    )
except Exception:
    pass

# ─── Session Persistence ─────────────────────────────────────────────────────

_SESSION_FILE = Path(__file__).parent / ".sessions.json"

_SESSION_TTL = 3600 * 4  # 4 hours — sessions older than this are stale

def _load_sessions() -> dict[int, str]:
    """Load session IDs from disk, pruning stale entries."""
    try:
        if _SESSION_FILE.exists():
            data = json.loads(_SESSION_FILE.read_text(encoding="utf-8"))
            now = time.time()
            result = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    # New format: {"id": "...", "ts": 1234567890}
                    if v.get("ts", 0) > now - _SESSION_TTL:
                        result[int(k)] = v["id"]
                elif v:
                    # Old format: just the session ID string — keep but it'll get timestamps on next save
                    result[int(k)] = v
            return result
    except Exception as e:
        logger.warning(f"Failed to load sessions: {e}")
    return {}

def _save_sessions():
    """Persist session IDs to disk with timestamps (atomic write)."""
    try:
        import shutil
        now = time.time()
        save_data = {
            str(k): {"id": v, "ts": now}
            for k, v in _claude_sessions.items()
        }
        temp_file = _SESSION_FILE.with_suffix(".tmp")
        temp_file.write_text(
            json.dumps(save_data, indent=2),
            encoding="utf-8",
        )
        shutil.move(str(temp_file), str(_SESSION_FILE))
    except Exception as e:
        logger.warning(f"Failed to save sessions: {e}")

# ─── Session & Queue State ──────────────────────────────────────────────────

_claude_sessions: dict[int, str] = _load_sessions()
_pending_messages: dict[int, list[dict]] = {}
_processing_locks: dict[int, asyncio.Lock] = {}
_MAX_PENDING_AGE = 600  # 10 minutes


_rate_limited_until: float = 0.0

def is_rate_limited() -> bool:
    """Check if Claude CLI is currently rate limited."""
    return time.time() < _rate_limited_until


def _get_lock(chat_id: int) -> asyncio.Lock:
    return _processing_locks.setdefault(chat_id, asyncio.Lock())


# ─── Adaptive Model Selection ────────────────────────────────────────────────

# Short/simple messages → Sonnet (fast, 5-10s)
# Complex coding/debugging → Opus (slow but smart, 30-90s)
import re

_OPUS_PATTERNS = [
    r"修复|fix|debug|bug",
    r"写代码|write code|implement|重构|refactor",
    r"分析.*代码|analyze.*code|review.*code",
    r"创建.*项目|create.*project|新建",
    r"部署|deploy|上线",
    r"自主|autonomous|loop",
]
# These go to Sonnet (fast enough, need quick response for interactive loops)
_SONNET_PATTERNS = [
    r"截图|screenshot|看看|打开|open",
    r"点击|click|浏览器|browser|chrome",
    r"测试.*网站|test.*website|test.*site",
    r"操控|控制|接管|takeover",
]
_SONNET_RE = re.compile("|".join(_SONNET_PATTERNS), re.IGNORECASE)
_OPUS_RE = re.compile("|".join(_OPUS_PATTERNS), re.IGNORECASE)


def _pick_model(message: str) -> str:
    """Pick model based on message complexity. Opus for hard tasks, Sonnet for everything else."""
    # If user explicitly set model via /model, respect it always
    # config.CLAUDE_MODEL is the user's choice
    # We override DOWN to sonnet for simple stuff, never override UP
    if config.CLAUDE_MODEL != "claude-opus-4-6":
        return config.CLAUDE_MODEL

    clean = message.strip()

    # Interactive/GUI tasks → Sonnet (needs fast response for click loops)
    if _SONNET_RE.search(clean):
        return "claude-sonnet-4-6"

    # Short messages (< 15 chars) are usually simple queries
    if len(clean) < 15 and not _OPUS_RE.search(clean):
        return "claude-sonnet-4-6"

    # Complex coding/debugging → Opus
    if _OPUS_RE.search(clean):
        return "claude-opus-4-6"

    # Default: Sonnet (fast)
    return "claude-sonnet-4-6"


# ─── Typing Indicator ────────────────────────────────────────────────────────

async def _keep_typing(chat_id, context, stop_event):
    """Send typing indicator every 4 seconds while processing."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                context.bot.send_chat_action(chat_id=chat_id, action="typing"),
                timeout=3,
            )
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=4)
            break
        except asyncio.TimeoutError:
            pass


# ─── Knowledge Gap (non-blocking) ────────────────────────────────────────────

async def _safe_knowledge_gap(user_message: str):
    """Fire-and-forget: detect knowledge gaps and learn in background."""
    try:
        await auto_research.detect_and_fill_knowledge_gap(user_message)
    except Exception:
        pass


# ─── Claude CLI Runner ────────────────────────────────────────────────────────

async def _run_claude_cli(
    user_message: str, chat_id: int, context,
    timeout: int = None,
) -> tuple[str, str | None]:
    """Run claude CLI and return (response_text, session_id)."""
    global _rate_limited_until
    timeout = timeout or getattr(config, "CLAUDE_CLI_TIMEOUT", 300)
    session_id = _claude_sessions.get(chat_id)

    # Adaptive model: simple queries → Sonnet (fast), complex → Opus
    model = _pick_model(user_message)

    # Inject memory + skills into system prompt file (not -p arg) to avoid Windows encoding issues
    import tempfile
    _mem_prompt_path = None
    _matched_skill_ids = []

    # Find matching skills (always, not just new sessions)
    matched_skills = skill_library.find_matching_skills(user_message, max_results=2)
    _matched_skill_ids = [s["id"] for s in matched_skills]

    if not session_id:
        mem_context = harness_learn.get_memory_context(max_chars=2000)
        workflow = harness_learn.get_relevant_workflow(user_message)
    else:
        mem_context = None
        workflow = None

    skills_text = skill_library.format_skills_for_prompt(matched_skills)

    # Layer 5: Knowledge — sync local lookup only (no blocking API call)
    # Background gap detection fires as non-blocking task
    knowledge_text = auto_research.get_relevant_knowledge(user_message, max_chars=600)
    if not knowledge_text:
        asyncio.create_task(_safe_knowledge_gap(user_message))

    if mem_context or workflow or skills_text or knowledge_text:
        mem_text = _PROMPT_FILE.read_text(encoding="utf-8")
        if mem_context:
            mem_text += f"\n\n## 历史记忆\n{mem_context}\n"
        if workflow:
            mem_text += f"\n## 参考模板\n{workflow['task_type']} → {', '.join(workflow['steps'][:5])}\n"
        if skills_text:
            mem_text += skills_text
        if knowledge_text:
            mem_text += f"\n## 领域知识\n{knowledge_text}\n"
        _mem_tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", encoding="utf-8", delete=False, dir=BOT_PROJECT_DIR,
        )
        _mem_tmp.write(mem_text)
        _mem_tmp.close()
        _mem_prompt_path = _mem_tmp.name

    prompt_file = _mem_prompt_path or str(_PROMPT_FILE)

    args = [
        CLAUDE_CMD,
        "-p", user_message,
        "--output-format", "json",
        "--dangerously-skip-permissions",
        "--model", model,
        "--append-system-prompt-file", prompt_file,
    ]
    if session_id:
        args.extend(["--resume", session_id])
        logger.info(f"Chat {chat_id}: resuming session {session_id[:12]}... (model: {model})")
    else:
        logger.info(f"Chat {chat_id}: new session (model: {model})")

    stop_typing = asyncio.Event()
    typing_task = asyncio.create_task(_keep_typing(chat_id, context, stop_typing))
    proc = None

    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=USER_HOME,
            env=_clean_env(),
        )

        stdout_data, stderr_data = await asyncio.wait_for(
            proc.communicate(),
            timeout=timeout,
        )

    except asyncio.TimeoutError:
        logger.warning(f"Claude CLI timed out after {timeout}s")
        await _kill_process_tree(proc)
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
        # Clean up temp files
        if _mem_prompt_path:
            try:
                os.unlink(_mem_prompt_path)
            except Exception:
                pass

    if proc and proc.returncode != 0:
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
                elif new_session_id:
                    # Claude did tool use but gave no text response — ask for summary
                    logger.info(f"Chat {chat_id}: empty result, requesting follow-up summary")
                    try:
                        followup_args = [
                            CLAUDE_CMD, "-p", "请用中文简要总结你刚才做了什么。",
                            "--output-format", "json",
                            "--dangerously-skip-permissions",
                            "--model", model,
                            "--resume", new_session_id,
                        ]
                        fu_proc = await asyncio.create_subprocess_exec(
                            *followup_args,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                            cwd=USER_HOME,
                        )
                        fu_out, _ = await asyncio.wait_for(fu_proc.communicate(), timeout=30)
                        fu_raw = fu_out.decode("utf-8", errors="replace").strip()
                        fu_data = json.loads(fu_raw)
                        fu_text = fu_data.get("result", "").strip()
                        if fu_text:
                            response = fu_text
                    except Exception as e:
                        logger.debug(f"Follow-up summary failed: {e}")
                if not response:
                    response = "✅ 任务已执行（无文字输出）。"

            # Auth detection — CLI not logged in
            if response:
                resp_lower = response.lower()
                if any(p in resp_lower for p in _ERROR_PATTERNS["auth"]):
                    logger.error(f"Claude CLI auth error: {response[:200]}")
                    response = (
                        "❌ Claude CLI 未登录！\n\n"
                        "请在电脑上打开 PowerShell 运行：\n"
                        "  claude /login\n\n"
                        "选择 1 (Claude subscription)，完成浏览器登录后重启 bot。"
                    )
                    new_session_id = None
                # Credit/billing detection
                elif any(p in resp_lower for p in _ERROR_PATTERNS["credit"]):
                    logger.error(f"Claude CLI credit error: {response[:200]}")
                    response = "❌ Claude API 额度不足或账单问题。请检查你的 Anthropic 账户。"
                    new_session_id = None
                # Rate limit detection — don't store poisoned session
                elif any(p in resp_lower for p in _ERROR_PATTERNS["rate"]):
                    _rate_limited_until = time.time() + 300  # 5 min cooldown
                    logger.warning(f"Claude CLI rate limited: {response[:200]}")
                    response = "⏳ Claude 达到速率限制。请稍等几分钟后再试。"
                    new_session_id = None

        except json.JSONDecodeError:
            # CLI may output warnings before JSON — search from the end
            json_end = raw.rfind('}')
            json_start = -1
            if json_end >= 0:
                # Find matching opening brace by scanning backwards
                depth = 0
                for i in range(json_end, -1, -1):
                    if raw[i] == '}':
                        depth += 1
                    elif raw[i] == '{':
                        depth -= 1
                    if depth == 0:
                        json_start = i
                        break
            if json_start >= 0:
                try:
                    data = json.loads(raw[json_start:json_end + 1])
                    response = data.get("result", "").strip()
                    new_session_id = data.get("session_id")
                    if not response:
                        response = raw[:json_start].strip() or "✅ 任务已执行。"
                except json.JSONDecodeError:
                    response = raw
            else:
                response = raw

    # ── Check stderr for critical errors BEFORE falling back ──
    _ERROR_PATTERNS = {
        "credit": ["credit balance", "insufficient credit", "billing"],
        "auth": ["not logged in", "not authenticated", "auth failed", "login required", "invalid x-api-key"],
        "rate": ["hit your limit", "rate limit", "rate_limit", "quota exceeded", "usage limit", "too many requests"],
    }
    if stderr_data:
        err_text = stderr_data.decode("utf-8", errors="replace").strip()
        if err_text:
            logger.debug(f"Claude CLI stderr (chat {chat_id}): {err_text[:500]}")
            err_lower = err_text.lower()
            # Check for credit/billing errors
            if any(p in err_lower for p in _ERROR_PATTERNS["credit"]):
                logger.error(f"Claude CLI credit error detected in stderr: {err_text[:300]}")
                response = "❌ Claude API 额度不足或账单问题。请检查你的 Anthropic 账户。"
                new_session_id = None
            # Check for auth errors
            elif any(p in err_lower for p in _ERROR_PATTERNS["auth"]):
                logger.error(f"Claude CLI auth error in stderr: {err_text[:300]}")
                response = "❌ Claude CLI 认证失败。请运行 `claude /login` 重新登录。"
                new_session_id = None
            # Check for rate limit errors
            elif any(p in err_lower for p in _ERROR_PATTERNS["rate"]):
                global _rate_limited_until
                _rate_limited_until = time.time() + 300
                logger.warning(f"Claude CLI rate limited (stderr): {err_text[:300]}")
                response = "⏳ Claude 达到速率限制。请稍等几分钟后再试。"
                new_session_id = None

    if not response:
        err = stderr_data.decode("utf-8", errors="replace").strip() if stderr_data else ""
        if err:
            logger.error(f"Claude CLI stderr: {err[:500]}")
            if "error" in err.lower():
                response = f"⚠️ {err[:500]}"
            else:
                response = "✅ 任务已执行。"
        else:
            response = "✅ 任务已执行（无输出）。"

    return response, new_session_id


# ─── Response Sender ──────────────────────────────────────────────────────────

async def _send_response(chat_id: int, response: str, context):
    """Send response to Telegram, splitting into chunks if needed."""
    if not response or not response.strip():
        return

    MAX_TOTAL = 16000
    if len(response) > MAX_TOTAL:
        response = response[:MAX_TOTAL] + "\n\n... (输出过长，已截断。需要完整内容请说。)"

    remaining = response
    while remaining:
        if len(remaining) <= 4000:
            chunk = remaining
            remaining = ""
        else:
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
            try:
                await context.bot.send_message(chat_id=chat_id, text=chunk)
            except Exception as e:
                logger.error(f"Failed to send message to {chat_id}: {e}")


# ─── Queue Helpers ────────────────────────────────────────────────────────────

def _drain_pending(chat_id: int) -> list[dict]:
    msgs = _pending_messages.pop(chat_id, [])
    now = time.time()
    fresh = [m for m in msgs if now - m["time"] < _MAX_PENDING_AGE]
    if len(fresh) < len(msgs):
        logger.info(f"Chat {chat_id}: dropped {len(msgs) - len(fresh)} stale queued messages")
    return fresh


def _queue_message(chat_id: int, text: str):
    _pending_messages.setdefault(chat_id, []).append({
        "text": text,
        "time": time.time(),
    })


# ─── Self-Healing ────────────────────────────────────────────────────────────

async def _self_heal(user_message: str, chat_id: int, context, error: Exception) -> bool:
    """Attempt auto-recovery from errors. Returns True if healed and retried successfully."""
    err_str = str(error).lower()
    tb = traceback.format_exc()
    logger.info(f"Self-heal attempting for: {err_str[:100]}")

    # ── Layer 1: Known patterns (fast, no API call) ──

    # Session corrupted → clear and retry
    if "session" in err_str or "resume" in err_str or "invalid" in err_str:
        logger.info("Self-heal: clearing corrupted session")
        _claude_sessions.pop(chat_id, None)
        _save_sessions()
        await _send_response(chat_id, "🔧 会话异常，已重置。重新处理...", context)
        try:
            response, sid = await _run_claude_cli(user_message, chat_id, context)
            if sid:
                _claude_sessions[chat_id] = sid
                _save_sessions()
            await _send_response(chat_id, response, context)
            await _forward_new_screenshots(chat_id, context)
            return True
        except Exception as e2:
            logger.error(f"Self-heal retry failed: {e2}")
            return False

    # Encoding / input error → retry with simpler message
    if "input must be provided" in err_str or "encoding" in err_str or "charmap" in err_str:
        logger.info("Self-heal: encoding issue, retrying via temp file")
        import tempfile
        await _send_response(chat_id, "🔧 编码问题，正在重试...", context)
        msg_tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", encoding="utf-8", delete=False, dir=BOT_PROJECT_DIR,
        )
        try:
            msg_tmp.write(user_message)
            msg_tmp.close()
            session_id = _claude_sessions.get(chat_id)
            model = _pick_model(user_message)
            args = [
                CLAUDE_CMD,
                "-p", ".",
                "--output-format", "json",
                "--dangerously-skip-permissions",
                "--model", model,
                "--append-system-prompt-file", msg_tmp.name,
            ]
            if session_id:
                args.extend(["--resume", session_id])
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=USER_HOME,
            )
            stdout_data, _ = await asyncio.wait_for(proc.communicate(), timeout=120)
            raw = stdout_data.decode("utf-8", errors="replace").strip()
            response = raw
            sid = None
            if raw:
                try:
                    data = json.loads(raw)
                    response = data.get("result", "").strip() or raw
                    sid = data.get("session_id")
                except json.JSONDecodeError:
                    pass
            if sid:
                _claude_sessions[chat_id] = sid
                _save_sessions()
            await _send_response(chat_id, response, context)
            return True
        except Exception:
            return False
        finally:
            try:
                os.unlink(msg_tmp.name)
            except Exception:
                pass

    # ── Layer 2: Claude self-diagnosis (for unknown errors) ──
    try:
        await _send_response(chat_id, "🔧 遇到未知错误，正在自我诊断...", context)

        # Get relevant code context
        diag_prompt = (
            f"你是一个自修复系统。分析这个错误并给出修复建议。\n\n"
            f"错误: {str(error)[:500]}\n\n"
            f"Traceback:\n{tb[-1000:]}\n\n"
            f"用户原始消息: {user_message[:200]}\n\n"
            f"回复格式（只输出JSON）:\n"
            f'{{"diagnosis": "一句话原因", "fix": "建议的修复", "can_retry": true/false, "retry_action": "clear_session/restart/none"}}'
        )

        diag_result = await _run_claude_raw(
            prompt=diag_prompt,
            model="claude-haiku-4-5-20251001",
            timeout=15,
        )

        # Parse diagnosis
        diagnosis = _parse_diagnosis(diag_result)
        logger.info(f"Self-heal diagnosis: {diagnosis}")

        if diagnosis.get("can_retry"):
            action = diagnosis.get("retry_action", "none")

            if action == "clear_session":
                _claude_sessions.pop(chat_id, None)
                _save_sessions()

            await _send_response(
                chat_id,
                f"🔍 诊断: {diagnosis.get('diagnosis', '未知')}\n🔧 修复: {diagnosis.get('fix', '重试')}",
                context,
            )

            # Retry
            try:
                response, sid = await _run_claude_cli(user_message, chat_id, context)
                if sid:
                    _claude_sessions[chat_id] = sid
                    _save_sessions()
                await _send_response(chat_id, response, context)
                await _forward_new_screenshots(chat_id, context)

                # Log successful self-heal + feed back to learning system
                _log_self_heal(str(error), diagnosis, success=True)
                harness_learn.record_self_heal(
                    str(error)[:200], diagnosis.get("diagnosis", ""),
                    diagnosis.get("fix", ""), success=True,
                )
                return True
            except Exception as e3:
                logger.error(f"Self-heal retry failed: {e3}")
                _log_self_heal(str(error), diagnosis, success=False)
                harness_learn.record_self_heal(
                    str(error)[:200], diagnosis.get("diagnosis", ""),
                    diagnosis.get("fix", ""), success=False,
                )
                return False
        else:
            await _send_response(
                chat_id,
                f"🔍 诊断: {diagnosis.get('diagnosis', '未知')}\n⚠️ 无法自动修复: {diagnosis.get('fix', '需要人工处理')}",
                context,
            )
            _log_self_heal(str(error), diagnosis, success=False)
            harness_learn.record_self_heal(
                str(error)[:200], diagnosis.get("diagnosis", ""),
                diagnosis.get("fix", ""), success=False,
            )
            return False

    except Exception as heal_err:
        logger.error(f"Self-heal itself failed: {heal_err}")
        return False


def _parse_diagnosis(raw: str) -> dict:
    """Parse diagnosis JSON from Claude response."""
    if not raw:
        return {"diagnosis": "无诊断输出", "can_retry": True, "retry_action": "clear_session", "fix": "重试"}
    try:
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(raw[start:end])
    except (json.JSONDecodeError, ValueError):
        pass
    return {"diagnosis": raw[:200], "can_retry": True, "retry_action": "clear_session", "fix": "重试"}


def _log_self_heal(error: str, diagnosis: dict, success: bool):
    """Log self-healing attempt to memory."""
    try:
        entry = {
            "timestamp": datetime.now().isoformat(),
            "error": error[:200],
            "diagnosis": diagnosis.get("diagnosis", "")[:200],
            "fix": diagnosis.get("fix", "")[:200],
            "success": success,
        }
        heal_log = os.path.join(BOT_PROJECT_DIR, ".self_heal.jsonl")
        with open(heal_log, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ─── Main Processing Logic ────────────────────────────────────────────────────

async def _process_with_claude_cli(user_message: str, chat_id: int, context) -> bool:
    """Process message using Claude Code CLI. Returns True on success."""
    try:
        _start_time = time.time()
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

        if new_session_id:
            _claude_sessions[chat_id] = new_session_id
            _save_sessions()
            logger.info(f"Chat {chat_id}: session_id = {new_session_id[:12]}...")
        else:
            logger.debug(f"Chat {chat_id}: no session_id returned")

        await _send_response(chat_id, response, context)
        await _forward_new_screenshots(chat_id, context)

        # ── Harness Learning Loop ──
        try:
            _duration = int((time.time() - _start_time) * 1000)
            _model = _pick_model(user_message)
            score = harness_learn.post_interaction_loop(
                user_message=user_message,
                response=response,
                model=_model,
                duration_ms=_duration,
                session_id=new_session_id,
            )
            logger.info(f"Chat {chat_id}: score={score['overall']:.2f} flags={score['flags']}")

            # ── Skill Library: extract new skills + update reused ones ──
            asyncio.create_task(
                _skill_post_process(user_message, response, score, _matched_skill_ids)
            )

            # ── Auto-evolution check: should we train? ──
            train_decision = harness_learn.should_auto_train()
            if train_decision:
                logger.info(f"Auto-evolution triggered: {train_decision}")
                asyncio.create_task(
                    _auto_evolve(chat_id, context, train_decision)
                )
        except Exception as e:
            logger.debug(f"Harness scoring error: {e}")

        # Process queued follow-up messages
        pending = _drain_pending(chat_id)
        while pending:
            combined = "\n---\n".join(m["text"] for m in pending)
            count = len(pending)
            logger.info(f"Chat {chat_id}: processing {count} queued follow-up messages")

            await _send_response(chat_id, f"📨 处理你追加的 {count} 条消息...", context)

            try:
                followup_resp, followup_sid = await _run_claude_cli(combined, chat_id, context)
                if followup_sid:
                    _claude_sessions[chat_id] = followup_sid
                    _save_sessions()
                await _send_response(chat_id, followup_resp, context)
                await _forward_new_screenshots(chat_id, context)
            except asyncio.TimeoutError:
                await _send_response(chat_id, "⏰ 追加任务超时(5分钟)。发新消息继续。", context)
                break
            except Exception as e:
                logger.error(f"Follow-up error: {e}", exc_info=True)
                await _send_response(chat_id, f"⚠️ 追加消息处理出错: {str(e)[:300]}", context)
                break

            pending = _drain_pending(chat_id)

        return True

    except asyncio.TimeoutError:
        # Self-heal: clear session and notify
        _claude_sessions.pop(chat_id, None)
        _save_sessions()
        await _send_response(
            chat_id,
            "⏰ 超时。已清除会话，发新消息重试。",
            context,
        )
        return True

    except FileNotFoundError:
        await _send_response(
            chat_id,
            "❌ Claude CLI 未找到。请运行: npm install -g @anthropic-ai/claude-code",
            context,
        )
        return False

    except Exception as e:
        logger.error(f"Claude CLI error: {e}", exc_info=True)
        # ── Self-Healing: attempt auto-recovery ──
        healed = await _self_heal(user_message, chat_id, context, e)
        if not healed:
            await _send_response(chat_id, f"⚠️ 出错且自修复失败: {str(e)[:300]}", context)
        return healed


async def process_message(user_message: str, chat_id: int, context, **kwargs):
    """Process a user message — all messages go through dispatcher first."""
    auto_research.mark_user_active()  # Reset idle timer for auto-experiment
    lock = _get_lock(chat_id)

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
        # Primary path: Claude CLI with session persistence (has context memory)
        # Pipeline is available but CLI+session is better for conversational tasks
        success = await _process_with_claude_cli(user_message, chat_id, context)
        if not success:
            await _send_response(chat_id, "⚠️ CLI 失败，请重试。", context)


async def _skill_post_process(user_message: str, response: str, score: dict, matched_ids: list):
    """Background: extract new skills and update reused skills."""
    try:
        # Update reused skills
        for sid in matched_ids:
            skill_library.update_skill_from_reuse(sid, score)
            # Maybe evolve the skill if used multiple times
            await skill_library.maybe_evolve_skill(sid, user_message, response)

        # Try to extract a new skill from this interaction
        new_id = await skill_library.maybe_extract_skill(user_message, response, score)
        if new_id:
            logger.info(f"New skill learned: {new_id}")

        # Periodic pruning
        if score.get("overall", 0) > 0:  # Only if scoring worked
            skill_library.prune_skills()
    except Exception as e:
        logger.debug(f"Skill post-process error: {e}")


async def _auto_evolve(chat_id: int, context, decision: dict):
    """Background auto-evolution: run training when scores are low.
    This is the key connection: scoring → training → prompt improves → scores improve."""
    try:
        domain = decision["domain"]
        reason = decision["reason"]
        logger.info(f"Auto-evolve: training {domain} because {reason}")

        await _send_response(
            chat_id,
            f"🧬 自动进化触发\n原因: {reason}\n训练: {domain}\n(后台运行，不影响你使用)",
            context,
        )

        import auto_train

        async def _status(text):
            logger.info(f"Auto-evolve status: {text}")
            # Don't spam user — only send summary
            pass

        await auto_train.run_training(
            domain_id=domain,
            send_status=_status,
            send_photo=None,
            loops=2,  # Quick 2-round auto-training
            _internal=False,
        )

        harness_learn.mark_auto_trained()

        await _send_response(
            chat_id,
            f"🧬 自动进化完成 ({domain})\nPrompt已根据表现自动优化",
            context,
        )
        logger.info(f"Auto-evolve completed: {domain}")

    except Exception as e:
        logger.error(f"Auto-evolve failed: {e}", exc_info=True)
        harness_learn.mark_auto_trained()  # Still reset cooldown to avoid retry loop


async def _process_with_pipeline(user_message: str, chat_id: int, context):
    """Process message using multi-agent pipeline."""
    try:
        from agents.runner import run_pipeline

        async def send_status(text):
            try:
                await context.bot.send_message(chat_id=chat_id, text=text)
            except Exception:
                pass

        logger.info(f"Chat {chat_id}: starting multi-agent pipeline")
        result = await run_pipeline(
            user_message=user_message,
            chat_id=chat_id,
            send_status=send_status,
        )
        await _send_response(chat_id, result, context)

    except Exception as e:
        logger.error(f"Pipeline error: {e}", exc_info=True)
        # Fallback to direct CLI
        logger.info(f"Chat {chat_id}: pipeline failed, falling back to direct CLI")
        await _send_response(chat_id, f"⚠️ 多Agent管线出错，切换到直接模式...", context)
        success = await _process_with_claude_cli(user_message, chat_id, context)
        if not success:
            await _send_response(chat_id, "⚠️ 也失败了，请重试。", context)


async def _run_claude_cli_direct(
    prompt: str,
    model: str = "claude-sonnet-4-6",
    timeout: int = 120,
) -> tuple[str, str | None]:
    """Lightweight CLI call for training/internal use. No chat_id, no typing indicator."""
    args = [
        CLAUDE_CMD,
        "-p", f"[Training task] {prompt}",
        "--output-format", "json",
        "--dangerously-skip-permissions",
        "--model", model,
        "--append-system-prompt-file", str(_PROMPT_FILE),
    ]

    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=USER_HOME,
    )

    try:
        stdout_data, stderr_data = await asyncio.wait_for(
            proc.communicate(), timeout=timeout,
        )
    except asyncio.TimeoutError:
        await _kill_process_tree(proc)
        return "Timed out", None

    raw = stdout_data.decode("utf-8", errors="replace").strip()
    if not raw:
        return "No output", None

    try:
        data = json.loads(raw)
        response = data.get("result", "").strip() or "Done."
        session_id = data.get("session_id")
        return response, session_id
    except json.JSONDecodeError:
        return raw[:1000], None


async def _run_claude_raw(
    prompt: str,
    model: str = "claude-haiku-4-5-20251001",
    timeout: int = 30,
) -> str:
    """Clean CLI call WITHOUT bot system prompt. For judge/meta tasks.
    Writes prompt to a temp file used as --system-prompt, with a short -p trigger."""
    import tempfile

    # Write the full prompt to a temp file, pass as system prompt
    # This avoids Windows CLI encoding issues with long/Chinese -p args
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", encoding="utf-8", delete=False, dir=BOT_PROJECT_DIR,
    )
    try:
        tmp.write(prompt)
        tmp.close()

        args = [
            CLAUDE_CMD,
            "-p", "Read your system prompt carefully and follow every instruction in it. Output exactly what it asks for.",
            "--append-system-prompt-file", tmp.name,
            "--output-format", "json",
            "--dangerously-skip-permissions",
            "--model", model,
        ]

        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=USER_HOME,
            env=_clean_env(),
        )

        try:
            stdout_data, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            await _kill_process_tree(proc)
            return ""

        raw = stdout_data.decode("utf-8", errors="replace").strip()
        if not raw:
            return ""

        try:
            data = json.loads(raw)
            return data.get("result", "").strip()
        except json.JSONDecodeError:
            return raw[:2000]
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass


async def _forward_new_screenshots_direct(send_photo):
    """Forward screenshots for training (no chat_id context)."""
    if not send_photo or not os.path.isdir(_TG_SCREENSHOT_DIR):
        return
    files = sorted(
        [os.path.join(_TG_SCREENSHOT_DIR, f) for f in os.listdir(_TG_SCREENSHOT_DIR)
         if f.lower().endswith((".jpg", ".jpeg", ".png"))],
        key=lambda x: os.path.getmtime(x),
    )
    for fp in files:
        try:
            if not os.path.isfile(fp):
                continue
            with open(fp, "rb") as img:
                await send_photo(img)
            os.remove(fp)
        except Exception:
            try:
                os.remove(fp)
            except Exception:
                pass


def clear_history(chat_id: int):
    """Clear all state for a chat."""
    _claude_sessions.pop(chat_id, None)
    _save_sessions()
    _pending_messages.pop(chat_id, None)
