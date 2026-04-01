"""
claude_agent.py — Harness Agent: 本地 Claude Code CLI（订阅额度）+ 多窗口编排 + 项目管理

主对话路径走 **异步子进程** ``asyncio.create_subprocess_exec``（禁止阻塞式 ``Popen``），
全局 ``CLI_SEMAPHORE`` 串行化所有 CLI 调用；单次调用 ``asyncio.wait_for(..., 45s)``，
超时则 ``kill`` 子进程树，避免僵尸与 TG 事件循环假死。

可选：``llm_http_client`` 仅用于历史清理/兼容，主推理不依赖计费 HTTP API。
"""
import asyncio
import json
import logging
import os
import re
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any
import config
import llm_http_client
import harness_learn
import skill_library
import auto_research
import vital_signs
import memory_engine as _memory_engine
from self_monitor import self_monitor as _self_monitor

# ─── SessionManager (multi-project routing) ──────────────────────────────────
try:
    from agents.sessions import SessionManager as _SessionManager
    _session_mgr = _SessionManager()
except Exception:
    _session_mgr = None

logger = logging.getLogger(__name__)

# 全库唯一 CLI 飞行：禁止并发多个 claude-code 进程（内存与交互爆炸）
CLI_SEMAPHORE: asyncio.Semaphore = asyncio.Semaphore(1)

# ─── Screenshot Forwarding ────────────────────────────────────────────────────

_TG_SCREENSHOT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_tg_screenshots")


def _safe_mtime(path: str) -> float:
    """Return file mtime, or 0 on error (file deleted between listing and stat)."""
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0


async def _forward_new_screenshots(chat_id: int, context, *, user_requested: bool = False):
    """Send new screenshots from _tg_screenshots/ to Telegram, then delete them.

    Only sends the LATEST screenshot unless the user explicitly asked for screenshots.
    This prevents flooding the chat when CLI takes multiple internal screenshots.
    """
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
    files.sort(key=_safe_mtime)

    # Only send the latest screenshot unless user explicitly asked
    if not user_requested and len(files) > 1:
        # Delete older screenshots silently, only send the newest
        for fp in files[:-1]:
            try:
                os.remove(fp)
            except Exception:
                pass
        files = files[-1:]

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
            try:
                os.remove(fp)
            except Exception:
                pass

# ─── Media Extraction from Response ──────────────────────────────────────────

# Patterns for file paths in Claude's response text
_MEDIA_EXTENSIONS = {
    "photo": {".png", ".jpg", ".jpeg", ".bmp", ".webp"},
    "animation": {".gif"},
    "video": {".mp4", ".avi", ".mkv", ".mov", ".webm"},
    "document": {".pdf", ".zip", ".tar", ".gz", ".xlsx", ".docx", ".csv", ".txt"},
}

# Regex to find Windows-style file paths (C:\...\file.ext or C:/..../file.ext)
# and Unix-style paths (/tmp/...) in text
_FILE_PATH_RE = re.compile(
    r'(?:'
    r'[A-Za-z]:[/\\](?:[^\s<>"\'|*?\n]+)'  # Windows path: C:\foo\bar.png or C:/foo/bar.png
    r'|'
    r'/(?:tmp|home|usr|var|mnt|opt)[/][^\s<>"\'|*?\n]+'  # Unix absolute path
    r')'
)


def _extract_media_paths(text: str) -> list[dict]:
    """Extract file paths from response text, categorized by media type.

    Returns list of {"path": str, "type": "photo"|"animation"|"video"|"document"}
    """
    if not text:
        return []

    results = []
    seen = set()

    for match in _FILE_PATH_RE.finditer(text):
        raw_path = match.group(0).rstrip(".,;:)>]}")  # strip trailing punctuation
        # Normalize path separators
        norm_path = raw_path.replace("/", os.sep).replace("\\", os.sep)

        if norm_path in seen:
            continue
        seen.add(norm_path)

        ext = os.path.splitext(norm_path)[1].lower()
        for media_type, extensions in _MEDIA_EXTENSIONS.items():
            if ext in extensions:
                results.append({"path": norm_path, "type": media_type})
                break

    return results


async def _send_extracted_media(chat_id: int, context, response: str):
    """Scan Claude's response for file paths and send matching media to Telegram.

    Skips files in _tg_screenshots/ (already handled by _forward_new_screenshots).
    """
    media_items = _extract_media_paths(response)
    if not media_items:
        return

    tg_dir_norm = os.path.normpath(_TG_SCREENSHOT_DIR).lower()

    for item in media_items:
        fpath = item["path"]

        # Skip files already handled by screenshot forwarding
        if os.path.normpath(fpath).lower().startswith(tg_dir_norm):
            continue

        if not os.path.isfile(fpath):
            logger.debug(f"Media file not found, skipping: {fpath}")
            continue

        # Skip very large files (>50MB for Telegram limit)
        try:
            fsize = os.path.getsize(fpath)
            if fsize > 50 * 1024 * 1024:
                logger.info(f"Skipping large file ({fsize // 1024 // 1024}MB): {fpath}")
                continue
            if fsize == 0:
                continue
        except OSError:
            continue

        basename = os.path.basename(fpath)
        try:
            with open(fpath, "rb") as f:
                if item["type"] == "photo":
                    await context.bot.send_photo(
                        chat_id=chat_id, photo=f,
                        caption=f"📎 {basename}",
                    )
                elif item["type"] == "animation":
                    await context.bot.send_animation(
                        chat_id=chat_id, animation=f,
                        caption=f"📎 {basename}",
                    )
                elif item["type"] == "video":
                    await context.bot.send_video(
                        chat_id=chat_id, video=f,
                        caption=f"📎 {basename}",
                    )
                elif item["type"] == "document":
                    await context.bot.send_document(
                        chat_id=chat_id, document=f,
                        caption=f"📎 {basename}",
                    )
            logger.info(f"Sent {item['type']} to TG: {basename}")
        except Exception as e:
            logger.warning(f"Failed to send media {fpath}: {e}")


# Bot project directory (for self-awareness)
BOT_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

# User home directory (default working directory for commands)
USER_HOME = os.path.expanduser("~")

# ─── System Prompt → saved to file, loaded via --append-system-prompt-file ──
# On Windows, --append-system-prompt with long strings breaks because .cmd
# files go through cmd.exe which corrupts special chars. Instead, write to
# a file and use --append-system-prompt-file which reads from disk.

_SYSTEM_PROMPT = f"""You are a Telegram bot on Windows 11. User controls you from phone.
Working directory: {BOT_PROJECT_DIR}
You have Bash, Read, Write, Edit tools with FULL computer access.

## RULES (ABSOLUTE — VIOLATION = FAILURE)
1. NEVER ask questions. NEVER. No exceptions. Forbidden patterns include:
   - "shall I?", "do you want?", "which option?", "could you provide/clarify/specify"
   - "请提供", "请问", "你能否", "是否需要", "要不要"
   - "appears to be cut off", "could you paste the rest", "can you share more"
   - "what do you mean", "did you mean", "which one", "please confirm"
   - ANY sentence ending with "?" directed at the user
   If the input seems incomplete, WORK WITH WHAT YOU HAVE. Interpret the intent and execute.
   If ambiguous, pick the MOST LIKELY interpretation and do it. NEVER ask.
2. Keep replies SHORT (user reads on phone). Reply in user's language (中文为主).
3. ALWAYS execute commands — never say "you could run..." — ACTUALLY RUN IT.
4. ALWAYS include a text reply after tool use. Never send empty or "(no output)".
5. Never close user's browser windows. Open new ones.
6. If something fails, try alternatives silently before reporting failure.
7. "继续"/"continue" → continue previous task from context.
8. You can see images (vision), search the web, control browser, mouse, keyboard, files — FULL computer.

## COMPUTER CONTROL (via Bash)
- Screenshot: `python pc_control.py screenshot`
- Click: `python pc_control.py click X Y` (add `--no-takeover` for sequences)
- Type: `python pc_control.py type "text"` | `smarttype "text"` (with verify)
- Keys: `python pc_control.py hotkey ctrl c` | `alt tab` | etc.
- Scroll: `python pc_control.py scroll -5` | Find: `findinput` | `findcolor R,G,B 30`
- Windows: `windowlist` | `focusedwindow`
- TG send: `python tg_direct.py send "msg"` | `tg_direct.py photo path "caption"`

## PRECISION CONTROL (most→least precise)
1. **Browser** → browser_click/browser_type (CSS selectors, no guessing)
2. **Desktop** → ui_tree + ui_click_element (accessibility tree, by name/ID)
3. **Any UI** → som_screenshot + som_click #N (numbered annotation)
4. **Fallback** → smartclick with coordinates

## PROJECTS
- "crypto"/"okx" → C:/Users/alexl/Desktop/crypto-analysis-/
- "bot"/"tg bot" → {BOT_PROJECT_DIR}
- "pet cad" → C:/Users/alexl/Desktop/pet_cad_v3/
- "六福" → C:/Users/alexl/Desktop/六福营销/

## MEMORY
Markdown: {BOT_PROJECT_DIR}/.bot_memory.md (append facts/profile updates)
JSON memory engine: {BOT_PROJECT_DIR}/.bot_memory.json (auto-managed, use memory_engine.py API)
Top shortcuts/patterns injected into prompt under "学习记忆" section.

## SELF-UPDATE / SELF-FIX
Modify own code at {BOT_PROJECT_DIR}, then restart:
`powershell -Command "Start-Sleep 2; Start-Process python -ArgumentList 'run.py' -WorkingDirectory '{BOT_PROJECT_DIR}'"` then stop old PID.

### 修复自己的bug步骤：
1. 扫描所有py文件语法: `cd "{BOT_PROJECT_DIR}" && for f in *.py; do python -m py_compile "$f" 2>&1 | grep -v "^$"; done`
2. 读取出错文件，找到bug，用Edit工具修复
3. 再次py_compile验证
4. 如果修改了bot.py、run.py等核心文件，热重载会自动生效（watchdog监控）

### 发送消息到Claude Code session：
用claude CLI的 `--resume` 发消息到已有session：
```bash
claude --resume SESSION_ID -p "你的消息" --output-format text --dangerously-skip-permissions
```
查找session ID: `ls ~/.claude/projects/*/sessions/*.jsonl | head -5`
或直接新开session: `claude -p "任务指令" --output-format text --dangerously-skip-permissions`

### 自我进化（复利闭环）：
运行 `python "{BOT_PROJECT_DIR}/smart_evolver.py"` 会自动循环执行7个进化任务。
进化队列在后台自动运行（evolve_watcher.py监控）。

### 自我学习闭环（核心）：
写代码 → 自动py_compile扫描 → 发现bug → 自动修复 → 提取修复skill → 下次不犯同样错误
每次完成任务后，系统会自动：
1. 扫描所有.py文件语法
2. 发现错误自动修复
3. 把修复经验保存为skill（复利：知识生成知识）
4. 评分低于0.6时自动触发训练
你可以主动调用 self_fix 工具扫描修复bug，也可以让系统自动做。

### Codex（Claude.ai/code 浏览器自动化）：
当CLI耗尽credits时，可以通过浏览器操控 claude.ai/code 继续工作：
1. 检查状态: `python codex_charger.py status`
2. 切换到Codex模式: `python codex_charger.py mode=codex`
3. 运行任务: `python -c "from codex_charger import CodexCharger; c=CodexCharger(); print(c.run_task_sync('你的任务'))"`
4. 前提：Chrome必须已登录 claude.ai，且开启CDP（Remote Debugging）
   启动Chrome调试: `Start-Process chrome -ArgumentList '--remote-debugging-port=9222'`

### Never-Die充能链（按顺序尝试）：
CLI订阅 → Codex浏览器 → 免费Web AI（ChatGPT/Grok）→ 缓存/模板回复

## TRADING CAPABILITIES (用户直接说中文即可)
你是一个具有完整交易能力的智能体。用户可能用自然语言给你交易指令：

### 快捷指令（已内置，秒级响应）：
- "价格 BTC" / "BTC多少钱" → 自动查 OKX 实时行情
- "大盘" / "行情" / "市场" → BTC/ETH/SOL 概览
- "扫描" / "alpha" → 执行 alpha_engine 信号扫描
- "持仓" / "仓位" → 显示当前持仓
- "开始交易" → 启动实盘交易引擎
- "停止" → 停止交易
- "交易" / "面板" → 打开 Trading Dashboard

### 高级交易操作（需要你执行命令）：
- 查看 OKX 行情详细: `python -c "import httpx; r=httpx.get('https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT-SWAP'); print(r.json())"`
- 查看实盘状态: `python -c "import live_trader; print(live_trader.format_live_status())"`
- 管理钱包: `python -c "import secure_wallet; print(secure_wallet.get_public_key())"`
- Alpha 扫描: `python -c "import asyncio; from alpha_engine import scan_alpha; print(asyncio.run(scan_alpha()))"`
- 技能库查看: `python -c "import skill_library; print(skill_library.search_skills('策略'))"`

### 自主开发流水线（/dev 命令）：
用户说"写一个xxx策略"或"开发一个xxx功能"时，自动调用:
`python -c "import asyncio; from pipeline.auto_dev_orchestrator import AutoDevOrchestrator; r=asyncio.run(AutoDevOrchestrator().run(task_goal='任务', target_rel_path='skills/xxx.py')); print(r)"`

### 后台运行中的自动化系统：
- InfiniteEvolver: 每30分钟自动生成→回测→晋升策略
- ProactiveAgent: 健康巡逻
- MarketMonitor: 行情异动推送
- MetaLearner: 每日模式分析

## SELF-HEALING
Click miss → ui_click_element → som_click → smartclick
Type fail → ui_type_element → smarttype (clipboard fallback)
Element missing → ui_tree or som_screenshot to find it
Never report failure until 2+ approaches tried.

{vital_signs.ALIVE_PROMPT}
"""

# Write system prompt to file (read by HTTP LLM layer via llm_http_client)
# ALWAYS overwrite — prompt may have been updated in code
_PROMPT_FILE = Path(BOT_PROJECT_DIR) / ".system_prompt.txt"
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


def _cli_hard_timeout() -> float:
    """Wall-clock budget per CLI invoke; default 45s (non-negotiable upper cap)."""
    try:
        v = float(getattr(config, "CLAUDE_CLI_ASYNC_TIMEOUT_SEC", 45.0))
    except (TypeError, ValueError):
        v = 45.0
    return max(5.0, min(45.0, v))


_SPINNER_LINE_RE = re.compile(
    r"^\s*([⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏█░▒▓╔╚║═─┃┏┓┗┛\[\]]+\s*)$"
)


def _strip_cli_progress_noise(text: str) -> str:
    lines_out: list[str] = []
    for line in (text or "").splitlines():
        s = line.strip()
        if not s:
            continue
        if _SPINNER_LINE_RE.match(line):
            continue
        if re.match(r"^Reading\s+.+\.{3}\s*$", s, re.I):
            continue
        lines_out.append(line)
    return "\n".join(lines_out).strip()


def _flatten_claude_json_message(obj: Any) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj.strip()
    if isinstance(obj, dict):
        if "content" in obj:
            inner = obj["content"]
            if isinstance(inner, list):
                parts: list[str] = []
                for block in inner:
                    if isinstance(block, dict):
                        if block.get("type") == "text":
                            parts.append(str(block.get("text", "")))
                        elif "text" in block:
                            parts.append(str(block["text"]))
                    elif isinstance(block, str):
                        parts.append(block)
                if parts:
                    return "\n".join(parts).strip()
            if isinstance(inner, str):
                return inner.strip()
        for k in ("result", "text", "output", "answer"):
            if k in obj and isinstance(obj[k], str) and obj[k].strip():
                return str(obj[k]).strip()
        for k in ("result", "message"):
            if k in obj:
                sub = _flatten_claude_json_message(obj[k])
                if sub:
                    return sub
    if isinstance(obj, list):
        acc = [_flatten_claude_json_message(x) for x in obj]
        return "\n".join(p for p in acc if p).strip()
    return ""


def _parse_claude_cli_stdout(data: bytes) -> tuple[str, str | None]:
    """Parse ``claude --output-format json`` stdout → (assistant_text, session_id_for_resume)."""
    if not data:
        return "", None
    raw = data.decode("utf-8", errors="replace").strip()
    if not raw:
        return "", None
    session_hint: str | None = None
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            sid = obj.get("session_id") or obj.get("sessionId") or obj.get("uuid")
            if isinstance(sid, str) and sid.strip():
                session_hint = sid.strip()
            text = _flatten_claude_json_message(obj)
            if text:
                return text, session_hint
    except (json.JSONDecodeError, TypeError, ValueError):
        pass
    return _strip_cli_progress_noise(raw), session_hint


def _normalize_cli_resume_id(stored: str | None) -> str | None:
    if not stored:
        return None
    s = str(stored).strip()
    if s.startswith("http:"):
        return None
    if s.startswith("cli:"):
        s = s[4:].strip()
    if len(s) < 8:
        return None
    return s


async def _kill_cli_subprocess(proc: asyncio.subprocess.Process | None) -> None:
    if proc is None:
        return
    try:
        from pipeline.cli_bridge import _kill_process_tree

        await _kill_process_tree(proc)
    except Exception as e:
        logger.debug("kill cli subprocess: %s", e)
        try:
            proc.kill()
            await asyncio.wait_for(proc.wait(), timeout=8.0)
        except Exception:
            pass


async def async_claude_code_prompt(
    prompt: str,
    *,
    cwd: str | None = None,
    resume: str | None = None,
    timeout_sec: float | None = None,
) -> tuple[str, str, str | None]:
    """
    One local ``claude -p`` round-trip under ``CLI_SEMAPHORE``.
    stdin=DEVNULL (non-interactive); stdout/stderr PIPE; ``wait_for`` + kill on timeout.
    Returns ``(text, stderr_tail, session_id)``.
    """
    from pipeline.cli_bridge import find_claude_executable

    tlim = float(timeout_sec) if timeout_sec is not None else _cli_hard_timeout()
    tlim = max(5.0, min(45.0, tlim))

    exe = find_claude_executable()
    if not Path(exe).is_file():
        return "", f"Claude CLI not found: {exe}", None

    body = (prompt or "").strip()
    if not body:
        return "", "empty prompt", None

    max_inline = 24000
    tmp_path: str | None = None
    try:
        if len(body) > max_inline:
            import tempfile

            fd, tmp_path = tempfile.mkstemp(
                suffix=".txt", prefix="ca_cli_", text=True, dir=BOT_PROJECT_DIR
            )
            os.close(fd)
            Path(tmp_path).write_text(body, encoding="utf-8")
            body = (
                "Read this UTF-8 file and complete the task. Output via normal CLI JSON channel. "
                f"File: {tmp_path}"
            )

        args: list[str] = [
            exe,
            "-p",
            body,
            "--output-format",
            "json",
            "--dangerously-skip-permissions",
        ]
        r = _normalize_cli_resume_id(resume)
        if r:
            args.extend(["--resume", r])

        wd = cwd or BOT_PROJECT_DIR
        proc: asyncio.subprocess.Process | None = None
        async with CLI_SEMAPHORE:
            try:
                proc = await asyncio.create_subprocess_exec(
                    *args,
                    stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=str(wd),
                    limit=2**20,
                )
            except (OSError, NotImplementedError) as e:
                return "", str(e)[:800], None

            try:
                assert proc.stdout is not None and proc.stderr is not None
                out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=tlim)
            except asyncio.TimeoutError:
                logger.warning(
                    "async_claude_code_prompt: timeout %.1fs, killing pid=%s",
                    tlim,
                    getattr(proc, "pid", None),
                )
                await _kill_cli_subprocess(proc)
                return "", f"timeout after {tlim}s", None

        err_tail = (err_b or b"").decode("utf-8", errors="replace")[-4000:]
        text, sid = _parse_claude_cli_stdout(out_b or b"")
        if not text and err_tail.strip():
            text = _strip_cli_progress_noise(err_tail)
        return text, err_tail, sid
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


async def reask_trade_json_via_cli(round_idx: int, previous_output: str) -> str:
    """Trade JSON repair via local Claude CLI (serialized; same semaphore as main bot)."""
    try:
        from dispatcher.llm_filter import TRADE_JSON_REMINDERS

        reminder = TRADE_JSON_REMINDERS[round_idx % len(TRADE_JSON_REMINDERS)]
    except Exception:
        reminder = "Output one JSON object only."
    block = (
        f"{reminder}\n\n"
        "先前输出无法通过校验。只输出一个 JSON 对象，键: action, pair, amount, price。\n\n"
        f"{str(previous_output)[:3800]}"
    )
    text, err, _ = await async_claude_code_prompt(
        "Reply with a single JSON object only. Keys: action, pair, amount, price. No markdown.\n\n"
        + block,
        timeout_sec=_cli_hard_timeout(),
    )
    return (text or err or "").strip()


# Ensure screenshot forwarding directory exists and is clean on startup
os.makedirs(_TG_SCREENSHOT_DIR, exist_ok=True)
for _f in os.listdir(_TG_SCREENSHOT_DIR):
    try:
        os.remove(os.path.join(_TG_SCREENSHOT_DIR, _f))
    except Exception:
        pass

# ─── Session Persistence ─────────────────────────────────────────────────────

_SESSION_FILE = Path(__file__).parent / ".sessions.json"

_SESSION_TTL = 3600 * 4  # 4 hours — sessions older than this are stale

def _load_sessions() -> tuple[dict[int, str], dict[int, float]]:
    """Load session IDs and their timestamps from disk, pruning stale entries."""
    try:
        if _SESSION_FILE.exists():
            data = json.loads(_SESSION_FILE.read_text(encoding="utf-8"))
            now = time.time()
            sessions = {}
            timestamps = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    # New format: {"id": "...", "ts": 1234567890}
                    ts = v.get("ts", 0)
                    if ts > now - _SESSION_TTL:
                        chat_id = int(k)
                        sessions[chat_id] = v["id"]
                        timestamps[chat_id] = ts
                elif v:
                    # Old format: just the session ID string — assign current time
                    chat_id = int(k)
                    sessions[chat_id] = v
                    timestamps[chat_id] = now
            return sessions, timestamps
    except Exception as e:
        logger.warning(f"Failed to load sessions: {e}")
    return {}, {}

def _save_sessions():
    """Persist session IDs to disk with timestamps (atomic write).
    Only updates the timestamp for sessions that were actually touched."""
    try:
        now = time.time()
        save_data = {
            str(k): {"id": v, "ts": _session_timestamps.get(k, now)}
            for k, v in _claude_sessions.items()
        }
        temp_file = _SESSION_FILE.with_suffix(".tmp")
        with open(str(temp_file), "w", encoding="utf-8") as f:
            json.dump(save_data, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(str(temp_file), str(_SESSION_FILE))
    except Exception as e:
        logger.warning(f"Failed to save sessions: {e}")

# ─── Session & Queue State ──────────────────────────────────────────────────

_claude_sessions, _session_timestamps = _load_sessions()


def _set_session(chat_id: int, session_id: str):
    """Store session ID and update its timestamp."""
    _claude_sessions[chat_id] = session_id
    _session_timestamps[chat_id] = time.time()


_pending_messages: dict[int, list[dict]] = {}
_processing_locks: dict[int, asyncio.Lock] = {}
_MAX_PENDING_AGE = 600  # 10 minutes

# ─── Task Tracker ─────────────────────────────────────────────────────────────
import itertools as _itertools
_task_id_counter = _itertools.count(1)
_running_tasks: dict[int, dict] = {}   # chat_id → {task_id, text, start_time}
_chat_workers: dict[int, asyncio.Task] = {}  # active queue workers per chat


def _next_task_id() -> int:
    return next(_task_id_counter)


def get_task_status() -> dict:
    """Return snapshot of running, queued, and worker tasks across all chats."""
    running = dict(_running_tasks)
    queued = {cid: list(msgs) for cid, msgs in _pending_messages.items() if msgs}
    workers = {cid: not t.done() for cid, t in _chat_workers.items() if not t.done()}
    concurrent_slots = llm_http_client.concurrency_snapshot()
    return {"running": running, "queued": queued, "workers": workers, "concurrent": concurrent_slots}


def cancel_queued_task(chat_id: int, task_id: int | None = None) -> int:
    """Cancel queued task(s) for a chat. Returns number of tasks removed.

    If task_id is None, clears all queued tasks for the chat.
    """
    queue = _pending_messages.get(chat_id, [])
    if not queue:
        return 0
    if task_id is None:
        count = len(queue)
        queue.clear()
        return count
    original = len(queue)
    queue[:] = [m for m in queue if m.get("task_id") != task_id]
    return original - len(queue)

# Periodic session/lock cleanup to prevent unbounded dict growth over weeks
_last_state_cleanup: float = 0.0
_STATE_CLEANUP_INTERVAL = 3600  # 1 hour


def _periodic_state_cleanup():
    """Prune stale entries from in-memory dicts to prevent unbounded growth."""
    global _last_state_cleanup
    now = time.time()
    if now - _last_state_cleanup < _STATE_CLEANUP_INTERVAL:
        return
    _last_state_cleanup = now

    # Prune sessions older than TTL
    stale_chats = [
        cid for cid, ts in _session_timestamps.items()
        if now - ts > _SESSION_TTL
    ]
    for cid in stale_chats:
        _claude_sessions.pop(cid, None)
        _session_timestamps.pop(cid, None)

    # Prune locks for chats with no session and no pending messages
    stale_locks = []
    for cid in list(_processing_locks.keys()):
        lock = _processing_locks.get(cid)
        if lock and cid not in _claude_sessions and cid not in _pending_messages and not lock.locked():
            stale_locks.append(cid)
    for cid in stale_locks:
        _processing_locks.pop(cid, None)

    # Prune empty pending message queues
    empty_queues = [cid for cid, msgs in _pending_messages.items() if not msgs]
    for cid in empty_queues:
        _pending_messages.pop(cid, None)

    # Prune completed chat workers
    done_workers = [cid for cid, t in _chat_workers.items() if t.done()]
    for cid in done_workers:
        _chat_workers.pop(cid, None)

    # Prune stale running tasks (stuck for >30min)
    stale_tasks = [cid for cid, info in _running_tasks.items()
                   if now - info.get("start_time", 0) > 1800]
    for cid in stale_tasks:
        _running_tasks.pop(cid, None)

    if stale_chats or stale_locks:
        logger.info(
            f"State cleanup: pruned {len(stale_chats)} stale sessions, "
            f"{len(stale_locks)} orphan locks"
        )
        _save_sessions()


# Safe to read/write without locks: asyncio is cooperative (single-threaded),
# so no concurrent mutation between await points.
_rate_limited_until: float = 0.0
_rate_limit_resume_task: asyncio.Task | None = None
_rate_limit_consecutive: int = 0  # For exponential backoff: 0→60s, 1→120s, 2→300s

# HTTP LLM concurrency is limited inside llm_http_client (aiohttp, LLM_HTTP_MAX_CONCURRENT).
_RATE_LIMIT_BACKOFF = [60, 120, 300]  # Exponential backoff schedule (seconds)

def is_rate_limited() -> bool:
    """Check if Claude CLI is currently rate limited."""
    return time.time() < _rate_limited_until

def _get_rate_limit_cooldown(parsed_seconds: int | None = None) -> int:
    """Get cooldown using exponential backoff: 60s → 120s → 300s on consecutive hits."""
    global _rate_limit_consecutive
    if parsed_seconds is not None and parsed_seconds > 0:
        # Use server-provided value if available and reasonable
        return min(parsed_seconds, 600)
    idx = min(_rate_limit_consecutive, len(_RATE_LIMIT_BACKOFF) - 1)
    return _RATE_LIMIT_BACKOFF[idx]


def _schedule_rate_limit_resume(cooldown_seconds: float):
    """Schedule auto-resume of pending work when rate limit resets."""
    global _rate_limit_resume_task

    async def _wait_and_resume():
        await asyncio.sleep(cooldown_seconds + 2)  # Wait for limit to reset + buffer
        logger.info("Rate limit expired — bot is ready for new requests.")
        # If autonomy engine has pending goals, resume them
        try:
            from agents.autonomy import get_autonomy_engine
            engine = get_autonomy_engine()
            if engine.get_active_goals() and not engine._running:
                engine.start(interval=15.0)
                logger.info("Autonomy engine auto-resumed after rate limit reset.")
        except Exception as e:
            logger.debug(f"Autonomy engine resume failed: {e}")

    # Cancel previous resume task if any
    if _rate_limit_resume_task and not _rate_limit_resume_task.done():
        _rate_limit_resume_task.cancel()

    try:
        _rate_limit_resume_task = asyncio.create_task(_wait_and_resume())
    except RuntimeError:
        pass  # No event loop — skip


def _get_lock(chat_id: int) -> asyncio.Lock:
    return _processing_locks.setdefault(chat_id, asyncio.Lock())


# ─── Adaptive Model Selection ────────────────────────────────────────────────

# Short/simple messages → Sonnet (fast, 5-10s)
# Complex coding/debugging → Opus (slow but smart, 30-90s)
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


_HAIKU_PATTERNS = [
    r"^(hi|hello|hey|ok|好|嗯|行|哦|ping|test|你好)$",
    r"^(status|状态|帮助|help|谢谢|thanks|/\w+)$",
]
_HAIKU_RE = re.compile("|".join(_HAIKU_PATTERNS), re.IGNORECASE)


def _pick_model(message: str) -> str:
    """Adaptive model: trivial messages → Haiku (cheapest), else → configured default."""
    msg = message.strip().lower()
    for pattern in _HAIKU_PATTERNS:
        if re.match(pattern, msg):
            return "claude-haiku-4-5-20251001"
    return config.CLAUDE_MODEL


# ─── Pipeline Routing ────────────────────────────────────────────────────────

_PIPELINE_PATTERNS = re.compile(
    r"(写一个|创建|build|create|implement|实现|开发|develop|重构|refactor|"
    r"修复.*所有|fix all|review.*code|代码审查|部署|deploy|"
    r"分析.*并.*修|analyze.*and.*fix|全面|comprehensive|"
    r"整个项目|whole project|entire|从头|from scratch|"
    r"多步|multi.?step|plan.*and|先.*然后|step by step)",
    re.IGNORECASE,
)


def _try_session_route(user_message: str, chat_id: int) -> str | None:
    """Check if message should be routed to a named SessionManager session.

    Returns the session name if a match is found, None otherwise.
    Only routes if:
    - SessionManager is available and has sessions
    - The message mentions a known project keyword
    - There's no active CLI session for this chat (would lose context)
    """
    try:
        if not _session_mgr or not hasattr(_session_mgr, 'sessions') or not _session_mgr.sessions:
            return None
        if chat_id in _claude_sessions:
            return None

        msg_lower = user_message.lower()
        for name, session in _session_mgr.sessions.items():
            # Match by session name or project directory name
            keywords = [name.lower()]
            proj_dir = getattr(session, "project_dir", "")
            if proj_dir:
                dir_name = os.path.basename(proj_dir.rstrip("/\\")).lower()
                if dir_name:
                    keywords.append(dir_name)
                    for part in dir_name.split("-"):
                        if len(part) >= 4:
                            keywords.append(part)

            if any(kw in msg_lower for kw in keywords if len(kw) >= 3):
                if not getattr(session, "busy", False):
                    return name
        return None
    except Exception as e:
        logger.debug(f"Session routing error: {e}")
        return None


def _should_use_pipeline(user_message: str, chat_id: int) -> bool:
    """Decide if a message should use the multi-agent pipeline vs direct CLI.

    Pipeline is better for: complex multi-step tasks, project-wide changes, build+test flows.
    CLI is better for: quick commands, simple questions, interactive GUI control, resumed sessions.
    """
    # Never use pipeline for resumed sessions (would lose context)
    if chat_id in _claude_sessions:
        return False

    # Short messages are never complex enough for pipeline
    if len(user_message) < 20:
        return False

    # Check if message matches complex task patterns
    if _PIPELINE_PATTERNS.search(user_message):
        return True

    # Very long messages (>500 chars) with project keywords suggest complex tasks
    if len(user_message) > 500 and any(kw in user_message.lower() for kw in
                                        ["bug", "fix", "code", "file", "project", "test"]):
        return True

    return False


# ─── Typing Indicator ────────────────────────────────────────────────────────

async def _keep_typing(chat_id, context, stop_event):
    """Send typing indicator every 4 seconds while processing.

    After 20 seconds, sends a brief progress message so user knows bot is still working.
    After 60 seconds, sends another reminder for very long tasks.
    """
    _elapsed = 0
    _notified_20s = False
    _notified_60s = False
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(
                context.bot.send_chat_action(chat_id=chat_id, action="typing"),
                timeout=3,
            )
        except Exception:
            pass

        # Progress notifications for long-running tasks
        if not _notified_20s and _elapsed >= 20:
            _notified_20s = True
            try:
                await context.bot.send_message(
                    chat_id=chat_id, text="⏳ 正在处理，请稍候..."
                )
            except Exception:
                pass
        if not _notified_60s and _elapsed >= 60:
            _notified_60s = True
            try:
                await context.bot.send_message(
                    chat_id=chat_id, text="🔄 任务较复杂，仍在执行中..."
                )
            except Exception:
                pass

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=4)
            break
        except asyncio.TimeoutError:
            _elapsed += 4


# ─── Knowledge Gap (non-blocking) ────────────────────────────────────────────

async def _safe_knowledge_gap(user_message: str):
    """Fire-and-forget: detect knowledge gaps and learn in background."""
    try:
        await auto_research.detect_and_fill_knowledge_gap(user_message)
    except Exception as e:
        logger.debug(f"Knowledge gap detection error: {e}")


_background_tasks: set = set()  # prevent GC of fire-and-forget tasks
_MAX_BACKGROUND_TASKS = 500

def _fire_and_forget(coro, name: str = "background"):
    """Create a background task that logs exceptions instead of silently losing them."""
    # Prune completed tasks to prevent unbounded growth
    completed = [t for t in _background_tasks if t.done()]
    for t in completed:
        _background_tasks.discard(t)
    if len(_background_tasks) >= _MAX_BACKGROUND_TASKS:
        logger.warning("Background task limit reached, dropping task")
        return

    async def _wrapper():
        try:
            await coro
        except Exception as e:
            logger.warning(f"Background task '{name}' error: {e}")
    task = asyncio.create_task(_wrapper())
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)
    return task


def _ensure_queue_worker(chat_id: int, context) -> None:
    """Start a background queue worker for this chat if one isn't already running."""
    existing = _chat_workers.get(chat_id)
    if existing and not existing.done():
        return  # Worker already active
    task = _fire_and_forget(
        _run_queued_tasks(chat_id, context),
        name=f"qworker-{chat_id}",
    )
    _chat_workers[chat_id] = task


async def _run_queued_tasks(chat_id: int, context) -> None:
    """Background worker: acquires per-chat lock then processes all queued messages individually."""
    lock = _get_lock(chat_id)
    try:
        async with lock:
            while True:
                all_pending = _drain_pending(chat_id)
                if not all_pending:
                    break
                # Process first message; re-queue the rest at front
                msg = all_pending[0]
                for m in reversed(all_pending[1:]):
                    _pending_messages.setdefault(chat_id, []).insert(0, m)
                tid = msg.get("task_id", "?")
                text = msg.get("text", "")
                remaining = len(_pending_messages.get(chat_id, []))
                queue_note = f" (+{remaining}条)" if remaining else ""
                logger.info(f"Chat {chat_id}: worker processing #{tid}{queue_note}")
                _running_tasks[chat_id] = {"task_id": tid, "text": text[:100], "start_time": time.time()}
                await _send_response(chat_id, f"📨 排队任务 #{tid}{queue_note}...", context)
                try:
                    success = await _process_with_llm(text, chat_id, context)
                    if not success:
                        web_ok = await _fallback_to_web_ai(text, chat_id, context)
                        if not web_ok:
                            await _fallback_cached_or_template(text, chat_id, context)
                except asyncio.TimeoutError:
                    await _send_response(chat_id, "⏰ 排队任务超时(5min)。发新消息继续。", context)
                    break
                except Exception as e:
                    logger.error(f"Worker queued task error: {e}", exc_info=True)
                    await _send_response(chat_id, f"⚠️ 排队任务出错: {str(e)[:200]}", context)
                    break
                finally:
                    _running_tasks.pop(chat_id, None)
    finally:
        _chat_workers.pop(chat_id, None)


# ─── Local Claude Code CLI (async subprocess + CLI_SEMAPHORE) ─

async def _run_llm_turn(
    user_message: str, chat_id: int, context,
    timeout: int = None,
) -> tuple[str, str | None, list, str]:
    """Primary turn: local ``claude -p`` via ``asyncio.create_subprocess_exec`` (never ``Popen``)."""
    global _rate_limited_until, _rate_limit_consecutive
    _ERROR_PATTERNS = {
        "credit": ["credit balance", "insufficient credit", "billing"],
        "auth": ["not logged in", "not authenticated", "auth failed", "login required", "invalid x-api-key",
                 "环境变量丢失", "环境变量", "api key 发给", "api_key", "请把你的"],
        "rate": ["hit your limit", "rate limit", "rate_limit", "quota exceeded", "usage limit", "too many requests", "429"],
    }
    if timeout is None:
        timeout = getattr(config, "CLAUDE_CLI_TIMEOUT", 300)
    _ = timeout  # legacy knob; wall-clock per invoke capped by _cli_hard_timeout()
    session_id = _claude_sessions.get(chat_id)

    # Adaptive model: simple queries → Sonnet (fast), complex → Opus
    model = _pick_model(user_message)

    # Inject memory + skills into system prompt file (not -p arg) to avoid Windows encoding issues
    import tempfile
    _mem_prompt_path = None
    _matched_skill_ids = []
    _tool_output_text = ""

    # Find matching skills (always, not just new sessions)
    matched_skills = skill_library.find_matching_skills(user_message, max_results=2)
    _matched_skill_ids = [s["id"] for s in matched_skills]

    if not session_id:
        mem_context = harness_learn.get_memory_context(max_chars=1500)
        workflow = harness_learn.get_relevant_workflow(user_message)
    else:
        mem_context = None
        workflow = None

    # Always inject user language profile (compact, ~200 chars)
    user_lang = harness_learn.get_user_language_summary(max_chars=300)

    skills_text = skill_library.format_skills_for_prompt(matched_skills)

    # Layer 5: Knowledge — sync local lookup only (no blocking API call)
    # Background gap detection fires as non-blocking task
    knowledge_text = auto_research.get_relevant_knowledge(user_message, max_chars=600)
    if not knowledge_text:
        _fire_and_forget(_safe_knowledge_gap(user_message), name="knowledge_gap")

    # Layer 6: RAG — retrieve similar past solutions
    rag_text = ""
    try:
        from agents.rag import get_solution_store
        solutions = get_solution_store().retrieve(user_message, top_k=2)
        if solutions:
            rag_text = get_solution_store().format_for_prompt(solutions, max_chars=400)
    except Exception as e:
        logger.debug(f"RAG solution retrieval failed: {e}")

    # Layer 7: Reflexion — inject recent insights to avoid past mistakes
    try:
        from agents.reflexion import get_reflexion_engine
        insights = get_reflexion_engine().get_all_insights(n=3)
        if insights:
            rag_text += "\n## 经验教训\n" + "\n".join(f"- {i}" for i in insights[-3:]) + "\n"
    except Exception as e:
        logger.debug(f"Reflexion engine failed: {e}")

    # Layer 8: Structured JSON memory — top shortcuts and high-success patterns
    mem_engine_context = ""
    try:
        mem_engine_context = _memory_engine.get_context_for_prompt(max_chars=400)
    except Exception as e:
        logger.debug(f"Memory engine context failed: {e}")

    try:
        if mem_context or workflow or skills_text or knowledge_text or user_lang or rag_text or mem_engine_context:
            mem_text = _PROMPT_FILE.read_text(encoding="utf-8")
            if user_lang:
                mem_text += f"\n\n## 用户画像\n{user_lang}\n"
            if mem_context:
                mem_text += f"\n## 历史记忆\n{mem_context}\n"
            if workflow:
                mem_text += f"\n## 参考模板\n{workflow['task_type']} → {', '.join(workflow['steps'][:5])}\n"
            if skills_text:
                mem_text += skills_text
            if knowledge_text:
                mem_text += f"\n## 领域知识\n{knowledge_text}\n"
            if rag_text:
                mem_text += f"\n{rag_text}\n"
            if mem_engine_context:
                mem_text += f"\n## 学习记忆\n{mem_engine_context}\n"
            _mem_tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", encoding="utf-8", delete=False, dir=BOT_PROJECT_DIR,
            )
            _mem_prompt_path = _mem_tmp.name
            try:
                _mem_tmp.write(mem_text)
            finally:
                _mem_tmp.close()
    except Exception as e:
        logger.warning(f"Failed to write temp prompt file: {e}")
        # Continue without enriched prompt — better than crashing

    prompt_file = _mem_prompt_path or str(_PROMPT_FILE)

    # For long messages (>8000 chars), write to a temp file to avoid Windows
    # command-line length limits (cmd.exe has ~32k limit, but encoding issues
    # can cause problems well before that)
    _msg_file_path = None
    if len(user_message) > 8000:
        try:
            _msg_file = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", encoding="utf-8", delete=False, dir=BOT_PROJECT_DIR,
            )
            _msg_file_path = _msg_file.name
            try:
                _msg_file.write(user_message)
            finally:
                _msg_file.close()
            # Use a short prompt that references the file
            cli_prompt = f"Read and respond to the user's message in this file: {_msg_file_path}"
        except OSError as e:
            logger.warning(f"Failed to write long message to file: {e}")
            # Truncate and pass directly as fallback
            cli_prompt = user_message[:8000] + "\n\n...(消息过长，已截断)"
    else:
        cli_prompt = user_message

    if session_id and str(session_id).startswith("http:"):
        await llm_http_client.clear_history(chat_id)
        session_id = None
        logger.info(f"Chat {chat_id}: dropped legacy HTTP session id")

    resume = _normalize_cli_resume_id(session_id)

    if resume:
        logger.info(f"Chat {chat_id}: Claude CLI turn (resume, model hint: {model})")
    else:
        logger.info(f"Chat {chat_id}: new Claude CLI session (model hint: {model})")

    stop_typing = asyncio.Event()
    typing_task = asyncio.create_task(_keep_typing(chat_id, context, stop_typing))
    stderr_data = b""
    new_session_id = None
    response = None

    try:
        from pathlib import Path as _Path

        if _msg_file_path:
            user_text = await asyncio.to_thread(
                _Path(_msg_file_path).read_text, encoding="utf-8"
            )
        else:
            user_text = cli_prompt

        system_text = await asyncio.to_thread(
            _Path(prompt_file).read_text, encoding="utf-8"
        )

        tlim = _cli_hard_timeout()
        combined = (
            f"(Model complexity hint: {model})\n\n"
            "=== PROJECT SYSTEM INSTRUCTIONS ===\n"
            f"{system_text[:180_000]}\n\n"
            "=== USER MESSAGE ===\n"
            f"{user_text[:200_000]}"
        )

        try:
            r_text, err_tail, sid_new = await async_claude_code_prompt(
                combined,
                cwd=BOT_PROJECT_DIR,
                resume=resume,
                timeout_sec=tlim,
            )
        except Exception as cli_e:
            logger.exception("Claude CLI call failed: %s", cli_e)
            r_text, err_tail, sid_new = "", str(cli_e)[:600], None

        stderr_data = (err_tail or "").encode("utf-8", errors="replace")
        _tool_output_text = err_tail or ""
        if err_tail and "timeout" in err_tail.lower():
            raise asyncio.TimeoutError()
        new_session_id = sid_new
        response = (r_text or "").strip()

        if not response:
            r2, err2, sid2 = await async_claude_code_prompt(
                "The last CLI turn produced no user-visible text. "
                "Summarize in 2-3 sentences in the user's language what went wrong or that output was empty.",
                cwd=BOT_PROJECT_DIR,
                resume=_normalize_cli_resume_id(sid_new) or resume,
                timeout_sec=tlim,
            )
            new_session_id = sid2 or new_session_id
            response = (r2 or "").strip()
            if err2:
                stderr_data = err2.encode("utf-8", errors="replace")
                _tool_output_text = err2 or _tool_output_text

    except asyncio.TimeoutError:
        logger.warning("Claude CLI timed out (wall %.1fs)", _cli_hard_timeout())
        raise

    finally:
        stop_typing.set()
        typing_task.cancel()
        try:
            await typing_task
        except asyncio.CancelledError:
            pass
        if _mem_prompt_path:
            try:
                os.unlink(_mem_prompt_path)
            except Exception:
                pass
        if _msg_file_path:
            try:
                os.unlink(_msg_file_path)
            except Exception:
                pass

    if response:
        resp_lower = response.lower()
        if any(p in resp_lower for p in _ERROR_PATTERNS["auth"]):
            logger.error("Claude CLI auth error: %s", response[:200])
            await llm_http_client.clear_history(chat_id)
            _claude_sessions.pop(chat_id, None)
            _session_timestamps.pop(chat_id, None)
            _save_sessions()
            response = "__AUTH_ERROR__"
            new_session_id = None
        elif len(response) < 300 and any(p in resp_lower for p in _ERROR_PATTERNS["credit"]):
            logger.error("Claude CLI credit error: %s", response[:200])
            response = "❌ LLM 额度或账单问题。请检查 API 密钥与账户。"
            new_session_id = None
        if (
            response
            and response != "__AUTH_ERROR__"
            and len(response) < 200
            and any(p in response.lower() for p in _ERROR_PATTERNS["rate"])
        ):
            logger.warning("Claude CLI rate limit in short response: %s", response[:200])
            cooldown = _get_rate_limit_cooldown()
            _rate_limit_consecutive += 1
            _rate_limited_until = time.time() + cooldown
            _schedule_rate_limit_resume(cooldown)
            response = f"⏳ LLM 限速中，{cooldown}s 后自动恢复。"
            new_session_id = None

    if stderr_data:
        err_text = stderr_data.decode("utf-8", errors="replace").strip()
        if err_text:
            err_lower = err_text.lower()
            logger.debug("HTTP LLM transport (chat %s): %s", chat_id, err_text[:500])
            if "auth failed" in err_lower or "not set" in err_lower or "401" in err_text or "403" in err_text:
                await llm_http_client.clear_history(chat_id)
                _claude_sessions.pop(chat_id, None)
                _session_timestamps.pop(chat_id, None)
                _save_sessions()
                response = "__AUTH_ERROR__"
                new_session_id = None
            elif "rate limit" in err_lower or "429" in err_text:
                cooldown = _get_rate_limit_cooldown()
                _rate_limit_consecutive += 1
                _rate_limited_until = time.time() + cooldown
                _schedule_rate_limit_resume(cooldown)
                response = f"⏳ 遇到限速，等待 {cooldown}s 后继续…"
                new_session_id = None
            elif any(p in err_lower for p in _ERROR_PATTERNS["credit"]):
                response = "❌ LLM 额度或账单问题。请检查 API 密钥与账户。"
                new_session_id = None

    if not response:
        err = stderr_data.decode("utf-8", errors="replace").strip() if stderr_data else ""
        if err:
            logger.error("Claude CLI error: %s", err[:500])
            response = f"⚠️ {err[:500]}"
        else:
            response = "✅ 已处理（无文本输出）。"

    return response, new_session_id, _matched_skill_ids, _tool_output_text


# ─── Response Sender ──────────────────────────────────────────────────────────

# Patterns that should NEVER appear in outgoing messages (security)
_DANGEROUS_OUTPUT_PATTERNS = [
    re.compile(r'sk-ant-[a-zA-Z0-9\-_]{20,}', re.IGNORECASE),  # Anthropic API keys
    re.compile(r'sk-[a-zA-Z0-9]{20,}'),  # OpenAI API keys
    re.compile(r'AIza[a-zA-Z0-9\-_]{30,}'),  # Google API keys
    re.compile(r'\b\d{9,10}:AA[A-Za-z0-9\-_]{30,}'),  # Telegram bot tokens
]
_SECRET_REQUEST_PATTERNS = re.compile(
    r'(api.?key|密钥|token|secret|password|credential|ANTHROPIC_API_KEY|OPENAI_API_KEY|环境变量丢失).{0,500}'
    r'(发给|send|provide|share|paste|输入|告诉|give|设置|重启|重新)',
    re.IGNORECASE | re.DOTALL,
)
# Secondary pattern: any message mentioning API key setup instructions
_API_KEY_INSTRUCTION_PATTERN = re.compile(
    r'(sk-ant-|ANTHROPIC_API_KEY|OPENAI_API_KEY|环境变量丢失|API Key 发给|把.*api.?key)',
    re.IGNORECASE,
)


def _sanitize_response(response: str) -> str:
    """Remove or redact sensitive content from outgoing messages."""
    # Redact actual API keys/tokens that might appear in output
    for pattern in _DANGEROUS_OUTPUT_PATTERNS:
        response = pattern.sub('[REDACTED]', response)

    # If Claude's response asks user to send API keys — replace with self-heal message
    if _SECRET_REQUEST_PATTERNS.search(response) or _API_KEY_INSTRUCTION_PATTERN.search(response):
        return (
            "⚠️ 检测到配置问题。Bot正在自动修复，无需手动操作。\n"
            "如果问题持续，请用 /status 查看状态。"
        )

    return response


def _filter_questions(text: str) -> str:
    """Strip question-asking sentences from bot responses (Rule #1: NEVER ask questions).
    If the entire response is a question, replace with a generic acknowledgment."""
    if not text:
        return text
    # Patterns that indicate the bot is asking the user for input
    question_patterns = [
        "could you paste", "could you provide", "could you share", "could you clarify",
        "can you paste", "can you provide", "can you share", "can you clarify",
        "appears to be cut off", "seems to be cut off", "seems incomplete",
        "could you send the rest", "paste the rest", "share the rest",
        "what do you mean", "did you mean", "what would you like",
        "please provide", "please clarify", "please share", "please paste",
        "请提供", "请问", "你能否", "是否需要", "能否提供", "请发送",
        "可以粘贴", "可以提供", "需要更多信息",
    ]
    lines = text.split("\n")
    filtered = []
    for line in lines:
        line_lower = line.lower().strip()
        if any(p in line_lower for p in question_patterns):
            continue  # Drop this line
        filtered.append(line)
    result = "\n".join(filtered).strip()
    # If we filtered everything away, provide a generic response
    if not result:
        result = "收到，正在处理..."
    return result


async def _send_response(chat_id: int, response: str, context):
    """Send response to Telegram, splitting into chunks if needed."""
    if not response or not response.strip():
        return

    # Security: sanitize outgoing messages
    response = _sanitize_response(response)

    # Rule #1 enforcement: filter out question-asking sentences
    response = _filter_questions(response)

    MAX_TOTAL = 16000
    if len(response) > MAX_TOTAL:
        response = response[:MAX_TOTAL] + "\n\n... (输出过长，已截断。需要完整内容请说。)"

    remaining = response
    _max_chunks = 20  # Safety limit to prevent infinite loop
    _chunk_count = 0
    while remaining and _chunk_count < _max_chunks:
        _chunk_count += 1
        if len(remaining) <= 4000:
            chunk = remaining
            remaining = ""
        else:
            break_pos = remaining.rfind("\n", 3000, 4000)
            if break_pos == -1:
                break_pos = 4000
            chunk = remaining[:break_pos]
            remaining = remaining[break_pos:].lstrip("\n")

        # Skip empty chunks to prevent sending empty messages / infinite loops
        if not chunk or not chunk.strip():
            continue

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
    fresh = [m for m in msgs if now - m.get("time", 0) < _MAX_PENDING_AGE]
    if len(fresh) < len(msgs):
        logger.info(f"Chat {chat_id}: dropped {len(msgs) - len(fresh)} stale queued messages")
    return fresh


_MAX_PENDING_QUEUE_SIZE = 20  # Max queued messages per chat to prevent memory bloat


def _queue_message(chat_id: int, text: str) -> int:
    """Queue a message; returns assigned task_id."""
    queue = _pending_messages.setdefault(chat_id, [])
    # Prune stale messages before adding
    now = time.time()
    queue[:] = [m for m in queue if now - m.get("time", 0) < _MAX_PENDING_AGE]
    if len(queue) >= _MAX_PENDING_QUEUE_SIZE:
        queue.pop(0)
        logger.warning(f"Chat {chat_id}: queue full ({_MAX_PENDING_QUEUE_SIZE}), dropped oldest")
    tid = _next_task_id()
    queue.append({
        "text": text,
        "time": now,
        "task_id": tid,
    })
    return tid


# ─── Self-Healing ────────────────────────────────────────────────────────────

_SELF_HEAL_DEPTH = 0

async def _self_heal(user_message: str, chat_id: int, context, error: Exception) -> bool:
    """Attempt auto-recovery from errors. Returns True if healed and retried successfully."""
    global _SELF_HEAL_DEPTH
    if _SELF_HEAL_DEPTH >= 2:
        logger.warning("Self-heal recursion limit reached, aborting")
        return False
    _SELF_HEAL_DEPTH += 1
    try:
        return await _self_heal_inner(user_message, chat_id, context, error)
    finally:
        _SELF_HEAL_DEPTH -= 1

async def _self_heal_inner(user_message: str, chat_id: int, context, error: Exception) -> bool:
    """Inner self-heal logic."""
    err_str = str(error).lower()
    tb = traceback.format_exc()
    logger.info(f"Self-heal attempting for: {err_str[:100]}")

    # ── Layer 1: Known patterns (fast, no API call) ──

    # Session corrupted → clear and retry
    if "session" in err_str or "resume" in err_str or "invalid" in err_str:
        logger.info("Self-heal: clearing corrupted session")
        await llm_http_client.clear_history(chat_id)
        _claude_sessions.pop(chat_id, None)
        _session_timestamps.pop(chat_id, None)
        _save_sessions()
        await _send_response(chat_id, "🔧 会话异常，已重置。重新处理...", context)
        try:
            response, sid, _, _ = await _run_llm_turn(user_message, chat_id, context)
            if sid:
                _set_session(chat_id, sid)
                _save_sessions()
            await _send_response(chat_id, response, context)
            return True
        except Exception as e2:
            logger.error(f"Self-heal retry failed: {e2}")
            return False

    # Encoding / input error → retry via Claude CLI (async subprocess)
    if "input must be provided" in err_str or "encoding" in err_str or "charmap" in err_str:
        logger.info("Self-heal: encoding issue, retrying via Claude CLI")
        await _send_response(chat_id, "🔧 编码问题，正在重试...", context)
        try:
            model = _pick_model(user_message)
            system_text = await asyncio.to_thread(_PROMPT_FILE.read_text, encoding="utf-8")
            combined = (
                f"(Model hint: {model})\n\n=== SYSTEM ===\n{system_text[:180_000]}\n\n"
                f"=== USER ===\n{user_message[:200_000]}"
            )
            r_text, err_tail, sid_new = await async_claude_code_prompt(
                combined,
                cwd=BOT_PROJECT_DIR,
                timeout_sec=_cli_hard_timeout(),
            )
            if err_tail:
                logger.debug("Self-heal CLI stderr tail: %s", err_tail[:400])
            if sid_new:
                _set_session(chat_id, sid_new)
                _save_sessions()
            out = (r_text or "").strip() or (err_tail or "⚠️ 重试失败")
            await _send_response(chat_id, out[:16000], context)
            return bool((r_text or "").strip())
        except Exception:
            return False

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

        diag_result = await _run_llm_raw(
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
                _session_timestamps.pop(chat_id, None)
                _save_sessions()

            await _send_response(
                chat_id,
                f"🔍 诊断: {diagnosis.get('diagnosis', '未知')}\n🔧 修复: {diagnosis.get('fix', '重试')}",
                context,
            )

            # Retry
            try:
                response, sid, _, _ = await _run_llm_turn(user_message, chat_id, context)
                if sid:
                    _set_session(chat_id, sid)
                    _save_sessions()
                await _send_response(chat_id, response, context)

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


_MAX_SELF_HEAL_LOG_SIZE = 1 * 1024 * 1024  # 1 MB max


def _log_self_heal(error: str, diagnosis: dict, success: bool):
    """Log self-healing attempt to memory. Auto-truncates when file exceeds 1 MB."""
    try:
        # Track in vital signs (lifecycle state machine)
        vital_signs.record_self_heal(success=success)
    except Exception:
        pass
    try:
        entry = {
            "timestamp": datetime.now().isoformat(),
            "error": error[:200],
            "diagnosis": diagnosis.get("diagnosis", "")[:200],
            "fix": diagnosis.get("fix", "")[:200],
            "success": success,
        }
        heal_log = os.path.join(BOT_PROJECT_DIR, ".self_heal.jsonl")
        # Truncate if file is too large
        try:
            if os.path.exists(heal_log) and os.path.getsize(heal_log) > _MAX_SELF_HEAL_LOG_SIZE:
                with open(heal_log, "r", encoding="utf-8") as rf:
                    lines = rf.readlines()
                tmp_path = heal_log + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as wf:
                    wf.writelines(lines[len(lines) // 2:])
                    wf.flush()
                    os.fsync(wf.fileno())
                os.replace(tmp_path, heal_log)
        except Exception:
            pass
        with open(heal_log, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ─── Main Processing Logic ────────────────────────────────────────────────────

async def _process_with_llm(user_message: str, chat_id: int, context) -> bool:
    """Process message via local Claude Code CLI (async subprocess). Returns True on success."""
    # Check rate limit before calling LLM — silently wait with exponential backoff
    if is_rate_limited():
        wait_s = max(1, int(_rate_limited_until - time.time()) + 1)
        logger.info(f"Chat {chat_id}: LLM rate limited, waiting {wait_s}s silently")
        await _send_response(chat_id, f"⏳ 等待{wait_s}s后继续...", context)
        await asyncio.sleep(min(wait_s, 300))
    try:
        _start_time = time.time()
        response, new_session_id, matched_skill_ids, tool_output_text = await _run_llm_turn(user_message, chat_id, context)

        # Session recovery: if response indicates session error, retry without resume
        _cli_retried = False
        resp_lower = response.lower() if response else ""
        _stale = response == "__STALE_SESSION__"
        if _stale or (response and (
            ("session" in resp_lower and "error" in resp_lower)
            or "invalid session" in resp_lower
            or ("could not find" in resp_lower and "session" in resp_lower)
            or "no conversation found" in resp_lower
        )):
            logger.warning(f"Chat {chat_id}: stale/invalid session, retrying fresh")
            _claude_sessions.pop(chat_id, None)
            _session_timestamps.pop(chat_id, None)
            _save_sessions()
            response, new_session_id, matched_skill_ids, tool_output_text = await _run_llm_turn(user_message, chat_id, context)
            _cli_retried = True

        # Auth error: session already cleared in _run_llm_turn, retry fresh immediately
        if response == "__AUTH_ERROR__" and not _cli_retried:
            logger.warning(f"Chat {chat_id}: CLI auth error, retrying fresh (no resume)")
            response, new_session_id, matched_skill_ids, tool_output_text = await _run_llm_turn(user_message, chat_id, context)
            if response == "__AUTH_ERROR__":
                logger.error(f"Chat {chat_id}: CLI auth error persists after retry")
                _self_monitor.record_service_failure("cli", "auth_error")
                await _send_response(
                    chat_id,
                    "⚠️ Claude Code 认证失败。请在本机终端运行 `claude` 登录，"
                    "并确认订阅有效（不依赖 .env 计费 API Key）。",
                    context,
                )
                return True  # We responded — do NOT trigger fallback chain

        # Detect raw auth/config errors that slipped through — safety net
        # Require STRONG signal: short response (<300 chars) + auth keyword
        # Long responses mentioning "api_key" are likely legitimate code discussions
        if response and len(response.strip()) < 300:
            _resp_lower = response.lower()
            _auth_strong = ["cli 未登录", "环境变量丢失", "not logged in", "auth failed",
                            "login required", "invalid x-api-key"]
            if any(p in _resp_lower for p in _auth_strong):
                logger.warning(f"Chat {chat_id}: CLI auth/config error detected, sending safe message")
                _self_monitor.record_service_failure("cli", "auth_error")
                await _send_response(
                    chat_id,
                    "⚠️ Claude Code 配置/登录异常。请检查本机 `claude` CLI 是否可用并已登录。",
                    context,
                )
                return True  # We responded — do NOT trigger fallback chain

        if new_session_id:
            _set_session(chat_id, new_session_id)
            _save_sessions()
            logger.info(f"Chat {chat_id}: session_id = {new_session_id[:12]}...")
        else:
            # Even without a new session_id, refresh timestamp to prevent
            # premature cleanup of actively-used sessions
            if chat_id in _session_timestamps:
                _session_timestamps[chat_id] = time.time()
            logger.debug(f"Chat {chat_id}: no session_id returned")

        await _send_response(chat_id, response, context)
        _wants_screenshot = any(k in user_message.lower() for k in
                                ["screenshot", "截图", "截屏", "屏幕", "看看屏幕", "screen"])
        await _forward_new_screenshots(chat_id, context, user_requested=_wants_screenshot)
        # Scan both the text response and raw tool outputs for media file paths (cap scan size)
        _media_scan_text = (response[:50_000] + "\n" + tool_output_text[:50_000])
        await _send_extracted_media(chat_id, context, _media_scan_text)

        # Record CLI success for service health tracking
        _self_monitor.record_service_success("cli")
        # Reset rate limit consecutive counter on success
        global _rate_limit_consecutive
        _rate_limit_consecutive = 0

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
            logger.info(f"Chat {chat_id}: score={score.get('overall', 0):.2f} flags={score.get('flags', [])}")

            # ── Vital Signs: record task for lifecycle tracking ──
            try:
                vital_signs.record_task(
                    success=score.get("overall", 0) >= 0.4,
                    duration_ms=_duration,
                )
            except Exception:
                pass

            # ── Skill Library: extract new skills + update reused ones ──
            _fire_and_forget(
                _skill_post_process(user_message, response, score, matched_skill_ids),
                name="skill_post_process",
            )

            # ── Self-Learning Loop: auto-compile check after code changes ──
            _fire_and_forget(
                _auto_compile_check(response, tool_output_text, chat_id, context),
                name="auto_compile_check",
            )

            # ── Auto-evolution check: should we train? ──
            train_decision = harness_learn.should_auto_train()
            if train_decision:
                logger.info(f"Auto-evolution triggered: {train_decision}")
                _fire_and_forget(
                    _auto_evolve(chat_id, context, train_decision),
                    name="auto_evolve",
                )
        except Exception as e:
            logger.debug(f"Harness scoring error: {e}")

        # Queued messages are handled by _run_queued_tasks worker (launched on queue)
        return True

    except asyncio.TimeoutError:
        _self_monitor.record_service_failure("cli", "timeout")
        # Self-heal: clear session and notify
        await llm_http_client.clear_history(chat_id)
        _claude_sessions.pop(chat_id, None)
        _session_timestamps.pop(chat_id, None)
        _save_sessions()
        await _send_response(
            chat_id,
            "⏰ 超时。已清除会话，正在尝试备用方案...",
            context,
        )
        return False  # Let fallback chain (API, web AI) handle it

    except Exception as e:
        _self_monitor.record_service_failure("cli", str(e)[:200])
        logger.error(f"HTTP LLM error: {e}", exc_info=True)
        # ── Self-Healing: attempt auto-recovery ──
        healed = await _self_heal(user_message, chat_id, context, e)
        if not healed:
            await _send_response(chat_id, f"⚠️ 出错且自修复失败: {str(e)[:300]}", context)
        return healed


async def _fallback_to_api_providers(
    user_message: str, chat_id: int, context, *, image_data: str | None = None
) -> bool:
    """HTTP API path: tier routing (Haiku/Mini vs heavy stack) + Claude→OpenAI→Gemini; then Codex CLI."""
    messages = [{"role": "user", "content": user_message}]
    try:
        from providers_router import execute_api_routed_turn

        if config.ANTHROPIC_API_KEY or config.OPENAI_API_KEY or config.GEMINI_API_KEY:
            logger.info("Chat %s: trying tier-routed API fallback", chat_id)
            if await execute_api_routed_turn(
                messages,
                chat_id,
                context,
                user_message_hint=user_message,
                image_data=image_data,
            ):
                _self_monitor.record_service_success("api_tiered")
                return True
            _self_monitor.record_service_failure("api_tiered", "all HTTP providers failed")
    except ImportError:
        logger.debug("execute_api_routed_turn unavailable")
    except Exception as e:
        logger.warning("Tier-routed API fallback error: %s", e)
        _self_monitor.record_service_failure("api_tiered", str(e)[:200])

    try:
        from providers_router import model_router

        if (
            "codex_cli" in model_router.available_providers()
            and model_router._stats["codex_cli"].is_healthy
        ):
            logger.info("Chat %s: trying Codex CLI fallback (local subscription)", chat_id)
            success = await model_router._call_provider(
                "codex_cli", user_message, chat_id, context
            )
            if success:
                model_router.record_success("codex_cli", 0)
                return True
            model_router.record_failure("codex_cli")
    except ImportError:
        logger.info("Chat %s: Codex CLI fallback unavailable", chat_id)
    return False


async def _fallback_to_web_ai(user_message: str, chat_id: int, context) -> bool:
    """Fallback: try free web AI when both CLI and API fail."""
    try:
        from web_ai import query_web_ai
        logger.info(f"Chat {chat_id}: API fallback failed, trying web AI directly")
        response, platform = await query_web_ai(user_message)
        if response:
            _self_monitor.record_service_success("webai")
            await _send_response(chat_id, f"[{platform}] {response}", context)
            return True
        _self_monitor.record_service_failure("webai", "empty response")
        return False
    except Exception as e:
        _self_monitor.record_service_failure("webai", str(e)[:200])
        logger.error(f"Web AI fallback failed: {e}")
        return False


async def _fallback_cached_or_template(user_message: str, chat_id: int, context) -> bool:
    """Last resort: try cached pattern commands, then send template response. NEVER silence."""
    try:
        from providers import _execute_cached_command
        cached_ok = await _execute_cached_command(user_message, chat_id, context)
        if cached_ok:
            return True
    except Exception:
        pass

    # Absolute last resort — template response
    retry_secs = getattr(config, "AUTO_RETRY_SECONDS", 60)
    await _send_response(
        chat_id,
        (
            f"All AI services are temporarily busy. I'll retry in {retry_secs}s.\n"
            "In the meantime, you can use:\n"
            "  /panel -- Quick actions\n"
            "  /screenshot -- Take screenshot\n"
            "  /status -- Check bot status\n"
            "  /clear -- Reset conversation"
        ),
        context,
    )
    return True  # We responded — bot stays alive


# ─── Multi-Intent NLP Layer ─────────────────────────────────────────────────────

# Per-chat conversation context: tracks tokens, projects, last analyzed entity
_chat_contexts: dict[int, dict] = {}
_CONTEXT_TTL = 3600 * 2  # 2 hours idle → reset context


def _cleanup_chat_contexts() -> None:
    """Remove stale entries when dict exceeds 100 entries. Hard cap at 500."""
    if len(_chat_contexts) <= 100:
        return
    now = time.time()
    stale = [cid for cid, ctx in _chat_contexts.items()
             if now - ctx.get("last_updated", 0) > _CONTEXT_TTL]
    for cid in stale:
        del _chat_contexts[cid]
    # Hard cap: if still too large, drop oldest
    if len(_chat_contexts) > 500:
        sorted_items = sorted(_chat_contexts.items(),
                              key=lambda x: x[1].get("last_updated", 0))
        for cid, _ in sorted_items[:len(_chat_contexts) - 200]:
            _chat_contexts.pop(cid, None)


def _get_chat_context(chat_id: int) -> dict:
    """Get or create conversation context for a chat."""
    _cleanup_chat_contexts()
    now = time.time()
    ctx = _chat_contexts.get(chat_id)
    if ctx is None or now - ctx.get("last_updated", 0) > _CONTEXT_TTL:
        ctx = {
            "tokens": [],           # [{address, symbol, ts}]
            "projects": [],         # [{name, ts}]
            "last_analyzed": None,  # Last analyzed entity (type, address/symbol)
            "last_updated": now,
        }
        _chat_contexts[chat_id] = ctx
    return ctx


def _update_chat_context(chat_id: int, message: str, response: str = ""):
    """Extract tokens/projects from message and response, update context."""
    ctx = _get_chat_context(chat_id)
    now = time.time()
    ctx["last_updated"] = now

    # Extract Solana token addresses (base58, 32–44 chars)
    token_addresses = re.findall(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b', message)
    for addr in token_addresses:
        if not any(t["address"] == addr for t in ctx["tokens"]):
            ctx["tokens"].insert(0, {"address": addr, "symbol": None, "ts": now})
        ctx["last_analyzed"] = {"type": "token", "address": addr, "symbol": None}

    # Extract token symbols like $BTC, #ETH, or "分析 SOL"
    sym_matches = re.findall(
        r'[$#]([A-Z]{2,10})\b|(?:分析|看|查|买|卖)\s+([A-Z]{2,10})\b',
        message.upper()
    )
    for groups in sym_matches:
        sym = next((s for s in groups if s), None)
        if sym and sym not in ('THE', 'AND', 'FOR', 'WITH', 'ARE', 'WAS'):
            if not any(t.get("symbol") == sym for t in ctx["tokens"]):
                ctx["tokens"].insert(0, {"address": None, "symbol": sym, "ts": now})
            if not token_addresses:
                ctx["last_analyzed"] = {"type": "token", "address": None, "symbol": sym}

    ctx["tokens"] = ctx["tokens"][:10]

    # Extract known project keywords
    proj_patterns = [
        ("crypto", r'crypto[\-\s]?analysis|crypto'),
        ("tg-bot", r'tg.?bot|bot项目'),
        ("pet-cad", r'pet.?cad'),
        ("六福", r'六福'),
    ]
    for proj_name, pat in proj_patterns:
        if re.search(pat, message, re.IGNORECASE):
            if not any(p["name"] == proj_name for p in ctx["projects"]):
                ctx["projects"].insert(0, {"name": proj_name, "ts": now})
    ctx["projects"] = ctx["projects"][:5]


# ── Intent Detection ────────────────────────────────────────────────────────────

_INTENT_SEP_RE = re.compile(
    r'\n\d+[.。、]\s*'                           # numbered list  1. xxx\n2. xxx
    r'|\n[•·－]\s*'                              # bullet lists
    r'|\b(?:然后(?:再)?|另外|同时|还(?:要|需要)|顺便|并且|以及'
    r'|and also|also please|plus|additionally|furthermore)\b',
    re.IGNORECASE,
)
_INTENT_ACTION_RE = re.compile(
    r'(?:分析|查|看|买|卖|转|发|截图|打开|搜索|计算|写|修复|测试|部署'
    r'|回测|扫描|检查|analyze|check|buy|sell|open|search|write|fix|test|scan)',
    re.IGNORECASE,
)


def _detect_intents(message: str) -> list[str]:
    """Split a message into multiple intent strings when separators are detected."""
    if len(message) < 15:
        return [message]

    # Numbered list (most reliable signal)
    numbered = re.split(r'\n\d+[.。、]\s*', message.strip())
    if len(numbered) > 1:
        intents = [s.strip() for s in numbered if s.strip() and len(s.strip()) > 3]
        if len(intents) >= 2:
            return intents

    # Separator words between action clauses
    parts = _INTENT_SEP_RE.split(message)
    parts = [p.strip() for p in parts if p.strip() and len(p.strip()) > 5]
    if len(parts) >= 2:
        action_parts = [p for p in parts if _INTENT_ACTION_RE.search(p)]
        if len(action_parts) >= 2:
            return action_parts

    return [message]


# ── Fuzzy Reference Resolution ──────────────────────────────────────────────────

_VAGUE_RE = re.compile(
    r'^(?:'
    r'(?:再|继续|还有|帮我看看|看看|分析一下|查一下)\s*(?:那个|这个|它)?(?:代币|token|合约|币)?\s*$'
    r'|(?:那个|这个|它)\s*(?:呢|怎么样|如何|的情况|分析)?\s*$'
    r'|(?:what about|how about|that one|the same)\s*(?:token|coin|contract)?\s*$'
    r')',
    re.IGNORECASE,
)
_FUZZY_PRONOUN_RE = re.compile(
    r'(?:那个|这个|刚才(?:说的)?|前面(?:说的)?|它|上次|the same|that|this one)',
    re.IGNORECASE,
)


def _resolve_fuzzy_message(message: str, chat_id: int) -> tuple[str, bool]:
    """
    Resolve vague references using context.
    Returns (resolved_message, was_resolved).
    """
    ctx = _get_chat_context(chat_id)
    last = ctx.get("last_analyzed")
    if not last:
        return message, False

    # Full vague message → replace entirely
    if _VAGUE_RE.match(message.strip()):
        ref = last.get("address") or last.get("symbol") or "该代币"
        resolved = f"分析代币 {ref}"
        logger.info(f"Chat {chat_id}: fuzzy resolved '{message}' → '{resolved}'")
        return resolved, True

    # Pronoun substitution within longer message
    if _FUZZY_PRONOUN_RE.search(message):
        ref = last.get("address") or last.get("symbol") or ""
        if ref:
            resolved = _FUZZY_PRONOUN_RE.sub(lambda m: ref, message, count=1)
            if resolved != message:
                logger.info(f"Chat {chat_id}: pronoun resolved '{message}' → '{resolved}'")
                return resolved, True

    return message, False


# ── Disambiguation ──────────────────────────────────────────────────────────────

_TRULY_AMBIGUOUS_RE = re.compile(
    r'^(?:看看|分析|查|check|analyze|看)\s*$',
    re.IGNORECASE,
)


def _check_disambiguation(message: str, chat_id: int) -> list[str] | None:
    """
    Return 2–3 possible interpretations if the message is truly ambiguous.
    Returns None if the message is clear enough to process directly.
    Only triggers for very short, action-only messages with multiple plausible targets.
    """
    ctx = _get_chat_context(chat_id)
    stripped = message.strip()

    if not _TRULY_AMBIGUOUS_RE.match(stripped):
        return None

    options = []
    if ctx.get("tokens"):
        tok = ctx["tokens"][0]
        ref = (tok.get("symbol") or ((tok.get("address") or "")[:8] + "...")) if tok else None
        if ref:
            options.append(f"分析代币 {ref}")

    if ctx.get("projects"):
        proj = ctx["projects"][0]["name"]
        options.append(f"查看{proj}项目状态")

    options.append("截图看当前屏幕")

    if len(options) >= 2:
        return options[:3]
    return None


# ── Proactive Suggestions ───────────────────────────────────────────────────────

_proactive_cooldown: dict[int, float] = {}
_PROACTIVE_COOLDOWN_SECS = 300  # 5 min between suggestions per chat


def _cleanup_proactive_cooldown() -> None:
    """Remove stale entries when dict exceeds 20 entries. Hard cap at 200."""
    if len(_proactive_cooldown) <= 20:
        return
    now = time.time()
    stale = [cid for cid, ts in _proactive_cooldown.items()
             if now - ts > _PROACTIVE_COOLDOWN_SECS * 2]
    for cid in stale:
        del _proactive_cooldown[cid]
    # Hard cap: if still too large after stale cleanup, drop oldest
    if len(_proactive_cooldown) > 200:
        sorted_items = sorted(_proactive_cooldown.items(), key=lambda x: x[1])
        for cid, _ in sorted_items[:len(_proactive_cooldown) - 100]:
            _proactive_cooldown.pop(cid, None)


def _get_proactive_suggestion(message: str, response: str, chat_id: int) -> str | None:
    """Generate a follow-up suggestion after certain analyses. Rate-limited."""
    _cleanup_proactive_cooldown()
    now = time.time()
    if now - _proactive_cooldown.get(chat_id, 0) < _PROACTIVE_COOLDOWN_SECS:
        return None
    if not response or len(response) < 80:
        return None

    msg_lower = message.lower()
    resp_lower = response.lower()

    # After token/on-chain analysis
    token_kws = ['代币', 'token', '合约', 'solana', '链上', 'onchain', '分析']
    if any(kw in msg_lower for kw in token_kws):
        ctx = _get_chat_context(chat_id)
        last = ctx.get("last_analyzed")
        ref = ""
        if last:
            ref = last.get("symbol") or (last.get("address", "")[:8] + "...") if last else ""
        suggestions = []
        if ref:
            suggestions.append(f"回测 {ref} 的MA策略 (`/ma-ribbon-backtest`)")
            suggestions.append(f"追踪 {ref} 聪明钱 (`/token-analyze`)")
        suggestions.append("扫全市场 (`/okx-top30`)")
        _proactive_cooldown[chat_id] = now
        return "💡 **建议下一步：**\n" + "\n".join(f"• {s}" for s in suggestions[:3])

    # After backtest
    backtest_kws = ['回测', 'backtest', '胜率', 'win rate', '策略']
    if any(kw in msg_lower for kw in backtest_kws) and any(
        kw in resp_lower for kw in ['胜率', '收益', '结果', 'result', 'win']
    ):
        _proactive_cooldown[chat_id] = now
        return "💡 **建议：** 调整参数继续优化，或用 `/okx-top30` 找更好的标的"

    return None


# ─── Main Processing Logic ────────────────────────────────────────────────────

async def process_message(user_message: str, chat_id: int, context, **kwargs) -> bool:
    """Process a user message — Never-Die fallback chain.

    Chain: Claude CLI -> API providers -> Web AI -> Cached/Template
    The bot should NEVER be silent.

    Returns True if the message was actually processed, False if only queued.
    """
    image_data = kwargs.get("image_data")
    auto_research.mark_user_active()  # Reset idle timer for auto-experiment
    _periodic_state_cleanup()  # Prune stale sessions/locks (rate-limited to once/hour)

    # ── NLP Pre-processing ────────────────────────────────────────────────────
    # 1. Update context with tokens/projects mentioned in this message
    _update_chat_context(chat_id, user_message)

    # 2. Disambiguation: only when message is truly a bare action word with no target
    disambig_options = _check_disambiguation(user_message, chat_id)
    if disambig_options:
        prompt_lines = "\n".join(f"  {i+1}. {opt}" for i, opt in enumerate(disambig_options))
        await _send_response(
            chat_id,
            f"🤔 **你是想：**\n{prompt_lines}\n\n直接回复数字或发完整指令",
            context,
        )
        return True

    # 3. Resolve fuzzy references ("那个代币" → actual address/symbol from context)
    resolved_message, was_resolved = _resolve_fuzzy_message(user_message, chat_id)
    if was_resolved:
        await _send_response(chat_id, f"🔍 理解为：{resolved_message}", context)
        user_message = resolved_message

    # 4. Multi-intent detection: split into separate tasks if separators found
    intents = _detect_intents(user_message)
    is_multi_intent = len(intents) > 1

    if is_multi_intent:
        logger.info(f"Chat {chat_id}: detected {len(intents)} intents")
        await _send_response(
            chat_id,
            f"📋 检测到 {len(intents)} 个任务，依次执行...",
            context,
        )
    # ── End NLP Pre-processing ─────────────────────────────────────────────────

    lock = _get_lock(chat_id)

    if lock.locked():
        tid = _queue_message(chat_id, user_message)
        queue_size = len(_pending_messages.get(chat_id, []))
        logger.info(f"Chat {chat_id}: queued message (#{tid}, {queue_size} pending)")
        _ensure_queue_worker(chat_id, context)  # Background worker will process when lock is free
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"⏳ 已排队 (任务#{tid}, 队列#{queue_size})。当前任务完成后处理。\n/tasks 查看 · /cancel 取消",
            )
        except Exception:
            pass
        return False  # Message was queued, NOT processed

    # Register this task as running
    running_tid = _next_task_id()
    _running_tasks[chat_id] = {
        "task_id": running_tid,
        "text": user_message[:100],
        "start_time": time.time(),
    }
    try:
        async with lock:
            # ── Multi-intent: process each intent sequentially ──
            if is_multi_intent:
                all_success = True
                for idx, intent in enumerate(intents):
                    if idx > 0:
                        await _send_response(chat_id, f"▶ 任务 {idx+1}/{len(intents)}：{intent[:60]}...", context)
                    _update_chat_context(chat_id, intent)
                    success = await _process_with_llm(intent, chat_id, context)
                    if not success and getattr(config, "NEVER_DIE_MODE", True):
                        if await _fallback_to_api_providers(intent, chat_id, context, image_data=image_data):
                            success = True
                    if not success:
                        all_success = False
                        await _send_response(chat_id, f"⚠️ 任务{idx+1}失败，继续下一个", context)
                # Proactive suggestion after last intent
                suggestion = _get_proactive_suggestion(user_message, "", chat_id)
                if suggestion:
                    await _send_response(chat_id, suggestion, context)
                return all_success

            # ── Single intent: normal routing ──
            # Route: named session for specific projects (e.g. "crypto" → crypto session)
            session_name = _try_session_route(user_message, chat_id)
            if session_name:
                try:
                    logger.info(f"Chat {chat_id}: routing to session '{session_name}'")
                    result = await _session_mgr.send(session_name, user_message, timeout=300)
                    await _send_response(chat_id, result, context)
                    _update_chat_context(chat_id, user_message, result)
                    return True
                except Exception as e:
                    logger.warning(f"Chat {chat_id}: session '{session_name}' failed ({e}), falling back")

            # Route: complex multi-step tasks → pipeline, simple → direct CLI
            if _should_use_pipeline(user_message, chat_id):
                try:
                    logger.info(f"Chat {chat_id}: routing to multi-agent pipeline")
                    await _process_with_pipeline(user_message, chat_id, context)
                    return True
                except Exception as e:
                    logger.warning(f"Chat {chat_id}: pipeline failed ({e}), falling back to CLI")

            # Primary path: Claude CLI with session persistence
            success = await _process_with_llm(user_message, chat_id, context)
            if success:
                # Proactive suggestion after successful single-intent response
                # (We don't have the response text here, but we can use context)
                suggestion = _get_proactive_suggestion(user_message, "", chat_id)
                if suggestion:
                    _fire_and_forget(
                        _send_response(chat_id, suggestion, context),
                        name="proactive_suggestion",
                    )
                return True

            # ── Never-Die Fallback Chain ──
            never_die = getattr(config, "NEVER_DIE_MODE", True)
            if not never_die:
                await _send_response(chat_id, "CLI failed. Please retry.", context)
                return True  # We responded, even if with error

            logger.warning(f"Chat {chat_id}: CLI failed, entering never-die fallback chain")

            # Step 2: Tier-routed API (Claude→OpenAI→Gemini, async SDK + retries)
            api_ok = await _fallback_to_api_providers(
                user_message, chat_id, context, image_data=image_data
            )
            if api_ok:
                return True

            # Step 3: Try free web AI (ChatGPT/Claude.ai/Gemini web)
            web_ok = await _fallback_to_web_ai(user_message, chat_id, context)
            if web_ok:
                return True

            # Step 4: Cached commands or template (NEVER silence)
            await _fallback_cached_or_template(user_message, chat_id, context)
            return True
    finally:
        _running_tasks.pop(chat_id, None)


async def _skill_post_process(user_message: str, response: str, score: dict, matched_ids: list):
    """Background: 4-layer skill lifecycle — observe, evaluate, decide, package."""
    try:
        # ── Layer 1-3: Run lifecycle (Observe → Evaluate → Decide) ──
        lifecycle_result = None
        try:
            from skill_lifecycle import run_lifecycle
            matched_skills = []
            for sid in matched_ids:
                s = skill_library._load_skill(sid)
                if s:
                    matched_skills.append(s)
            lifecycle_result = run_lifecycle(user_message, response, score, matched_skills)
            logger.debug(
                "SkillLifecycle: task=%s policy=%s action=%s",
                lifecycle_result["task"]["type"],
                lifecycle_result["policy"]["decision"],
                lifecycle_result["action"],
            )
        except Exception as e:
            logger.debug(f"Skill lifecycle unavailable: {e}")

        # ── Layer 4: Update reused skills ──
        for sid in matched_ids:
            skill_library.update_skill_from_reuse(sid, score)
            await skill_library.maybe_evolve_skill(sid, user_message, response)

        # ── Extract new skill (gated by lifecycle evaluation) ──
        new_id = await skill_library.maybe_extract_skill(user_message, response, score)
        if new_id:
            logger.info(f"New skill learned: {new_id}")
            try:
                from skill_lifecycle import record_promotion
                task_type = lifecycle_result["task"]["type"] if lifecycle_result else "unknown"
                eval_score = (lifecycle_result["evaluation"]["total_score"]
                              if lifecycle_result and lifecycle_result.get("evaluation") else 0.0)
                record_promotion(new_id, task_type, eval_score)
            except Exception:
                pass
            try:
                vital_signs.record_skill_created()
            except Exception:
                pass

        # ── Periodic pruning ──
        if score.get("overall", 0) > 0:
            skill_library.prune_skills()
    except Exception as e:
        logger.debug(f"Skill post-process error: {e}")


async def _auto_compile_check(response: str, tool_output_text: str, chat_id: int, context):
    """Self-learning loop: after code changes, auto-scan for syntax errors.
    If bugs found → auto-fix via CLI → extract fix as a skill (compound interest)."""
    try:
        combined = (response or "") + "\n" + (tool_output_text or "")
        combined_lower = combined.lower()
        # Only trigger if response indicates code was modified
        code_signals = ["write(", "edit(", "wrote to", "edited", "created file",
                        "修改了", "写入了", "已保存", ".py", "py_compile"]
        if not any(sig in combined_lower for sig in code_signals):
            return
        # Quick py_compile scan of all bot .py files
        import py_compile
        errors = []
        for fname in os.listdir(BOT_PROJECT_DIR):
            if not fname.endswith(".py"):
                continue
            fpath = os.path.join(BOT_PROJECT_DIR, fname)
            try:
                py_compile.compile(fpath, doraise=True)
            except py_compile.PyCompileError as e:
                errors.append({"file": fname, "error": str(e)})
        if not errors:
            logger.debug("Auto-compile check: all .py files clean ✅")
            return
        # Found syntax errors! Log and attempt auto-fix
        err_summary = "\n".join(f"  {e['file']}: {e['error'][:120]}" for e in errors[:5])
        logger.warning(f"Auto-compile found {len(errors)} syntax error(s):\n{err_summary}")
        # Try to auto-fix via CLI (send fix request to fresh session)
        fix_prompt = (
            f"以下.py文件有语法错误，请用Edit工具修复（不要问问题，直接修复）：\n{err_summary}\n"
            f"工作目录: {BOT_PROJECT_DIR}"
        )
        try:
            fix_resp, fix_sid, _, _ = await asyncio.wait_for(
                _run_llm_turn(fix_prompt, -(abs(chat_id) + 1000000), context, timeout=120),
                timeout=130,
            )
            # Verify fix worked
            still_broken = []
            for e in errors:
                fpath = os.path.join(BOT_PROJECT_DIR, e["file"])
                try:
                    py_compile.compile(fpath, doraise=True)
                except py_compile.PyCompileError:
                    still_broken.append(e["file"])
            if not still_broken:
                logger.info(f"✅ Auto-fix successful! Fixed {len(errors)} syntax error(s)")
                # Extract fix as a skill (compound interest: learn from self-repair)
                fix_score = {"overall": 0.9, "flags": ["auto_self_fix"]}
                new_skill = await skill_library.maybe_extract_skill(
                    f"auto-fix syntax errors: {err_summary[:200]}",
                    fix_resp or "auto-fixed",
                    fix_score,
                )
                if new_skill:
                    logger.info(f"🧬 Learned self-fix skill: {new_skill}")
                # Record successful self-heal
                harness_learn.record_self_heal(
                    err_summary[:200], "syntax errors detected by auto-compile",
                    "auto-fixed via CLI", success=True,
                )
            else:
                logger.warning(f"Auto-fix partial: {still_broken} still broken")
                harness_learn.record_self_heal(
                    err_summary[:200], "syntax errors detected by auto-compile",
                    f"partial fix, still broken: {still_broken}", success=False,
                )
        except Exception as fix_err:
            logger.warning(f"Auto-fix attempt failed: {fix_err}")
            harness_learn.record_self_heal(
                err_summary[:200], "syntax errors detected by auto-compile",
                f"fix failed: {fix_err}", success=False,
            )
    except Exception as e:
        logger.debug(f"Auto-compile check error: {e}")


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
        success = await _process_with_llm(user_message, chat_id, context)
        if not success:
            await _send_response(chat_id, "⚠️ 也失败了，请重试。", context)


async def _run_llm_stateless_training(
    prompt: str,
    model: str = "claude-sonnet-4-6",
    timeout: int = 120,
) -> tuple[str, str | None]:
    """Single-shot Claude CLI for training/internal use (same semaphore + 45s cap)."""
    _ = timeout
    user_body = f"[Training task] (model hint: {model})\n{prompt}"
    if len(user_body) > 8000:
        user_body = user_body[:8000]
    system_text = await asyncio.to_thread(_PROMPT_FILE.read_text, encoding="utf-8")
    combined = f"{system_text[:120_000]}\n\n=== TRAINING TASK ===\n{user_body}"
    text, err, _ = await async_claude_code_prompt(
        combined,
        cwd=BOT_PROJECT_DIR,
        timeout_sec=_cli_hard_timeout(),
    )
    if err and not (text or "").strip():
        return f"Error: {err[:500]}", None
    return (text or "Done.").strip(), None


async def _run_llm_raw(
    prompt: str,
    model: str = "claude-haiku-4-5-20251001",
    timeout: int = 30,
) -> str:
    """Stateless Claude CLI for judge/meta tasks; never raises."""
    _ = timeout
    try:
        body = (
            "Follow the user message exactly. Output only what is requested, no preamble.\n\n"
            f"(Model hint: {model})\n\n{(prompt or '')[:120_000]}"
        )
        text, err, _ = await async_claude_code_prompt(
            body,
            cwd=BOT_PROJECT_DIR,
            timeout_sec=_cli_hard_timeout(),
        )
        if err and not (text or "").strip():
            return ""
        return (text or "").strip()[:2000]
    except Exception as e:
        logger.debug("_run_llm_raw failed: %s", e, exc_info=True)
        return ""


async def _repair_trade_json_via_haiku(round_idx: int, bad_snippet: str) -> str:
    """Re-prompt via local Claude CLI (serialized); reminders from dispatcher.llm_filter."""
    return await reask_trade_json_via_cli(round_idx, bad_snippet)


async def sanitize_llm_trade_output_with_retries(raw_llm_text: str, max_retries: int = 3):
    """Pydantic gate + up to ``max_retries`` Haiku repair rounds (no prose trade spam)."""
    from dispatcher.llm_filter import sanitize_trade_directive_with_retries

    text = (raw_llm_text or "").strip()
    if not text:
        return None
    return await sanitize_trade_directive_with_retries(
        text,
        reask=lambda ri, prev: _repair_trade_json_via_haiku(ri, prev),
        max_retries=max_retries,
    )


async def _forward_new_screenshots_direct(send_photo):
    """Forward screenshots for training (no chat_id context)."""
    if not send_photo or not os.path.isdir(_TG_SCREENSHOT_DIR):
        return
    files = sorted(
        [os.path.join(_TG_SCREENSHOT_DIR, f) for f in os.listdir(_TG_SCREENSHOT_DIR)
         if f.lower().endswith((".jpg", ".jpeg", ".png"))],
        key=lambda x: _safe_mtime(x),
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
    llm_http_client.clear_history_sync(chat_id)
    _claude_sessions.pop(chat_id, None)
    _session_timestamps.pop(chat_id, None)
    _save_sessions()
    _pending_messages.pop(chat_id, None)
    # Don't delete the lock - it may be held by another coroutine
    # _processing_locks.pop(chat_id, None)  # removed: causes hangs


# ─── Public SessionManager API ───────────────────────────────────────────────

def get_session_manager():
    """Get the global SessionManager instance (or None if unavailable)."""
    return _session_mgr


def create_project_session(name: str, project_dir: str, model: str = "claude-sonnet-4-6"):
    """Create a named project session for auto-routing.

    Example:
        create_project_session("crypto", "C:/Users/alexl/Desktop/crypto-analysis-")
        # Now messages mentioning "crypto" will route to that session automatically.
    """
    if not _session_mgr:
        logger.warning("SessionManager not available")
        return None
    return _session_mgr.create(name, project_dir, model)


async def llm_strategy_json(
    user_prompt: str,
    *,
    system_prompt: str | None = None,
    model_hint: str | None = None,
    timeout_sec: float = 30.0,
) -> dict[str, Any]:
    """
    Strategy JSON via local Claude CLI (not billing HTTP). ``timeout_sec`` is capped by
    ``_cli_hard_timeout()`` (default 45s max). Parse failures → ``confidence=0``; never raises.
    """
    _ = timeout_sec
    sp = system_prompt or (
        "You are a disciplined trading assistant. Output exactly one JSON object with keys: "
        "action (string), confidence (number from 0 to 1), reason (string). "
        "No markdown code fences, no text outside the JSON object."
    )
    mh = model_hint or getattr(config, "TASK_TIER_FAST_CLAUDE", None)
    combined = f"{sp}\n\n(Model routing hint: {mh})\n\n=== TASK ===\n{(user_prompt or '')[:120_000]}"
    try:
        text, err, _ = await async_claude_code_prompt(
            combined,
            cwd=BOT_PROJECT_DIR,
            timeout_sec=_cli_hard_timeout(),
        )
    except Exception as e:
        logger.exception("llm_strategy_json CLI: %s", e)
        return llm_http_client.strategy_json_safe_default(reason="exception", detail=str(e)[:300])
    if not (text or "").strip():
        return llm_http_client.strategy_json_safe_default(
            reason="cli_empty",
            detail=(err or "")[:500],
        )
    parsed = llm_http_client.extract_json_object_from_llm_text(text)
    if not parsed:
        return llm_http_client.strategy_json_safe_default(reason="parse_error", detail=text[:200])
    try:
        conf = float(parsed.get("confidence", 0) or 0)
    except (TypeError, ValueError):
        conf = 0.0
    conf = max(0.0, min(1.0, conf))
    return {
        "ok": True,
        "action": str(parsed.get("action", "hold") or "hold")[:64],
        "confidence": conf,
        "reason": str(parsed.get("reason", "") or "")[:500],
        "raw": parsed,
    }


# Legacy names — map to async Claude CLI path.
_run_claude_cli = _run_llm_turn
_process_with_claude_cli = _process_with_llm
_run_claude_cli_direct = _run_llm_stateless_training
_run_claude_raw = _run_llm_raw
