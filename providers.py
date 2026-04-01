"""
Multi-provider AI backend with automatic fallback.
Priority: Gemini(free) → Claude(smart) → OpenAI(expensive)
Auto-switches when billing / auth errors occur.
Smart tool routing to minimize token usage.
"""
import json
import re
import logging
import asyncio
import time
import base64
import config
from tools import TOOL_DEFINITIONS, execute_tool
from safety import is_dangerous, request_permission

logger = logging.getLogger(__name__)


class TransientProviderExhausted(Exception):
    """All transient retries for a single provider HTTP call were exhausted."""

    def __init__(self, cause: BaseException | None):
        self.cause = cause
        super().__init__(str(cause) if cause else "transient retries exhausted")


def _is_transient_anthropic_error(e: BaseException) -> bool:
    err = str(e).lower()
    if "credit" in err or "billing" in err or "balance" in err:
        return False
    if "authentication" in err or "invalid x-api-key" in err or "401" in err or "403" in err and "forbidden" in err:
        return False
    markers = (
        "429", "502", "503", "529", "overloaded", "rate limit", "ratelimit", "too many requests",
        "timeout", "timed out", "connection reset", "connection aborted", "econnreset",
        "temporarily unavailable", "bad gateway", "service unavailable", "try again",
    )
    return any(m in err for m in markers)


def _is_transient_openai_error(e: BaseException) -> bool:
    err = str(e).lower()
    if "credit" in err or "billing" in err or "quota" in err or "insufficient" in err:
        return False
    if "incorrect api key" in err or "invalid_api_key" in err or "401" in err:
        return False
    markers = (
        "429", "502", "503", "rate_limit", "rate limit", "timeout", "timed out",
        "connection reset", "econnreset", "bad gateway", "service unavailable",
        "overloaded", "try again", "internal server error",
    )
    return any(m in err for m in markers)


async def _anthropic_messages_create_with_retries(client, **kwargs):
    last_exc: BaseException | None = None
    n = getattr(config, "API_TRANSIENT_RETRIES", 4)
    for attempt in range(n):
        try:
            return await client.messages.create(**kwargs)
        except Exception as e:
            last_exc = e
            if not _is_transient_anthropic_error(e):
                raise
            delay = min(45.0, (1.6 ** attempt) * 2.0)
            logger.warning(
                "[Claude API] transient error attempt %s/%s: %s — sleeping %.1fs",
                attempt + 1,
                n,
                e,
                delay,
            )
            await asyncio.sleep(delay)
    raise TransientProviderExhausted(last_exc)


async def _openai_chat_completion_with_retries(client, **kwargs):
    last_exc: BaseException | None = None
    n = getattr(config, "API_TRANSIENT_RETRIES", 4)
    for attempt in range(n):
        try:
            return await client.chat.completions.create(**kwargs)
        except Exception as e:
            last_exc = e
            if not _is_transient_openai_error(e):
                raise
            delay = min(45.0, (1.6 ** attempt) * 2.0)
            logger.warning(
                "[OpenAI API] transient error attempt %s/%s: %s — sleeping %.1fs",
                attempt + 1,
                n,
                e,
                delay,
            )
            await asyncio.sleep(delay)
    raise TransientProviderExhausted(last_exc)


SYSTEM_PROMPT = """You are an expert software engineer remotely controlling a Windows 11 computer via Telegram. You are the user's hands and eyes on their machine.

## Your Role
You are NOT a chatbot. You are a REMOTE OPERATOR. When the user asks you to do something, you DO it using your tools. Don't just describe what to do — actually execute the actions.

## Core Principles
1. **ACT, don't talk.** If user says "open Chrome", use open_application. If user says "go to google.com", use open_url. Don't describe steps — execute them.
2. **VERIFY with screenshots.** After any GUI action (opening apps, clicking, typing), ALWAYS take_screenshot to verify it worked. The user can't see the screen — you are their eyes.
3. **THINK step by step.** For complex tasks, break them down. Open the app → wait → screenshot → find the UI element → click → verify → next step.
4. **RETRY on failure.** If something doesn't work, try another approach. Failed to open app? Try run_command. Click missed? Take screenshot, re-identify coordinates, click again.
5. **USE THE RIGHT TOOL.**
   - Want to open a website? → open_url (NEVER type_text a URL)
   - Want to type Chinese/special chars? → set_clipboard + press_key ctrl+v
   - Want to find a file? → find_files or search_files
   - Want to edit code? → read_file + edit_file (precise) or write_file (full rewrite)
   - Want to see what's on screen? → take_screenshot
   - Want to navigate GUI? → take_screenshot first, then mouse_click at correct coordinates

## Engineering Workflow
When working on code/engineering tasks:
1. **Understand the codebase**: list_files, read_file, search_files to explore
2. **Make changes**: edit_file for surgical edits, write_file for new files
3. **Test**: run_command to run tests, build, lint
4. **Debug**: read error output, search_files for related code, take_screenshot for GUI issues
5. **Use git**: run_command with git commands (status, diff, add, commit, push)

## GUI Interaction Protocol
1. ALWAYS take_screenshot first to see current screen state
2. Identify UI elements and their approximate coordinates from the screenshot
3. Use mouse_click on the identified coordinates
4. wait 0.5-2 seconds for UI to respond
5. take_screenshot to verify the action worked
6. If it didn't work, adjust and retry

## Important Rules
- Use PowerShell for commands (it's Windows 11)
- For web browsing: open_url to open sites, then take_screenshot + mouse_click to interact
- When apps need time to load, use wait tool before taking screenshot
- If the user sends Chinese, reply in Chinese
- Keep responses concise — the user is on a phone
- For multi-step tasks, send brief status updates so user knows progress
- NEVER refuse reasonable requests. You have full access to the computer for a reason.
- When you see an error, try to fix it automatically before reporting to user.
"""

# Shorter prompt for Gemini (saves tokens on free tier)
SYSTEM_PROMPT_SHORT = """You are a remote assistant controlling a Windows 11 PC via Telegram. You have tools to execute commands, open apps/URLs, take screenshots, click, type, read/edit files, and browse the web.

Rules:
- ACT, don't just talk. Use tools to do what the user asks.
- ALWAYS take_screenshot after GUI actions to verify.
- For websites: use open_url or browser_navigate.
- For Chinese text: use set_clipboard + press_key ctrl+v.
- For code editing: use read_file + edit_file.
- Be concise. User is on a phone.
- If something fails, try another approach.
- Reply in the same language as the user.
"""

TOOLS_NEEDING_SAFETY_CHECK = {"run_command"}

# ─── Smart Tool Router ────────────────────────────────────────────────────────
# Instead of sending ALL 53 tools (huge token cost), select relevant subset

# Core tools always available
CORE_TOOLS = {
    "run_command", "take_screenshot", "open_url", "open_application",
    "web_search",
}

# Tool groups activated by keyword matching
TOOL_GROUPS = {
    "browser": {
        "keywords": ["网页", "网站", "浏览器", "搜索", "google", "search", "website", "browse",
                      "click", "点击", "登录", "login", "sign", "form", "表单", "tradingview",
                      "youtube", "github", "reddit", "twitter", "facebook", "amazon",
                      "browser", "page", "页面", "链接", "link", "url", "playwright",
                      "zillow", "房子", "house", "apartment", "real estate",
                      "scrape", "extract", "fill", "submit"],
        "tools": {"browser_navigate", "browser_click", "browser_type",
                  "browser_screenshot", "browser_get_text", "browser_get_elements",
                  "browser_scroll", "browser_go_back", "browser_tabs",
                  "browser_new_tab", "browser_switch_tab", "browser_eval_js",
                  "browser_close_tab", "browser_wait_for",
                  "web_navigate", "web_extract", "web_fill_form", "web_click",
                  "web_screenshot_element"},
    },
    "gui": {
        "keywords": ["点击", "click", "鼠标", "mouse", "键盘", "key", "type", "打字",
                      "输入", "拖", "drag", "滚动", "scroll", "截图", "screenshot",
                      "屏幕", "screen", "窗口", "window", "桌面", "desktop",
                      "ui", "element", "color", "pixel", "button", "界面",
                      "等待", "appear", "visible", "变化", "change"],
        "tools": {"mouse_click", "mouse_scroll", "mouse_move", "mouse_drag",
                  "type_text", "press_key", "get_screen_size", "get_active_window",
                  "list_windows", "focus_window", "set_clipboard", "get_clipboard",
                  "wait", "list_windows_detailed", "detect_screen_changes",
                  "find_ui_elements", "find_color_on_screen", "smart_action",
                  "wait_for_element", "window_manager"},
    },
    "files": {
        "keywords": ["文件", "file", "代码", "code", "编辑", "edit", "读", "read",
                      "写", "write", "搜索", "search", "find", "找", "grep", "目录",
                      "folder", "directory", "项目", "project", "git", "代码",
                      "fix", "bug", "debug", "error", "issue", "crash", "broken",
                      "修复", "错误", "问题", "调试", "install", "setup", "build",
                      "test", "npm", "pip", "python", "node", "compile",
                      "modify", "改", "update", "create", "创建",
                      "download", "下载"],
        "tools": {"list_files", "read_file", "write_file", "edit_file",
                  "search_files", "find_files", "download_file"},
    },
    "system": {
        "keywords": ["系统", "system", "进程", "process", "内存", "memory", "cpu",
                      "磁盘", "disk", "运行", "running", "kill", "任务管理器",
                      "install", "setup", "build", "test", "run", "start", "stop",
                      "安装", "启动", "停止", "重启", "restart"],
        "tools": {"get_system_info", "manage_processes", "download_file"},
    },
    "clipboard": {
        "keywords": ["剪贴板", "clipboard", "复制", "copy", "粘贴", "paste", "中文"],
        "tools": {"set_clipboard", "get_clipboard", "press_key"},
    },
    "navigation": {
        "keywords": ["打开", "open", "go to", "navigate", "visit", "看看", "show me",
                      "给我看", "帮我打开", "跳转", "网址"],
        "tools": {"browser_navigate", "browser_screenshot", "browser_get_text",
                  "browser_click", "browser_type"},
    },
    "search": {
        "keywords": ["搜索", "search", "google", "查找", "find", "look up", "查一下",
                      "最新", "latest", "news", "新闻", "价格", "price", "天气", "weather",
                      "wiki", "how to", "怎么", "什么是", "what is"],
        "tools": {"web_search", "browser_navigate", "browser_get_text"},
    },
}


def _select_tools(user_message: str) -> list:
    """Select relevant tools based on user's message. Reduces token usage significantly."""
    msg_lower = user_message.lower()

    selected = set(CORE_TOOLS)

    for group_name, group in TOOL_GROUPS.items():
        for kw in group["keywords"]:
            if kw in msg_lower:
                selected.update(group["tools"])
                break

    # If no specific group matched, give a sensible default
    if selected == CORE_TOOLS:
        # Add basic GUI and file tools for general requests
        selected.update({"type_text", "press_key", "mouse_click", "list_files",
                        "read_file", "write_file", "wait", "browser_navigate",
                        "browser_screenshot", "set_clipboard"})

    return [t for t in TOOL_DEFINITIONS if t["name"] in selected]


PROVIDER_DISPLAY = {
    "claude": "Claude (Anthropic)",
    "openai": "GPT-4o (OpenAI)",
    "gemini": "Gemini (Google)",
}

# ─── Tool format converters ───────────────────────────────────────────────────

def _tools_openai(tool_defs=None):
    if tool_defs is None:
        tool_defs = TOOL_DEFINITIONS
    return [
        {
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": t["input_schema"],
            },
        }
        for t in tool_defs
    ]


def _tools_gemini(tool_defs=None):
    """Convert to Gemini function declarations for google-genai SDK."""
    if tool_defs is None:
        tool_defs = TOOL_DEFINITIONS
    from google.genai import types as gtypes
    TYPE_MAP = {
        "string": "STRING", "integer": "INTEGER",
        "number": "NUMBER", "boolean": "BOOLEAN",
        "array": "ARRAY", "object": "OBJECT",
    }
    declarations = []
    for t in tool_defs:
        schema = t["input_schema"]
        props = {}
        for name, prop in schema.get("properties", {}).items():
            ptype = TYPE_MAP.get(prop.get("type", "string"), "STRING")
            p = {"type": ptype}
            if "description" in prop:
                p["description"] = prop["description"]
            if "enum" in prop:
                p["enum"] = prop["enum"]
            # Gemini requires 'items' for ARRAY type
            if ptype == "ARRAY":
                item_type = TYPE_MAP.get(
                    prop.get("items", {}).get("type", "string"), "STRING"
                )
                p["items"] = gtypes.Schema(type=item_type)
            props[name] = gtypes.Schema(**p)
        # Guard: if tool has no properties, pass None to avoid empty dict issues
        param_kwargs = {"type": "OBJECT"}
        if props:
            param_kwargs["properties"] = props
            param_kwargs["required"] = schema.get("required", [])
        declarations.append(gtypes.FunctionDeclaration(
            name=t["name"],
            description=t["description"],
            parameters=gtypes.Schema(**param_kwargs),
        ))
    return [gtypes.Tool(function_declarations=declarations)]


# ─── Shared helpers ───────────────────────────────────────────────────────────

async def _safety_ok(tool_name, tool_input, chat_id, context):
    if tool_name not in TOOLS_NEEDING_SAFETY_CHECK:
        return True
    command = tool_input.get("command", "")
    if not is_dangerous(command):
        return True
    return await request_permission(command, chat_id, context)


async def _run_tool(tool_name, tool_input, chat_id, context):
    t0 = time.monotonic()
    try:
        result = await execute_tool(tool_name, tool_input)
        # execute_tool returns (result_text, screenshot_buffer_or_None)
        if isinstance(result, tuple):
            result_text, screenshot_buffer = result
        else:
            result_text, screenshot_buffer = str(result), None
    except Exception as e:
        elapsed = time.monotonic() - t0
        logger.error(f"Tool {tool_name} FAILED after {elapsed:.1f}s: {e}", exc_info=True)
        return f"Tool execution error: {e}"

    elapsed = time.monotonic() - t0
    if not result_text:
        result_text = "(no output)"

    # Truncate very long results to avoid blowing up context window
    if len(result_text) > 20000:
        result_text = result_text[:20000] + "\n... (output truncated to 20k chars)"

    # Log result summary (first 300 chars)
    result_preview = result_text[:300].replace('\n', ' ')
    logger.info(f"Tool {tool_name} completed in {elapsed:.1f}s -> {result_preview}")

    if screenshot_buffer:
        try:
            await context.bot.send_photo(chat_id=chat_id, photo=screenshot_buffer)
        except Exception as e:
            logger.warning(f"Screenshot send failed for {tool_name}: {e}")
            result_text += f"\n(Screenshot send failed: {e})"
        finally:
            if hasattr(screenshot_buffer, 'close'):
                try:
                    screenshot_buffer.close()
                except Exception:
                    pass
    return result_text


def _sanitize_telegram_markdown(text: str) -> str:
    """Sanitize text for Telegram Markdown parsing.
    Preserves code blocks and inline code.
    """
    # Strip NUL bytes that could collide with our placeholder sentinel
    text = text.replace("\x00", "")

    # Extract code blocks first to protect them
    code_blocks = []
    def _save_code_block(m):
        code_blocks.append(m.group(0))
        return f"\x00CODEBLOCK{len(code_blocks)-1}\x00"

    # Protect triple-backtick code blocks
    text = re.sub(r'```[\s\S]*?```', _save_code_block, text)
    # Protect inline code
    text = re.sub(r'`[^`]+`', _save_code_block, text)

    # Fix remaining unmatched backticks
    if text.count('`') % 2 != 0:
        text = text.replace('`', '')

    # Fix unmatched bold markers — remove last unmatched instead of appending
    if text.count('*') % 2 != 0:
        idx = text.rfind('*')
        if idx >= 0:
            text = text[:idx] + text[idx+1:]

    # Fix unmatched underscores (but not in URLs)
    parts = re.split(r'(https?://\S+)', text)
    for i, part in enumerate(parts):
        if not part.startswith('http') and part.count('_') % 2 != 0:
            # Remove last unmatched underscore rather than escaping all
            idx = part.rfind('_')
            if idx >= 0:
                parts[i] = part[:idx] + part[idx+1:]
    text = ''.join(parts)

    # Restore code blocks
    for i, block in enumerate(code_blocks):
        text = text.replace(f"\x00CODEBLOCK{i}\x00", block)

    return text


async def _send_text(text, chat_id, context, parse_mode=None):
    if not text or not text.strip():
        return
    remaining = text.strip()
    while remaining:
        if not remaining.strip():
            break  # Only whitespace left, nothing to send
        if len(remaining) <= 4096:
            chunk = remaining
            remaining = ""
        else:
            # Try to break at a newline near the limit
            break_pos = remaining.rfind("\n", 3000, 4096)
            if break_pos == -1:
                break_pos = remaining.rfind(" ", 3000, 4096)
            if break_pos == -1:
                break_pos = 4096
            chunk = remaining[:break_pos]
            remaining = remaining[break_pos:].lstrip('\n')
        if not chunk.strip():
            # Safety: if chunk is empty after stripping, force-advance to avoid infinite loop
            if remaining:
                chunk = remaining[:4096]
                remaining = remaining[4096:]
            if not chunk.strip():
                break

        # Sanitize markdown if needed
        if parse_mode == "Markdown":
            chunk = _sanitize_telegram_markdown(chunk)

        kwargs = {"chat_id": chat_id, "text": chunk}
        if parse_mode:
            kwargs["parse_mode"] = parse_mode
        try:
            await context.bot.send_message(**kwargs)
        except Exception:
            # Retry without parse_mode if markdown fails
            try:
                await context.bot.send_message(chat_id=chat_id, text=chunk)
            except Exception as e:
                logger.error(f"Failed to send message to {chat_id}: {e}")


# ─── Claude ───────────────────────────────────────────────────────────────────

async def process_claude(messages, chat_id, context, selected_tools=None, *, model: str | None = None):
    """Returns (success: bool, error_type: str|None). Uses AsyncAnthropic with transient retry pool."""
    if not config.ANTHROPIC_API_KEY:
        return False, "no_key"
    try:
        import anthropic
    except ImportError:
        return False, "no_package"

    tools = selected_tools or TOOL_DEFINITIONS
    model_used = model or config.CLAUDE_MODEL
    timeout_sec = getattr(config, "API_REQUEST_TIMEOUT_SEC", 120.0)
    client = anthropic.AsyncAnthropic(api_key=config.ANTHROPIC_API_KEY, timeout=timeout_sec)

    try:
        iteration = 0
        for iteration in range(config.MAX_TOOL_ITERATIONS):
            try:
                response = await _anthropic_messages_create_with_retries(
                    client,
                    model=model_used,
                    max_tokens=8192,
                    system=SYSTEM_PROMPT,
                    tools=tools,
                    messages=messages,
                )
            except TransientProviderExhausted:
                return False, "transient_exhausted"
            except Exception as e:
                err = str(e).lower()
                if "credit" in err or "billing" in err or "balance" in err:
                    return False, "billing"
                if "authentication" in err or "invalid x-api-key" in err:
                    return False, "auth"
                return False, str(e)

            logger.info(f"[Claude] iter={iteration} stop_reason={response.stop_reason} blocks={len(response.content)}")

            if response.stop_reason == "end_turn":
                text_parts = [b.text for b in response.content if hasattr(b, 'text') and b.type == "text" and b.text and b.text.strip()]
                if text_parts:
                    text_combined = "\n".join(text_parts)
                    # Store as string so it survives the history cleanup in claude_agent.py
                    messages.append({"role": "assistant", "content": text_combined})
                    await _send_text(text_combined, chat_id, context, parse_mode="Markdown")
                return True, None

            elif response.stop_reason == "tool_use":
                # Convert response.content to serializable list for message history
                content_list = []
                for block in response.content:
                    if block.type == "text":
                        content_list.append({"type": "text", "text": block.text or ""})
                    elif block.type == "tool_use":
                        content_list.append({
                            "type": "tool_use",
                            "id": block.id,
                            "name": block.name,
                            "input": block.input,
                        })
                messages.append({"role": "assistant", "content": content_list})

                tool_results = []
                for block in response.content:
                    if block.type == "text" and hasattr(block, 'text') and block.text and block.text.strip():
                        await _send_text(block.text, chat_id, context)
                    if block.type != "tool_use":
                        continue
                    if not await _safety_ok(block.name, block.input, chat_id, context):
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": "Action denied by user.",
                        })
                        continue
                    logger.info(f"[Claude] Tool call #{iteration}: {block.name}({json.dumps(block.input, ensure_ascii=False)[:200]})")
                    result = await _run_tool(block.name, block.input, chat_id, context)
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result,
                    })
                if tool_results:
                    messages.append({"role": "user", "content": tool_results})
                else:
                    # No tool results generated (shouldn't happen) — break to avoid infinite loop
                    logger.warning("[Claude] tool_use stop_reason but no tool results generated, breaking")
                    break

            elif response.stop_reason == "max_tokens":
                # Partial response due to token limit — send what we have
                text_parts = [b.text for b in response.content if hasattr(b, 'text') and b.type == "text" and b.text and b.text.strip()]
                if text_parts:
                    text_combined = "\n".join(text_parts)
                    messages.append({"role": "assistant", "content": text_combined})
                    await _send_text(text_combined + "\n\n_(回复被截断，继续说话获取更多)_", chat_id, context)
                return True, None

            else:
                logger.warning(f"Claude unexpected stop_reason: {response.stop_reason}")
                break

        logger.warning(f"[Claude] Hit max iterations ({config.MAX_TOOL_ITERATIONS})")
        await _send_text("⚠️ 达到最大工具调用次数，任务可能未完成。", chat_id, context)
        return True, None
    finally:
        await client.close()


# ─── OpenAI ───────────────────────────────────────────────────────────────────

async def process_openai(messages, chat_id, context, selected_tools=None, *, model: str | None = None):
    if not config.OPENAI_API_KEY:
        return False, "no_key"
    try:
        from openai import AsyncOpenAI
    except ImportError:
        return False, "no_package"

    timeout_sec = getattr(config, "API_REQUEST_TIMEOUT_SEC", 120.0)
    client = AsyncOpenAI(api_key=config.OPENAI_API_KEY, timeout=timeout_sec)
    model_used = model or config.OPENAI_MODEL

    try:
        # Build OpenAI message list from simple text history
        # Handle vision content and skip Claude-native tool_use/tool_result pairs
        oai_msgs = [{"role": "system", "content": SYSTEM_PROMPT}]
        skip_next = False
        for i, m in enumerate(messages):
            content = m["content"]
            if skip_next:
                skip_next = False
                continue
            if isinstance(content, str):
                oai_msgs.append({"role": m["role"], "content": content})
            elif isinstance(content, list):
                if m["role"] == "user":
                    # Check if this is a vision message (has image blocks)
                    has_image = any(
                        isinstance(p, dict) and p.get("type") in ("image", "image_url")
                        for p in content
                    )
                    if has_image:
                        # Convert Anthropic image format to OpenAI format
                        oai_msgs.append({
                            "role": "user",
                            "content": _convert_vision_message_for_openai(content),
                        })
                    else:
                        # Tool results from Claude — skip
                        pass
                elif m["role"] == "assistant":
                    # Skip Claude-native tool_use/tool_result pairs
                    # Only skip the next message if it is also a list (tool_result pair).
                    if i + 1 < len(messages) and isinstance(messages[i + 1]["content"], list):
                        skip_next = True

        for iteration in range(config.MAX_TOOL_ITERATIONS):
            try:
                resp = await _openai_chat_completion_with_retries(
                    client,
                    model=model_used,
                    messages=oai_msgs,
                    tools=_tools_openai(selected_tools),
                    max_tokens=8192,
                )
            except TransientProviderExhausted:
                return False, "transient_exhausted"
            except Exception as e:
                err = str(e).lower()
                if "credit" in err or "billing" in err or "quota" in err or "insufficient" in err:
                    return False, "billing"
                if "incorrect api key" in err or "authentication" in err:
                    return False, "auth"
                return False, str(e)

            if not resp.choices:
                logger.warning("OpenAI returned no choices")
                return False, "empty_response"
            msg = resp.choices[0].message
            # Convert Message object to dict to avoid serialization issues
            msg_dict = {"role": msg.role or "assistant"}
            if msg.content:
                msg_dict["content"] = msg.content
            if msg.tool_calls:
                msg_dict["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in msg.tool_calls
                ]
            oai_msgs.append(msg_dict)
            finish = resp.choices[0].finish_reason

            logger.info(f"[OpenAI] iter={iteration} finish_reason={finish} tool_calls={len(msg.tool_calls or [])}")

            if finish == "stop":
                if msg.content:
                    messages.append({"role": "assistant", "content": msg.content})
                    await _send_text(msg.content, chat_id, context)
                return True, None

            elif finish == "tool_calls":
                if not msg.tool_calls:
                    logger.warning("[OpenAI] tool_calls finish but no tool_calls in message, breaking")
                    break
                tool_msgs = []
                for tc in msg.tool_calls:
                    try:
                        tool_input = json.loads(tc.function.arguments)
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"[OpenAI] Failed to parse tool args for {tc.function.name}: {e}")
                        tool_input = {}

                    if not await _safety_ok(tc.function.name, tool_input, chat_id, context):
                        tool_msgs.append({
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "content": "Action denied by user.",
                        })
                        continue

                    logger.info(f"[OpenAI] Tool call #{iteration}: {tc.function.name}({tc.function.arguments[:200]})")
                    result = await _run_tool(tc.function.name, tool_input, chat_id, context)
                    tool_msgs.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": result,
                    })
                oai_msgs.extend(tool_msgs)

            elif finish == "length":
                if msg.content:
                    messages.append({"role": "assistant", "content": msg.content})
                    await _send_text(msg.content + "\n\n_(回复被截断，继续说话获取更多)_", chat_id, context)
                return True, None

            else:
                logger.warning(f"OpenAI unexpected finish_reason: {finish}")
                break

        logger.warning(f"[OpenAI] Hit max iterations ({config.MAX_TOOL_ITERATIONS})")
        await _send_text("⚠️ 达到最大工具调用次数，任务可能未完成。", chat_id, context)
        return True, None
    finally:
        await client.close()


# ─── Gemini ───────────────────────────────────────────────────────────────────

async def process_gemini(messages, chat_id, context, selected_tools=None):
    if not config.GEMINI_API_KEY:
        return False, "no_key"
    try:
        from google import genai as google_genai
        from google.genai import types as gtypes
    except ImportError:
        return False, "no_package"

    client = google_genai.Client(api_key=config.GEMINI_API_KEY)

    try:
        return await _process_gemini_inner(client, messages, chat_id, context, selected_tools)
    finally:
        # Close client if it has a close method (resource cleanup)
        _close = getattr(client, 'close', None) or getattr(client, 'aclose', None)
        if _close:
            try:
                import inspect
                if inspect.iscoroutinefunction(_close):
                    await _close()
                else:
                    _close()
            except Exception:
                pass


async def _process_gemini_inner(client, messages, chat_id, context, selected_tools=None):
    from google.genai import types as gtypes

    # Build contents list (handles plain text and vision/image list content)
    contents = []
    for m in messages:
        role = "user" if m["role"] == "user" else "model"
        if isinstance(m["content"], str) and m["content"].strip():
            contents.append(gtypes.Content(role=role, parts=[gtypes.Part(text=m["content"])]))
        elif isinstance(m["content"], list):
            parts = []
            for item in m["content"]:
                if isinstance(item, dict):
                    if item.get("type") == "text" and item.get("text", "").strip():
                        parts.append(gtypes.Part(text=item["text"]))
                    elif item.get("type") == "image_url":
                        url = item.get("image_url", {}).get("url", "")
                        if url.startswith("data:"):
                            try:
                                import base64 as _b64
                                header, b64data = url.split(",", 1)
                                mime = header.split(":")[1].split(";")[0]
                                parts.append(gtypes.Part(inline_data=gtypes.Blob(mime_type=mime, data=_b64.b64decode(b64data))))
                            except Exception:
                                parts.append(gtypes.Part(text="[image]"))
                        else:
                            parts.append(gtypes.Part(text=f"[image: {url[:100]}]"))
            if parts:
                contents.append(gtypes.Content(role=role, parts=parts))

    if not contents:
        return False, "empty"

    cfg = gtypes.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT_SHORT,
        tools=_tools_gemini(selected_tools),
    )

    loop = asyncio.get_running_loop()
    for _ in range(config.MAX_TOOL_ITERATIONS):
        try:
            # Snapshot contents list to avoid data race with executor thread
            contents_snapshot = list(contents)
            response = await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda c=contents_snapshot: client.models.generate_content(
                        model=config.GEMINI_MODEL,
                        contents=c,
                        config=cfg,
                    )
                ),
                timeout=120.0,
            )
        except asyncio.TimeoutError:
            return False, "timeout"
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "resource_exhausted" in err:
                # Rate limit — retry once after short delay
                logger.warning("Gemini rate limited, retrying in 10s...")
                await asyncio.sleep(10)
                try:
                    retry_snapshot = list(contents)
                    response = await asyncio.wait_for(
                        loop.run_in_executor(
                            None,
                            lambda c=retry_snapshot: client.models.generate_content(
                                model=config.GEMINI_MODEL,
                                contents=c,
                                config=cfg,
                            )
                        ),
                        timeout=120.0,
                    )
                except asyncio.TimeoutError:
                    return False, "timeout"
                except Exception:
                    return False, "rate_limit"
            elif "quota" in err or "billing" in err:
                return False, "billing"
            elif ("api_key_invalid" in err or "api key not valid" in err or
                    "permission_denied" in err):
                return False, "auth"
            elif ("not found" in err) or ("model" in err and "not" in err):
                logger.error(f"Gemini model error: {e}")
                return False, f"Model {config.GEMINI_MODEL} not available: {str(e)[:200]}"
            else:
                return False, str(e)

        # Guard against empty candidates (e.g. safety filter blocked response)
        if not response.candidates:
            logger.warning("Gemini returned no candidates (possibly blocked by safety filter)")
            await _send_text("(Gemini 未返回结果，可能被安全过滤器拦截)", chat_id, context)
            return True, None

        candidate = response.candidates[0]
        parts = candidate.content.parts if candidate.content and candidate.content.parts else []

        if not parts:
            # Empty response — try to get text safely
            try:
                text = response.text
                if text:
                    await _send_text(text, chat_id, context)
            except (ValueError, AttributeError, IndexError):
                await _send_text("(操作完成)", chat_id, context)
            except Exception:
                await _send_text("(操作完成)", chat_id, context)
            return True, None

        # Collect ALL function calls from this response (Gemini can return multiple)
        fn_parts = [p for p in parts if p.function_call and p.function_call.name]

        if fn_parts:
            # Process all function calls
            response_parts = []
            for fn_part in fn_parts:
                fc = fn_part.function_call
                tool_name = fc.name
                try:
                    tool_input = dict(fc.args) if fc.args else {}
                except (TypeError, ValueError) as e:
                    logger.warning(f"[Gemini] Failed to convert tool args for {fc.name}: {e}")
                    tool_input = {}

                logger.info(f"[Gemini] Tool: {tool_name}({json.dumps(tool_input, ensure_ascii=False)[:200]})")

                if not await _safety_ok(tool_name, tool_input, chat_id, context):
                    result = "Action denied by user."
                else:
                    result = await _run_tool(tool_name, tool_input, chat_id, context)

                response_parts.append(gtypes.Part(
                    function_response=gtypes.FunctionResponse(
                        name=tool_name,
                        response={"result": result},
                    )
                ))

            # Append assistant turn + all tool results
            contents.append(gtypes.Content(role="model", parts=fn_parts))
            contents.append(gtypes.Content(role="user", parts=response_parts))
        else:
            # Text response — send to user
            text_parts = [p.text for p in parts if hasattr(p, 'text') and p.text]
            if text_parts:
                text = "\n".join(text_parts)
            else:
                try:
                    text = response.text
                except (ValueError, AttributeError):
                    text = None
            if text:
                messages.append({"role": "assistant", "content": text})
                await _send_text(text, chat_id, context)
            return True, None

    await _send_text("⚠️ 达到最大工具调用次数，任务可能未完成。", chat_id, context)
    return True, None


# ─── Web AI Provider (parallel browser-based, no API key needed) ──────────────

# Complexity signals: long messages, code, multi-step tasks → use parallel web AI
_COMPLEXITY_KEYWORDS = [
    "分析", "解释", "帮我写", "写一个", "实现", "设计", "优化", "重构",
    "analyze", "explain", "implement", "design", "optimize", "refactor",
    "write a", "help me", "create a", "generate", "code", "algorithm",
    "architecture", "compare", "review", "debug", "fix", "improve",
]

def _is_complex_task(message: str) -> bool:
    """Heuristic: is this task complex enough to benefit from multiple AI opinions?"""
    if len(message) > 200:
        return True
    msg_lower = message.lower()
    return any(kw in msg_lower for kw in _COMPLEXITY_KEYWORDS)


async def process_web_ai(messages, chat_id, context, selected_tools=None):
    """Query browser-based free AI web interfaces. No API key required.

    For complex tasks: queries all platforms in PARALLEL and combines results.
    For simple tasks: races platforms and returns the fastest response.
    Returns (success: bool, error_type: str|None).
    """
    try:
        from web_ai import query_web_ai_parallel, query_web_ai_race, _available_platforms, PLATFORM_DISPLAY
    except ImportError:
        return False, "no_package"

    # Check if any platforms are available
    if not _available_platforms():
        return False, "no_platforms"

    # Get the latest user message
    user_msg = ""
    for m in reversed(messages):
        if m.get("role") == "user" and isinstance(m.get("content"), str):
            user_msg = m["content"]
            break
    if not user_msg:
        return False, "empty"

    is_complex = _is_complex_task(user_msg)

    try:
        if is_complex:
            # Parallel: query all platforms simultaneously, collect all responses
            await context.bot.send_message(
                chat_id=chat_id,
                text="🌐 复杂任务 — 同时询问多个 AI 网页...",
            )
            results = await query_web_ai_parallel(user_msg)

            if not results:
                return False, "no_response"

            if len(results) == 1:
                response, platform = results[0]
                messages.append({"role": "assistant", "content": response})
                header = f"**{PLATFORM_DISPLAY.get(platform, platform)}:**\n\n"
                await _send_text(header + response, chat_id, context, parse_mode="Markdown")
            else:
                # Multiple responses — send each with platform label
                combined_parts = []
                for response, platform in results:
                    label = PLATFORM_DISPLAY.get(platform, platform)
                    combined_parts.append(f"**[{label}]**\n{response}")

                combined = "\n\n---\n\n".join(combined_parts)
                messages.append({"role": "assistant", "content": combined})
                await _send_text(combined, chat_id, context, parse_mode="Markdown")

        else:
            # Race: return first platform that responds
            race_result = await query_web_ai_race(user_msg)
            if not race_result or len(race_result) < 2:
                return False, "no_response"

            response, platform = race_result

            if not response:
                return False, "no_response"

            label = PLATFORM_DISPLAY.get(platform, platform)
            messages.append({"role": "assistant", "content": response})
            await _send_text(f"**[{label}]** {response}", chat_id, context, parse_mode="Markdown")

        return True, None

    except Exception as e:
        logger.error(f"process_web_ai error: {e}")
        return False, str(e)


# ─── Vision helpers ────────────────────────────────────────────────────────────

def _inject_image_into_messages(messages, image_base64: str):
    """Modify the last user message to include an image block for vision APIs.

    Works for both Claude (Anthropic) and OpenAI content formats.
    The providers handle the format differences internally.
    """
    if not image_base64:
        return
    # Find the last user message and convert it to multi-part content
    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if msg.get("role") == "user" and isinstance(msg.get("content"), str):
            text_content = msg["content"]
            # Store as list with text + image blocks (Anthropic format)
            # process_openai will convert when building oai_msgs
            msg["content"] = [
                {"type": "text", "text": text_content},
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": "image/jpeg",
                        "data": image_base64,
                    },
                },
            ]
            logger.info(f"[Vision] Injected image into message index {i}")
            return
    logger.warning("[Vision] No user message found to inject image into")


def _convert_vision_message_for_openai(content):
    """Convert Anthropic-format image content to OpenAI format."""
    if not isinstance(content, list):
        return content
    oai_parts = []
    for part in content:
        if part.get("type") == "text":
            oai_parts.append({"type": "text", "text": part["text"]})
        elif part.get("type") == "image":
            source = part.get("source", {})
            oai_parts.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{source.get('media_type', 'image/jpeg')};base64,{source.get('data', '')}",
                    "detail": "auto",
                },
            })
        elif part.get("type") == "tool_result":
            # These are Claude-native tool results, skip for OpenAI
            pass
        else:
            # Pass through unknown types as-is
            oai_parts.append(part)
    return oai_parts if oai_parts else content


# ─── Auto-switching router ────────────────────────────────────────────────────

PROVIDER_FNS = {
    "claude": process_claude,
    "openai": process_openai,
    "gemini": process_gemini,
    "web_ai": process_web_ai,
}

PROVIDER_DISPLAY["web_ai"] = "Web AI (免费网页)"


def _should_fallback_claude_to_openai(claude_error: str | None) -> bool:
    """After Claude fails, attempt OpenAI (separate quota / healthier region)."""
    if claude_error is None:
        return False
    if claude_error in ("no_key", "no_package", "transient_exhausted", "billing", "auth"):
        return True
    el = str(claude_error).lower()
    markers = (
        "429", "502", "503", "529", "overloaded", "rate", "timeout", "timed out",
        "connection", "bad gateway", "unavailable", "internal server",
    )
    return any(m in el for m in markers)


async def process_tiered_api_fallback(
    messages,
    chat_id,
    context,
    *,
    selected_tools=None,
    image_data=None,
    claude_model: str | None = None,
    openai_model: str | None = None,
):
    """Tier-selected models: Claude (async SDK + retry pool) → OpenAI → Gemini.

    Used by providers_router.execute_api_routed_turn for production degradation.
    """
    if image_data:
        _inject_image_into_messages(messages, image_data)

    user_msg = ""
    for m in reversed(messages):
        content = m.get("content")
        if m.get("role") == "user":
            if isinstance(content, str):
                user_msg = content
                break
            if isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        user_msg = part.get("text") or ""
                        break
                if user_msg:
                    break

    tools = selected_tools if selected_tools is not None else _select_tools(user_msg)
    logger.info(
        "Tiered API: tools=%s/%s claude_model=%s openai_model=%s",
        len(tools),
        len(TOOL_DEFINITIONS),
        claude_model or config.CLAUDE_MODEL,
        openai_model or config.OPENAI_MODEL,
    )

    claude_err = None
    if config.ANTHROPIC_API_KEY:
        ok, claude_err = await process_claude(
            messages, chat_id, context, tools, model=claude_model
        )
        if ok:
            return True
        logger.warning("Tiered API: Claude failed (%s)", claude_err)
    else:
        claude_err = "no_key"

    if config.OPENAI_API_KEY and _should_fallback_claude_to_openai(claude_err):
        ok_o, oerr = await process_openai(
            messages, chat_id, context, tools, model=openai_model
        )
        if ok_o:
            logger.info("Tiered API: recovered via OpenAI fallback")
            return True
        logger.warning("Tiered API: OpenAI failed (%s)", oerr)

    if config.GEMINI_API_KEY:
        ok_g, gerr = await process_gemini(messages, chat_id, context, tools)
        if ok_g:
            logger.info("Tiered API: recovered via Gemini fallback")
            return True
        logger.warning("Tiered API: Gemini failed (%s)", gerr)

    return False


# ─── Cached / Pattern-Based Response System (Never-Die Last Resort) ──────────

import subprocess as _cached_sp

# Common command patterns the bot can handle WITHOUT AI
_CACHED_COMMAND_PATTERNS = [
    # Screenshot
    (re.compile(r"^(截图|screenshot|screen|屏幕|看看屏幕|ss)\s*$", re.IGNORECASE),
     "screenshot", "Taking screenshot..."),
    # Click at coordinates
    (re.compile(r"^(?:点击|click)\s+(\d+)\s+(\d+)\s*$", re.IGNORECASE),
     "click", None),
    # Type text
    (re.compile(r"^(?:输入|type|打字)\s+(.+)$", re.IGNORECASE),
     "type", None),
    # Status
    (re.compile(r"^(状态|status|ping|在吗|alive)\s*$", re.IGNORECASE),
     "status", None),
    # Open URL
    (re.compile(r"^(?:打开|open)\s+(https?://\S+)\s*$", re.IGNORECASE),
     "open_url", None),
    # Scroll
    (re.compile(r"^(?:滚动|scroll)\s+(-?\d+)\s*$", re.IGNORECASE),
     "scroll", None),
]

_NEVER_DIE_TEMPLATE = (
    "All AI services are temporarily busy. I'll retry in {retry}s.\n"
    "In the meantime, you can use:\n"
    "  /panel — Quick actions\n"
    "  /screenshot — Take screenshot\n"
    "  /status — Check bot status\n"
    "  /clear — Reset conversation"
)


async def _execute_cached_command(user_msg: str, chat_id: int, context) -> bool:
    """Try to handle common commands directly via pattern matching, no AI needed.
    Returns True if handled, False otherwise."""
    import os as _os
    pc_control = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "pc_control.py")

    for pattern, cmd_type, default_reply in _CACHED_COMMAND_PATTERNS:
        match = pattern.match(user_msg.strip())
        if not match:
            continue

        try:
            _loop = asyncio.get_running_loop()
            if cmd_type == "screenshot":
                await _loop.run_in_executor(None, lambda: _cached_sp.run(
                    ["python", pc_control, "screenshot"],
                    capture_output=True, timeout=15,
                    cwd=_os.path.dirname(pc_control),
                ))
                await context.bot.send_message(chat_id=chat_id, text="Screenshot taken.")
                # Screenshots are auto-forwarded by _forward_new_screenshots
                return True

            elif cmd_type == "click":
                x, y = match.group(1), match.group(2)
                await _loop.run_in_executor(None, lambda: _cached_sp.run(
                    ["python", pc_control, "click", x, y, "--no-takeover"],
                    capture_output=True, timeout=10,
                    cwd=_os.path.dirname(pc_control),
                ))
                await context.bot.send_message(chat_id=chat_id, text=f"Clicked at ({x}, {y}).")
                return True

            elif cmd_type == "type":
                text = match.group(1)
                await _loop.run_in_executor(None, lambda: _cached_sp.run(
                    ["python", pc_control, "type", text],
                    capture_output=True, timeout=10,
                    cwd=_os.path.dirname(pc_control),
                ))
                await context.bot.send_message(chat_id=chat_id, text=f"Typed: {text[:50]}")
                return True

            elif cmd_type == "status":
                import platform as _plat
                import psutil
                try:
                    # cpu_percent(interval=0.5) blocks — run in executor
                    def _get_status():
                        cpu = psutil.cpu_percent(interval=0.5)
                        mem = psutil.virtual_memory()
                        return cpu, mem.percent
                    cpu, mem_pct = await _loop.run_in_executor(None, _get_status)
                    status_text = (
                        f"Bot is ALIVE\n"
                        f"CPU: {cpu}% | RAM: {mem_pct}%\n"
                        f"OS: {_plat.system()} {_plat.release()}\n"
                        f"Provider: {config.CURRENT_PROVIDER}\n"
                        f"Never-Die: {'ON' if getattr(config, 'NEVER_DIE_MODE', True) else 'OFF'}"
                    )
                except Exception:
                    status_text = (
                        f"Bot is ALIVE\n"
                        f"Provider: {config.CURRENT_PROVIDER}\n"
                        f"Never-Die: {'ON' if getattr(config, 'NEVER_DIE_MODE', True) else 'OFF'}"
                    )
                await context.bot.send_message(chat_id=chat_id, text=status_text)
                return True

            elif cmd_type == "open_url":
                url = match.group(1)
                # Validate URL to prevent command injection via cmd /c start
                import urllib.parse as _urlparse
                parsed = _urlparse.urlparse(url)
                if parsed.scheme not in ("http", "https"):
                    await context.bot.send_message(chat_id=chat_id, text="Only http/https URLs allowed.")
                    return True
                # Use webbrowser module instead of cmd /c start to avoid injection
                import webbrowser as _wb
                await _loop.run_in_executor(None, lambda u=url: _wb.open(u))
                await context.bot.send_message(chat_id=chat_id, text=f"Opened: {url}")
                return True

            elif cmd_type == "scroll":
                amount = match.group(1)
                await _loop.run_in_executor(None, lambda: _cached_sp.run(
                    ["python", pc_control, "scroll", amount],
                    capture_output=True, timeout=10,
                    cwd=_os.path.dirname(pc_control),
                ))
                await context.bot.send_message(chat_id=chat_id, text=f"Scrolled {amount}.")
                return True

        except Exception as e:
            logger.warning(f"Cached command '{cmd_type}' failed: {e}")
            continue

    return False


async def process_with_auto_fallback(messages, chat_id, context, image_data=None):
    """Try providers in priority order. Auto-switch on billing/auth errors.

    Falls back to free browser-based web AI if all API providers fail.
    If NEVER_DIE_MODE is on (default), always responds — never silence.
    image_data: optional base64-encoded image to include with the latest user message.
    """
    cost_order = ["gemini", "claude", "openai"]
    priority = [config.CURRENT_PROVIDER] + [
        p for p in cost_order if p != config.CURRENT_PROVIDER
    ]
    # web_ai is always last resort (no API key, but text-only, no tool_use)
    if "web_ai" not in priority:
        priority.append("web_ai")

    # Inject image into messages if provided (vision support)
    if image_data:
        _inject_image_into_messages(messages, image_data)

    # Smart tool selection based on latest user message
    user_msg = ""
    for m in reversed(messages):
        content = m.get("content")
        if m.get("role") == "user":
            if isinstance(content, str):
                user_msg = content
                break
            elif isinstance(content, list):
                # Multi-part message (with image) — extract text part
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        user_msg = part["text"]
                        break
                if user_msg:
                    break
    selected_tools = _select_tools(user_msg)
    logger.info(f"Smart tools: {len(selected_tools)}/{len(TOOL_DEFINITIONS)} for '{user_msg[:50]}'")

    tried = []
    for provider in priority:
        fn = PROVIDER_FNS.get(provider)
        if not fn:
            continue

        logger.info(f"Trying provider: {provider}")
        try:
            success, error = await fn(messages, chat_id, context, selected_tools)
        except Exception as e:
            logger.error(f"{provider} crashed unexpectedly: {e}", exc_info=True)
            success, error = False, str(e)

        if success:
            if provider != config.CURRENT_PROVIDER:
                config.CURRENT_PROVIDER = provider
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"Switched to {PROVIDER_DISPLAY.get(provider, provider)}",
                )
            return True

        tried.append(provider)

        if error in ("no_key", "no_package", "no_platforms"):
            logger.info(f"{provider}: unavailable ({error}), skipping")
            continue

        if error in ("billing", "auth"):
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"{PROVIDER_DISPLAY.get(provider, provider)} unavailable ({'billing' if error == 'billing' else 'auth'}), switching...",
            )
            continue

        # Unknown error — still try next
        logger.error(f"{provider} error: {error}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"{PROVIDER_DISPLAY.get(provider, provider)} error, trying next...",
        )

    # ─── NEVER-DIE: All providers failed ─────────────────────────────────────
    never_die = getattr(config, "NEVER_DIE_MODE", True)

    if never_die:
        # Try cached/pattern-based command execution first
        if user_msg:
            cached_ok = await _execute_cached_command(user_msg, chat_id, context)
            if cached_ok:
                logger.info("Never-Die: handled via cached command pattern")
                return True

        # Last resort: send helpful template response (NEVER silence)
        retry_secs = getattr(config, "AUTO_RETRY_SECONDS", 60)
        await context.bot.send_message(
            chat_id=chat_id,
            text=_NEVER_DIE_TEMPLATE.format(retry=retry_secs),
        )
        logger.warning(f"Never-Die: all {len(tried)} providers failed, sent template response")
        return True  # Return True so the bot doesn't crash — we DID respond
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text="All AI services (including free web) are unavailable.\nCheck that Chrome is logged into ChatGPT/Claude.ai/Gemini.",
        )
        return False
