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
import config
from tools import TOOL_DEFINITIONS, execute_tool
from safety import is_dangerous, request_permission

logger = logging.getLogger(__name__)

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
# Instead of sending ALL 39 tools (huge token cost), select relevant subset

# Core tools always available
CORE_TOOLS = {
    "run_command", "take_screenshot", "open_url", "open_application",
}

# Tool groups activated by keyword matching
TOOL_GROUPS = {
    "browser": {
        "keywords": ["网页", "网站", "浏览器", "搜索", "google", "search", "website", "browse",
                      "click", "点击", "登录", "login", "sign", "form", "表单", "tradingview",
                      "youtube", "github", "reddit", "twitter", "facebook", "amazon",
                      "browser", "page", "页面", "链接", "link", "url", "playwright",
                      "zillow", "房子", "house", "apartment", "real estate"],
        "tools": {"browser_navigate", "browser_click", "browser_type",
                  "browser_screenshot", "browser_get_text", "browser_get_elements",
                  "browser_scroll", "browser_go_back", "browser_tabs",
                  "browser_new_tab", "browser_switch_tab", "browser_eval_js",
                  "browser_close_tab", "browser_wait_for"},
    },
    "gui": {
        "keywords": ["点击", "click", "鼠标", "mouse", "键盘", "key", "type", "打字",
                      "输入", "拖", "drag", "滚动", "scroll", "截图", "screenshot",
                      "屏幕", "screen", "窗口", "window", "桌面", "desktop"],
        "tools": {"mouse_click", "mouse_scroll", "mouse_move", "mouse_drag",
                  "type_text", "press_key", "get_screen_size", "get_active_window",
                  "list_windows", "focus_window", "set_clipboard", "get_clipboard",
                  "wait"},
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
    tool_defs = tool_defs or TOOL_DEFINITIONS
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
    tool_defs = tool_defs or TOOL_DEFINITIONS
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
    try:
        result_text, screenshot_buffer = await execute_tool(tool_name, tool_input)
    except Exception as e:
        return f"Tool execution error: {e}"
    if screenshot_buffer:
        try:
            await context.bot.send_photo(chat_id=chat_id, photo=screenshot_buffer)
        except Exception as e:
            result_text += f"\n(Screenshot send failed: {e})"
    return result_text


async def _send_text(text, chat_id, context, parse_mode=None):
    if not text or not text.strip():
        return
    remaining = text
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

async def process_claude(messages, chat_id, context, selected_tools=None):
    """Returns (success: bool, error_type: str|None)"""
    if not config.ANTHROPIC_API_KEY:
        return False, "no_key"
    try:
        import anthropic
    except ImportError:
        return False, "no_package"

    tools = selected_tools or TOOL_DEFINITIONS
    client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    for _ in range(config.MAX_TOOL_ITERATIONS):
        try:
            response = client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=8192,
                system=SYSTEM_PROMPT,
                tools=tools,
                messages=messages,
            )
        except Exception as e:
            err = str(e).lower()
            if "credit" in err or "billing" in err or "balance" in err:
                return False, "billing"
            if "authentication" in err or "invalid x-api-key" in err:
                return False, "auth"
            return False, str(e)

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            for block in response.content:
                if block.type == "text" and block.text.strip():
                    await _send_text(block.text, chat_id, context, parse_mode="Markdown")
            return True, None

        elif response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type == "text" and block.text.strip():
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
                logger.info(f"[Claude] Tool: {block.name}({json.dumps(block.input, ensure_ascii=False)[:200]})")
                result = await _run_tool(block.name, block.input, chat_id, context)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": result,
                })
            messages.append({"role": "user", "content": tool_results})

    return True, None


# ─── OpenAI ───────────────────────────────────────────────────────────────────

async def process_openai(messages, chat_id, context, selected_tools=None):
    if not config.OPENAI_API_KEY:
        return False, "no_key"
    try:
        from openai import AsyncOpenAI
    except ImportError:
        return False, "no_package"

    client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)

    # Build OpenAI message list from simple text history
    # Only include text-only messages to avoid broken tool_call/tool_result pairs
    # from other providers (Claude's native format uses list content)
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
            # Skip list-content messages (Claude native tool_use/tool_result format)
            # and also skip the next message if it's a tool_result response to avoid
            # unpaired tool_call messages that would cause OpenAI API errors
            if m["role"] == "assistant":
                # Only skip the next message if it is also a list (tool_result pair).
                # If the next message is a plain string, it should not be skipped.
                if i + 1 < len(messages) and isinstance(messages[i + 1]["content"], list):
                    skip_next = True

    for _ in range(config.MAX_TOOL_ITERATIONS):
        try:
            resp = await client.chat.completions.create(
                model=config.OPENAI_MODEL,
                messages=oai_msgs,
                tools=_tools_openai(selected_tools),
                max_tokens=8192,
            )
        except Exception as e:
            err = str(e).lower()
            if "credit" in err or "billing" in err or "quota" in err or "insufficient" in err:
                return False, "billing"
            if "incorrect api key" in err or "authentication" in err:
                return False, "auth"
            return False, str(e)

        msg = resp.choices[0].message
        oai_msgs.append(msg)
        finish = resp.choices[0].finish_reason

        if finish == "stop":
            if msg.content:
                await _send_text(msg.content, chat_id, context)
            return True, None

        elif finish == "tool_calls":
            tool_msgs = []
            for tc in msg.tool_calls or []:
                try:
                    tool_input = json.loads(tc.function.arguments)
                except Exception:
                    tool_input = {}

                if not await _safety_ok(tc.function.name, tool_input, chat_id, context):
                    tool_msgs.append({
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": "Action denied by user.",
                    })
                    continue

                logger.info(f"[OpenAI] Tool: {tc.function.name}({tc.function.arguments[:200]})")
                result = await _run_tool(tc.function.name, tool_input, chat_id, context)
                tool_msgs.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })
            oai_msgs.extend(tool_msgs)

    return True, None


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

    # Build contents list (text only, provider-agnostic)
    contents = []
    for m in messages:
        if isinstance(m["content"], str) and m["content"].strip():
            role = "user" if m["role"] == "user" else "model"
            contents.append(gtypes.Content(role=role, parts=[gtypes.Part(text=m["content"])]))

    if not contents:
        return False, "empty"

    cfg = gtypes.GenerateContentConfig(
        system_instruction=SYSTEM_PROMPT_SHORT,
        tools=_tools_gemini(selected_tools),
    )

    loop = asyncio.get_running_loop()
    for _ in range(config.MAX_TOOL_ITERATIONS):
        try:
            response = await loop.run_in_executor(
                None,
                # Note: the default arg `c=contents` captures the list reference.
                # Since `contents` is mutated in-place (appended to each iteration),
                # the lambda always sees the latest state, which is the intended behavior.
                lambda c=contents: client.models.generate_content(
                    model=config.GEMINI_MODEL,
                    contents=c,
                    config=cfg,
                )
            )
        except Exception as e:
            err = str(e).lower()
            if "429" in err or "resource_exhausted" in err:
                # Rate limit — retry once after short delay
                logger.warning("Gemini rate limited, retrying in 5s...")
                await asyncio.sleep(5)
                try:
                    response = await loop.run_in_executor(
                        None,
                        lambda c=contents: client.models.generate_content(
                            model=config.GEMINI_MODEL,
                            contents=c,
                            config=cfg,
                        )
                    )
                except Exception:
                    return False, "billing"
            elif "quota" in err or "billing" in err:
                return False, "billing"
            elif ("api_key_invalid" in err or "api key not valid" in err or
                    "permission_denied" in err):
                return False, "auth"
            elif "not found" in err or "model" in err and "not" in err:
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
                tool_input = dict(fc.args) if fc.args else {}

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
                await _send_text(text, chat_id, context)
            return True, None

    return True, None


# ─── Auto-switching router ────────────────────────────────────────────────────

PROVIDER_FNS = {
    "claude": process_claude,
    "openai": process_openai,
    "gemini": process_gemini,
}


async def process_with_auto_fallback(messages, chat_id, context):
    """Try providers in priority order. Auto-switch on billing/auth errors."""
    cost_order = ["gemini", "claude", "openai"]
    priority = [config.CURRENT_PROVIDER] + [
        p for p in cost_order if p != config.CURRENT_PROVIDER
    ]

    # Smart tool selection based on latest user message
    user_msg = ""
    for m in reversed(messages):
        if m.get("role") == "user" and isinstance(m.get("content"), str):
            user_msg = m["content"]
            break
    selected_tools = _select_tools(user_msg)
    logger.info(f"Smart tools: {len(selected_tools)}/{len(TOOL_DEFINITIONS)} for '{user_msg[:50]}'")

    tried = []
    for provider in priority:
        fn = PROVIDER_FNS.get(provider)
        if not fn:
            continue

        logger.info(f"Trying provider: {provider}")
        success, error = await fn(messages, chat_id, context, selected_tools)

        if success:
            if provider != config.CURRENT_PROVIDER:
                old = config.CURRENT_PROVIDER
                config.CURRENT_PROVIDER = provider
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"✅ 已自动切换到 {PROVIDER_DISPLAY[provider]}",
                )
            return True

        tried.append(provider)

        if error == "no_key":
            logger.info(f"{provider}: no API key, skipping")
            continue

        if error == "no_package":
            logger.info(f"{provider}: package not installed, skipping")
            continue

        if error in ("billing", "auth"):
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"⚠️ {PROVIDER_DISPLAY[provider]} 无法使用（{'余额不足' if error == 'billing' else '认证失败'}），自动切换...",
            )
            continue

        # Unknown error — still try next
        logger.error(f"{provider} error: {error}")
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ {PROVIDER_DISPLAY[provider]} 出错，尝试下一个...",
        )

    await context.bot.send_message(
        chat_id=chat_id,
        text="❌ 所有 AI 服务都不可用。\n请检查 .env 里的 API key 和账户余额。",
    )
    return False
