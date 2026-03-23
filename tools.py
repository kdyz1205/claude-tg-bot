import os
import asyncio
import subprocess
import platform
import psutil
import json
import time
import config
from screenshots import capture_screenshot
import browser_agent

# ─── Tool Definitions (Anthropic canonical format) ────────────────────────────
# These get auto-converted for OpenAI / Gemini in providers.py

TOOL_DEFINITIONS = [
    # === Shell & System ===
    {
        "name": "run_command",
        "description": "Execute a shell command on the Windows 11 computer. Use PowerShell syntax by default. Returns stdout and stderr. Good for: installing software, running scripts, git operations, npm/pip, file operations, checking ports, network diagnostics, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "command": {
                    "type": "string",
                    "description": "The command to execute (PowerShell or CMD)",
                },
                "shell": {
                    "type": "string",
                    "enum": ["powershell", "cmd"],
                    "description": "Which shell to use (default: powershell)",
                },
                "working_directory": {
                    "type": "string",
                    "description": "Working directory for the command. Defaults to user home.",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in seconds (default: 30, max: 300)",
                },
            },
            "required": ["command"],
        },
    },
    {
        "name": "get_system_info",
        "description": "Get current system information: OS, CPU usage, memory usage, disk usage, running processes, and network interfaces.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "manage_processes",
        "description": "List or search running processes. Can filter by name. Useful for finding what's running, checking if an app is open, finding PIDs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "search"],
                    "description": "Action to perform (default: list top processes)",
                },
                "query": {
                    "type": "string",
                    "description": "Process name to search for (for 'search' action)",
                },
            },
            "required": [],
        },
    },
    # === Screenshot & Screen ===
    {
        "name": "take_screenshot",
        "description": "Capture a screenshot of the current screen. ALWAYS use this after performing GUI actions to verify the result. You should call this frequently to see what's on screen.",
        "input_schema": {
            "type": "object",
            "properties": {
                "region": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Optional [x, y, width, height] region to capture. Omit for full screen.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "get_screen_size",
        "description": "Get the screen resolution (width, height). Use this before clicking to know the coordinate space.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    # === Application & Window Management ===
    {
        "name": "open_application",
        "description": "Open an application by name or path. Common names: chrome, firefox, notepad, vscode, code, explorer, terminal, calc, word, excel, paint, taskmgr.",
        "input_schema": {
            "type": "object",
            "properties": {
                "app_name": {
                    "type": "string",
                    "description": "Application name (e.g., 'chrome', 'notepad', 'vscode') or full path",
                },
            },
            "required": ["app_name"],
        },
    },
    {
        "name": "open_url",
        "description": "Open a URL in the default web browser. ALWAYS use this tool when the user wants to visit a website. Do NOT use type_text or run_command to open URLs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL to open (e.g., 'https://www.google.com' or 'google.com')",
                },
            },
            "required": ["url"],
        },
    },
    {
        "name": "get_active_window",
        "description": "Get information about the currently active/focused window: title, position, size.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "list_windows",
        "description": "List all visible windows with their titles, positions, and sizes. Useful for finding which windows are open and their locations.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "focus_window",
        "description": "Bring a window to the foreground by its title (partial match). Use list_windows first to see available windows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Window title or partial title to match",
                },
            },
            "required": ["title"],
        },
    },
    # === Keyboard & Mouse ===
    {
        "name": "type_text",
        "description": "Type text at the current cursor position. For ASCII text only. For non-ASCII (Chinese, special chars), use set_clipboard + press_key ctrl+v instead.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "The text to type (ASCII only for reliable input)",
                },
                "interval": {
                    "type": "number",
                    "description": "Seconds between each keystroke (default: 0.02)",
                },
            },
            "required": ["text"],
        },
    },
    {
        "name": "press_key",
        "description": "Press a keyboard shortcut or key. Use pyautogui key names. Common: enter, tab, escape, backspace, delete, space, up, down, left, right, home, end, pageup, pagedown, f1-f12. Combos with +: ctrl+c, ctrl+v, ctrl+z, ctrl+s, ctrl+a, ctrl+shift+esc, alt+tab, alt+f4, win+d, win+e, win+r.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keys": {
                    "type": "string",
                    "description": "Key or key combination (e.g., 'enter', 'ctrl+c', 'alt+f4')",
                },
                "presses": {
                    "type": "integer",
                    "description": "Number of times to press (default: 1)",
                },
            },
            "required": ["keys"],
        },
    },
    {
        "name": "mouse_click",
        "description": "Click the mouse at specific screen coordinates. Always take_screenshot first to identify where to click.",
        "input_schema": {
            "type": "object",
            "properties": {
                "x": {"type": "integer", "description": "X coordinate"},
                "y": {"type": "integer", "description": "Y coordinate"},
                "button": {
                    "type": "string",
                    "enum": ["left", "right", "middle"],
                    "description": "Mouse button (default: left)",
                },
                "clicks": {
                    "type": "integer",
                    "description": "Number of clicks (1=single, 2=double, 3=triple)",
                },
            },
            "required": ["x", "y"],
        },
    },
    {
        "name": "mouse_scroll",
        "description": "Scroll the mouse wheel at current position or specified coordinates. Positive = scroll up, negative = scroll down.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amount": {
                    "type": "integer",
                    "description": "Scroll amount. Positive=up, negative=down. Typically 3-5 for a normal scroll.",
                },
                "x": {"type": "integer", "description": "Optional X coordinate to scroll at"},
                "y": {"type": "integer", "description": "Optional Y coordinate to scroll at"},
            },
            "required": ["amount"],
        },
    },
    {
        "name": "mouse_move",
        "description": "Move the mouse to specific coordinates without clicking.",
        "input_schema": {
            "type": "object",
            "properties": {
                "x": {"type": "integer", "description": "X coordinate"},
                "y": {"type": "integer", "description": "Y coordinate"},
            },
            "required": ["x", "y"],
        },
    },
    {
        "name": "mouse_drag",
        "description": "Drag the mouse from one position to another (click and hold, then release). Good for drag-and-drop, selecting text, resizing windows.",
        "input_schema": {
            "type": "object",
            "properties": {
                "start_x": {"type": "integer", "description": "Start X coordinate"},
                "start_y": {"type": "integer", "description": "Start Y coordinate"},
                "end_x": {"type": "integer", "description": "End X coordinate"},
                "end_y": {"type": "integer", "description": "End Y coordinate"},
                "button": {
                    "type": "string",
                    "enum": ["left", "right"],
                    "description": "Mouse button (default: left)",
                },
                "duration": {
                    "type": "number",
                    "description": "Duration of drag in seconds (default: 0.5)",
                },
            },
            "required": ["start_x", "start_y", "end_x", "end_y"],
        },
    },
    # === Clipboard ===
    {
        "name": "set_clipboard",
        "description": "Set the system clipboard content. Use this + press_key ctrl+v to paste non-ASCII text (Chinese, special characters, code blocks, etc.) into any application.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Text to put on the clipboard",
                },
            },
            "required": ["text"],
        },
    },
    {
        "name": "get_clipboard",
        "description": "Get the current clipboard content. Useful to see what was copied.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    # === File Operations ===
    {
        "name": "list_files",
        "description": "List files and folders in a directory with details (size, modification date).",
        "input_schema": {
            "type": "object",
            "properties": {
                "directory": {
                    "type": "string",
                    "description": "Directory path to list",
                },
                "recursive": {
                    "type": "boolean",
                    "description": "If true, list recursively (max 200 entries)",
                },
                "pattern": {
                    "type": "string",
                    "description": "File pattern filter (e.g., '*.py', '*.js')",
                },
            },
            "required": ["directory"],
        },
    },
    {
        "name": "read_file",
        "description": "Read the contents of a text file. Supports line range for large files.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file",
                },
                "start_line": {
                    "type": "integer",
                    "description": "Start reading from this line (1-based, default: 1)",
                },
                "end_line": {
                    "type": "integer",
                    "description": "Read up to this line (default: start_line + 200)",
                },
            },
            "required": ["path"],
        },
    },
    {
        "name": "write_file",
        "description": "Write or create a file with the given content. Overwrites if exists.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file to create/write",
                },
                "content": {
                    "type": "string",
                    "description": "Content to write to the file",
                },
            },
            "required": ["path", "content"],
        },
    },
    {
        "name": "edit_file",
        "description": "Edit a file by replacing a specific string with another. More precise than write_file for modifications.",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the file to edit",
                },
                "old_text": {
                    "type": "string",
                    "description": "The exact text to find and replace",
                },
                "new_text": {
                    "type": "string",
                    "description": "The replacement text",
                },
                "replace_all": {
                    "type": "boolean",
                    "description": "Replace all occurrences (default: false, replaces first only)",
                },
            },
            "required": ["path", "old_text", "new_text"],
        },
    },
    {
        "name": "search_files",
        "description": "Search for text/pattern in files within a directory. Like grep/ripgrep. Returns matching lines with file paths and line numbers.",
        "input_schema": {
            "type": "object",
            "properties": {
                "directory": {
                    "type": "string",
                    "description": "Directory to search in",
                },
                "pattern": {
                    "type": "string",
                    "description": "Text or regex pattern to search for",
                },
                "file_pattern": {
                    "type": "string",
                    "description": "File glob pattern to filter (e.g., '*.py', '*.js'). Default: all files.",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum number of results (default: 50)",
                },
            },
            "required": ["directory", "pattern"],
        },
    },
    {
        "name": "find_files",
        "description": "Find files by name pattern in a directory tree. Like the 'find' command.",
        "input_schema": {
            "type": "object",
            "properties": {
                "directory": {
                    "type": "string",
                    "description": "Root directory to search from",
                },
                "name_pattern": {
                    "type": "string",
                    "description": "Filename pattern (e.g., '*.py', 'config*', 'package.json')",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Maximum results (default: 50)",
                },
            },
            "required": ["directory", "name_pattern"],
        },
    },
    # === Wait / Timing ===
    {
        "name": "wait",
        "description": "Wait for a specified duration. Use this when you need to wait for an application to load, a page to render, or an animation to complete before taking a screenshot or performing the next action.",
        "input_schema": {
            "type": "object",
            "properties": {
                "seconds": {
                    "type": "number",
                    "description": "Seconds to wait (0.1 to 30)",
                },
                "reason": {
                    "type": "string",
                    "description": "Why we're waiting (for logging)",
                },
            },
            "required": ["seconds"],
        },
    },
    # === Browser Automation (Playwright) ===
    {
        "name": "browser_navigate",
        "description": "Navigate the automated browser to a URL. This opens a REAL browser window (Chromium) that you can fully control. Better than open_url for tasks that need interaction. Use this for: searching, filling forms, reading web content, clicking links.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL to navigate to (e.g., 'google.com', 'https://tradingview.com')",
                },
            },
            "required": ["url"],
        },
    },
    {
        "name": "browser_click",
        "description": "Click an element in the browser by CSS selector or visible text. Examples: 'Sign in', '#login-button', '.nav-link', 'button[type=submit]'. Much more reliable than screenshot+mouse_click for web interactions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "selector": {
                    "type": "string",
                    "description": "CSS selector or visible text of the element to click",
                },
            },
            "required": ["selector"],
        },
    },
    {
        "name": "browser_type",
        "description": "Type text into a form field in the browser. Identifies fields by CSS selector, placeholder text, or label. Supports non-ASCII (Chinese etc.).",
        "input_schema": {
            "type": "object",
            "properties": {
                "selector": {
                    "type": "string",
                    "description": "CSS selector, placeholder text, or label of the input field",
                },
                "text": {
                    "type": "string",
                    "description": "Text to type into the field",
                },
                "press_enter": {
                    "type": "boolean",
                    "description": "Press Enter after typing (default: false)",
                },
            },
            "required": ["selector", "text"],
        },
    },
    {
        "name": "browser_screenshot",
        "description": "Take a screenshot of the current browser page (not the whole screen, just the browser). Faster and cleaner than take_screenshot for web tasks.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "browser_get_text",
        "description": "Get all visible text content of the current browser page. Good for reading articles, search results, documentation, etc.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "browser_get_elements",
        "description": "List interactive elements (links, buttons, inputs) on the current page. Helps you know what you can click/interact with.",
        "input_schema": {
            "type": "object",
            "properties": {
                "selector": {
                    "type": "string",
                    "description": "Optional CSS selector to filter elements. Omit to get all interactive elements.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "browser_scroll",
        "description": "Scroll the browser page up or down.",
        "input_schema": {
            "type": "object",
            "properties": {
                "direction": {
                    "type": "string",
                    "enum": ["up", "down"],
                    "description": "Scroll direction (default: down)",
                },
                "amount": {
                    "type": "integer",
                    "description": "Scroll amount (1-10, default: 3)",
                },
            },
            "required": [],
        },
    },
    {
        "name": "browser_go_back",
        "description": "Go back to the previous page in the browser.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "browser_tabs",
        "description": "List all open browser tabs.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "browser_new_tab",
        "description": "Open a new browser tab, optionally with a URL.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "Optional URL to open in the new tab",
                },
            },
            "required": [],
        },
    },
    {
        "name": "browser_switch_tab",
        "description": "Switch to a browser tab by index number. Use browser_tabs to see available tabs.",
        "input_schema": {
            "type": "object",
            "properties": {
                "index": {
                    "type": "integer",
                    "description": "Tab index (0-based)",
                },
            },
            "required": ["index"],
        },
    },
    {
        "name": "browser_close_tab",
        "description": "Close the current browser tab.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "browser_eval_js",
        "description": "Execute JavaScript code in the browser page. Useful for advanced interactions, extracting data, or manipulating the page.",
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "JavaScript code to execute",
                },
            },
            "required": ["code"],
        },
    },
    {
        "name": "browser_wait_for",
        "description": "Wait for a specific element to appear on the page. Useful after navigation or clicking that triggers loading.",
        "input_schema": {
            "type": "object",
            "properties": {
                "selector": {
                    "type": "string",
                    "description": "CSS selector to wait for",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Max wait time in milliseconds (default: 10000)",
                },
            },
            "required": ["selector"],
        },
    },
]

# App name -> executable mapping
APP_ALIASES = {
    "chrome": "chrome",
    "google chrome": "chrome",
    "google": "chrome",
    "firefox": "firefox",
    "edge": "msedge",
    "microsoft edge": "msedge",
    "notepad": "notepad",
    "notepad++": "notepad++",
    "calculator": "calc",
    "calc": "calc",
    "explorer": "explorer",
    "file explorer": "explorer",
    "cmd": "cmd",
    "terminal": "wt",
    "windows terminal": "wt",
    "powershell": "powershell",
    "task manager": "taskmgr",
    "paint": "mspaint",
    "word": "winword",
    "excel": "excel",
    "powerpoint": "powerpnt",
    "outlook": "outlook",
    "vscode": "code",
    "code": "code",
    "visual studio code": "code",
    "spotify": "spotify",
    "discord": "discord",
    "slack": "slack",
    "teams": "teams",
    "obs": "obs64",
    "cursor": "cursor",
}


# ─── Tool Implementations ────────────────────────────────────────────────────

async def execute_run_command(command: str, shell: str = "powershell",
                               working_directory: str = None, timeout: int = None) -> str:
    """Execute a shell command and return output."""
    try:
        timeout = min(timeout or config.COMMAND_TIMEOUT, 300)
        cwd = working_directory or os.path.expanduser("~")

        if shell == "powershell":
            args = ["powershell", "-NoProfile", "-Command", command]
        else:
            args = ["cmd", "/c", command]

        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None,
            lambda: subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=cwd,
            ),
        )

        output = ""
        if result.stdout:
            output += result.stdout
        if result.stderr:
            output += f"\n[STDERR]: {result.stderr}"
        if result.returncode != 0:
            output += f"\n[Exit code: {result.returncode}]"

        return output.strip() or "(no output)"
    except subprocess.TimeoutExpired:
        return f"Command timed out after {timeout} seconds."
    except Exception as e:
        return f"Error executing command: {e}"


def execute_take_screenshot(region=None):
    """Capture a screenshot. Returns the BytesIO buffer."""
    return capture_screenshot(region=region)


def execute_get_screen_size() -> str:
    """Get screen resolution."""
    import pyautogui
    w, h = pyautogui.size()
    return f"Screen resolution: {w} x {h}"


async def execute_open_application(app_name: str) -> str:
    """Open an application by name or path."""
    try:
        resolved = APP_ALIASES.get(app_name.lower(), app_name)
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: os.startfile(resolved))
        return f"Opened '{app_name}' (resolved to '{resolved}')."
    except Exception as e:
        try:
            result = await execute_run_command(f"Start-Process '{app_name}'")
            return f"Opened '{app_name}' via Start-Process. {result}"
        except Exception as e2:
            return f"Failed to open '{app_name}': {e} | {e2}"


async def execute_open_url(url: str) -> str:
    """Open a URL in the default browser and bring it to foreground."""
    # Ensure URL has a scheme
    if not url.startswith(("http://", "https://", "file://")):
        url = "https://" + url
    try:
        # Use Start-Process which reliably opens AND focuses the browser on Windows
        result = await execute_run_command(f'Start-Process "{url}"')
        # Give the browser a moment to open
        await asyncio.sleep(1.5)
        return f"Opened URL in browser: {url}"
    except Exception as e:
        # Fallback to webbrowser module
        import webbrowser
        try:
            webbrowser.open(url)
            await asyncio.sleep(1.5)
            return f"Opened URL: {url}"
        except Exception as e2:
            return f"Failed to open URL '{url}': {e2}"


def execute_get_active_window() -> str:
    """Get info about the active window."""
    import pyautogui
    try:
        win = pyautogui.getActiveWindow()
        if win:
            return (f"Active window: '{win.title}'\n"
                    f"Position: ({win.left}, {win.top})\n"
                    f"Size: {win.width} x {win.height}")
        return "No active window found."
    except Exception as e:
        return f"Error getting active window: {e}"


async def execute_list_windows() -> str:
    """List all visible windows."""
    try:
        result = await execute_run_command(
            'Get-Process | Where-Object {$_.MainWindowTitle -ne ""} | '
            'Select-Object Id, ProcessName, MainWindowTitle | '
            'Format-Table -AutoSize | Out-String -Width 200'
        )
        return result
    except Exception as e:
        return f"Error listing windows: {e}"


async def execute_focus_window(title: str) -> str:
    """Focus a window by title."""
    try:
        ps_cmd = (
            f'$w = Get-Process | Where-Object {{$_.MainWindowTitle -like "*{title}*"}} | Select-Object -First 1; '
            f'if ($w) {{ '
            f'  Add-Type -TypeDefinition \'using System; using System.Runtime.InteropServices; '
            f'  public class Win32 {{ [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd); '
            f'  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow); }}\'; '
            f'  [Win32]::ShowWindow($w.MainWindowHandle, 9); '
            f'  [Win32]::SetForegroundWindow($w.MainWindowHandle); '
            f'  "Focused: $($w.MainWindowTitle)" '
            f'}} else {{ "No window found matching: {title}" }}'
        )
        result = await execute_run_command(ps_cmd)
        return result
    except Exception as e:
        return f"Error focusing window: {e}"


def execute_type_text(text: str, interval: float = 0.02) -> str:
    """Type text at current cursor position."""
    import pyautogui
    try:
        pyautogui.write(text, interval=interval)
        return f"Typed {len(text)} characters."
    except Exception as e:
        return f"Error typing text: {e}"


def execute_press_key(keys: str, presses: int = 1) -> str:
    """Press a key or key combination."""
    import pyautogui
    try:
        key_list = [k.strip().lower() for k in keys.split("+")]
        for _ in range(presses):
            if len(key_list) == 1:
                pyautogui.press(key_list[0])
            else:
                pyautogui.hotkey(*key_list)
        return f"Pressed '{keys}' x{presses}."
    except Exception as e:
        return f"Error pressing key: {e}"


def execute_mouse_click(x: int, y: int, button: str = "left", clicks: int = 1) -> str:
    """Click mouse at coordinates."""
    import pyautogui
    try:
        pyautogui.click(x=x, y=y, button=button, clicks=clicks)
        return f"Clicked ({x}, {y}) with {button} button ({clicks}x)."
    except Exception as e:
        return f"Error clicking: {e}"


def execute_mouse_scroll(amount: int, x: int = None, y: int = None) -> str:
    """Scroll mouse wheel."""
    import pyautogui
    try:
        kwargs = {"clicks": amount}
        if x is not None and y is not None:
            kwargs["x"] = x
            kwargs["y"] = y
        pyautogui.scroll(**kwargs)
        direction = "up" if amount > 0 else "down"
        return f"Scrolled {direction} by {abs(amount)} clicks."
    except Exception as e:
        return f"Error scrolling: {e}"


def execute_mouse_move(x: int, y: int) -> str:
    """Move mouse to coordinates."""
    import pyautogui
    try:
        pyautogui.moveTo(x, y)
        return f"Mouse moved to ({x}, {y})."
    except Exception as e:
        return f"Error moving mouse: {e}"


def execute_mouse_drag(start_x: int, start_y: int, end_x: int, end_y: int,
                        button: str = "left", duration: float = 0.5) -> str:
    """Drag mouse from start to end."""
    import pyautogui
    try:
        pyautogui.moveTo(start_x, start_y)
        pyautogui.drag(end_x - start_x, end_y - start_y, duration=duration, button=button)
        return f"Dragged from ({start_x},{start_y}) to ({end_x},{end_y})."
    except Exception as e:
        return f"Error dragging: {e}"


async def execute_set_clipboard(text: str) -> str:
    """Set clipboard content via PowerShell."""
    try:
        # Write to temp file to avoid escaping issues
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(text)
            tmp_path = f.name
        await execute_run_command(
            f'Get-Content -Path "{tmp_path}" -Raw | Set-Clipboard',
        )
        os.unlink(tmp_path)
        return f"Clipboard set ({len(text)} chars). Use Ctrl+V to paste."
    except Exception as e:
        return f"Error setting clipboard: {e}"


async def execute_get_clipboard() -> str:
    """Get clipboard content."""
    try:
        result = await execute_run_command("Get-Clipboard")
        return f"Clipboard content:\n{result}"
    except Exception as e:
        return f"Error getting clipboard: {e}"


def execute_list_files(directory: str, recursive: bool = False, pattern: str = None) -> str:
    """List files in a directory."""
    try:
        import fnmatch
        from datetime import datetime
        entries = []

        if recursive:
            count = 0
            for root, dirs, files in os.walk(directory):
                # Skip hidden and common ignore dirs
                dirs[:] = [d for d in dirs if not d.startswith('.') and d not in
                           ('node_modules', '__pycache__', '.git', 'venv', '.venv')]
                for name in files:
                    if pattern and not fnmatch.fnmatch(name, pattern):
                        continue
                    full = os.path.join(root, name)
                    rel = os.path.relpath(full, directory)
                    try:
                        stat = os.stat(full)
                        size = _format_size(stat.st_size)
                        mtime = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M')
                        entries.append(f"  {size:>10s}  {mtime}  {rel}")
                    except:
                        entries.append(f"{'':>10s}  {'':16s}  {rel}")
                    count += 1
                    if count >= 200:
                        entries.append(f"\n... (truncated at 200 entries)")
                        return "\n".join(entries)
        else:
            items = os.listdir(directory)
            for name in sorted(items):
                if pattern and not fnmatch.fnmatch(name, pattern):
                    continue
                full = os.path.join(directory, name)
                try:
                    stat = os.stat(full)
                    is_dir = os.path.isdir(full)
                    size = "<DIR>" if is_dir else _format_size(stat.st_size)
                    mtime = datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M')
                    entries.append(f"  {size:>10s}  {mtime}  {name}")
                except:
                    entries.append(f"{'':>10s}  {'':16s}  {name}")

        if not entries:
            return "(empty directory or no matches)"
        return "\n".join(entries)
    except Exception as e:
        return f"Error listing directory: {e}"


def _format_size(size_bytes):
    for unit in ('B', 'KB', 'MB', 'GB'):
        if size_bytes < 1024:
            return f"{size_bytes:.0f}{unit}" if unit == 'B' else f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f}TB"


def execute_read_file(path: str, start_line: int = 1, end_line: int = None) -> str:
    """Read a text file with optional line range."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            all_lines = f.readlines()

        total = len(all_lines)
        start = max(1, start_line) - 1  # 0-indexed
        end = min(end_line or (start + 200), total)

        lines = all_lines[start:end]
        numbered = []
        for i, line in enumerate(lines, start=start + 1):
            numbered.append(f"{i:4d} | {line.rstrip()}")

        content = "\n".join(numbered)
        header = f"File: {path} ({total} lines total, showing {start+1}-{end})\n"
        if end < total:
            content += f"\n... ({total - end} more lines)"
        return header + content or "(empty file)"
    except Exception as e:
        return f"Error reading file: {e}"


def execute_write_file(path: str, content: str) -> str:
    """Write content to a file."""
    try:
        # Create directories if needed
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        lines = content.count('\n') + 1
        return f"File written: {path} ({len(content)} chars, {lines} lines)"
    except Exception as e:
        return f"Error writing file: {e}"


def execute_edit_file(path: str, old_text: str, new_text: str, replace_all: bool = False) -> str:
    """Edit a file by replacing text."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        if old_text not in content:
            return f"Error: old_text not found in {path}. Make sure the text matches exactly (including whitespace)."

        if replace_all:
            count = content.count(old_text)
            new_content = content.replace(old_text, new_text)
        else:
            count = 1
            new_content = content.replace(old_text, new_text, 1)

        with open(path, "w", encoding="utf-8") as f:
            f.write(new_content)

        return f"Edited {path}: replaced {count} occurrence(s)."
    except Exception as e:
        return f"Error editing file: {e}"


async def execute_search_files(directory: str, pattern: str,
                                file_pattern: str = None, max_results: int = 50) -> str:
    """Search for text in files."""
    try:
        import re
        results = []
        count = 0

        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in
                       ('node_modules', '__pycache__', '.git', 'venv', '.venv', 'dist', 'build')]
            for fname in files:
                if file_pattern:
                    import fnmatch
                    if not fnmatch.fnmatch(fname, file_pattern):
                        continue
                # Skip binary files
                if any(fname.endswith(ext) for ext in ('.exe', '.dll', '.bin', '.jpg', '.png', '.gif', '.zip', '.tar', '.gz', '.pyc')):
                    continue

                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                        for line_num, line in enumerate(f, 1):
                            if re.search(pattern, line):
                                rel = os.path.relpath(fpath, directory)
                                results.append(f"{rel}:{line_num}: {line.rstrip()}")
                                count += 1
                                if count >= max_results:
                                    results.append(f"\n... (max {max_results} results reached)")
                                    return "\n".join(results)
                except:
                    pass

        if not results:
            return f"No matches found for '{pattern}' in {directory}"
        return "\n".join(results)
    except Exception as e:
        return f"Error searching files: {e}"


async def execute_find_files(directory: str, name_pattern: str, max_results: int = 50) -> str:
    """Find files by name pattern."""
    try:
        import fnmatch
        results = []
        count = 0

        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in
                       ('node_modules', '__pycache__', '.git', 'venv', '.venv')]
            for fname in files:
                if fnmatch.fnmatch(fname, name_pattern):
                    rel = os.path.relpath(os.path.join(root, fname), directory)
                    results.append(rel)
                    count += 1
                    if count >= max_results:
                        results.append(f"... (max {max_results} results reached)")
                        return "\n".join(results)
            # Also match directory names
            for dname in dirs:
                if fnmatch.fnmatch(dname, name_pattern):
                    rel = os.path.relpath(os.path.join(root, dname), directory)
                    results.append(f"[DIR] {rel}")

        if not results:
            return f"No files found matching '{name_pattern}' in {directory}"
        return "\n".join(results)
    except Exception as e:
        return f"Error finding files: {e}"


def execute_manage_processes(action: str = "list", query: str = None) -> str:
    """List or search processes."""
    try:
        procs = []
        for proc in psutil.process_iter(["pid", "name", "memory_percent", "cpu_percent", "status"]):
            try:
                procs.append(proc.info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        if action == "search" and query:
            procs = [p for p in procs if query.lower() in (p.get("name", "") or "").lower()]
            if not procs:
                return f"No processes found matching '{query}'."

        procs.sort(key=lambda p: p.get("memory_percent", 0) or 0, reverse=True)
        procs = procs[:30]

        lines = [f"{'PID':>8s}  {'Name':<30s}  {'Memory%':>8s}  {'Status':<12s}"]
        lines.append("-" * 65)
        for p in procs:
            lines.append(
                f"{p.get('pid','?'):>8}  {(p.get('name','') or '?'):<30s}  "
                f"{(p.get('memory_percent',0) or 0):>7.1f}%  {(p.get('status','') or '?'):<12s}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"Error listing processes: {e}"


async def execute_wait(seconds: float, reason: str = None) -> str:
    """Wait for a specified duration."""
    seconds = max(0.1, min(seconds, 30))
    await asyncio.sleep(seconds)
    msg = f"Waited {seconds}s"
    if reason:
        msg += f" ({reason})"
    return msg


def execute_get_system_info() -> str:
    """Get system information."""
    try:
        cpu_pct = psutil.cpu_percent(interval=1)
        mem = psutil.virtual_memory()
        disk = psutil.disk_usage("C:\\")

        info = [
            f"OS: {platform.system()} {platform.release()} ({platform.version()})",
            f"Machine: {platform.machine()}",
            f"CPU: {cpu_pct}% usage ({psutil.cpu_count()} cores)",
            f"Memory: {mem.percent}% used ({mem.used // (1024**3)}GB / {mem.total // (1024**3)}GB)",
            f"Disk C:\\: {disk.percent}% used ({disk.used // (1024**3)}GB / {disk.total // (1024**3)}GB)",
            "",
            "Top processes by memory:",
        ]

        procs = []
        for proc in psutil.process_iter(["pid", "name", "memory_percent"]):
            try:
                procs.append(proc.info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        procs.sort(key=lambda p: p.get("memory_percent", 0) or 0, reverse=True)
        for p in procs[:10]:
            info.append(f"  {p['name']} (PID {p['pid']}): {p.get('memory_percent', 0):.1f}% mem")

        return "\n".join(info)
    except Exception as e:
        return f"Error getting system info: {e}"


# Screenshot sentinel for the agent loop
SCREENSHOT_SENTINEL = "__SCREENSHOT__"


async def execute_tool(tool_name: str, tool_input: dict):
    """Dispatch a tool call. Returns (result_string, screenshot_buffer_or_None)."""
    screenshot_buffer = None

    if tool_name == "run_command":
        result = await execute_run_command(
            tool_input["command"],
            tool_input.get("shell", "powershell"),
            tool_input.get("working_directory"),
            tool_input.get("timeout"),
        )
    elif tool_name == "take_screenshot":
        screenshot_buffer = execute_take_screenshot(tool_input.get("region"))
        result = "Screenshot captured and sent to user."
    elif tool_name == "get_screen_size":
        result = execute_get_screen_size()
    elif tool_name == "open_application":
        result = await execute_open_application(tool_input["app_name"])
    elif tool_name == "open_url":
        result = await execute_open_url(tool_input["url"])
    elif tool_name == "get_active_window":
        result = execute_get_active_window()
    elif tool_name == "list_windows":
        result = await execute_list_windows()
    elif tool_name == "focus_window":
        result = await execute_focus_window(tool_input["title"])
    elif tool_name == "type_text":
        result = execute_type_text(
            tool_input["text"],
            tool_input.get("interval", 0.02),
        )
    elif tool_name == "press_key":
        result = execute_press_key(
            tool_input["keys"],
            tool_input.get("presses", 1),
        )
    elif tool_name == "mouse_click":
        result = execute_mouse_click(
            tool_input["x"],
            tool_input["y"],
            tool_input.get("button", "left"),
            tool_input.get("clicks", 1),
        )
    elif tool_name == "mouse_scroll":
        result = execute_mouse_scroll(
            tool_input["amount"],
            tool_input.get("x"),
            tool_input.get("y"),
        )
    elif tool_name == "mouse_move":
        result = execute_mouse_move(tool_input["x"], tool_input["y"])
    elif tool_name == "mouse_drag":
        result = execute_mouse_drag(
            tool_input["start_x"], tool_input["start_y"],
            tool_input["end_x"], tool_input["end_y"],
            tool_input.get("button", "left"),
            tool_input.get("duration", 0.5),
        )
    elif tool_name == "set_clipboard":
        result = await execute_set_clipboard(tool_input["text"])
    elif tool_name == "get_clipboard":
        result = await execute_get_clipboard()
    elif tool_name == "list_files":
        result = execute_list_files(
            tool_input["directory"],
            tool_input.get("recursive", False),
            tool_input.get("pattern"),
        )
    elif tool_name == "read_file":
        result = execute_read_file(
            tool_input["path"],
            tool_input.get("start_line", 1),
            tool_input.get("end_line"),
        )
    elif tool_name == "write_file":
        result = execute_write_file(tool_input["path"], tool_input["content"])
    elif tool_name == "edit_file":
        result = execute_edit_file(
            tool_input["path"],
            tool_input["old_text"],
            tool_input["new_text"],
            tool_input.get("replace_all", False),
        )
    elif tool_name == "search_files":
        result = await execute_search_files(
            tool_input["directory"],
            tool_input["pattern"],
            tool_input.get("file_pattern"),
            tool_input.get("max_results", 50),
        )
    elif tool_name == "find_files":
        result = await execute_find_files(
            tool_input["directory"],
            tool_input["name_pattern"],
            tool_input.get("max_results", 50),
        )
    elif tool_name == "manage_processes":
        result = execute_manage_processes(
            tool_input.get("action", "list"),
            tool_input.get("query"),
        )
    elif tool_name == "wait":
        result = await execute_wait(
            tool_input["seconds"],
            tool_input.get("reason"),
        )
    elif tool_name == "get_system_info":
        result = execute_get_system_info()
    # === Browser Automation (Playwright) ===
    elif tool_name == "browser_navigate":
        result = await browser_agent.browser_navigate(tool_input["url"])
    elif tool_name == "browser_click":
        result = await browser_agent.browser_click(tool_input["selector"])
    elif tool_name == "browser_type":
        result = await browser_agent.browser_type(
            tool_input["selector"],
            tool_input["text"],
            tool_input.get("press_enter", False),
        )
    elif tool_name == "browser_screenshot":
        buf = await browser_agent.browser_screenshot()
        if buf:
            screenshot_buffer = buf
            result = "Browser screenshot captured and sent to user."
        else:
            result = "Failed to capture browser screenshot."
    elif tool_name == "browser_get_text":
        result = await browser_agent.browser_get_text()
    elif tool_name == "browser_get_elements":
        result = await browser_agent.browser_get_elements(tool_input.get("selector"))
    elif tool_name == "browser_scroll":
        result = await browser_agent.browser_scroll(
            tool_input.get("direction", "down"),
            tool_input.get("amount", 3),
        )
    elif tool_name == "browser_go_back":
        result = await browser_agent.browser_go_back()
    elif tool_name == "browser_tabs":
        result = await browser_agent.browser_tabs()
    elif tool_name == "browser_new_tab":
        result = await browser_agent.browser_new_tab(tool_input.get("url"))
    elif tool_name == "browser_switch_tab":
        result = await browser_agent.browser_switch_tab(tool_input["index"])
    elif tool_name == "browser_close_tab":
        result = await browser_agent.browser_close_tab()
    elif tool_name == "browser_eval_js":
        result = await browser_agent.browser_eval_js(tool_input["code"])
    elif tool_name == "browser_wait_for":
        result = await browser_agent.browser_wait_for(
            tool_input["selector"],
            tool_input.get("timeout", 10000),
        )
    else:
        result = f"Unknown tool: {tool_name}"

    # Truncate very long results
    if len(result) > 15000:
        result = result[:15000] + "\n... (output truncated)"

    return result, screenshot_buffer
