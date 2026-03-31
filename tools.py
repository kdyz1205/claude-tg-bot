import os
import asyncio
import subprocess
import platform
import psutil
import json
import time
import logging
import config
from screenshots import capture_screenshot
import browser_agent

logger = logging.getLogger(__name__)

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
                "verify": {
                    "type": "boolean",
                    "description": "If true, take before/after screenshots to verify the click had a visible effect. Retries once on miss. Default: false.",
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
    # === Advanced Web Scraping & Cross-App ===
    {
        "name": "web_navigate",
        "description": "Smart navigation: go to a URL, wait for full load, and auto-dismiss cookie banners/popups. Optionally wait for a specific element. Better than browser_navigate for scraping tasks.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "URL to navigate to (e.g., 'https://example.com')",
                },
                "wait_for_selector": {
                    "type": "string",
                    "description": "Optional CSS selector to wait for after page load (e.g., '.main-content', '#results')",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Navigation timeout in milliseconds (default: 30000)",
                },
            },
            "required": ["url"],
        },
    },
    {
        "name": "web_extract",
        "description": "Extract structured data from the current browser page. Without selectors: auto-extracts title, meta description, headings, links, images, and main text. With selectors: extracts text for each named CSS selector.",
        "input_schema": {
            "type": "object",
            "properties": {
                "selectors": {
                    "type": "object",
                    "description": "Optional dict of {name: css_selector} for custom extraction. Omit for auto-extract of all page data.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "web_fill_form",
        "description": "Smart form filling: provide a dict of {label_or_placeholder: value} and it finds inputs by label, placeholder, name, id, or aria-label and fills them. Handles text inputs, selects, checkboxes, and radio buttons.",
        "input_schema": {
            "type": "object",
            "properties": {
                "fields": {
                    "type": "object",
                    "description": "Dict of {field_identifier: value}. Field identifier can be label text, placeholder, name attribute, id, or CSS selector.",
                },
            },
            "required": ["fields"],
        },
    },
    {
        "name": "web_click",
        "description": "Find an element by visible text OR CSS selector, scroll it into view, and click. Retries once if click fails. More robust than browser_click for dynamic pages.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Visible text of the element to click (partial match)",
                },
                "selector": {
                    "type": "string",
                    "description": "CSS selector of the element to click",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Max wait time in milliseconds (default: 10000)",
                },
            },
            "required": [],
        },
    },
    {
        "name": "web_screenshot_element",
        "description": "Take a screenshot of a specific element (not the whole page). Finds element by CSS selector or visible text. Useful for capturing a chart, table, card, or any specific part of a page.",
        "input_schema": {
            "type": "object",
            "properties": {
                "selector": {
                    "type": "string",
                    "description": "CSS selector or visible text to identify the element",
                },
                "path": {
                    "type": "string",
                    "description": "Optional file path to save the screenshot to",
                },
            },
            "required": ["selector"],
        },
    },
    # === File Download ===
    {
        "name": "download_file",
        "description": "Download a file from a URL to a local path using PowerShell. Supports HTTP/HTTPS. Good for fetching installers, assets, data files, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "The URL to download from",
                },
                "destination": {
                    "type": "string",
                    "description": "Local file path to save to (e.g. C:\\Users\\user\\Downloads\\file.zip)",
                },
            },
            "required": ["url", "destination"],
        },
    },
    # === Web Search ===
    {
        "name": "web_search",
        "description": "Search the web using DuckDuckGo. Returns top results with titles, URLs, and snippets. Use this when you need current information, prices, news, documentation, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query",
                },
                "max_results": {
                    "type": "integer",
                    "description": "Number of results to return (default: 5, max: 10)",
                },
            },
            "required": ["query"],
        },
    },
    # === Advanced Screen Analysis ===
    {
        "name": "list_windows_detailed",
        "description": "List ALL open windows with titles, process names, PIDs, and pixel positions/sizes. More detailed than list_windows. Useful for multi-window awareness: finding overlapping windows, determining which window covers the click target, etc.",
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "detect_screen_changes",
        "description": "Compare two screenshot files and return a list of rectangular regions that changed. Useful for verifying that an action (click, type, scroll) had the intended visual effect.",
        "input_schema": {
            "type": "object",
            "properties": {
                "before_path": {
                    "type": "string",
                    "description": "Path to the 'before' screenshot image",
                },
                "after_path": {
                    "type": "string",
                    "description": "Path to the 'after' screenshot image",
                },
            },
            "required": ["before_path", "after_path"],
        },
    },
    {
        "name": "find_ui_elements",
        "description": "Analyze a screenshot to detect probable clickable UI elements (buttons, inputs, cards) using edge detection. Returns bounding boxes with heuristic type labels. Useful when you need to find interactive elements without DOM access.",
        "input_schema": {
            "type": "object",
            "properties": {
                "image_path": {
                    "type": "string",
                    "description": "Path to the screenshot image to analyze",
                },
            },
            "required": ["image_path"],
        },
    },
    {
        "name": "find_color_on_screen",
        "description": "Scan the current screen for UI elements matching a specific RGB color (within tolerance). Returns center coordinates and bounding boxes of matching regions. Useful for finding colored buttons, status indicators, etc.",
        "input_schema": {
            "type": "object",
            "properties": {
                "r": {"type": "integer", "description": "Red channel (0-255)"},
                "g": {"type": "integer", "description": "Green channel (0-255)"},
                "b": {"type": "integer", "description": "Blue channel (0-255)"},
                "tolerance": {
                    "type": "integer",
                    "description": "Max per-channel deviation (default: 20)",
                },
            },
            "required": ["r", "g", "b"],
        },
    },
    # === Smart / Self-Healing Tools ===
    {
        "name": "smart_action",
        "description": "Execute a UI action described in natural language, e.g. 'click the Send button', 'type hello in the search box'. Uses adaptive detection with color matching, cached patterns, and self-healing retries. More robust than raw mouse_click for GUI automation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action_description": {
                    "type": "string",
                    "description": "Natural language description of the action, e.g. 'click the Send button', 'type hello in the search box'",
                },
            },
            "required": ["action_description"],
        },
    },
    {
        "name": "wait_for_element",
        "description": "Wait until a described UI element appears on screen. Repeatedly takes screenshots and scans for the element. Returns when found or on timeout. Good for waiting after navigation, app launch, or async operations.",
        "input_schema": {
            "type": "object",
            "properties": {
                "description": {
                    "type": "string",
                    "description": "Description of the element to wait for, e.g. 'the login button', 'a loading spinner', 'the search results'",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Maximum seconds to wait (default: 10, max: 30)",
                },
            },
            "required": ["description"],
        },
    },
    {
        "name": "window_manager",
        "description": "Advanced window management: list all windows, focus/minimize/maximize/restore a window by name, or tile windows side by side. More powerful than focus_window.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["list", "focus", "minimize", "maximize", "restore", "arrange"],
                    "description": "Window management action to perform",
                },
                "target": {
                    "type": "string",
                    "description": "Window title or partial name to target (required for focus/minimize/maximize/restore)",
                },
            },
            "required": ["action"],
        },
    },
    # ═══ Vision Engine: Precision Computer Control ═══
    {
        "name": "som_screenshot",
        "description": "Take a screenshot with Set-of-Mark annotation: every UI element gets a numbered label. Returns BOTH the annotated image (with numbered boxes) and an element list. Use this INSTEAD of take_screenshot when you need to click something — then use som_click with the element number. Much more precise than guessing coordinates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "region": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": "Optional [x, y, width, height] region. Omit for full screen.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "som_click",
        "description": "Click a UI element by its SoM number from the last som_screenshot. Example: if the annotated screenshot shows the Submit button as #7, use som_click with element_id=7. MUCH more precise than mouse_click with coordinates.",
        "input_schema": {
            "type": "object",
            "properties": {
                "element_id": {
                    "type": "integer",
                    "description": "The element number shown on the annotated screenshot (e.g. 7 for element #7)",
                },
                "button": {
                    "type": "string",
                    "enum": ["left", "right", "middle"],
                    "description": "Mouse button (default: left)",
                },
                "clicks": {
                    "type": "integer",
                    "description": "Number of clicks (1=single, 2=double). Default: 1",
                },
            },
            "required": ["element_id"],
        },
    },
    {
        "name": "ui_tree",
        "description": "Get the Windows accessibility tree for a desktop application. Returns a structured list of all UI elements (buttons, inputs, menus, etc.) with their names, types, and automation IDs. Like a DOM for desktop apps. Use this to find exact element names before clicking with ui_click_element.",
        "input_schema": {
            "type": "object",
            "properties": {
                "window_title": {
                    "type": "string",
                    "description": "Partial window title to target. Omit for the currently focused window.",
                },
                "max_depth": {
                    "type": "integer",
                    "description": "Maximum tree depth to traverse (default: 4, max: 6)",
                },
            },
            "required": [],
        },
    },
    {
        "name": "ui_click_element",
        "description": "Click a desktop UI element by its name or automation ID (from ui_tree). No coordinates needed — finds the element through the Windows accessibility tree. Much more reliable than mouse_click for desktop apps.",
        "input_schema": {
            "type": "object",
            "properties": {
                "window_title": {
                    "type": "string",
                    "description": "Window to search in (partial match). Omit for focused window.",
                },
                "element_name": {
                    "type": "string",
                    "description": "The element's name/text (e.g. 'Submit', 'Save As', 'File')",
                },
                "element_auto_id": {
                    "type": "string",
                    "description": "The element's automation ID (from ui_tree). More reliable than name.",
                },
                "element_type": {
                    "type": "string",
                    "description": "Control type filter: Button, Edit, MenuItem, TabItem, ListItem, etc.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "ui_type_element",
        "description": "Type text into a desktop input field found by accessibility tree. Finds the input by name or automation ID, clicks it, and types. Handles non-ASCII (Chinese, etc.) automatically via clipboard.",
        "input_schema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Text to type into the input field",
                },
                "window_title": {
                    "type": "string",
                    "description": "Window to search in. Omit for focused window.",
                },
                "element_name": {
                    "type": "string",
                    "description": "Name of the input element",
                },
                "element_auto_id": {
                    "type": "string",
                    "description": "Automation ID of the input element",
                },
                "clear_first": {
                    "type": "boolean",
                    "description": "Clear existing text before typing (default: true)",
                },
            },
            "required": ["text"],
        },
    },
    {
        "name": "smart_ui_click",
        "description": "Click a UI element using an automatic 4-step fallback chain: (0) browser_click via CSS selector if browser_selector provided, (1) ui_click_element via accessibility tree, (2) som_screenshot + som_click, (3) smartclick at fallback coordinates. Use this when you want maximum reliability without manually trying each method.",
        "input_schema": {
            "type": "object",
            "properties": {
                "element_name": {
                    "type": "string",
                    "description": "Name or text of the element to click. Case-insensitive, partial match supported.",
                },
                "browser_selector": {
                    "type": "string",
                    "description": "CSS selector or visible text for browser_click (step 0). Provide when the target is in a browser page. Example: '#submit-btn', 'Sign in', 'button[type=submit]'.",
                },
                "window_title": {
                    "type": "string",
                    "description": "Partial window title to target. Omit for the currently focused window.",
                },
                "element_type": {
                    "type": "string",
                    "description": "Control type hint: Button, Edit, MenuItem, etc. Improves SoM fallback matching.",
                },
                "fallback_x": {
                    "type": "integer",
                    "description": "X coordinate for last-resort smartclick if all other methods fail.",
                },
                "fallback_y": {
                    "type": "integer",
                    "description": "Y coordinate for last-resort smartclick if all other methods fail.",
                },
            },
            "required": ["element_name"],
        },
    },
    {
        "name": "suggest_tool",
        "description": "Given an action description, suggests the most precise tool to use. Considers whether you're in a browser or desktop app and recommends selector-based, accessibility tree, or SoM methods. Use this when unsure which tool is best for an interaction.",
        "input_schema": {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "description": "What you want to do, e.g. 'click the Submit button', 'type in the search box'",
                },
            },
            "required": ["action"],
        },
    },
    # === Self-Fix & Session Control ===
    {
        "name": "self_fix",
        "description": "Scan bot's own Python files for syntax errors & bugs, fix them automatically. Use when user asks to 'fix bugs', '修复bug', '自修复'. Runs py_compile on all .py files, finds errors, reads broken files, applies fixes.",
        "input_schema": {
            "type": "object",
            "properties": {
                "scope": {
                    "type": "string",
                    "enum": ["syntax", "deep", "all"],
                    "description": "syntax = py_compile only, deep = also check imports/runtime, all = full audit",
                },
            },
        },
    },
    {
        "name": "send_to_session",
        "description": "Send a message/prompt to a Claude Code CLI session. Use when user says '发送到session', 'send to session', '给session发消息'. Can resume existing session or start new one.",
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "The message/prompt to send to the session",
                },
                "session_id": {
                    "type": "string",
                    "description": "Session ID to resume (optional). If omitted, starts a new session.",
                },
                "project_dir": {
                    "type": "string",
                    "description": "Working directory for the session (defaults to bot project dir)",
                },
            },
            "required": ["message"],
        },
    },
    {
        "name": "codex_task",
        "description": "Run a task via Codex (claude.ai/code browser automation). Use when CLI credits are exhausted, or user says '用codex', 'codex充能', 'web执行'. Requires Chrome logged into claude.ai with CDP enabled.",
        "input_schema": {
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description": "The task prompt to send to Claude Code web",
                },
                "timeout": {
                    "type": "integer",
                    "description": "Timeout in seconds (default 600)",
                },
            },
            "required": ["prompt"],
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


# ─── PowerShell Escaping ─────────────────────────────────────────────────────

def _ps_escape_single(s: str) -> str:
    """Escape a string for use inside PowerShell single quotes.
    Single quotes in PS are escaped by doubling them: ' → ''"""
    return s.replace("'", "''")


def _ps_escape_double(s: str) -> str:
    """Escape a string for use inside PowerShell double quotes.
    Escape backticks, dollars, and double quotes."""
    return s.replace('`', '``').replace('"', '`"').replace('$', '`$')


# ─── Tool Implementations ────────────────────────────────────────────────────

async def execute_run_command(command: str, shell: str = "powershell",
                               working_directory: str = None, timeout: int = None) -> str:
    """Execute a shell command and return output."""
    try:
        timeout = max(1, min(timeout or config.COMMAND_TIMEOUT, 300))
        cwd = working_directory or os.path.expanduser("~")

        if not os.path.isdir(cwd):
            return f"Error: working directory does not exist: {cwd}"

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

        _MAX_CMD_OUTPUT = 50000  # 50KB cap to avoid huge outputs
        output = ""
        if result.stdout:
            output += result.stdout[:_MAX_CMD_OUTPUT]
            if len(result.stdout) > _MAX_CMD_OUTPUT:
                output += f"\n... (stdout truncated, {len(result.stdout)} total chars)"
        if result.stderr:
            stderr_text = result.stderr[:_MAX_CMD_OUTPUT]
            output += f"\n[STDERR]: {stderr_text}"
            if len(result.stderr) > _MAX_CMD_OUTPUT:
                output += f"\n... (stderr truncated, {len(result.stderr)} total chars)"
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
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, lambda: os.startfile(resolved))
        return f"Opened '{app_name}' (resolved to '{resolved}')."
    except Exception as e:
        try:
            result = await execute_run_command(f"Start-Process '{_ps_escape_single(app_name)}'")
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
        result = await execute_run_command(f'Start-Process "{_ps_escape_double(url)}"')
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
    """Get info about the active window using Win32 API (more reliable than pyautogui on Windows 11)."""
    try:
        from pc_control import detect_focused_window
        info = detect_focused_window()
        if info and "error" not in info:
            return (f"Active window: '{info.get('title', 'Unknown')}'\n"
                    f"Process: {info.get('process_name', 'unknown')} (PID {info.get('pid', '?')})\n"
                    f"Position: ({info.get('x', 0)}, {info.get('y', 0)})\n"
                    f"Size: {info.get('width', 0)} x {info.get('height', 0)}")
        elif info and "error" in info:
            return f"Error getting active window: {info['error']}"
    except Exception:
        pass
    # Fallback to pyautogui
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
            'Select-Object -First 100 Id, ProcessName, MainWindowTitle | '
            'Format-Table -AutoSize | Out-String -Width 200'
        )
        return result
    except Exception as e:
        return f"Error listing windows: {e}"


async def execute_focus_window(title: str) -> str:
    """Focus a window by title."""
    try:
        safe_title = _ps_escape_double(title).replace('[', '`[').replace(']', '`]').replace('*', '`*').replace('?', '`?')
        ps_cmd = (
            f'$w = Get-Process | Where-Object {{$_.MainWindowTitle -like "*{safe_title}*"}} | Select-Object -First 1; '
            f'if ($w) {{ '
            f'  Add-Type -TypeDefinition \'using System; using System.Runtime.InteropServices; '
            f'  public class Win32 {{ [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd); '
            f'  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow); }}\'; '
            f'  [Win32]::ShowWindow($w.MainWindowHandle, 9); '
            f'  [Win32]::SetForegroundWindow($w.MainWindowHandle); '
            f'  "Focused: $($w.MainWindowTitle)" '
            f'}} else {{ "No window found matching: {safe_title}" }}'
        )
        result = await execute_run_command(ps_cmd)
        return result
    except Exception as e:
        return f"Error focusing window: {e}"


def _clamp_coords(x: int, y: int) -> tuple[int, int]:
    """Clamp coordinates to virtual screen bounds (multi-monitor safe).

    On multi-monitor setups, coordinates can be negative (monitors to the
    left/above the primary). This uses the full virtual desktop area.
    """
    try:
        import ctypes
        user32 = ctypes.windll.user32
        vl = user32.GetSystemMetrics(76)  # SM_XVIRTUALSCREEN
        vt = user32.GetSystemMetrics(77)  # SM_YVIRTUALSCREEN
        vw = user32.GetSystemMetrics(78)  # SM_CXVIRTUALSCREEN
        vh = user32.GetSystemMetrics(79)  # SM_CYVIRTUALSCREEN
        if vw > 0 and vh > 0:
            x = max(vl, min(x, vl + vw - 1))
            y = max(vt, min(y, vt + vh - 1))
            return x, y
    except Exception:
        pass
    # Fallback: single monitor
    import pyautogui
    screen_w, screen_h = pyautogui.size()
    x = max(0, min(x, screen_w - 1))
    y = max(0, min(y, screen_h - 1))
    return x, y


def _clipboard_paste(text: str) -> bool:
    """Set clipboard to text and paste via Ctrl+V. Returns True on success."""
    import pyautogui
    import tempfile as _tmp_mod
    tmp_path = None
    try:
        with _tmp_mod.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(text)
            tmp_path = f.name
        cp = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"(Get-Content -Path '{_ps_escape_single(tmp_path)}' -Raw).TrimEnd(\"`r`n\") | Set-Clipboard"],
            capture_output=True, timeout=10,
        )
        if cp.returncode != 0:
            logger.warning(f"Set-Clipboard failed (rc={cp.returncode}): {cp.stderr[:200]}")
            return False
        pyautogui.hotkey('ctrl', 'v')
        return True
    except Exception:
        return False
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass


# Special key names that should be pressed, not typed
_SPECIAL_KEYS = {
    "enter", "return", "tab", "escape", "esc", "backspace", "delete", "del",
    "space", "up", "down", "left", "right", "home", "end", "pageup", "pagedown",
    "insert", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10",
    "f11", "f12", "capslock", "numlock", "scrolllock", "printscreen",
}

# Map common aliases to pyautogui key names
_KEY_ALIASES = {
    "return": "enter", "esc": "escape", "del": "delete",
}


def execute_type_text(text: str, interval: float = 0.02) -> str:
    """Type text at current cursor position with verification and smart fallback.

    - Handles special key names (Enter, Tab, Escape, etc.) when passed as the sole text.
    - Falls back to clipboard paste for non-ASCII or if direct typing verification fails.
    - After typing, takes before/after screenshots to verify text appeared.
    """
    import pyautogui
    try:
        # Handle special key names passed as text (e.g. "Enter", "Tab")
        text_lower = text.strip().lower()
        resolved_key = _KEY_ALIASES.get(text_lower, text_lower)
        if resolved_key in _SPECIAL_KEYS:
            pyautogui.press(resolved_key)
            return f"Pressed special key: {resolved_key}"

        # Check if text is ASCII-safe for pyautogui.write
        is_ascii = all(ord(c) < 128 for c in text)

        if is_ascii:
            # Take a before snapshot for verification
            before_img = None
            cx, cy = pyautogui.position()
            try:
                import numpy as np
                from PIL import ImageGrab
                screen_w, screen_h = pyautogui.size()
                region_left = max(0, cx - 200)
                region_top = max(0, cy - 30)
                region_right = min(screen_w, cx + 200)
                region_bottom = min(screen_h, cy + 30)
                before_img = np.array(ImageGrab.grab(
                    bbox=(region_left, region_top, region_right, region_bottom)))
            except Exception:
                pass

            pyautogui.write(text, interval=interval)

            # Verification: compare before/after screenshots
            typing_verified = True  # assume success unless we can prove otherwise
            try:
                if before_img is not None:
                    import numpy as np
                    from PIL import ImageGrab
                    time.sleep(0.15)
                    after_img = np.array(ImageGrab.grab(
                        bbox=(region_left, region_top, region_right, region_bottom)))
                    if before_img.shape == after_img.shape:
                        diff = np.abs(after_img.astype(np.int16) - before_img.astype(np.int16))
                        change_pct = np.any(diff > 12, axis=2).sum() / max(diff[:, :, 0].size, 1) * 100
                        typing_verified = change_pct > 0.05
                        logger.debug("type_text verify: %.2f%% change near cursor (%d,%d)", change_pct, cx, cy)
            except Exception:
                pass

            if not typing_verified:
                # Typing appeared to have no effect -- retry with clipboard paste
                logger.info("type_text: direct typing showed no change, retrying via clipboard")
                pyautogui.hotkey("ctrl", "a")
                time.sleep(0.05)
                pyautogui.press("delete")
                time.sleep(0.05)
                if _clipboard_paste(text):
                    return f"Pasted {len(text)} characters (clipboard fallback after typing had no visible effect)."
                # If even clipboard failed, report the original typing
                return f"Typed {len(text)} characters (could not verify text appeared on screen)."

            return f"Typed {len(text)} characters."
        else:
            # Non-ASCII: use clipboard paste
            if _clipboard_paste(text):
                return f"Pasted {len(text)} characters (non-ASCII, used clipboard)."
            return "Error: clipboard paste failed for non-ASCII text."

    except Exception as e:
        # If direct typing failed, try clipboard paste as last resort
        logger.warning("type_text direct typing failed (%s), trying clipboard paste", e)
        if _clipboard_paste(text):
            return f"Pasted {len(text)} characters (fallback after typing error: {e})."
        return f"Error typing text: {e}"


def execute_press_key(keys: str, presses: int = 1) -> str:
    """Press a key or key combination."""
    import pyautogui
    try:
        # Normalize key names to lowercase (e.g. "Ctrl" -> "ctrl")
        key_list = [k.strip().lower() for k in keys.split("+")]
        for _ in range(presses):
            if len(key_list) == 1:
                pyautogui.press(key_list[0])
            else:
                pyautogui.hotkey(*key_list)
        return f"Pressed '{keys}' x{presses}."
    except Exception as e:
        return f"Error pressing key: {e}"


def execute_mouse_click(x: int, y: int, button: str = "left", clicks: int = 1,
                        verify: bool = False) -> str:
    """Click mouse at coordinates (clamped to screen bounds).

    When verify=True, takes before/after screenshots and retries with
    jitter offsets (3px left/right/up/down) if no visual change is detected.
    """
    import pyautogui
    import numpy as np
    try:
        # Emergency stop check
        try:
            from pc_control import check_emergency_stop
            check_emergency_stop()
        except (ImportError, RuntimeError) as e:
            if isinstance(e, RuntimeError):
                return f"BLOCKED: {e}"
        x, y = _clamp_coords(x, y)

        if not verify:
            pyautogui.click(x=x, y=y, button=button, clicks=clicks)
            return f"Clicked ({x}, {y}) with {button} button ({clicks}x)."

        # --- Verified click with self-healing jitter retry ---
        from PIL import ImageGrab

        def _grab():
            img = ImageGrab.grab(all_screens=True)
            if img.mode != "RGB":
                img = img.convert("RGB")
            return np.array(img)

        def _changed(before, after, cx, cy, radius=150):
            h, w = before.shape[:2]
            y1, y2 = max(0, cy - radius), min(h, cy + radius)
            x1, x2 = max(0, cx - radius), min(w, cx + radius)
            b = before[y1:y2, x1:x2].astype(np.int16)
            a = after[y1:y2, x1:x2].astype(np.int16)
            if b.shape != a.shape:
                return True  # size mismatch counts as change
            diff = np.abs(a - b)
            pct = np.any(diff > 12, axis=2).sum() / max(diff[:, :, 0].size, 1) * 100
            return pct > 0.05

        # Attempt 1: exact coordinates
        before = _grab()
        pyautogui.click(x=x, y=y, button=button, clicks=clicks)
        time.sleep(0.35)
        after = _grab()

        if _changed(before, after, x, y):
            return f"Clicked ({x}, {y}) with {button} button ({clicks}x). Verified: screen changed."

        logger.info("mouse_click verify: attempt 1 at (%d,%d) - no change detected", x, y)

        # Attempt 2: exact coordinates retry (sometimes animations delay)
        time.sleep(0.15)
        before2 = _grab()
        pyautogui.click(x=x, y=y, button=button, clicks=clicks)
        time.sleep(0.4)
        after2 = _grab()

        if _changed(before2, after2, x, y):
            return (f"Clicked ({x}, {y}) with {button} button ({clicks}x). "
                    f"First click had no visible effect; RETRIED and screen changed on second attempt.")

        logger.info("mouse_click verify: attempt 2 at (%d,%d) - no change detected", x, y)

        # Attempt 3: try 2 jitter offsets (3px left, 3px down -- covers most cases)
        for dx, dy, label in [(-3, 0, "3px left"), (0, 3, "3px down")]:
            jx, jy = _clamp_coords(x + dx, y + dy)
            time.sleep(0.1)
            before_j = _grab()
            pyautogui.click(x=jx, y=jy, button=button, clicks=clicks)
            time.sleep(0.35)
            after_j = _grab()

            if _changed(before_j, after_j, jx, jy):
                logger.info("mouse_click verify: jitter %s at (%d,%d) succeeded", label, jx, jy)
                return (f"Clicked ({jx}, {jy}) with {button} button ({clicks}x). "
                        f"Original ({x},{y}) had no effect; self-healed with jitter {label}.")

        return (f"Clicked ({x}, {y}) with {button} button ({clicks}x). "
                f"WARNING: No visible change detected after 4 attempts. "
                f"The target may be inactive, already selected, or the coordinates may be slightly off. "
                f"Try take_screenshot to verify the current screen state.")
    except Exception as e:
        return f"Error clicking: {e}"


def execute_mouse_scroll(amount: int, x: int = None, y: int = None) -> str:
    """Scroll mouse wheel."""
    import pyautogui
    try:
        kwargs = {"clicks": amount}
        if x is not None and y is not None:
            x, y = _clamp_coords(x, y)
            kwargs["x"] = x
            kwargs["y"] = y
        pyautogui.scroll(**kwargs)
        direction = "up" if amount > 0 else "down"
        return f"Scrolled {direction} by {abs(amount)} clicks."
    except Exception as e:
        return f"Error scrolling: {e}"


def execute_mouse_move(x: int, y: int) -> str:
    """Move mouse to coordinates (clamped to screen bounds)."""
    import pyautogui
    try:
        x, y = _clamp_coords(x, y)
        pyautogui.moveTo(x, y)
        return f"Mouse moved to ({x}, {y})."
    except Exception as e:
        return f"Error moving mouse: {e}"


def execute_mouse_drag(start_x: int, start_y: int, end_x: int, end_y: int,
                        button: str = "left", duration: float = 0.5) -> str:
    """Drag mouse from start to end (coordinates clamped to screen bounds)."""
    import pyautogui
    try:
        start_x, start_y = _clamp_coords(start_x, start_y)
        end_x, end_y = _clamp_coords(end_x, end_y)
        pyautogui.moveTo(start_x, start_y)
        pyautogui.drag(end_x - start_x, end_y - start_y, duration=duration, button=button)
        return f"Dragged from ({start_x},{start_y}) to ({end_x},{end_y})."
    except Exception as e:
        return f"Error dragging: {e}"


async def execute_set_clipboard(text: str) -> str:
    """Set clipboard content via PowerShell."""
    tmp_path = None
    try:
        # Write to temp file to avoid escaping issues
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
            f.write(text)
            tmp_path = f.name
        await execute_run_command(
            f"Get-Content -Path '{_ps_escape_single(tmp_path)}' -Raw | Set-Clipboard",
        )
        return f"Clipboard set ({len(text)} chars). Use Ctrl+V to paste."
    except Exception as e:
        return f"Error setting clipboard: {e}"
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)


async def execute_get_clipboard() -> str:
    """Get clipboard content."""
    try:
        result = await execute_run_command("Get-Clipboard")
        # Cap clipboard content to avoid huge returns
        if len(result) > 10000:
            result = result[:10000] + f"\n... (truncated, {len(result)} total chars)"
        return f"Clipboard content:\n{result}"
    except Exception as e:
        return f"Error getting clipboard: {e}"


def execute_list_files(directory: str, recursive: bool = False, pattern: str = None) -> str:
    """List files in a directory."""
    try:
        if not os.path.isdir(directory):
            return f"Error: directory does not exist: {directory}"
        import fnmatch
        from datetime import datetime
        entries = []

        if recursive:
            count = 0
            for root, dirs, files in os.walk(directory, followlinks=False):
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
                    except Exception:
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
                except Exception:
                    entries.append(f"{'':>10s}  {'':16s}  {name}")
                if len(entries) >= 500:
                    entries.append(f"\n... (truncated at 500 entries)")
                    break

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
        # Guard against huge files that could OOM
        file_size = os.path.getsize(path)
        if file_size > 10 * 1024 * 1024:  # 10 MB
            return f"Error: file is too large ({file_size / (1024*1024):.1f} MB). Skipping to avoid memory issues. Max supported size: 10 MB."
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
        return (header + content) if content else "(empty file)"
    except FileNotFoundError:
        return f"Error: file not found: {path}"
    except Exception as e:
        return f"Error reading file: {e}"


_MAX_WRITE_BYTES = 10 * 1024 * 1024  # 10 MB write limit

def execute_write_file(path: str, content: str) -> str:
    """Write content to a file. Uses atomic write to prevent data loss on crash."""
    try:
        if not path or not path.strip():
            return "Error: file path cannot be empty."
        if len(content) > _MAX_WRITE_BYTES:
            return f"Error: content too large ({len(content)} bytes, limit is {_MAX_WRITE_BYTES} bytes)."
        # Create directories if needed
        dir_path = os.path.dirname(os.path.abspath(path))
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        # Atomic write: tmp file + os.replace to prevent corruption
        import tempfile
        fd, tmp = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        lines = content.count('\n') + 1
        return f"File written: {path} ({len(content)} chars, {lines} lines)"
    except Exception as e:
        return f"Error writing file: {e}"


def execute_edit_file(path: str, old_text: str, new_text: str, replace_all: bool = False) -> str:
    """Edit a file by replacing text. Uses atomic write to prevent data loss."""
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

        # Atomic write: write to temp file then rename to prevent data loss on crash
        import tempfile
        dir_path = os.path.dirname(path) or "."
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".tmp",
                                          dir=dir_path, delete=False) as tmp:
            tmp.write(new_content)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp_name = tmp.name
        os.replace(tmp_name, path)

        return f"Edited {path}: replaced {count} occurrence(s)."
    except Exception as e:
        # Clean up temp file on failure
        try:
            if 'tmp_name' in locals() and os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass
        return f"Error editing file: {e}"


async def execute_search_files(directory: str, pattern: str,
                                file_pattern: str = None, max_results: int = 50) -> str:
    """Search for text in files."""
    try:
        import re
        import fnmatch
        # Limit pattern length and complexity to prevent ReDoS
        if len(pattern) > 500:
            return "Error: regex pattern too long (max 500 chars)"
        # Reject patterns with known catastrophic backtracking structures
        _dangerous_patterns = [r'(.*){', r'(.+){', r'(.+)+', r'(.*)+', r'([^a]*a)*']
        for dp in _dangerous_patterns:
            if dp in pattern:
                return f"Error: regex pattern contains potentially unsafe backtracking construct"
        try:
            compiled = re.compile(pattern)
        except re.error as e:
            return f"Invalid regex pattern '{pattern}': {e}"
        results = []
        count = 0

        for root, dirs, files in os.walk(directory):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d not in
                       ('node_modules', '__pycache__', '.git', 'venv', '.venv', 'dist', 'build')]
            for fname in files:
                if file_pattern:
                    if not fnmatch.fnmatch(fname, file_pattern):
                        continue
                # Skip binary files
                if any(fname.endswith(ext) for ext in ('.exe', '.dll', '.bin', '.jpg', '.png', '.gif', '.zip', '.tar', '.gz', '.pyc')):
                    continue

                fpath = os.path.join(root, fname)
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='replace') as f:
                        for line_num, line in enumerate(f, 1):
                            if compiled.search(line):
                                rel = os.path.relpath(fpath, directory)
                                # Cap individual line length to prevent huge results
                                display_line = line.rstrip()[:500]
                                results.append(f"{rel}:{line_num}: {display_line}")
                                count += 1
                                if count >= max_results:
                                    results.append(f"\n... (max {max_results} results reached)")
                                    return "\n".join(results)
                except Exception:
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
    """Get system information.

    NOTE: psutil.cpu_percent(interval=1) is a blocking call that sleeps for 1 second.
    This function should be called from an executor (e.g. loop.run_in_executor) when
    used from async context to avoid blocking the event loop.
    """
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
            name = p.get('name') or 'unknown'
            pid = p.get('pid') or '?'
            info.append(f"  {name} (PID {pid}): {p.get('memory_percent', 0):.1f}% mem")

        return "\n".join(info)
    except Exception as e:
        return f"Error getting system info: {e}"


_MAX_DOWNLOAD_BYTES = 100 * 1024 * 1024  # 100 MB download limit

async def execute_download_file(url: str, destination: str) -> str:
    """Download a file from a URL to a local path via PowerShell."""
    try:
        # Validate URL scheme
        if not url.startswith(("http://", "https://")):
            return "Error: only http/https URLs are supported."
        # Validate destination path — block paths outside working dir
        abs_dest = os.path.realpath(os.path.abspath(destination))
        cwd = os.path.realpath(os.path.abspath("."))
        if not abs_dest.startswith(cwd + os.sep) and abs_dest != cwd:
            return f"Error: destination must be within working directory ({cwd})"
        # Create destination directory if needed
        dest_dir = os.path.dirname(abs_dest)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)
        # Use validated absolute path (not raw user input) to prevent symlink attacks
        ps_cmd = (
            f"Invoke-WebRequest -Uri '{_ps_escape_single(url)}' -OutFile '{_ps_escape_single(abs_dest)}' "
            f'-UseBasicParsing -ErrorAction Stop'
        )
        result = await execute_run_command(ps_cmd, timeout=120)
        if os.path.exists(abs_dest):
            size = os.path.getsize(abs_dest)
            # Enforce size limit — delete if too large
            if size > _MAX_DOWNLOAD_BYTES:
                os.unlink(abs_dest)
                return f"Error: downloaded file too large ({size} bytes, limit is {_MAX_DOWNLOAD_BYTES} bytes). Deleted."
            return f"Downloaded '{url}' to '{destination}' ({size} bytes). {result}".strip()
        else:
            return f"Download may have failed. PowerShell output: {result}"
    except Exception as e:
        return f"Error downloading file: {e}"


async def execute_web_search(query: str, max_results: int = 5) -> str:
    """Search the web using DuckDuckGo."""
    try:
        import urllib.parse
        import re
        max_results = min(max_results or 5, 10)
        encoded = urllib.parse.quote_plus(query)
        url = f"https://html.duckduckgo.com/html/?q={encoded}"
        loop = asyncio.get_running_loop()

        def _fetch():
            import requests
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            resp = requests.get(url, headers=headers, timeout=10)
            return resp.text

        html = await loop.run_in_executor(None, _fetch)
        # Cap HTML to prevent huge memory usage from unexpected responses
        html = html[:500000]

        # Parse results
        results = []
        # Find result blocks (simplified pattern - DuckDuckGo HTML structure)
        blocks = re.findall(r'<a rel="nofollow" class="result__a" href="(.*?)".*?>(.*?)</a>.*?<a class="result__snippet".*?>(.*?)</a>', html, re.DOTALL)

        for i, (href, title, snippet) in enumerate(blocks[:max_results]):
            # Clean HTML tags
            title = re.sub(r'<.*?>', '', title).strip()
            snippet = re.sub(r'<.*?>', '', snippet).strip()
            # Decode URL
            if href.startswith("//duckduckgo.com/l/?uddg="):
                try:
                    href = urllib.parse.unquote(href.split("uddg=")[1].split("&")[0])
                except (IndexError, ValueError):
                    pass
            results.append(f"[{i+1}] {title}\n    {href}\n    {snippet}\n")

        # Fallback: if primary regex failed (DuckDuckGo HTML may have changed),
        # try a more generous extraction of links and text
        if not results:
            # Look for any links with href and nearby text
            fallback_blocks = re.findall(
                r'<a[^>]+href="(https?://[^"]+)"[^>]*>(.*?)</a>',
                html, re.DOTALL
            )
            seen_urls = set()
            for href, title in fallback_blocks:
                title_clean = re.sub(r'<.*?>', '', title).strip()
                if not title_clean or len(title_clean) < 3:
                    continue
                if href in seen_urls or 'duckduckgo.com' in href:
                    continue
                seen_urls.add(href)
                results.append(f"[{len(results)+1}] {title_clean}\n    {href}\n")
                if len(results) >= max_results:
                    break

        if not results:
            return f"No results found for: {query}"
        return f"Search results for '{query}':\n\n" + "\n".join(results)
    except Exception as e:
        return f"Search error: {e}"


async def execute_list_windows_detailed() -> str:
    """List all open windows with titles, positions, sizes, and process info.

    Uses PowerShell + Win32 API to get precise window rectangles, which is
    more informative than the basic Get-Process approach.
    """
    try:
        ps_cmd = (
            'Add-Type -TypeDefinition @"\n'
            'using System; using System.Runtime.InteropServices; using System.Text;\n'
            'public class WinEnum {\n'
            '  public delegate bool EnumProc(IntPtr hWnd, IntPtr lParam);\n'
            '  [DllImport("user32.dll")] public static extern bool EnumWindows(EnumProc cb, IntPtr lParam);\n'
            '  [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hWnd);\n'
            '  [DllImport("user32.dll")] public static extern int GetWindowTextLength(IntPtr hWnd);\n'
            '  [DllImport("user32.dll", CharSet=CharSet.Unicode)] public static extern int GetWindowText(IntPtr hWnd, StringBuilder sb, int max);\n'
            '  [DllImport("user32.dll")] public static extern bool GetWindowRect(IntPtr hWnd, out RECT r);\n'
            '  [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, out uint pid);\n'
            '  [StructLayout(LayoutKind.Sequential)] public struct RECT { public int L,T,R,B; }\n'
            '}\n'
            '"@\n'
            '$results = [System.Collections.ArrayList]::new()\n'
            '[WinEnum]::EnumWindows({\n'
            '  param($h,$l)\n'
            '  if ([WinEnum]::IsWindowVisible($h) -and [WinEnum]::GetWindowTextLength($h) -gt 0) {\n'
            '    $sb = [System.Text.StringBuilder]::new(512)\n'
            '    [WinEnum]::GetWindowText($h, $sb, 512) | Out-Null\n'
            '    $r = New-Object WinEnum+RECT\n'
            '    [WinEnum]::GetWindowRect($h, [ref]$r) | Out-Null\n'
            '    $pid = [uint32]0\n'
            '    [WinEnum]::GetWindowThreadProcessId($h, [ref]$pid) | Out-Null\n'
            '    $pname = try { (Get-Process -Id $pid -ErrorAction SilentlyContinue).ProcessName } catch { "?" }\n'
            '    $w = $r.R - $r.L; $h2 = $r.B - $r.T\n'
            '    if ($w -gt 0 -and $h2 -gt 0) {\n'
            '      $results.Add("$pid|$pname|$($sb.ToString())|$($r.L)|$($r.T)|$w|$h2") | Out-Null\n'
            '    }\n'
            '  }\n'
            '  return $true\n'
            '}, [IntPtr]::Zero) | Out-Null\n'
            '$results -join "`n"'
        )
        raw = await execute_run_command(ps_cmd, timeout=10)
        if not raw or raw.startswith("Error"):
            # Fallback to simpler approach
            return await execute_list_windows()

        lines = [l.strip() for l in raw.strip().splitlines() if "|" in l]
        header = f"{'PID':>7s}  {'Process':<20s}  {'X':>5s} {'Y':>5s} {'W':>5s} {'H':>5s}  Title"
        output = [header, "-" * 90]
        for line in lines[:200]:  # Cap at 200 windows to prevent huge output
            parts = line.split("|", 6)
            if len(parts) < 7:
                continue
            pid, pname, title, lx, ly, w, h = parts
            output.append(f"{pid:>7s}  {pname:<20s}  {lx:>5s} {ly:>5s} {w:>5s} {h:>5s}  {title}")
        return "\n".join(output) if len(output) > 2 else "(no visible windows found)"
    except Exception as e:
        return f"Error listing windows: {e}"


def execute_detect_screen_changes(before_path: str, after_path: str) -> str:
    """Compare two screenshot files and describe changed regions."""
    try:
        from screenshots import detect_changes
        import json as _json
        regions = detect_changes(before_path, after_path)
        if not regions:
            return "No changes detected between the two screenshots."
        summary = f"Detected {len(regions)} changed region(s):\n"
        summary += _json.dumps(regions[:20], indent=2)
        if len(regions) > 20:
            summary += f"\n... ({len(regions) - 20} more regions omitted)"
        return summary
    except FileNotFoundError as e:
        return f"Error: file not found: {e}"
    except Exception as e:
        return f"Error detecting changes: {e}"


def execute_find_ui_elements(image_path: str) -> str:
    """Detect UI elements in a screenshot."""
    try:
        from screenshots import find_ui_elements
        import json as _json
        elements = find_ui_elements(image_path)
        if not elements:
            return "No UI elements detected in the image."
        # Group by kind for a readable summary
        kinds = {}
        for el in elements:
            k = el.get("kind", "element")
            kinds[k] = kinds.get(k, 0) + 1
        summary_parts = [f"{v} {k}(s)" for k, v in kinds.items()]
        summary = f"Detected {len(elements)} UI elements ({', '.join(summary_parts)}):\n"
        summary += _json.dumps(elements[:40], indent=2)
        if len(elements) > 40:
            summary += f"\n... ({len(elements) - 40} more elements omitted)"
        return summary
    except FileNotFoundError:
        return f"Error: image file not found: {image_path}"
    except Exception as e:
        return f"Error finding UI elements: {e}"


def execute_find_color_on_screen(r: int, g: int, b: int, tolerance: int = 20) -> str:
    """Scan the screen for regions matching an RGB color."""
    try:
        # Import from pc_control which already has this logic
        import pc_control
        results = pc_control.find_element_by_color((r, g, b), tolerance=tolerance)
        if not results:
            return f"No regions found matching RGB({r},{g},{b}) with tolerance {tolerance}."
        import json as _json
        return (f"Found {len(results)} region(s) matching RGB({r},{g},{b}) "
                f"(tolerance={tolerance}):\n{_json.dumps(results[:20], indent=2)}")
    except Exception as e:
        return f"Error scanning for color: {e}"


# ─── Lazy singleton for AdaptiveController ────────────────────────────────────

def _get_adaptive_controller():
    """Lazy-initialise and return the AdaptiveController singleton."""
    from adaptive_controller import get_adaptive_controller
    return get_adaptive_controller()


# ─── Smart Action Tool ────────────────────────────────────────────────────────

async def execute_smart_action(action_description: str) -> str:
    """Execute a UI action described in natural language using adaptive detection.

    Parses the description to decide between click and type actions, then
    delegates to AdaptiveController which uses color matching, cached patterns,
    and self-healing retries.
    """
    try:
        ctrl = _get_adaptive_controller()
        desc_lower = action_description.lower().strip()

        # Detect if it's a "type X in Y" action
        type_patterns = [
            # "type hello in the search box"
            ("type ", " in "),
            ("type ", " into "),
            ("enter ", " in "),
            ("enter ", " into "),
            ("write ", " in "),
            ("write ", " into "),
            ("input ", " in "),
            ("input ", " into "),
        ]

        for verb, prep in type_patterns:
            if desc_lower.startswith(verb) and prep in desc_lower:
                verb_end = len(verb)
                prep_idx = desc_lower.index(prep, verb_end)
                text_to_type = action_description[verb_end:prep_idx].strip().strip('"').strip("'")
                target_field = action_description[prep_idx + len(prep):].strip().strip('"').strip("'")
                if text_to_type and target_field:
                    logger.info("smart_action: typing '%s' into '%s'", text_to_type, target_field)
                    result = await ctrl.find_and_type(text_to_type, target_description=target_field)
                    parts = [f"Action: type '{text_to_type}' into '{target_field}'"]
                    parts.append(f"Target found: {result.get('target_found', False)}")
                    parts.append(f"Typed: {result.get('typed', False)}")
                    parts.append(f"Verified: {result.get('verified', False)}")
                    parts.append(f"Success: {result.get('success', False)}")
                    return "\n".join(parts)

        # Default: treat as a click action
        # Strip leading "click " or "press " or "tap " if present
        click_target = desc_lower
        for prefix in ("click ", "click on ", "press ", "tap ", "hit ", "select "):
            if click_target.startswith(prefix):
                click_target = action_description[len(prefix):].strip()
                break
        else:
            click_target = action_description.strip()

        logger.info("smart_action: clicking '%s'", click_target)
        result = await ctrl.adaptive_click(click_target)

        parts = [f"Action: click '{click_target}'"]
        parts.append(f"Success: {result.get('success', False)}")
        if result.get('success'):
            parts.append(f"Clicked at: ({result.get('x', '?')}, {result.get('y', '?')})")
        parts.append(f"Method: {result.get('method_used', 'unknown')}")
        parts.append(f"Attempts: {result.get('attempts', 0)}")
        parts.append(f"Cached: {result.get('cached', False)}")
        if not result.get('success'):
            parts.append("HINT: Try take_screenshot to see current screen state, "
                         "then use mouse_click with exact coordinates.")
        return "\n".join(parts)

    except Exception as e:
        logger.exception("smart_action failed")
        return f"Error in smart_action: {e}"


# ─── Wait For Element Tool ────────────────────────────────────────────────────

async def execute_wait_for_element(description: str, timeout: int = 10) -> str:
    """Wait until a described UI element appears on screen.

    Repeatedly takes screenshots and compares them to detect when a
    meaningful visual change occurs (indicating the element appeared).
    Does NOT click -- purely observational.
    """
    try:
        timeout = max(1, min(timeout, 30))
        poll_interval = 1.0  # seconds between checks
        start_time = time.time()
        attempts = 0

        import numpy as np
        from PIL import ImageGrab

        baseline = None

        while True:
            elapsed = time.time() - start_time
            if elapsed >= timeout:
                return (f"TIMEOUT after {timeout}s waiting for '{description}'. "
                        f"Checked {attempts} times. Try take_screenshot to see current state.")

            attempts += 1
            logger.info("wait_for_element: attempt %d for '%s' (%.1fs elapsed)",
                        attempts, description, elapsed)

            try:
                current = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: np.array(ImageGrab.grab())
                )

                if baseline is None:
                    # First screenshot is the baseline; wait and compare next
                    baseline = current
                else:
                    # Compare with baseline to see if something changed
                    if baseline.shape == current.shape:
                        diff = np.abs(current.astype(np.int16) - baseline.astype(np.int16))
                        change_pct = np.any(diff > 15, axis=2).sum() / max(diff[:, :, 0].size, 1) * 100
                        if change_pct > 0.5:
                            return (f"Screen changed ({change_pct:.1f}% difference) after {attempts} "
                                    f"attempt(s) ({elapsed:.1f}s) — '{description}' likely appeared. "
                                    f"Use take_screenshot to verify.")
                    else:
                        # Resolution changed — treat as a significant change
                        return (f"Screen resolution changed after {attempts} attempt(s) "
                                f"({elapsed:.1f}s) — '{description}' likely appeared.")

            except Exception as det_err:
                logger.debug("wait_for_element screenshot error: %s", det_err)

            remaining = timeout - (time.time() - start_time)
            if remaining <= 0:
                break
            await asyncio.sleep(min(poll_interval, remaining))

        return (f"TIMEOUT after {timeout}s: no visual change detected for '{description}'. "
                f"Checked {attempts} times.")
    except Exception as e:
        logger.exception("wait_for_element failed")
        return f"Error in wait_for_element: {e}"


# ─── Window Manager Tool ─────────────────────────────────────────────────────

async def execute_window_manager(action: str, target: str = None) -> str:
    """Advanced window management using Win32 API via PowerShell."""
    try:
        if action == "list":
            return await execute_list_windows_detailed()

        if action == "arrange":
            # Tile the two most recent foreground windows side by side
            ps_cmd = (
                'Add-Type -TypeDefinition @"\n'
                'using System; using System.Runtime.InteropServices;\n'
                'public class WinArrange {\n'
                '  [DllImport("user32.dll")] public static extern bool SetWindowPos(\n'
                '    IntPtr hWnd, IntPtr hAfter, int X, int Y, int W, int H, uint flags);\n'
                '  [DllImport("user32.dll")] public static extern bool IsWindowVisible(IntPtr hWnd);\n'
                '  [DllImport("user32.dll")] public static extern int GetWindowTextLength(IntPtr hWnd);\n'
                '}\n'
                '"@\n'
                '$screen = [System.Windows.Forms.Screen]::PrimaryScreen.WorkingArea\n'
                '$halfW = [int]($screen.Width / 2)\n'
                '$wins = Get-Process | Where-Object {\n'
                '  $_.MainWindowTitle -ne "" -and $_.MainWindowHandle -ne [IntPtr]::Zero\n'
                '} | Sort-Object -Property StartTime -Descending | Select-Object -First 2\n'
                'if ($wins.Count -ge 2) {\n'
                '  [WinArrange]::SetWindowPos($wins[0].MainWindowHandle, [IntPtr]::Zero,\n'
                '    $screen.Left, $screen.Top, $halfW, $screen.Height, 0x0040) | Out-Null\n'
                '  [WinArrange]::SetWindowPos($wins[1].MainWindowHandle, [IntPtr]::Zero,\n'
                '    $screen.Left + $halfW, $screen.Top, $halfW, $screen.Height, 0x0040) | Out-Null\n'
                '  "Tiled: $($wins[0].MainWindowTitle) (left) | $($wins[1].MainWindowTitle) (right)"\n'
                '} elseif ($wins.Count -eq 1) {\n'
                '  "Only one window with a title found: $($wins[0].MainWindowTitle). Need at least 2 to tile."\n'
                '} else {\n'
                '  "No windows with titles found to arrange."\n'
                '}\n'
            )
            # Need System.Windows.Forms for screen info
            ps_full = 'Add-Type -AssemblyName System.Windows.Forms; ' + ps_cmd
            return await execute_run_command(ps_full, timeout=10)

        # Actions that need a target: focus, minimize, maximize, restore
        if not target:
            return f"Error: action '{action}' requires a 'target' window name."

        # SW_* constants for ShowWindow
        show_cmds = {
            "focus": 9,      # SW_RESTORE (then SetForegroundWindow)
            "minimize": 6,   # SW_MINIMIZE
            "maximize": 3,   # SW_MAXIMIZE
            "restore": 9,    # SW_RESTORE
        }
        sw_cmd = show_cmds.get(action)
        if sw_cmd is None:
            return f"Error: unknown window_manager action '{action}'. Use list/focus/minimize/maximize/restore/arrange."

        safe_target = _ps_escape_double(target).replace('[', '`[').replace(']', '`]').replace('*', '`*').replace('?', '`?')
        # Windows 11 blocks SetForegroundWindow unless the calling thread has input focus.
        # Workaround: attach to the target thread's input queue, then call SetForegroundWindow.
        ps_cmd = (
            f'Add-Type -TypeDefinition \'using System; using System.Runtime.InteropServices; '
            f'public class WinMgr {{ '
            f'  [DllImport("user32.dll")] public static extern bool SetForegroundWindow(IntPtr hWnd); '
            f'  [DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow); '
            f'  [DllImport("user32.dll")] public static extern bool BringWindowToTop(IntPtr hWnd); '
            f'  [DllImport("user32.dll")] public static extern uint GetWindowThreadProcessId(IntPtr hWnd, IntPtr pid); '
            f'  [DllImport("user32.dll")] public static extern IntPtr GetForegroundWindow(); '
            f'  [DllImport("kernel32.dll")] public static extern uint GetCurrentThreadId(); '
            f'  [DllImport("user32.dll")] public static extern bool AttachThreadInput(uint idAttach, uint idAttachTo, bool fAttach); '
            f'}}\'; '
            f'$procs = Get-Process | Where-Object {{$_.MainWindowTitle -like "*{safe_target}*"}}; '
            f'if ($procs.Count -eq 0) {{ "No window found matching: {safe_target}" }} '
            f'else {{ '
            f'  foreach ($p in $procs) {{ '
            f'    [WinMgr]::ShowWindow($p.MainWindowHandle, {sw_cmd}) | Out-Null; '
        )
        if action == "focus":
            # Attach to foreground thread to gain focus permission, then SetForegroundWindow
            ps_cmd += (
                f'    $fgWnd = [WinMgr]::GetForegroundWindow(); '
                f'    $fgThread = [WinMgr]::GetWindowThreadProcessId($fgWnd, [IntPtr]::Zero); '
                f'    $curThread = [WinMgr]::GetCurrentThreadId(); '
                f'    [WinMgr]::AttachThreadInput($curThread, $fgThread, $true) | Out-Null; '
                f'    [WinMgr]::SetForegroundWindow($p.MainWindowHandle) | Out-Null; '
                f'    [WinMgr]::BringWindowToTop($p.MainWindowHandle) | Out-Null; '
                f'    [WinMgr]::AttachThreadInput($curThread, $fgThread, $false) | Out-Null; '
            )
        ps_cmd += (
            f'    "Action {action} on: $($p.MainWindowTitle) (PID $($p.Id))" '
            f'  }} '
            f'}} '
        )
        return await execute_run_command(ps_cmd, timeout=10)

    except Exception as e:
        return f"Error in window_manager: {e}"


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
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(None, execute_get_active_window)
    elif tool_name == "list_windows":
        result = await execute_list_windows()
    elif tool_name == "focus_window":
        result = await execute_focus_window(tool_input["title"])
    elif tool_name == "type_text":
        # Wrap blocking sync functions (with time.sleep) in executor
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_type_text(
                tool_input["text"],
                tool_input.get("interval", 0.02),
            ),
        )
    elif tool_name == "press_key":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_press_key(
                tool_input["keys"],
                tool_input.get("presses", 1),
            ),
        )
    elif tool_name == "mouse_click":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_mouse_click(
                tool_input["x"],
                tool_input["y"],
                tool_input.get("button", "left"),
                tool_input.get("clicks", 1),
                tool_input.get("verify", False),
            ),
        )
    elif tool_name == "mouse_scroll":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_mouse_scroll(
                tool_input["amount"],
                tool_input.get("x"),
                tool_input.get("y"),
            ),
        )
    elif tool_name == "mouse_move":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_mouse_move(tool_input["x"], tool_input["y"]),
        )
    elif tool_name == "mouse_drag":
        # mouse_drag uses duration param which blocks with time.sleep
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_mouse_drag(
                tool_input["start_x"], tool_input["start_y"],
                tool_input["end_x"], tool_input["end_y"],
                tool_input.get("button", "left"),
                tool_input.get("duration", 0.5),
            ),
        )
    elif tool_name == "set_clipboard":
        result = await execute_set_clipboard(tool_input["text"])
    elif tool_name == "get_clipboard":
        result = await execute_get_clipboard()
    elif tool_name == "list_files":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_list_files(
                tool_input["directory"],
                tool_input.get("recursive", False),
                tool_input.get("pattern"),
            ),
        )
    elif tool_name == "read_file":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_read_file(
                tool_input["path"],
                tool_input.get("start_line", 1),
                tool_input.get("end_line"),
            ),
        )
    elif tool_name == "write_file":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_write_file(tool_input["path"], tool_input["content"]),
        )
    elif tool_name == "edit_file":
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_edit_file(
                tool_input["path"],
                tool_input["old_text"],
                tool_input["new_text"],
                tool_input.get("replace_all", False),
            ),
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
        _loop = asyncio.get_running_loop()
        result = await _loop.run_in_executor(
            None, lambda: execute_manage_processes(
                tool_input.get("action", "list"),
                tool_input.get("query"),
            ),
        )
    elif tool_name == "wait":
        result = await execute_wait(
            tool_input["seconds"],
            tool_input.get("reason"),
        )
    elif tool_name == "get_system_info":
        # Run in executor since psutil.cpu_percent(interval=1) blocks for 1 second
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, execute_get_system_info)
    elif tool_name == "download_file":
        result = await execute_download_file(
            tool_input["url"],
            tool_input["destination"],
        )
    elif tool_name == "web_search":
        result = await execute_web_search(
            tool_input["query"],
            tool_input.get("max_results", 5),
        )
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
    # === Advanced Web Scraping & Cross-App ===
    elif tool_name == "web_navigate":
        result = await browser_agent.smart_navigate(
            tool_input["url"],
            tool_input.get("wait_for_selector"),
            tool_input.get("timeout", 30000),
        )
    elif tool_name == "web_extract":
        result = await browser_agent.extract_page_data(tool_input.get("selectors"))
    elif tool_name == "web_fill_form":
        result = await browser_agent.fill_form(tool_input["fields"])
    elif tool_name == "web_click":
        result = await browser_agent.wait_and_click(
            text=tool_input.get("text"),
            selector=tool_input.get("selector"),
            timeout=tool_input.get("timeout", 10000),
        )
    elif tool_name == "web_screenshot_element":
        buf = await browser_agent.screenshot_element(
            tool_input["selector"],
            tool_input.get("path"),
        )
        if buf:
            screenshot_buffer = buf
            result = "Element screenshot captured and sent to user."
        else:
            result = "Failed to capture element screenshot. Element may not be found or visible."
    # === Advanced Screen Analysis Tools ===
    elif tool_name == "list_windows_detailed":
        result = await execute_list_windows_detailed()
    elif tool_name == "detect_screen_changes":
        result = execute_detect_screen_changes(
            tool_input["before_path"],
            tool_input["after_path"],
        )
    elif tool_name == "find_ui_elements":
        result = execute_find_ui_elements(tool_input["image_path"])
    elif tool_name == "find_color_on_screen":
        result = execute_find_color_on_screen(
            tool_input["r"],
            tool_input["g"],
            tool_input["b"],
            tool_input.get("tolerance", 20),
        )
    # === Smart / Self-Healing Tools ===
    elif tool_name == "smart_action":
        result = await execute_smart_action(tool_input["action_description"])
    elif tool_name == "wait_for_element":
        result = await execute_wait_for_element(
            tool_input["description"],
            tool_input.get("timeout", 10),
        )
    elif tool_name == "window_manager":
        result = await execute_window_manager(
            tool_input["action"],
            tool_input.get("target"),
        )
    # === Vision Engine: Precision Computer Control ===
    elif tool_name == "som_screenshot":
        try:
            import vision_engine
            _loop = asyncio.get_running_loop()
            raw_buf, ann_buf, elements = await _loop.run_in_executor(
                None, lambda: vision_engine.som_screenshot(tool_input.get("region")),
            )
            vision_engine.set_som_elements(elements)
            screenshot_buffer = ann_buf  # Send annotated version to user
            if elements:
                elem_lines = []
                for e in elements:
                    elem_lines.append(
                        f"  #{e['id']} [{e['kind']}] at ({e['cx']},{e['cy']}) size {e['w']}x{e['h']}"
                    )
                result = (
                    f"SoM Screenshot: {len(elements)} UI elements detected.\n"
                    f"Use som_click with the element # to click precisely.\n\n"
                    + "\n".join(elem_lines)
                )
            else:
                result = "SoM Screenshot taken but no UI elements detected. Try take_screenshot instead."
        except Exception as e:
            result = f"SoM screenshot error: {e}"
    elif tool_name == "som_click":
        try:
            import vision_engine
            elements = vision_engine.get_som_elements()
            if not elements:
                result = "No SoM elements cached. Take a som_screenshot first."
            else:
                _loop = asyncio.get_running_loop()
                eid = int(tool_input["element_id"])  # Coerce to int (LLM may send string)
                result = await _loop.run_in_executor(
                    None, lambda: vision_engine.som_click(
                        eid, elements,
                        tool_input.get("button", "left"),
                        tool_input.get("clicks", 1),
                    ),
                )
        except Exception as e:
            result = f"SoM click error: {e}"
    elif tool_name == "ui_tree":
        try:
            import vision_engine
            _loop = asyncio.get_running_loop()
            tree = await _loop.run_in_executor(
                None, lambda: vision_engine.AccessibilityTree.get_tree(
                    window_title=tool_input.get("window_title"),
                    max_depth=min(tool_input.get("max_depth", 4), 6),
                ),
            )
            result = json.dumps(tree, indent=2, ensure_ascii=False, default=str)
        except Exception as e:
            result = f"UI tree error: {e}"
    elif tool_name == "ui_click_element":
        try:
            import vision_engine
            _loop = asyncio.get_running_loop()
            result = await _loop.run_in_executor(
                None, lambda: vision_engine.AccessibilityTree.click_element(
                    window_title=tool_input.get("window_title"),
                    element_name=tool_input.get("element_name"),
                    element_auto_id=tool_input.get("element_auto_id"),
                    element_type=tool_input.get("element_type"),
                ),
            )
        except Exception as e:
            result = f"UI click error: {e}"
    elif tool_name == "ui_type_element":
        try:
            import vision_engine
            _loop = asyncio.get_running_loop()
            result = await _loop.run_in_executor(
                None, lambda: vision_engine.AccessibilityTree.type_into(
                    window_title=tool_input.get("window_title"),
                    text=tool_input["text"],
                    element_name=tool_input.get("element_name"),
                    element_auto_id=tool_input.get("element_auto_id"),
                    clear_first=tool_input.get("clear_first", True),
                ),
            )
        except Exception as e:
            result = f"UI type error: {e}"
    elif tool_name == "smart_ui_click":
        try:
            import vision_engine
            attempts = []
            # Step 0: browser_click (highest precision for web targets)
            browser_selector = tool_input.get("browser_selector")
            if browser_selector:
                try:
                    br_result = await browser_agent.browser_click(browser_selector)
                    if "error" not in br_result.lower() and "not found" not in br_result.lower():
                        result = json.dumps({
                            "success": True,
                            "method": "browser_click",
                            "message": br_result,
                            "attempts": [f"browser_click: {br_result}"],
                        }, ensure_ascii=False)
                        attempts = None  # signal early exit
                    else:
                        attempts.append(f"browser_click: {br_result}")
                except Exception as _be:
                    attempts.append(f"browser_click: exception: {_be}")

            if attempts is not None:
                # Steps 1-3: accessibility tree → SoM → smartclick
                _loop = asyncio.get_running_loop()
                res = await _loop.run_in_executor(
                    None, lambda: vision_engine.smart_ui_click(
                        element_name=tool_input["element_name"],
                        window_title=tool_input.get("window_title"),
                        element_type=tool_input.get("element_type"),
                        fallback_x=tool_input.get("fallback_x"),
                        fallback_y=tool_input.get("fallback_y"),
                    ),
                )
                # Prepend browser_click attempts if any
                if attempts:
                    res.setdefault("attempts", [])
                    res["attempts"] = attempts + res["attempts"]
                result = json.dumps(res, indent=2, ensure_ascii=False)
        except Exception as e:
            result = f"smart_ui_click error: {e}"
    elif tool_name == "suggest_tool":
        try:
            import vision_engine
            ctrl = _get_adaptive_controller()
            ctx = await ctrl.detect_context()
            suggestion = vision_engine.suggest_best_tool(tool_input["action"], ctx)
            result = json.dumps(suggestion, indent=2, ensure_ascii=False)
        except Exception as e:
            result = f"Suggest tool error: {e}"
    elif tool_name == "self_fix":
        try:
            scope = tool_input.get("scope", "syntax")
            bot_dir = os.path.dirname(os.path.abspath(__file__))
            errors = []
            fixed = []

            # Step 1: py_compile all .py files
            py_files = sorted(Path(bot_dir).glob("*.py"))
            for pf in py_files:
                try:
                    proc = await asyncio.get_running_loop().run_in_executor(
                        None, lambda f=pf: subprocess.run(
                            [sys.executable, "-m", "py_compile", str(f)],
                            capture_output=True, text=True, timeout=10,
                        )
                    )
                    if proc.returncode != 0:
                        errors.append({"file": pf.name, "error": (proc.stderr or proc.stdout).strip()})
                except Exception as e:
                    errors.append({"file": pf.name, "error": str(e)})

            if errors:
                result = f"🔍 Found {len(errors)} syntax error(s):\n" + "\n".join(
                    f"  ❌ {e['file']}: {e['error'][:200]}" for e in errors
                )
                result += "\n\n→ Read each file, fix the errors, then call self_fix again to verify."
            else:
                result = f"✅ All {len(py_files)} Python files compile clean. No syntax errors."

            if scope in ("deep", "all"):
                result += "\n\n📋 Deep scan: check import errors and runtime issues manually with `run_command`."
        except Exception as e:
            result = f"Self-fix error: {e}"

    elif tool_name == "codex_task":
        try:
            from codex_charger import CodexCharger
            prompt = tool_input["prompt"]
            timeout = tool_input.get("timeout", 600)
            charger = CodexCharger()
            loop = asyncio.get_running_loop()
            codex_result = await loop.run_in_executor(
                None, lambda: charger.run_task_sync(prompt)
            )
            if codex_result["success"]:
                result = f"✅ Codex完成 ({codex_result['duration']:.0f}s):\n{codex_result['output'][:3000]}"
            else:
                result = f"❌ Codex失败: {codex_result['error'][:500]}\n输出: {codex_result['output'][:1000]}"
        except ImportError:
            result = "❌ codex_charger模块不可用"
        except Exception as e:
            result = f"Codex error: {e}"

    elif tool_name == "send_to_session":
        try:
            import shutil
            message = tool_input["message"]
            session_id = tool_input.get("session_id")
            project_dir = tool_input.get("project_dir") or os.path.dirname(os.path.abspath(__file__))

            # Find claude CLI
            claude_cmd = None
            for c in [shutil.which("claude.cmd"), shutil.which("claude"),
                       str(Path.home() / "AppData/Roaming/npm/claude.cmd")]:
                if c and Path(c).is_file():
                    claude_cmd = c
                    break
            if not claude_cmd:
                claude_cmd = "claude.cmd"

            cmd = [claude_cmd, "-p", message,
                   "--output-format", "text",
                   "--dangerously-skip-permissions"]
            if session_id:
                cmd.extend(["--resume", session_id])

            proc = await asyncio.get_running_loop().run_in_executor(
                None, lambda: subprocess.run(
                    cmd, capture_output=True, text=True,
                    timeout=300, cwd=project_dir,
                    encoding="utf-8", errors="replace",
                )
            )
            output = (proc.stdout or "").strip()
            if proc.returncode == 0 and output:
                result = f"✅ Session响应:\n{output[:3000]}"
            elif proc.returncode == 0:
                result = "✅ 已发送到session（无输出）"
            else:
                result = f"❌ Session错误 (code {proc.returncode}):\n{(proc.stderr or output)[:1000]}"
        except subprocess.TimeoutExpired:
            result = "⏳ Session超时（5分钟），任务可能仍在执行"
        except Exception as e:
            result = f"Session error: {e}"

    else:
        result = f"Unknown tool: {tool_name}"

    # Guard against None result and truncate very long results
    if result is None:
        result = "(no output)"
    if len(result) > 15000:
        result = result[:15000] + "\n... (output truncated)"

    return result, screenshot_buffer
