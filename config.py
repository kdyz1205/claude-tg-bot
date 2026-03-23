"""
config.py — Central configuration for the Telegram bot.

All settings can be overridden via environment variables in .env
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Always load .env from the same directory as this file
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ─── Authentication ───────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

_raw_user_id = os.getenv("AUTHORIZED_USER_ID", "0")
try:
    AUTHORIZED_USER_ID = int(_raw_user_id)
except ValueError:
    AUTHORIZED_USER_ID = 0

# ─── AI Models ────────────────────────────────────────────────────────────────
CLAUDE_MODEL = "claude-sonnet-4-6"
OPENAI_MODEL = "gpt-4o"
GEMINI_MODEL = "gemini-2.5-flash"
CURRENT_PROVIDER = os.getenv("DEFAULT_PROVIDER", "claude")

# ─── Claude Code CLI Settings ────────────────────────────────────────────────
BRIDGE_MODE = os.getenv("BRIDGE_MODE", "true").lower() == "true"
CLAUDE_CLI_TIMEOUT = int(os.getenv("CLAUDE_CLI_TIMEOUT", "300"))  # 5 min default

# ─── Conversation & Processing ────────────────────────────────────────────────
MAX_CONVERSATION_HISTORY = 80
MAX_TOOL_ITERATIONS = 25
COMMAND_TIMEOUT = 30  # seconds for shell commands

# ─── Screenshot ───────────────────────────────────────────────────────────────
SCREENSHOT_QUALITY = 75
MAX_SCREENSHOT_WIDTH = 1920

# ─── File Storage ─────────────────────────────────────────────────────────────
TELEGRAM_FILES_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "telegram_files")
DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "Downloads")

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(Path(__file__).parent, "bot.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ─── Validation ──────────────────────────────────────────────────────────
# Create required directories at import time so they always exist
os.makedirs(TELEGRAM_FILES_DIR, exist_ok=True)
os.makedirs(DOWNLOADS_DIR, exist_ok=True)

# ─── Safety ───────────────────────────────────────────────────────────────────
# Patterns that trigger a permission prompt in API mode.
# In CLI mode (Bridge), Claude Code handles its own permissions.
# Note: taskkill and Stop-Process removed - commonly needed for legitimate tasks
DANGEROUS_PATTERNS = [
    r"rm\s+-rf\s+/",          # rm -rf / (root deletion)
    r"del\s+/[sfq].*[\\\/]Windows",  # Deleting Windows system files
    r"format\s+[a-zA-Z]:",    # Formatting drives
    r"shutdown\s+/[sr]",      # Shutdown/restart system
    r"reg\s+delete.*HKLM",    # Deleting system registry keys
    r"diskpart",              # Disk partitioning
    r"bcdedit",               # Boot config editing
    r"Set-ExecutionPolicy\s+Unrestricted",  # Unrestricted execution
    r"Remove-Item.*-Recurse.*[\\\/]Windows",  # Recursive delete Windows
    r"Remove-Item.*-Recurse.*[\\\/]Program Files",  # Recursive delete Program Files
]
