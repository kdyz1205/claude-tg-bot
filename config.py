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

_raw_user_id = os.getenv("AUTHORIZED_USER_ID", "")
try:
    AUTHORIZED_USER_ID = int(_raw_user_id) if _raw_user_id.strip() else None
except ValueError:
    import logging as _cfg_log
    _cfg_log.getLogger(__name__).warning(f"Invalid AUTHORIZED_USER_ID: '{_raw_user_id}' — must be numeric")
    AUTHORIZED_USER_ID = None
if AUTHORIZED_USER_ID is None:
    import warnings
    warnings.warn(
        "AUTHORIZED_USER_ID is not set. Bot will reject ALL messages. "
        "Set AUTHORIZED_USER_ID in .env to your Telegram user ID.",
        stacklevel=2,
    )

# ─── AI Models ────────────────────────────────────────────────────────────────
CLAUDE_MODEL = os.getenv("CLAUDE_MODEL", "claude-opus-4-6")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
CURRENT_PROVIDER = os.getenv("DEFAULT_PROVIDER", "claude")

# ─── Claude Code CLI Settings ────────────────────────────────────────────────
BRIDGE_MODE = os.getenv("BRIDGE_MODE", "true").lower() == "true"
# Harness mode: use browser AI as PRIMARY, CLI only for computer control
# When True, overrides BRIDGE_MODE for non-tool tasks
HARNESS_MODE = os.getenv("HARNESS_MODE", "true").lower() == "true"
try:
    CLAUDE_CLI_TIMEOUT = int(os.getenv("CLAUDE_CLI_TIMEOUT", "1800"))  # 30 min default
except ValueError:
    CLAUDE_CLI_TIMEOUT = 1800

# ─── Conversation & Processing ────────────────────────────────────────────────
MAX_CONVERSATION_HISTORY = 80
MAX_TOOL_ITERATIONS = 25
COMMAND_TIMEOUT = 30  # seconds for shell commands

# ─── Screenshot ───────────────────────────────────────────────────────────────
SCREENSHOT_QUALITY = 85
MAX_SCREENSHOT_WIDTH = 1920

# ─── File Storage ─────────────────────────────────────────────────────────────
TELEGRAM_FILES_DIR = os.path.join(os.path.expanduser("~"), "Desktop", "telegram_files")
DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "Downloads")

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.log")
_log_level_raw = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = _log_level_raw if _log_level_raw in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL") else "INFO"

# ─── Validation ──────────────────────────────────────────────────────────
# Create required directories at import time so they always exist
try:
    os.makedirs(TELEGRAM_FILES_DIR, exist_ok=True)
    os.makedirs(DOWNLOADS_DIR, exist_ok=True)
except OSError:
    pass  # Non-fatal: directories may be on unavailable paths in some environments

# ─── Safety ───────────────────────────────────────────────────────────────────
# Patterns that trigger a permission prompt in API mode.
# In CLI mode (Bridge), Claude Code handles its own permissions.
# Note: taskkill and Stop-Process removed - commonly needed for legitimate tasks
DANGEROUS_PATTERNS = [
    r"rm\s+-rf\s+/",          # rm -rf / (root deletion)
    r"del\s+/[sfq].*[\\\/]Windows",  # Deleting Windows system files
    r"\bformat\b\s+[a-zA-Z]:",    # Formatting drives
    r"\bshutdown\b\s+/[sr]",      # Shutdown/restart system
    r"\breg\b\s+delete.*HKLM",    # Deleting system registry keys
    r"\bdiskpart\b",              # Disk partitioning
    r"\bbcdedit\b",               # Boot config editing
    r"\bSet-ExecutionPolicy\b\s+Unrestricted",  # Unrestricted execution
    r"\bRemove-Item\b.*-Recurse.*[\\\/]Windows",  # Recursive delete Windows
    r"\bRemove-Item\b.*-Recurse.*[\\\/]Program Files",  # Recursive delete Program Files
]

# ─── AI Capabilities ───────────────────────────────────────────────────────────
ENABLE_VISION = True
ENABLE_WEB_SEARCH = True

# ─── Self-Monitor ─────────────────────────────────────────────────────────────
SELF_MONITOR_ENABLED = os.getenv("SELF_MONITOR_ENABLED", "true").lower() == "true"
try:
    SELF_MONITOR_INTERVAL = int(os.getenv("SELF_MONITOR_INTERVAL", "60"))
except ValueError:
    SELF_MONITOR_INTERVAL = 60

# ─── Session Learning ────────────────────────────────────────────────────────
SESSION_LEARNING_ENABLED = os.getenv("SESSION_LEARNING_ENABLED", "true").lower() == "true"
try:
    SESSION_LEARNING_INTERVAL = int(os.getenv("SESSION_LEARNING_INTERVAL", "50"))
except ValueError:
    SESSION_LEARNING_INTERVAL = 50

# ─── Proactive Agent ─────────────────────────────────────────────────────────
PROACTIVE_AGENT_ENABLED = os.getenv("PROACTIVE_AGENT_ENABLED", "true").lower() == "true"

# ─── Market Monitor ───────────────────────────────────────────────────────────
MARKET_MONITOR_ENABLED = os.getenv("MARKET_MONITOR_ENABLED", "true").lower() == "true"

# ─── Never-Die Fallback Chain ────────────────────────────────────────────────
# The bot should NEVER stop responding. When one provider fails, switch to next.
# Order: CLI (free Plan tokens) → API providers → web AI (free) → cached responses
FALLBACK_CHAIN = ["claude_cli", "claude_api", "openai", "gemini", "web_ai", "cached"]
NEVER_DIE_MODE = os.getenv("NEVER_DIE_MODE", "true").lower() == "true"
try:
    AUTO_RETRY_SECONDS = int(os.getenv("AUTO_RETRY_SECONDS", "60"))
except ValueError:
    AUTO_RETRY_SECONDS = 60

# ─── Heartbeat ───────────────────────────────────────────────────────────────
HEARTBEAT_ENABLED = os.getenv("HEARTBEAT_ENABLED", "true").lower() == "true"
try:
    HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "1800"))  # 30 minutes
except ValueError:
    HEARTBEAT_INTERVAL = 1800
try:
    HEARTBEAT_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "60"))  # 60 seconds
except ValueError:
    HEARTBEAT_TIMEOUT = 60
