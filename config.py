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

# Task-tier API routing (see providers_router.classify_task_tier)
# Fast / cheap: quick judgments, formatting, extraction
TASK_TIER_FAST_CLAUDE = os.getenv("TASK_TIER_FAST_CLAUDE", "claude-3-5-haiku-20241022")
TASK_TIER_FAST_OPENAI = os.getenv("TASK_TIER_FAST_OPENAI", "gpt-4o-mini")
# Heavy: strategy, multimodal, long reasoning (empty string → use CLAUDE_MODEL / OPENAI_MODEL)
TASK_TIER_HEAVY_CLAUDE = os.getenv("TASK_TIER_HEAVY_CLAUDE", "").strip() or None
TASK_TIER_HEAVY_OPENAI = os.getenv("TASK_TIER_HEAVY_OPENAI", "").strip() or None

try:
    API_TRANSIENT_RETRIES = max(1, int(os.getenv("API_TRANSIENT_RETRIES", "4")))
except ValueError:
    API_TRANSIENT_RETRIES = 4
try:
    API_REQUEST_TIMEOUT_SEC = float(os.getenv("API_REQUEST_TIMEOUT_SEC", "120"))
except ValueError:
    API_REQUEST_TIMEOUT_SEC = 120.0

# ─── Claude Code CLI Settings ────────────────────────────────────────────────
BRIDGE_MODE = os.getenv("BRIDGE_MODE", "true").lower() == "true"
# Harness mode: use browser AI as PRIMARY, CLI only for computer control
# When True, overrides BRIDGE_MODE for non-tool tasks
HARNESS_MODE = os.getenv("HARNESS_MODE", "true").lower() == "true"
try:
    CLAUDE_CLI_TIMEOUT = int(os.getenv("CLAUDE_CLI_TIMEOUT", "1800"))  # 30 min default
except ValueError:
    CLAUDE_CLI_TIMEOUT = 1800
try:
    _cli_async_t = float(os.getenv("CLAUDE_CLI_ASYNC_TIMEOUT_SEC", "45"))
except ValueError:
    _cli_async_t = 45.0
# Hard cap 45s per invoke: avoids TG event-loop freeze + zombie CLI on prompts
CLAUDE_CLI_ASYNC_TIMEOUT_SEC = max(5.0, min(45.0, _cli_async_t))

# ─── HTTP LLM (aiohttp: Ollama / Anthropic / OpenAI) — replaces Claude CLI subprocess ──
LLM_HTTP_BACKEND = os.getenv("LLM_HTTP_BACKEND", "auto").strip().lower()
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.2")
try:
    LLM_HTTP_MAX_CONCURRENT = max(1, int(os.getenv("LLM_HTTP_MAX_CONCURRENT", "8")))
except ValueError:
    LLM_HTTP_MAX_CONCURRENT = 8
try:
    MAX_HTTP_LLM_HISTORY_MSGS = max(4, int(os.getenv("MAX_HTTP_LLM_HISTORY_MSGS", "40")))
except ValueError:
    MAX_HTTP_LLM_HISTORY_MSGS = 40
OPENAI_API_BASE = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
ANTHROPIC_API_BASE = os.getenv("ANTHROPIC_API_BASE", "https://api.anthropic.com").rstrip("/")
try:
    ANTHROPIC_MAX_TOKENS = max(256, int(os.getenv("ANTHROPIC_MAX_TOKENS", "8192")))
except ValueError:
    ANTHROPIC_MAX_TOKENS = 8192

# HTTP LLM token budget (0 = unlimited). Tracked in tracker/quota.HttpLlmTokenBudget.
try:
    LLM_DAILY_TOKEN_BUDGET = max(0, int(os.getenv("LLM_DAILY_TOKEN_BUDGET", "0")))
except ValueError:
    LLM_DAILY_TOKEN_BUDGET = 0

# Comma-separated model ids to try after primary on overload (same backend only).
_raw_fb = os.getenv("LLM_HTTP_FALLBACK_MODELS", "").strip()
LLM_HTTP_FALLBACK_MODELS: list[str] = [m.strip() for m in _raw_fb.split(",") if m.strip()]
if not LLM_HTTP_FALLBACK_MODELS:
    LLM_HTTP_FALLBACK_MODELS = [
        m for m in (
            TASK_TIER_FAST_CLAUDE,
            "claude-3-5-haiku-20241022",
            OLLAMA_MODEL,
        ) if m
    ]

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
    # Linux-specific dangerous patterns
    r"\bdd\b\s+if=",                  # dd raw disk write
    r"\bmkfs\b",                      # Filesystem formatting
    r"\bchmod\s+777\b",              # World-writable permissions
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

# ─── On-chain WebSocket RPCs (onchain_tracker / onchain_ws_listen) ────────────
# Public defaults; override with your own Alchemy/Infura/QuickNode WSS if needed.
ONCHAIN_ETH_WSS = os.getenv("ONCHAIN_ETH_WSS", "wss://ethereum-rpc.publicnode.com").strip()
ONCHAIN_BSC_WSS = os.getenv("ONCHAIN_BSC_WSS", "wss://bsc-rpc.publicnode.com").strip()
ONCHAIN_SOL_WSS = os.getenv("ONCHAIN_SOL_WSS", "wss://api.mainnet-beta.solana.com").strip()
SOLANA_RPC_HTTP = os.getenv("SOLANA_RPC_HTTP", "https://api.mainnet-beta.solana.com").strip()

# ─── Never-Die Fallback Chain ────────────────────────────────────────────────
# The bot should NEVER stop responding. When one provider fails, switch to next.
# Order: CLI (free Plan tokens) → API providers → web AI (free) → cached responses
FALLBACK_CHAIN = ["claude_cli", "claude_api", "openai", "gemini", "web_ai", "cached"]
NEVER_DIE_MODE = os.getenv("NEVER_DIE_MODE", "true").lower() == "true"
try:
    AUTO_RETRY_SECONDS = int(os.getenv("AUTO_RETRY_SECONDS", "60"))
except ValueError:
    AUTO_RETRY_SECONDS = 60

# ─── Telegram Bot API (HTTPX) ───────────────────────────────────────────────
# PTB defaults to read_timeout=5s; slow networks / large editMessageText → "Timed out".
try:
    TELEGRAM_HTTP_READ_TIMEOUT = float(os.getenv("TELEGRAM_HTTP_READ_TIMEOUT", "45"))
except ValueError:
    TELEGRAM_HTTP_READ_TIMEOUT = 45.0
TELEGRAM_HTTP_READ_TIMEOUT = max(10.0, min(120.0, TELEGRAM_HTTP_READ_TIMEOUT))
try:
    TELEGRAM_HTTP_WRITE_TIMEOUT = float(os.getenv("TELEGRAM_HTTP_WRITE_TIMEOUT", "45"))
except ValueError:
    TELEGRAM_HTTP_WRITE_TIMEOUT = 45.0
TELEGRAM_HTTP_WRITE_TIMEOUT = max(10.0, min(120.0, TELEGRAM_HTTP_WRITE_TIMEOUT))
try:
    TELEGRAM_HTTP_CONNECT_TIMEOUT = float(os.getenv("TELEGRAM_HTTP_CONNECT_TIMEOUT", "15"))
except ValueError:
    TELEGRAM_HTTP_CONNECT_TIMEOUT = 15.0
TELEGRAM_HTTP_CONNECT_TIMEOUT = max(5.0, min(60.0, TELEGRAM_HTTP_CONNECT_TIMEOUT))
try:
    TELEGRAM_HTTP_POOL_TIMEOUT = float(os.getenv("TELEGRAM_HTTP_POOL_TIMEOUT", "10"))
except ValueError:
    TELEGRAM_HTTP_POOL_TIMEOUT = 10.0
TELEGRAM_HTTP_POOL_TIMEOUT = max(2.0, min(60.0, TELEGRAM_HTTP_POOL_TIMEOUT))
# getUpdates long-poll must not expire before Telegram returns (often up to ~50s).
try:
    TELEGRAM_GET_UPDATES_READ_TIMEOUT = float(os.getenv("TELEGRAM_GET_UPDATES_READ_TIMEOUT", "70"))
except ValueError:
    TELEGRAM_GET_UPDATES_READ_TIMEOUT = 70.0
TELEGRAM_GET_UPDATES_READ_TIMEOUT = max(30.0, min(180.0, TELEGRAM_GET_UPDATES_READ_TIMEOUT))

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
