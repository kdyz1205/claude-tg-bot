import os
from pathlib import Path
from dotenv import load_dotenv

# Always load .env from the same directory as this file
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
_raw_user_id = os.getenv("AUTHORIZED_USER_ID", "0")
try:
    AUTHORIZED_USER_ID = int(_raw_user_id)
except ValueError:
    AUTHORIZED_USER_ID = 0

CLAUDE_MODEL = "claude-sonnet-4-6"
OPENAI_MODEL = "gpt-4o"
GEMINI_MODEL = "gemini-2.5-flash"

# Multi-provider settings
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
CURRENT_PROVIDER = os.getenv("DEFAULT_PROVIDER", "claude")  # claude | openai | gemini

BRIDGE_MODE = os.getenv("BRIDGE_MODE", "true").lower() == "true"

MAX_CONVERSATION_HISTORY = 80  # More history for complex multi-step tasks
SCREENSHOT_QUALITY = 75
MAX_SCREENSHOT_WIDTH = 1920
COMMAND_TIMEOUT = 30
MAX_TOOL_ITERATIONS = 25  # More iterations for complex engineering tasks

DANGEROUS_PATTERNS = [
    r"rm\s+-rf",
    r"del\s+/[sfq]",
    r"rmdir",
    r"format\s+[a-zA-Z]:",
    r"shutdown",
    r"restart",
    r"reg\s+delete",
    r"reg\s+add",
    r"netsh",
    r"net\s+user",
    r"taskkill",
    r"diskpart",
    r"bcdedit",
    r"sfc\s+/",
    r"dism",
    r"Remove-Item.*-Recurse",
    r"Stop-Process",
    r"Set-ExecutionPolicy",
    r"Uninstall-",
    r"Clear-Content",
]
