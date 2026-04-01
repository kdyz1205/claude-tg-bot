"""
Telegram command registry — single place for command bindings, /help sections,
and BotFather-style menu entries. Handlers stay in bot.py; this package only
holds metadata and registration helpers to avoid drift between help text and
actual CommandHandler wiring.
"""

from tg_registry.catalog import (
    START_FOOTER_COMMANDS,
    format_help_message,
    get_core_menu_commands,
)
from tg_registry.registration import COMMAND_BINDINGS, register_command_handlers

__all__ = [
    "COMMAND_BINDINGS",
    "START_FOOTER_COMMANDS",
    "format_help_message",
    "get_core_menu_commands",
    "register_command_handlers",
]
