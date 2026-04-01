"""
Telegram command registry — single place for command bindings, /help sections,
and BotFather-style menu entries. Handlers stay in bot.py; this package only
holds metadata and registration helpers to avoid drift between help text and
actual CommandHandler wiring.
"""

from tg_registry.catalog import (
    START_FOOTER_COMMANDS,
    format_help_message,
    telegram_menu_bot_commands,
)
from tg_registry.registration import COMMAND_BINDINGS, TRAIN_COMMAND_SUFFIXES, register_command_handlers

__all__ = [
    "COMMAND_BINDINGS",
    "START_FOOTER_COMMANDS",
    "TRAIN_COMMAND_SUFFIXES",
    "format_help_message",
    "register_command_handlers",
    "telegram_menu_bot_commands",
]
