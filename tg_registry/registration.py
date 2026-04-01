"""
Declarative mapping: Telegram command name(s) → handler attribute name on bot.py.

Blueprint purge: **only** ``/start`` and ``/trade`` are registered. All other behavior
must enter through ``jarvis_plain_text_entry`` → ``gateway.jarvis_semantic``.
"""

from __future__ import annotations

from typing import Any, Callable

from telegram.ext import Application, CommandHandler

# (command names..., handler_dict_key) — physical surface = 2 commands only
COMMAND_BINDINGS: list[tuple[tuple[str, ...], str]] = [
    (("start",), "jarvis_start"),
    (("trade",), "jarvis_trade"),
]

# Kept empty for backward-compatible imports; train_* handlers are removed.
TRAIN_COMMAND_SUFFIXES: tuple[str, ...] = ()


def register_command_handlers(
    app: Application,
    auth_filter: Any,
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Wire slash commands from COMMAND_BINDINGS only (no train_* aliases)."""
    missing = [key for _, key in COMMAND_BINDINGS if key not in handlers]
    if missing:
        raise RuntimeError(
            "register_command_handlers: missing handler keys: "
            + ", ".join(sorted(set(missing)))
        )

    for names, key in COMMAND_BINDINGS:
        fn = handlers[key]
        for name in names:
            app.add_handler(CommandHandler(name, fn, filters=auth_filter))
