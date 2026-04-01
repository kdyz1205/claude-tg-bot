"""
Declarative mapping: Telegram command name(s) → handler attribute name on bot.py.

Blueprint：仅 ``/start``、``/trade``、``/t`` 注册为 CommandHandler；其余斜杠由
``jarvis_plain_text_entry`` → Jarvis 语义层处理。
"""

from __future__ import annotations

from typing import Any, Callable

from telegram.ext import Application, CommandHandler

COMMAND_BINDINGS: list[tuple[tuple[str, ...], str]] = [
    (("start",), "jarvis_start"),
    (("trade", "t"), "jarvis_trade"),
]


def register_command_handlers(
    app: Application,
    auth_filter: Any,
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Wire commands from COMMAND_BINDINGS only."""
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
