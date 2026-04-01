"""
Declarative mapping: Telegram command name(s) → handler attribute name on bot.py.

仅注册核心斜杠 ``/start`` 与 ``/trade``（及 ``/t``）。其余 ``/…`` 不注册
CommandHandler，由 ``bot.py`` 的 ``MessageHandler(filters.COMMAND, …)`` 交给
``handle_message`` → 主对话 / Jarvis；避免「幽灵斜杠」绕过语义层。
"""

from __future__ import annotations

from typing import Any, Callable

from telegram.ext import Application, CommandHandler

# (command names..., handler_dict_key) — 仅此两条硬接线
COMMAND_BINDINGS: list[tuple[tuple[str, ...], str]] = [
    (("start",), "start"),
    (("trade", "t"), "trade_dashboard_command"),
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
