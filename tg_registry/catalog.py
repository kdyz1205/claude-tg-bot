"""
Human-facing command catalog: /help text and Telegram menu entries.

Kept separate from ``registration.py`` so wording can evolve without touching
handler wiring order.
"""

from __future__ import annotations

from telegram import BotCommand

# Shown under /start (short; detailed list is /help)
START_FOOTER_COMMANDS = "/start /trade"

# /help：与 registration 一致，只承诺已注册的斜杠；其余能力走 Jarvis 自然语言
HELP_SECTIONS: list[tuple[str, list[str]]] = [
    (
        "⚙️ 已注册斜杠",
        [
            "/start — 主控台与实盘/Paper 视图（网关面板 + gw:* 回调）",
            "/trade — 极速手动交易面板",
            "/help — 本清单",
        ],
    ),
    (
        "💬 Jarvis",
        [
            "策略、造物、风控、交易意图 → 直接打字即可，由语义层路由。",
            "其它以 / 开头的命令未注册，将收到简短提示（请用侧栏或自然语言）。",
        ],
    ),
]


def format_help_message() -> str:
    lines = ["🤖 Bot 功能一览（按类别）\n"]
    for title, items in HELP_SECTIONS:
        lines.append(title + "\n")
        for it in items:
            lines.append(f"  {it}\n")
        lines.append("")
    lines.append("直接说就行，不用客气。")
    return "".join(lines)


def get_core_menu_commands() -> list[BotCommand]:
    """
    唯一 Telegram 侧栏菜单源：主进程与网关共用 ``set_my_commands``。
    仅两项；其余指令仍可通过打字或 registration 使用，不进入 Bot 菜单。
    """
    return [
        BotCommand("start", "🚀 主控台与实盘大盘"),
        BotCommand("trade", "⚔️ 极速手动交易面板"),
    ]
