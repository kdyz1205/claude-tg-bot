"""
Human-facing command catalog and Telegram menu entries.

Blueprint mode: slash surface is **only** ``/start`` (main panel) and ``/trade`` (manual hint).
All other capabilities are reached via natural language → ``gateway.jarvis_semantic`` (main bot)
or remain available as typed commands where ``bot.py`` still registers handlers (no menu pollution).
"""

from __future__ import annotations

from telegram import BotCommand

START_FOOTER_COMMANDS = "/start /trade"

HELP_SECTIONS: list[tuple[str, list[str]]] = [
    (
        "🧠 Jarvis 网关（极简）",
        [
            "/start — 主控台：持仓摘要、引擎启停、刷新、纸/实盘切换",
            "/trade — 手动交易说明（实际买卖请用自然语言，由语义层路由）",
            "其余需求直接打字：闲聊 / 交易意图 / 写代码与因子 → 自动分流，无需记命令表。",
        ],
    ),
]


def format_help_message() -> str:
    lines = ["🤖 命令一览（极简模式）\n"]
    for title, items in HELP_SECTIONS:
        lines.append(title + "\n")
        for it in items:
            lines.append(f"  {it}\n")
        lines.append("")
    lines.append("主机器人如仍注册其他 / 指令，可手动输入；本菜单仅展示 /start 与 /trade。")
    return "".join(lines)


def telegram_menu_bot_commands() -> list[BotCommand]:
    """Telegram ``set_my_commands`` palette — blueprint: two entries only."""
    return [
        BotCommand("start", "主控台·持仓·引擎"),
        BotCommand("trade", "手动交易说明"),
    ]
