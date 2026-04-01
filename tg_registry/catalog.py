"""
Human-facing command catalog: /help text and Telegram menu entries.

Kept separate from ``registration.py`` so wording can evolve without touching
handler wiring order.
"""

from __future__ import annotations

from telegram import BotCommand

# Shown under /start (short; detailed list is /help)
START_FOOTER_COMMANDS = "/live /chain /trade /portfolio /help"

# Sections for /help — trimmed to commands that stay registered (see tg_registry/registration.py)
HELP_SECTIONS: list[tuple[str, list[str]]] = [
    (
        "⚙️ 核心",
        [
            "/start — 欢迎 + 实盘/Paper 视图",
            "/help — 本清单",
            "/panel /ping /clear /q /quick — 面板与快捷",
            "/model /provider — 模型与路由",
            "/status — Bot 与系统状态",
            "/cancel — 取消排队任务",
            "/train、/train_file_ops … — 子代理训练",
        ],
    ),
    (
        "🔗 交易与链上",
        [
            "/chain — 链上面板（快照 + 按钮）",
            "/portfolio — 聚合持仓",
            "/strategy — 策略总控",
            "/trade (/t) — 综合交易键盘",
            "/live start|stop|status — 实盘调度",
            "/paper — 模拟盘",
            "/buy /sell /positions /settings /pnl",
            "/wallet_setup /wallet_delete",
        ],
    ),
    (
        "🚀 OKX · 信号",
        [
            "/okx_trade /okx /okx_account",
            "/signal /signal_stats /alpha /arb",
        ],
    ),
    (
        "🐋 聪明钱与报告",
        [
            "/onchain /whales /track /wallets /addwallet",
            "/report /risk /performance",
            "/evolution /evostatus",
        ],
    ),
    (
        "💬 自然语言",
        [
            "网关模式：策略、造物、风控 → 直接中文说即可（不必记斜杠）。",
            "粘贴 Solana CA — 狙击卡片（主 bot）",
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


def minimal_slash_menu_commands() -> list[BotCommand]:
    """
    Telegram 侧栏菜单 — 短列表，避免客户端截断；与网关实际注册的斜杠一致。

    未出现在菜单里的指令仍可在主进程 ``python bot.py`` 中通过打字使用（若仍在 registration 中）。
    """
    return [
        BotCommand("start", "入门 / 实盘·Paper 视图"),
        BotCommand("live", "实盘 start|stop|status"),
        BotCommand("chain", "链上面板"),
        BotCommand("trade", "综合交易键盘"),
        BotCommand("portfolio", "聚合持仓"),
        BotCommand("status", "Bot / 系统状态"),
        BotCommand("help", "命令清单"),
        BotCommand("wallet_setup", "配置 Solana 钱包"),
    ]


def telegram_menu_bot_commands() -> list[BotCommand]:
    """Alias: bot.py post_init 与网关共用同一短菜单。"""
    return minimal_slash_menu_commands()
