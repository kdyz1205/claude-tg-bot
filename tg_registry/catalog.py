"""
Human-facing command catalog: /help text and Telegram menu entries.

Kept separate from ``registration.py`` so wording can evolve without touching
handler wiring order.
"""

from __future__ import annotations

from telegram import BotCommand

# Shown under /start (short; detailed list is /help)
START_FOOTER_COMMANDS = "/start /trade"

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
            "/trade (/t) — 综合交易键盘（含 💼 Wallet 链上钱包）",
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


def get_core_menu_commands() -> list[BotCommand]:
    """
    唯一 Telegram 侧栏菜单源：主进程与网关共用 ``set_my_commands``。
    仅两项；其余指令仍可通过打字或 registration 使用，不进入 Bot 菜单。
    """
    return [
        BotCommand("start", "🚀 主控台与实盘大盘"),
        BotCommand("trade", "⚔️ 极速手动交易面板"),
    ]
