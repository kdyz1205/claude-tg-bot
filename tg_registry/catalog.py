"""
Human-facing command catalog: /help text and Telegram menu entries.

Kept separate from ``registration.py`` so wording can evolve without touching
handler wiring order.
"""

from __future__ import annotations

from telegram import BotCommand

# Shown under /start (short; detailed list is /help)
START_FOOTER_COMMANDS = "/chain /portfolio /strategy /panel /help"

# Sections for /help — title + bullet lines (no trailing double newlines inside items)
HELP_SECTIONS: list[tuple[str, list[str]]] = [
    (
        "⚙️ 核心与会话",
        [
            "/start — 欢迎 + 选择实盘/Paper 面板视图",
            "/help — 全部分类命令（本清单）",
            "/panel — 控制面板",
            "/ping /clear — 延迟测试、清空对话",
            "/q /quick — 快捷动作",
        ],
    ),
    (
        "🤖 Claude 与任务",
        [
            "/dev — AutoDev 写代码到指定 .py",
            "/kill — 终止卡死的 Claude CLI 并清队列",
            "/tasks /cancel — 任务队列",
            "/bridge — 桥接状态",
            "/screenshot — 截图",
            "/model /provider — 模型与路由",
            "/status /quota — Bot 状态与 AI 用量",
            "/sessions /learn /score — 会话与学习",
            "/train — 训练子代理；/train_file_ops … 等同",
            "/session_control /ms — 多会话控制",
        ],
    ),
    (
        "🔗 链上 (Solana DEX) 与聚合",
        [
            "/chain — 链上面板（OKX+DEX+钱包 快照）",
            "/portfolio — 聚合持仓长文",
            "/strategy — 策略总控（实盘/模拟/奇点）",
            "/buy <CA> [金额] /sell <CA> [%]",
            "/positions (/pos) — DEX 跟踪仓",
            "/settings — 交易设置",
            "/pnl — 链上 PnL",
            "/trade (/t) — 综合交易面板",
            "/paper — 模拟",
            "/live start|stop|status — 实盘调度",
            "/wallet_setup /wallet_delete — 钱包",
        ],
    ),
    (
        "🚀 OKX 与回测",
        [
            "/okx_trade start|stop|live|paper",
            "/okx [SYMBOL] — 行情",
            "/okx_account — 余额",
            "/okx_backtest /okx_top30",
            "/ma_ribbon_backtest /ma_ribbon_screener",
        ],
    ),
    (
        "📡 信号 · Alpha · 套利",
        [
            "/signal /signal_stats",
            "/alpha — Alpha 扫描",
            "/arb — 套利摘要",
            "/token_analyze — 代币分析",
            "/optimize — 策略优化",
        ],
    ),
    (
        "🐋 链上监控与聪明钱",
        [
            "/onchain — 链上指令",
            "/search /whales /track",
            "/wallets /addwallet",
        ],
    ),
    (
        "📊 报告 · 风险 · 表现",
        [
            "/report — 信号/收益报告",
            "/risk — 风险参数",
            "/performance — 表现统计",
        ],
    ),
    (
        "🧬 进化 · 自主 · 技能",
        [
            "/evolution /evostatus — 进化引擎",
            "/evolve /strategy_evolve",
            "/skills — 技能库",
            "/autonomy /consciousness",
        ],
    ),
    (
        "🔧 代码健康与自愈",
        [
            "/selfcheck /repairs /repair_status",
            "/code_health /selfrepair",
        ],
    ),
    (
        "📈 其他",
        [
            "/dashboard — 仪表盘",
            "/codex — Codex 任务",
            "/health /vital — 健康检查",
            "/monitor /proactive /market",
            "/memory — 记忆",
        ],
    ),
    (
        "💬 中文快捷（自然语言）",
        [
            "「价格 BTC」— 行情",
            "「大盘」— 市场概览",
            "「扫描」— Alpha",
            "「持仓」— 同 /portfolio",
            "「链上」— 链上面板",
            "粘贴 Solana CA — 狙击卡片",
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


def telegram_menu_bot_commands() -> list[BotCommand]:
    """Subset for Telegram command menu (long menus get truncated in clients)."""
    return [
        BotCommand("chain", "🔗 链上交易 (真实快照+买卖)"),
        BotCommand("portfolio", "💼 OKX+钱包+DEX 持仓"),
        BotCommand("strategy", "🧠 策略总控 (实盘/模拟/奇点)"),
        BotCommand("okx_trade", "🚀 OKX 策略 (start/stop/live)"),
        BotCommand("trade", "💹 综合交易面板"),
        BotCommand("live", "🔴 实盘 start|stop|status"),
        BotCommand("wallet_setup", "🔐 配置 Solana 钱包"),
        BotCommand("alpha", "🔍 Alpha 扫描"),
        BotCommand("evolution", "🧬 进化引擎状态"),
        BotCommand("okx", "📊 OKX 行情"),
        BotCommand("provider", "🤖 AI 模型"),
        BotCommand("status", "📡 Bot 状态"),
        BotCommand("help", "❓ 全部分类命令"),
    ]
