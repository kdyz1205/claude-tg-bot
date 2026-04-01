"""
Declarative mapping: Telegram command name(s) → handler attribute name on bot.py.

The second element of each tuple must match a key in the dict passed to
``register_command_handlers`` (typically built beside ``create_application``).
"""

from __future__ import annotations

from typing import Any, Callable

from telegram.ext import Application, CommandHandler

# (command names..., handler_dict_key)
COMMAND_BINDINGS: list[tuple[tuple[str, ...], str]] = [
    # —— 核心与会话 ——
    (("start",), "start"),
    (("help",), "help_command"),
    (("ping",), "ping"),
    (("clear",), "clear"),
    (("panel",), "panel_command"),
    (("q",), "quick_action"),
    (("quick",), "quick_action"),
    # —— Claude / 桥接 / 任务 ——
    (("dev",), "dev_command"),
    (("kill",), "kill_command"),
    (("tasks",), "tasks_command"),
    (("cancel",), "cancel_command"),
    (("bridge",), "bridge_command"),
    (("screenshot",), "quick_screenshot"),
    (("model",), "model_command"),
    (("provider",), "provider_command"),
    (("status",), "status_command"),
    (("quota",), "quota_command"),
    (("sessions",), "sessions_command"),
    (("learn",), "learn_command"),
    (("score",), "score_command"),
    (("train",), "train_command"),
    (("session_control",), "session_control_command"),
    (("ms",), "multi_session_command"),
    # —— 健康与监控 ——
    (("health",), "health_command"),
    (("vital",), "vital_command"),
    (("monitor",), "monitor_command"),
    (("proactive",), "proactive_command"),
    (("market",), "market_command"),
    (("memory",), "memory_command"),
    # —— 链上交易 (DEX) + 聚合面板 ——
    (("chain",), "chain_command"),
    (("portfolio",), "portfolio_command"),
    (("strategy",), "strategy_command"),
    (("buy",), "buy_command"),
    (("sell",), "sell_command"),
    (("positions", "pos"), "positions_command"),
    (("settings",), "trade_settings_command"),
    (("pnl",), "pnl_command"),
    (("trade", "t"), "trade_dashboard_command"),
    (("paper",), "paper_command"),
    (("live",), "live_command"),
    (("wallet_setup",), "wallet_setup_command"),
    (("wallet_delete",), "wallet_delete_command"),
    # —— OKX ——
    (("okx",), "okx_command"),
    (("okx_account",), "okx_account_command"),
    (("okx_trade",), "okx_trade_command"),
    (("okx_backtest",), "okx_backtest_command"),
    (("okx_top30",), "okx_top30_command"),
    (("ma_ribbon_backtest",), "ma_ribbon_backtest_command"),
    (("ma_ribbon_screener",), "ma_ribbon_screener_command"),
    # —— 信号 / Alpha / 套利 ——
    (("signal",), "signal_command"),
    (("signal_stats",), "signal_stats_command"),
    (("alpha",), "alpha_command"),
    (("arb",), "arb_command"),
    (("token_analyze",), "token_analyze_command"),
    (("optimize",), "optimize_command"),
    # —— 链上聪明钱 / 论文 ——
    (("onchain",), "onchain_command"),
    (("search",), "search_command"),
    (("whales",), "whales_command"),
    (("track",), "track_command"),
    (("wallets",), "wallets_command"),
    (("addwallet",), "addwallet_command"),
    # —— 报告与风险 ——
    (("report",), "report_command"),
    (("risk",), "risk_command"),
    (("performance",), "performance_command"),
    # —— 进化 / 自主 / 技能 ——
    (("evolution",), "evolution_command"),
    (("evolve",), "evolve_command"),
    (("strategy_evolve",), "strategy_evolve_command"),
    (("evostatus",), "evostatus_command"),
    (("skills",), "skills_command"),
    (("autonomy",), "autonomy_command"),
    (("consciousness",), "consciousness_command"),
    # —— 代码自愈与质量 ——
    (("selfcheck",), "selfcheck_command"),
    (("repairs",), "repairs_command"),
    (("repair_status",), "repair_status_command"),
    (("code_health",), "code_health_command"),
    (("selfrepair",), "selfrepair_command"),
    # —— 仪表盘 / 外部工具 ——
    (("dashboard",), "dashboard_command"),
    (("codex",), "codex_command"),
]

# /train_<domain> 共用 train_command
TRAIN_COMMAND_SUFFIXES: tuple[str, ...] = (
    "file_ops",
    "code_edit",
    "computer_control",
    "browser",
    "obedience",
    "all",
    "stop",
    "reset",
)


def register_command_handlers(
    app: Application,
    auth_filter: Any,
    handlers: dict[str, Callable[..., Any]],
) -> None:
    """Wire all text commands from COMMAND_BINDINGS + train_* aliases."""
    missing = [key for _, key in COMMAND_BINDINGS if key not in handlers]
    if missing:
        raise RuntimeError(
            "register_command_handlers: missing handler keys: "
            + ", ".join(sorted(set(missing)))
        )
    if "train_command" not in handlers:
        raise RuntimeError("register_command_handlers: train_command required")

    for names, key in COMMAND_BINDINGS:
        fn = handlers[key]
        for name in names:
            app.add_handler(CommandHandler(name, fn, filters=auth_filter))

    train_fn = handlers["train_command"]
    for domain in TRAIN_COMMAND_SUFFIXES:
        app.add_handler(CommandHandler(f"train_{domain}", train_fn, filters=auth_filter))
