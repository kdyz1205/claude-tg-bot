"""
Compatibility facade: web metrics live in ``web_dashboard``; Telegram inline panel
templates live in ``gateway.tg_panel``. Importing ``dashboard`` stays stable for ``bot.py``.

TG 面板数据面：持仓等数值由 ``trading.portfolio_snapshot`` 进程内缓存（可选 Redis 镜像，
见 ``get_snapshot_for_gateway``）；不要在 ``dashboard`` 里做链上/交易所计算。
"""

from __future__ import annotations

from gateway.tg_panel import (
    GW_CB,
    tg_gw_build_back_keyboard,
    tg_gw_build_main_keyboard,
    tg_gw_build_positions_keyboard,
    tg_gw_escape_v2,
    tg_gw_mode_label,
    tg_gw_render_callback_pending_text,
    tg_gw_render_home_text,
    tg_gw_render_positions_text,
    tg_gw_render_strategy_text,
)
from web_dashboard import (
    _flask_available,
    app,
    get_stats_text,
    record_message,
    start_dashboard,
)

__all__ = [
    "GW_CB",
    "tg_gw_build_back_keyboard",
    "tg_gw_build_main_keyboard",
    "tg_gw_build_positions_keyboard",
    "tg_gw_escape_v2",
    "tg_gw_mode_label",
    "tg_gw_render_callback_pending_text",
    "tg_gw_render_home_text",
    "tg_gw_render_positions_text",
    "tg_gw_render_strategy_text",
    "record_message",
    "start_dashboard",
    "get_stats_text",
    "app",
    "_flask_available",
]
