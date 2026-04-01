"""
Backward-compatible aliases for ``gateway.tg_front`` (legacy ``tg_gw_*`` names).
"""

from __future__ import annotations

from gateway.tg_front import GW_CB as GW_CB
from gateway.tg_front import build_back_keyboard as tg_gw_build_back_keyboard
from gateway.tg_front import build_main_keyboard as tg_gw_build_main_keyboard
from gateway.tg_front import build_positions_keyboard as tg_gw_build_positions_keyboard
from gateway.tg_front import escape_v2 as tg_gw_escape_v2
from gateway.tg_front import mode_label as tg_gw_mode_label
from gateway.tg_front import render_home_text as tg_gw_render_home_text
from gateway.tg_front import render_positions_text as tg_gw_render_positions_text
from gateway.tg_front import render_strategy_text as tg_gw_render_strategy_text


def tg_gw_render_callback_pending_text() -> str:
    """Deprecated: instant path uses plain ``⏳ 正在切换…`` in ``telegram_bot``."""
    return tg_gw_escape_v2("⏳ 正在切换…")
