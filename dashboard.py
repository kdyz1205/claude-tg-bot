"""
Compatibility facade for the web dashboard.

- **Web**: metrics and Flask app live in ``web_dashboard``.
- **Telegram gateway panel**: 极简看板文案与键盘在 ``gateway.tg_front``，由 ``gateway.telegram_bot``
  的 ``/start`` 与 ``gw:*`` 回调驱动。本模块不 re-export PTB UI。

Importing ``dashboard`` remains stable for ``bot.py`` (``start_dashboard``, ``get_stats_text``, …).
"""

from __future__ import annotations

from dataclasses import dataclass

from web_dashboard import (
    _flask_available,
    app,
    get_stats_text,
    record_message,
    start_dashboard,
)


@dataclass(frozen=True)
class GatewayPanelConfig:
    """Pure data: inline panel callback prefix (must match ``gateway.tg_front.GW_CB``)."""

    callback_prefix: str = "gw"


# Single source of truth for the prefix string is ``gateway.tg_front.GW_CB``; kept here for data-only imports.
GW_CB = GatewayPanelConfig.callback_prefix

__all__ = [
    "GatewayPanelConfig",
    "GW_CB",
    "record_message",
    "start_dashboard",
    "get_stats_text",
    "app",
    "_flask_available",
]
