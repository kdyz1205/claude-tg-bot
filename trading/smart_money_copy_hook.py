"""
聪明钱「寄生跟单」桥接 — 在链上雷达已发出的共识/鲸鱼信号后，可选触发实盘跟单。

- 由 ``onchain_tracker.SmartMoneyTracker`` 在 **多钱包共识** 达成时 ``asyncio.create_task`` 调用。
- 业务处理器通过 ``register_copy_trade_handler`` 注册（通常在 ``live_trader.install_smart_money_copy_trade_bridge``）。
- 环境变量：``SMART_MONEY_COPY_TRADE_ENABLED=1`` 才派发；金额见 ``SMART_MONEY_COPY_TRADE_SOL``。

⚠️ Meme 极高风险：默认关闭；开启前务必自测 mint 解析与滑点。
"""

from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

_CopyHandler = Optional[Callable[..., Awaitable[None]]]
_handler: _CopyHandler = None


def register_copy_trade_handler(fn: Callable[..., Awaitable[None]]) -> None:
    """注册唯一异步回调 ``async def fn(*, contract, token, buys, source, ...)``。"""
    global _handler
    _handler = fn
    logger.info("smart_money_copy_hook: handler registered")


def copy_trade_enabled() -> bool:
    v = (os.environ.get("SMART_MONEY_COPY_TRADE_ENABLED") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


async def dispatch_consensus_copy(
    *,
    contract: str,
    token: str,
    buys: list[dict[str, Any]],
) -> None:
    """共识买入后尝试跟单（无 handler 或关闭时立即返回）。"""
    if not copy_trade_enabled():
        return
    if _handler is None:
        logger.debug("smart_money_copy: no handler (call live_trader.install_smart_money_copy_trade_bridge)")
        return
    try:
        await _handler(
            contract=contract,
            token=token,
            buys=buys,
            source="smart_money_consensus",
        )
    except Exception:
        logger.exception("smart_money_copy: handler failed")
