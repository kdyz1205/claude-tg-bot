"""
OKX public WebSocket hub — tickers + multi-interval candles, no REST polling for prices.

- ``while True`` outer loop + try/except + exponential backoff + jitter (self-healing).
- Thread-safe last prices for sync readers (e.g. ``dex_trader``).
- Async ``ensure_started()`` for candle/ticker consumers.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import threading
import time
from collections import deque
from typing import Any, Optional

log = logging.getLogger(__name__)

OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"

_MAX_CANDLES_PER_KEY = 80
_START_LOCK = asyncio.Lock()
_hub_task: asyncio.Task | None = None
_started = False

# instId -> last trade / mark price from tickers channel
_ticker_lock = threading.Lock()
_last_ticker_px: dict[str, float] = {}
_last_ticker_ts: dict[str, float] = {}

# (instId, bar) -> deque of candle rows (OKX format list)
_candle_lock = threading.Lock()
_candle_buf: dict[tuple[str, str], deque[list[Any]]] = {}


def _buf_key(inst_id: str, bar: str) -> tuple[str, str]:
    return (inst_id.strip(), bar.strip())


def _append_candle_row(inst_id: str, bar: str, row: list[Any]) -> None:
    if len(row) < 6:
        return
    key = _buf_key(inst_id, bar)
    with _candle_lock:
        dq = _candle_buf.setdefault(key, deque(maxlen=_MAX_CANDLES_PER_KEY))
        ts = str(row[0])
        if dq and str(dq[-1][0]) == ts:
            dq[-1] = row
        else:
            dq.append(row)


def get_last_price_usdt(inst_id: str) -> float:
    """Sync-safe best-effort last price from ticker stream (0 if unknown)."""
    with _ticker_lock:
        return float(_last_ticker_px.get(inst_id, 0.0) or 0.0)


def get_spot_usd_map() -> dict[str, float]:
    """ETH / BNB / SOL / BTC keys for whale USD notionals (fallback defaults if empty)."""
    with _ticker_lock:
        out = {
            "ETH": float(_last_ticker_px.get("ETH-USDT", 0) or 0),
            "BNB": float(_last_ticker_px.get("BNB-USDT", 0) or 0),
            "SOL": float(_last_ticker_px.get("SOL-USDT", 0) or 0),
            "BTC": float(_last_ticker_px.get("BTC-USDT", 0) or 0),
            "_ts": float(_last_ticker_ts.get("_bundle", 0) or 0),
        }
    if out["ETH"] <= 0:
        out["ETH"] = 3000.0
    if out["BNB"] <= 0:
        out["BNB"] = 400.0
    if out["SOL"] <= 0:
        out["SOL"] = 150.0
    if out["BTC"] <= 0:
        out["BTC"] = 60_000.0
    return out


def get_candles_sync(inst_id: str, bar: str, limit: int = 20) -> list[list[Any]]:
    """Copy last ``limit`` rows from WS buffer, oldest-first (matches OKX REST order)."""
    key = _buf_key(inst_id, bar)
    with _candle_lock:
        dq = _candle_buf.get(key)
        if not dq:
            return []
        rows = list(dq)[-limit:]
    def _ts_key(r: list[Any]) -> int:
        try:
            return int(r[0])
        except (TypeError, ValueError):
            return 0

    rows.sort(key=_ts_key)
    return rows


# Keep in sync with onchain_filter.DEFAULT_SYMBOLS (avoid import cycle).
HUB_DEFAULT_SYMBOLS: list[str] = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
    "DOGE-USDT", "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT",
    "MATIC-USDT", "UNI-USDT", "LTC-USDT", "BCH-USDT", "ATOM-USDT",
    "FIL-USDT", "ARB-USDT", "OP-USDT", "APT-USDT", "SUI-USDT",
    "NEAR-USDT", "INJ-USDT", "TIA-USDT", "SEI-USDT", "PEPE-USDT",
    "WIF-USDT", "BONK-USDT", "RENDER-USDT", "FET-USDT", "ONDO-USDT",
]


async def ensure_started(subscribe_symbols: list[str] | None = None) -> None:
    """Idempotent: start background WS task if not running."""
    global _hub_task, _started
    async with _START_LOCK:
        if _hub_task is not None and not _hub_task.done():
            return
        subscribe_symbols = list(subscribe_symbols or HUB_DEFAULT_SYMBOLS)
        _hub_task = asyncio.create_task(
            _run_hub_loop(subscribe_symbols),
            name="okx_public_ws_hub",
        )
        _started = True
        await asyncio.sleep(1.0)


def _build_subscribe_args(symbols: list[str]) -> list[dict[str, str]]:
    args: list[dict[str, str]] = []
    for px in ("ETH-USDT", "BNB-USDT", "SOL-USDT", "BTC-USDT"):
        args.append({"channel": "tickers", "instId": px})
    bars = ("3m", "5m", "1H")
    for sym in symbols:
        s = sym.strip()
        if not s:
            continue
        for b in bars:
            args.append({"channel": f"candle{b}", "instId": s})
    return args


async def _run_hub_loop(symbols: list[str]) -> None:
    try:
        import aiohttp
    except ImportError:
        log.error("okx_ws_hub: aiohttp required")
        return

    backoff = 2.0
    max_backoff = 90.0
    chunk = 85

    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=0, connect=30, sock_read=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(OKX_WS_PUBLIC, heartbeat=25) as ws:
                    backoff = 2.0
                    all_args = _build_subscribe_args(symbols)
                    for i in range(0, len(all_args), chunk):
                        part = all_args[i : i + chunk]
                        await ws.send_str(json.dumps({"op": "subscribe", "args": part}))
                        await asyncio.sleep(0.05)
                    log.info(
                        "okx_ws_hub: subscribed tickers + %s candle streams (%s symbols)",
                        len(all_args) - 4,
                        len(symbols),
                    )
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                payload = json.loads(msg.data)
                            except json.JSONDecodeError:
                                continue
                            if payload.get("event") == "error":
                                log.warning("okx_ws_hub ws error evt: %s", payload)
                                continue
                            arg = payload.get("arg") or {}
                            ch = str(arg.get("channel", ""))
                            inst = str(arg.get("instId", ""))
                            data = payload.get("data") or []
                            if ch == "tickers" and data:
                                tick = data[0] if isinstance(data[0], dict) else {}
                                px = float(tick.get("last", 0) or tick.get("lastPx", 0) or 0)
                                if px > 0 and inst:
                                    now = time.time()
                                    with _ticker_lock:
                                        _last_ticker_px[inst] = px
                                        _last_ticker_ts[inst] = now
                                        _last_ticker_ts["_bundle"] = now
                            elif ch.startswith("candle") and inst and data:
                                bar = ch.replace("candle", "", 1)
                                for row in data:
                                    if isinstance(row, list):
                                        _append_candle_row(inst, bar, row)
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning("okx_ws_hub disconnected: %s — reconnect in %.1fs", e, backoff)
            jitter = random.uniform(0, min(5.0, backoff * 0.2))
            await asyncio.sleep(min(max_backoff, backoff) + jitter)
            backoff = min(max_backoff, backoff * 1.75 + random.uniform(0, 0.5))


async def get_sol_usd() -> float:
    await ensure_started()
    p = get_last_price_usdt("SOL-USDT")
    return p if p > 0 else 150.0
