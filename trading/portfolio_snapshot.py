"""
Background portfolio snapshot for Telegram UI + web dashboard.

Heavy work (wallet RPC, OKX signed REST, DEX price refresh) runs on an asyncio
polling loop. Handlers read only get_snapshot() — O(1) memory copy.

OKX, wallet, and DEX legs refresh concurrently via asyncio.gather.

Optional: set REDIS_URL to mirror JSON at key claude_tg_bot:portfolio (TTL 180s).
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_data: dict[str, Any] = {
    "updated_at": 0.0,
    "sol_price": 0.0,
    "sol_chg_pct": 0.0,
    "wallet": {
        "ok": False,
        "pubkey_short": "",
        "sol_bal": 0.0,
        "token_count": 0,
        "tokens": [],
    },
    "okx": {
        "ok": False,
        "has_keys": False,
        "total_equity_usd": 0.0,
        "usdt_available": 0.0,
        "positions": [],
        "error": "",
    },
    "dex": {
        "positions": [],
        "total_invested_sol": 0.0,
        "total_value_sol": 0.0,
    },
    "last_error": "",
}

_redis_warned = False


def _empty() -> dict[str, Any]:
    return {
        "updated_at": 0.0,
        "sol_price": 0.0,
        "sol_chg_pct": 0.0,
        "wallet": {
            "ok": False,
            "pubkey_short": "",
            "sol_bal": 0.0,
            "token_count": 0,
            "tokens": [],
        },
        "okx": {
            "ok": False,
            "has_keys": False,
            "total_equity_usd": 0.0,
            "usdt_available": 0.0,
            "positions": [],
            "error": "",
        },
        "dex": {
            "positions": [],
            "total_invested_sol": 0.0,
            "total_value_sol": 0.0,
        },
        "last_error": "",
    }


def get_snapshot() -> dict[str, Any]:
    """Thread-safe shallow+deep copy for readers (no network)."""
    with _lock:
        snap = copy.deepcopy(_data)
    snap["age_sec"] = max(0.0, time.time() - float(snap.get("updated_at") or 0))
    return snap


def _publish_redis(blob: dict[str, Any]) -> None:
    global _redis_warned
    url = (os.getenv("REDIS_URL") or "").strip()
    if not url:
        return
    try:
        import redis  # type: ignore

        r = redis.Redis.from_url(url, decode_responses=True)
        r.set("claude_tg_bot:portfolio", json.dumps(blob, default=str), ex=180)
    except Exception as e:
        if not _redis_warned:
            logger.warning("portfolio_snapshot: Redis publish skipped: %s", e)
            _redis_warned = True


async def _fetch_sol_ticker() -> tuple[float, float]:
    try:
        import httpx

        async with httpx.AsyncClient(timeout=3.0) as c:
            r = await c.get("https://www.okx.com/api/v5/market/ticker?instId=SOL-USDT")
            d = r.json().get("data", [{}])[0]
            p = float(d.get("last", 0) or 0)
            o = float(d.get("open24h", 0) or 0)
            chg = ((p - o) / o * 100) if o > 0 else 0.0
            return p, chg
    except Exception as e:
        logger.debug("portfolio_snapshot SOL ticker: %s", e)
        with _lock:
            return float(_data.get("sol_price") or 0), float(_data.get("sol_chg_pct") or 0)


async def _label_wallet_tokens(raw: list[dict]) -> list[dict]:
    if not raw:
        return []
    try:
        import dex_trader as dex
    except ImportError:
        dex = None
    out: list[dict] = []
    for t in raw[:12]:
        m = (t.get("mint") or "").strip()
        amt = float(t.get("amount") or 0)
        short = f"{m[:4]}…{m[-4:]}" if len(m) > 12 else (m or "?")
        label = short
        if dex and m:
            try:
                info = await asyncio.wait_for(dex.lookup_token(m), timeout=1.4)
                if info:
                    label = (info.get("symbol") or info.get("name") or short)[:14]
            except Exception:
                pass
        out.append({"label": label, "amount": amt, "mint": m})
    return out


async def _leg_sol_ticker() -> tuple[tuple[float, float], list[str]]:
    errs: list[str] = []
    try:
        return await _fetch_sol_ticker(), errs
    except Exception as e:
        errs.append(f"ticker:{e}")
        with _lock:
            return (float(_data.get("sol_price") or 0), float(_data.get("sol_chg_pct") or 0)), errs


async def _leg_wallet() -> tuple[dict[str, Any], list[str]]:
    errs: list[str] = []
    default_wallet = _empty()["wallet"]
    try:
        import secure_wallet as wallet
    except ImportError:
        return default_wallet, errs

    if not wallet.wallet_exists():
        return default_wallet, errs

    tks = None
    try:
        bal = await asyncio.wait_for(wallet.get_sol_balance(), timeout=4.0)
    except Exception:
        bal = 0.0
    raw: list[dict] = []
    try:
        tks = await asyncio.wait_for(wallet.get_token_balances(), timeout=8.0)
        if tks:
            raw = sorted(tks, key=lambda x: float(x.get("amount") or 0), reverse=True)[:14]
    except Exception as e:
        errs.append(f"wallet_tokens:{e}")
    pk = ""
    try:
        pk = wallet.get_public_key() or ""
    except Exception:
        pk = ""
    short_pk = f"{pk[:6]}…{pk[-4:]}" if len(pk) > 12 else (pk or "")
    labeled = await _label_wallet_tokens(raw)
    tc = len(tks) if tks is not None else len(labeled)
    block = {
        "ok": True,
        "pubkey_short": short_pk,
        "sol_bal": float(bal or 0),
        "token_count": tc,
        "tokens": labeled,
    }
    return block, errs


async def _leg_okx() -> tuple[dict[str, Any], list[str]]:
    errs: list[str] = []
    block: dict[str, Any] = copy.deepcopy(_empty()["okx"])
    try:
        from trading.okx_executor import OKXExecutor

        ex = OKXExecutor()
        ex.load_state()
        block["has_keys"] = ex.has_api_keys()
        if not ex.has_api_keys():
            return block, errs
        bal_r = await asyncio.wait_for(ex.get_account_balance(), timeout=12.0)
        if bal_r.get("ok"):
            block["ok"] = True
            block["total_equity_usd"] = float(bal_r.get("total_equity") or 0)
            block["usdt_available"] = float(bal_r.get("usdt_available") or 0)
        else:
            block["error"] = str(bal_r.get("reason", "balance_failed"))[:200]
        pos_raw = await asyncio.wait_for(ex.get_exchange_positions(), timeout=12.0)
        rows: list[dict[str, Any]] = []
        for p in pos_raw or []:
            try:
                pos_sz = float(p.get("pos", 0) or 0)
            except (TypeError, ValueError):
                pos_sz = 0.0
            if abs(pos_sz) < 1e-12:
                continue
            rows.append(
                {
                    "instId": p.get("instId", ""),
                    "pos": pos_sz,
                    "notionalUsd": float(p.get("notionalUsd", 0) or 0),
                    "avgPx": float(p.get("avgPx", 0) or 0),
                    "upl": float(p.get("upl", 0) or 0),
                    "uplRatio": float(p.get("uplRatio", 0) or 0),
                    "posSide": p.get("posSide", ""),
                }
            )
        block["positions"] = rows
    except Exception as e:
        errs.append(f"okx:{e}")
        block["error"] = str(e)[:200]
    return block, errs


async def _leg_dex() -> tuple[dict[str, Any], list[str]]:
    errs: list[str] = []
    block: dict[str, Any] = copy.deepcopy(_empty()["dex"])
    try:
        import dex_trader as dex

        await asyncio.wait_for(dex.refresh_positions(), timeout=25.0)
        open_p = dex.get_open_positions()
        inv = sum(float(x.get("amount_sol", 0) or 0) for x in open_p)
        val = sum(
            float(x.get("current_value_sol", x.get("amount_sol", 0)) or 0) for x in open_p
        )
        block["positions"] = copy.deepcopy(open_p)
        block["total_invested_sol"] = inv
        block["total_value_sol"] = val
    except ImportError:
        pass
    except Exception as e:
        errs.append(f"dex:{e}")
        block["error"] = str(e)[:200]
    return block, errs


async def refresh_once() -> None:
    """One full refresh — concurrent legs, then merge into global snapshot."""
    global _data
    snap = _empty()
    snap["updated_at"] = time.time()
    err_parts: list[str] = []

    results = await asyncio.gather(
        _leg_sol_ticker(),
        _leg_wallet(),
        _leg_okx(),
        _leg_dex(),
        return_exceptions=True,
    )

    (sol_p, sol_c) = (0.0, 0.0)
    if isinstance(results[0], BaseException):
        err_parts.append(f"ticker:{results[0]}")
        with _lock:
            sol_p, sol_c = float(_data.get("sol_price") or 0), float(_data.get("sol_chg_pct") or 0)
    else:
        (sol_p, sol_c), e0 = results[0]
        err_parts.extend(e0)

    if isinstance(results[1], BaseException):
        err_parts.append(f"wallet:{results[1]}")
        snap["wallet"]["error"] = str(results[1])[:200]
    else:
        wblk, e1 = results[1]
        snap["wallet"] = wblk
        err_parts.extend(e1)

    if isinstance(results[2], BaseException):
        err_parts.append(f"okx:{results[2]}")
        snap["okx"]["error"] = str(results[2])[:200]
    else:
        oblk, e2 = results[2]
        snap["okx"] = oblk
        err_parts.extend(e2)

    if isinstance(results[3], BaseException):
        err_parts.append(f"dex:{results[3]}")
        snap["dex"]["error"] = str(results[3])[:200]
    else:
        dblk, e3 = results[3]
        snap["dex"] = dblk
        err_parts.extend(e3)

    snap["sol_price"] = sol_p
    snap["sol_chg_pct"] = sol_c

    if err_parts:
        snap["last_error"] = "; ".join(err_parts)[:500]

    with _lock:
        _data = snap

    _publish_redis(snap)


async def run_background_loop(interval_sec: float = 12.0) -> None:
    """Never returns; swallow errors and keep polling."""
    while True:
        try:
            await refresh_once()
        except Exception as e:
            logger.warning("portfolio_snapshot refresh_once: %s", e)
        await asyncio.sleep(max(3.0, float(interval_sec)))
