"""
Polymarket CLOB execution gateway (Phase 14).

Uses Polymarket's official py-clob-client: L1 auth derives API credentials via
EIP-712-signed headers; `create_order` builds a CTF `Order` struct and signs it
(EIP-712) through `OrderBuilder` / py-order-utils — private key never leaves
this process except for signing.

Env:
  POLYMARKET_PRIVATE_KEY or POLY_PRIVATE_KEY — hex EOA key (0x optional)
  POLY_CLOB_HOST — default https://clob.polymarket.com
  POLY_SIGNATURE_TYPE — 0=EOA, 1=Magic/email, 2=browser proxy (default 0)
  POLY_FUNDER — optional funder (proxy) address for sig types 1/2
  POLY_TEST_TOKEN_ID — conditional token id for `run_minimal_usdc_test_order`
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Optional

try:
    from dotenv import load_dotenv

    load_dotenv(override=True)
except ImportError:
    pass

log = logging.getLogger(__name__)

# ── Kelly sizing (binary YES contract, limit price = implied prob / stake cost) ──


def kelly_fraction_yes(
    p_model: float,
    market_price: float,
    *,
    scale: float = 0.5,
    max_fraction: float = 0.25,
) -> float:
    """
    Fraction of bankroll to allocate to a Polymarket YES (or single-outcome)
    buy at `market_price` in (0,1), given subjective win prob `p_model`.

    Full Kelly for a $1 payoff contract purchased at price pi:
        f* = (p - pi) / (1 - pi)   for p > pi, else 0
    `scale` applies fractional Kelly (e.g. 0.5 = half-Kelly). Result capped by
    `max_fraction` to avoid tail risk from model error.
    """
    p = float(p_model)
    pi = float(market_price)
    if p <= pi or pi <= 1e-9 or pi >= 1.0 - 1e-9:
        return 0.0
    f_full = (p - pi) / (1.0 - pi)
    f = max(0.0, f_full * float(scale))
    return min(f, max_fraction)


def usdc_notional_from_kelly(
    bankroll_usdc: float,
    p_model: float,
    market_price: float,
    *,
    scale: float = 0.5,
    max_fraction: float = 0.25,
    cap_usdc: Optional[float] = None,
) -> float:
    """USDC stake from Kelly fraction × bankroll, optionally hard-capped."""
    br = max(0.0, float(bankroll_usdc))
    f = kelly_fraction_yes(p_model, market_price, scale=scale, max_fraction=max_fraction)
    stake = br * f
    if cap_usdc is not None:
        stake = min(stake, max(0.0, float(cap_usdc)))
    return max(0.0, stake)


def shares_from_usdc_buy(price: float, usdc: float) -> float:
    """BUY: USDC spent ≈ price * size (shares)."""
    px = max(1e-9, float(price))
    return max(0.0, float(usdc) / px)


# ── CLOB client & execution ───────────────────────────────────────────────────

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.constants import POLYGON
    from py_clob_client.exceptions import PolyApiException, PolyException

    _PY_CLOB = True
except ImportError:
    ClobClient = None  # type: ignore
    OrderArgs = None  # type: ignore
    OrderType = None  # type: ignore
    POLYGON = 137  # type: ignore
    PolyApiException = PolyException = Exception  # type: ignore
    _PY_CLOB = False


class PolyExecutorError(RuntimeError):
    pass


def _normalize_hex_key(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        raise PolyExecutorError("POLYMARKET_PRIVATE_KEY / POLY_PRIVATE_KEY is empty")
    return s if s.startswith("0x") else "0x" + s


def load_private_key() -> str:
    k = os.getenv("POLYMARKET_PRIVATE_KEY") or os.getenv("POLY_PRIVATE_KEY") or ""
    return _normalize_hex_key(k)


def build_clob_client() -> Any:
    if not _PY_CLOB:
        raise PolyExecutorError("py-clob-client is not installed")
    host = os.getenv("POLY_CLOB_HOST", "https://clob.polymarket.com").rstrip("/")
    key = load_private_key()
    sig_type = int(os.getenv("POLY_SIGNATURE_TYPE", "0"))
    funder = (os.getenv("POLY_FUNDER") or "").strip() or None
    client = ClobClient(
        host,
        chain_id=POLYGON,
        key=key,
        signature_type=sig_type,
        funder=funder,
    )
    creds = client.create_or_derive_api_creds()
    if creds is None:
        raise PolyExecutorError("create_or_derive_api_creds returned None")
    client.set_api_creds(creds)
    return client


def _parse_midpoint(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        return v if 0 < v < 1 else None
    if isinstance(raw, dict):
        for k in ("mid", "price", "p"):
            if k in raw and raw[k] is not None:
                try:
                    v = float(raw[k])
                    if 0 < v < 1:
                        return v
                except (TypeError, ValueError):
                    pass
    if isinstance(raw, str):
        try:
            v = float(raw)
            if 0 < v < 1:
                return v
        except ValueError:
            pass
    return None


def resolve_limit_price(client: Any, token_id: str, hint: Optional[float]) -> float:
    if hint is not None:
        h = float(hint)
        if 1e-9 < h < 1.0 - 1e-9:
            return h
    mid = _parse_midpoint(client.get_midpoint(token_id))
    if mid is None:
        raise PolyExecutorError("Could not resolve limit price (midpoint)")
    return mid


def _min_order_size_shares(client: Any, token_id: str) -> float:
    try:
        book = client.get_order_book(token_id)
        mos = getattr(book, "min_order_size", None) or (
            book.get("min_order_size") if isinstance(book, dict) else None
        )
        if mos is not None:
            return max(float(mos), 1e-9)
    except Exception as e:
        log.debug("min_order_size fetch skipped: %s", e)
    return 1e-9


def post_order_with_backoff(
    client: Any,
    signed_order: Any,
    *,
    order_type: Any = None,
    max_retries: int = 6,
    base_delay: float = 0.35,
    max_delay: float = 14.0,
) -> Any:
    """POST signed order with exponential backoff (CLOB REST resilience)."""
    if order_type is None:
        order_type = OrderType.GTC
    delay = base_delay
    last: Optional[BaseException] = None
    for attempt in range(max(1, max_retries)):
        try:
            return client.post_order(signed_order, order_type)
        except (PolyApiException, PolyException, OSError, TimeoutError) as e:
            last = e
            log.warning(
                "poly CLOB post_order retry %s/%s: %s",
                attempt + 1,
                max_retries,
                e,
            )
        except Exception as e:
            last = e
            log.warning(
                "poly CLOB post_order unexpected retry %s/%s: %s",
                attempt + 1,
                max_retries,
                e,
            )
        time.sleep(delay)
        delay = min(delay * 1.85, max_delay)
    assert last is not None
    raise last


def create_signed_buy_order(
    client: Any,
    token_id: str,
    price: float,
    size_shares: float,
) -> Any:
    """EIP-712 signed limit BUY (via py-clob OrderBuilder)."""
    if size_shares <= 0:
        raise PolyExecutorError("size_shares must be positive")
    oa = OrderArgs(token_id=token_id, price=float(price), size=float(size_shares), side="BUY")
    return client.create_order(oa)


def execute_polymarket_buy_yes(
    token_id: str,
    usdc_notional: float,
    *,
    limit_price: Optional[float] = None,
    max_retries: int = 6,
) -> dict[str, Any]:
    """
    Place a GTC BUY on the YES (conditional) `token_id` for ~`usdc_notional` USDC.

    Signing pipeline: build_clob_client → create_order (EIP-712) → post_order_with_backoff.
    """
    if not _PY_CLOB:
        return {"ok": False, "reason": "py-clob-client not installed"}
    client = build_clob_client()
    px = resolve_limit_price(client, token_id, limit_price)
    min_sz = _min_order_size_shares(client, token_id)
    sz = shares_from_usdc_buy(px, usdc_notional)
    if sz < min_sz:
        sz = min_sz
    signed = create_signed_buy_order(client, token_id, px, sz)
    resp = post_order_with_backoff(client, signed, max_retries=max_retries)
    return {
        "ok": True,
        "token_id": token_id,
        "price": px,
        "size_shares": sz,
        "usdc_approx": px * sz,
        "response": resp,
        "maker": client.get_address(),
    }


def execute_polymarket_signal(
    signal: dict[str, Any],
    live_cfg: Optional[dict[str, Any]] = None,
    *,
    max_retries: int = 6,
) -> dict[str, Any]:
    """
    Execute a POLY_MARKET signal dict.

    Expected keys:
      token_id (str): outcome token to BUY
      model_prob | p_model | prob_yes (float): edge model P(YES)
      market_price | limit_price (float, optional)
      bankroll_usdc (float, optional) — else live_cfg['poly_bankroll_usdc']
    """
    live_cfg = live_cfg or {}
    token_id = (
        signal.get("token_id")
        or signal.get("yes_token_id")
        or signal.get("clob_token_id")
        or ""
    )
    token_id = str(token_id).strip()
    if not token_id:
        return {"ok": False, "reason": "missing token_id"}

    p_model = signal.get("model_prob")
    if p_model is None:
        p_model = signal.get("p_model", signal.get("prob_yes"))
    if p_model is None:
        return {"ok": False, "reason": "missing model_prob / p_model"}
    p_model = float(p_model)

    mkt = signal.get("market_price", signal.get("limit_price", signal.get("implied_prob")))
    mkt_f = float(mkt) if mkt is not None else None

    bankroll = float(
        signal.get("bankroll_usdc")
        or live_cfg.get("poly_bankroll_usdc")
        or live_cfg.get("poly_bankroll_usd", 0.0)
        or 0.0
    )
    kelly_scale = float(
        live_cfg.get("poly_kelly_fraction", live_cfg.get("poly_fractional_kelly", 0.5))
    )
    kelly_cap = float(live_cfg.get("poly_kelly_max_fraction", 0.25))
    cap_raw = live_cfg.get("poly_max_usdc_per_trade", live_cfg.get("poly_max_stake_usd"))
    cap_usdc_f = float(cap_raw) if cap_raw is not None else None
    min_stake = live_cfg.get("poly_min_stake_usd")
    min_stake_f = float(min_stake) if min_stake is not None else None

    client = build_clob_client()
    px = resolve_limit_price(client, token_id, mkt_f)

    stake = usdc_notional_from_kelly(
        bankroll,
        p_model,
        px,
        scale=kelly_scale,
        max_fraction=kelly_cap,
        cap_usdc=cap_usdc_f,
    )
    if stake <= 0:
        return {
            "ok": False,
            "reason": "Kelly stake is zero (no edge at this price or missing bankroll)",
            "p_model": p_model,
            "price": px,
        }

    if min_stake_f is not None and stake < min_stake_f:
        stake = min_stake_f
        if cap_usdc_f is not None:
            stake = min(stake, cap_usdc_f)

    ot_name = str(live_cfg.get("poly_order_type") or "GTC").upper()
    order_type = getattr(OrderType, ot_name, OrderType.GTC)

    min_sz = _min_order_size_shares(client, token_id)
    sz = shares_from_usdc_buy(px, stake)
    if sz < min_sz:
        sz = min_sz
    signed = create_signed_buy_order(client, token_id, px, sz)
    resp = post_order_with_backoff(
        client, signed, order_type=order_type, max_retries=max_retries
    )
    return {
        "ok": True,
        "token_id": token_id,
        "price": px,
        "size_shares": sz,
        "usdc_approx": px * sz,
        "kelly_stake_target_usdc": stake,
        "order_type": ot_name,
        "response": resp,
        "maker": client.get_address(),
    }


async def execute_polymarket_signal_async(
    signal: dict[str, Any],
    live_cfg: Optional[dict[str, Any]] = None,
    *,
    max_retries: int = 6,
) -> dict[str, Any]:
    """Async wrapper: runs blocking CLOB client in a thread."""
    return await asyncio.to_thread(
        execute_polymarket_signal,
        signal,
        live_cfg,
        max_retries=max_retries,
    )


async def execute_polymarket_buy_yes_async(
    token_id: str,
    usdc_notional: float,
    *,
    limit_price: Optional[float] = None,
    max_retries: int = 6,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        execute_polymarket_buy_yes,
        token_id,
        usdc_notional,
        limit_price=limit_price,
        max_retries=max_retries,
    )


def run_minimal_usdc_test_order(usdc: float = 0.1) -> dict[str, Any]:
    """
    Minimal live test: GTC BUY sized for ~`usdc` USDC at midpoint.

    Requires POLYMARKET_PRIVATE_KEY (or POLY_PRIVATE_KEY) and POLY_TEST_TOKEN_ID.
    """
    tid = (os.getenv("POLY_TEST_TOKEN_ID") or "").strip()
    if not tid:
        return {
            "ok": False,
            "skipped": True,
            "reason": "Set POLY_TEST_TOKEN_ID to a live conditional token_id",
        }
    try:
        return execute_polymarket_buy_yes(tid, usdc_notional=usdc, limit_price=None)
    except Exception as e:
        log.exception("run_minimal_usdc_test_order failed")
        return {"ok": False, "reason": str(e)}


if __name__ == "__main__":
    import json

    print(json.dumps(run_minimal_usdc_test_order(0.1), indent=2, default=str))
