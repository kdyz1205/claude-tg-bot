"""
dex_trader.py — Solana DEX Trading Core

Core trading functions:
1. Token info lookup (DexScreener API)
2. Safety checks (mint authority, freeze authority, LP status, top holders)
3. Buy/Sell execution (paper mode + future live mode via Jupiter)
4. Position tracking with real-time PnL
5. Settings management (slippage, priority fees, MEV protection)
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join(BASE_DIR, "_dex_settings.json")
POSITIONS_FILE = os.path.join(BASE_DIR, "_dex_positions.json")

_sol_price_cache = {"price": 83.0, "ts": 0}


def _get_sol_price_sync() -> float:
    """Get SOL/USD price with 60s cache. Sync-safe for paper calculations."""
    if time.time() - _sol_price_cache["ts"] < 60:
        return _sol_price_cache["price"]
    try:
        import httpx
        r = httpx.get("https://www.okx.com/api/v5/market/ticker?instId=SOL-USDT", timeout=3)
        p = float(r.json().get("data", [{}])[0].get("last", 0))
        if p > 0:
            _sol_price_cache["price"] = p
            _sol_price_cache["ts"] = time.time()
    except Exception:
        pass
    return _sol_price_cache["price"]

# ─── Default Settings ───
DEFAULT_SETTINGS = {
    "buy_slippage_pct": 15,
    "sell_slippage_pct": 20,
    "priority_fee_sol": 0.005,     # Normal trade
    "snipe_priority_sol": 0.02,    # Snipe trade
    "mev_protection": True,
    "auto_buy_sol": 0.5,           # Default quick-buy amount
    "buy_buttons": [0.1, 0.3, 0.5, 1.0],  # Quick-buy presets
    "sell_buttons": [25, 50, 75, 100],      # Sell % presets
    "auto_approve": False,         # Skip confirmation on buy
    "trailing_stop_pct": None,     # None = disabled, e.g. 20 = sell if drops 20% from peak
    "default_tp_pct": 100,         # Default take profit (+100% = 2x)
    "default_sl_pct": -30,         # Default stop loss
}


def _atomic_save(filepath: str, data):
    """Atomic JSON save with fsync."""
    tmp = filepath + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, filepath)
    except Exception as e:
        logger.warning(f"atomic save failed {filepath}: {e}")


def _load_json(filepath: str, default=None):
    """Safe JSON load."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if data is not None else (default if default is not None else {})
    except (FileNotFoundError, json.JSONDecodeError):
        return default if default is not None else {}


# ─── Settings ───

def get_settings() -> dict:
    s = dict(DEFAULT_SETTINGS)
    s.update(_load_json(SETTINGS_FILE, {}))
    return s

def update_settings(**kwargs):
    s = get_settings()
    s.update(kwargs)
    _atomic_save(SETTINGS_FILE, s)
    return s


# ─── Token Info Lookup ───

async def lookup_token(address: str) -> Optional[dict]:
    """Fetch comprehensive token info from DexScreener.
    Returns a rich token card dict or None.
    """
    try:
        import httpx
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{address}")
            if resp.status_code != 200:
                return None
            data = resp.json()
            pairs = data.get("pairs") or []
            if not pairs:
                return None

            # Best pair by liquidity
            pair = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))

            base = pair.get("baseToken", {})
            quote = pair.get("quoteToken", {})
            liq = pair.get("liquidity", {})
            txns = pair.get("txns", {})

            # Volume data
            h24 = txns.get("h24", {})
            h1 = txns.get("h1", {})
            m5 = txns.get("m5", {})

            info = {
                "address": base.get("address", address),
                "name": base.get("name", "?"),
                "symbol": base.get("symbol", "?"),
                "chain": pair.get("chainId", "solana"),
                "dex": pair.get("dexId", "?"),
                "pair_address": pair.get("pairAddress", ""),
                "pair_url": pair.get("url", f"https://dexscreener.com/solana/{address}"),

                # Price
                "price_usd": float(pair.get("priceUsd", 0) or 0),
                "price_native": float(pair.get("priceNative", 0) or 0),

                # Market data
                "mcap": float(pair.get("marketCap", 0) or pair.get("fdv", 0) or 0),
                "fdv": float(pair.get("fdv", 0) or 0),
                "liquidity_usd": float(liq.get("usd", 0) or 0),

                # Volume
                "vol_24h": float(pair.get("volume", {}).get("h24", 0) or 0),
                "vol_1h": float(pair.get("volume", {}).get("h1", 0) or 0),
                "vol_5m": float(pair.get("volume", {}).get("m5", 0) or 0),

                # Transactions
                "buys_24h": int(h24.get("buys", 0) or 0),
                "sells_24h": int(h24.get("sells", 0) or 0),
                "buys_1h": int(h1.get("buys", 0) or 0),
                "sells_1h": int(h1.get("sells", 0) or 0),
                "buys_5m": int(m5.get("buys", 0) or 0),
                "sells_5m": int(m5.get("sells", 0) or 0),

                # Price changes
                "change_5m": float(pair.get("priceChange", {}).get("m5", 0) or 0),
                "change_1h": float(pair.get("priceChange", {}).get("h1", 0) or 0),
                "change_6h": float(pair.get("priceChange", {}).get("h6", 0) or 0),
                "change_24h": float(pair.get("priceChange", {}).get("h24", 0) or 0),

                # Age
                "created_at": pair.get("pairCreatedAt", 0),
                "age_hours": 0,

                # Safety (basic — will enhance with on-chain checks later)
                "safety": {
                    "has_socials": bool(pair.get("info", {}).get("socials")),
                    "has_website": bool(pair.get("info", {}).get("websites")),
                },
            }

            # Calculate age
            created = info["created_at"]
            if created and created > 0:
                if created > 1e12:  # milliseconds
                    created = created / 1000
                info["age_hours"] = round((time.time() - created) / 3600, 1)

            return info
    except Exception as e:
        logger.debug(f"Token lookup failed for {address}: {e}")
        return None


def format_token_card(info: dict) -> str:
    """Format token info as a Telegram-friendly card."""
    if not info:
        return "Token not found"

    price = info.get("price_usd", 0)
    mcap = info.get("mcap", 0)
    liq = info.get("liquidity_usd", 0)
    age = info.get("age_hours", 0)

    # Price formatting
    if price >= 1:
        price_str = f"${price:,.4f}"
    elif price >= 0.001:
        price_str = f"${price:.6f}"
    elif price >= 0.0000001:
        price_str = f"${price:.10f}"
    else:
        price_str = f"${price:.12f}"

    # Safety indicators
    safety = info.get("safety", {})
    safety_icons = []
    if safety.get("has_website"):
        safety_icons.append("\U0001f310")
    if safety.get("has_socials"):
        safety_icons.append("\U0001f4f1")
    if liq >= 50000:
        safety_icons.append("\U0001f4a7")
    elif liq >= 10000:
        safety_icons.append("\U0001f4a6")
    else:
        safety_icons.append("\u26a0\ufe0f")

    safety_str = " ".join(safety_icons) if safety_icons else "\u2753"

    # Change indicators
    def chg(v):
        if v > 0: return f"+{v:.1f}%"
        return f"{v:.1f}%"

    # Buy/Sell ratio
    b5 = info.get("buys_5m", 0)
    s5 = info.get("sells_5m", 0)
    b1h = info.get("buys_1h", 0)
    s1h = info.get("sells_1h", 0)

    # Age formatting
    if age < 1:
        age_str = f"{age*60:.0f}m"
    elif age < 24:
        age_str = f"{age:.1f}h"
    elif age < 720:
        age_str = f"{age/24:.1f}d"
    else:
        age_str = f"{age/720:.0f}mo"

    card = (
        f"{'─' * 28}\n"
        f"  {info.get('name', '?')} (${info.get('symbol', '?')})\n"
        f"{'─' * 28}\n"
        f"\n"
        f"\U0001f4b0 Price: {price_str}\n"
        f"\U0001f4ca MCap: ${mcap:,.0f} | Liq: ${liq:,.0f}\n"
        f"\U0001f4c8 5m: {chg(info.get('change_5m', 0))} | 1h: {chg(info.get('change_1h', 0))} | 24h: {chg(info.get('change_24h', 0))}\n"
        f"\U0001f504 5m: {b5}B/{s5}S | 1h: {b1h}B/{s1h}S\n"
        f"\U0001f48e Vol 1h: ${info.get('vol_1h', 0):,.0f} | 24h: ${info.get('vol_24h', 0):,.0f}\n"
        f"\u23f0 Age: {age_str} | {safety_str}\n"
        f"\U0001f517 {info.get('chain', 'solana')} | {info.get('dex', '?')}\n"
        f"\n"
        f"CA: {info.get('address', '?')[:20]}...{info.get('address', '?')[-6:]}\n"
    )
    return card


# ─── Position Management ───

def get_positions() -> list:
    return _load_json(POSITIONS_FILE, [])

def save_positions(positions: list):
    _atomic_save(POSITIONS_FILE, positions)

def get_open_positions() -> list:
    return [p for p in get_positions() if p.get("status") == "open"]

def get_position_by_address(address: str) -> Optional[dict]:
    for p in get_positions():
        if p.get("address") == address and p.get("status") == "open":
            return p
    return None


async def refresh_positions() -> list:
    """Update all open positions with current prices."""
    positions = get_positions()
    open_pos = [p for p in positions if p.get("status") == "open"]

    if not open_pos:
        return positions

    import httpx
    async with httpx.AsyncClient(timeout=15) as client:
        for pos in open_pos:
            addr = pos.get("address", "")
            if not addr:
                continue
            try:
                await asyncio.sleep(0.3)  # Rate limit
                resp = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{addr}")
                if resp.status_code != 200:
                    continue
                data = resp.json()
                pairs = data.get("pairs") or []
                if not pairs:
                    continue
                best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
                current_price = float(best.get("priceUsd", 0) or 0)
                if current_price <= 0:
                    continue

                entry = pos.get("entry_price", 0)
                if entry > 0:
                    pos["current_price"] = current_price
                    pos["pnl_pct"] = round(((current_price - entry) / entry) * 100, 2)
                    pos["current_value_sol"] = pos.get("amount_sol", 0) * (1 + pos["pnl_pct"] / 100)
                    pos["last_updated"] = time.time()

                    # Track peak
                    pos["peak_pnl"] = max(pos.get("peak_pnl", 0), pos["pnl_pct"])
                    pos["trough_pnl"] = min(pos.get("trough_pnl", 0), pos["pnl_pct"])
            except Exception as e:
                logger.debug(f"Refresh failed for {pos.get('symbol', '?')}: {e}")

    save_positions(positions)
    return positions


def format_positions() -> str:
    """Format all positions for Telegram display."""
    positions = get_positions()
    open_pos = [p for p in positions if p.get("status") == "open"]

    if not open_pos:
        return "\U0001f4c2 No open positions\n\nPaste a token CA to start trading"

    total_invested = sum(p.get("amount_sol", 0) for p in open_pos)
    total_value = sum(p.get("current_value_sol", p.get("amount_sol", 0)) for p in open_pos)
    total_pnl = total_value - total_invested
    pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0

    emoji = "\U0001f7e2" if total_pnl >= 0 else "\U0001f534"

    lines = [
        f"\U0001f4ca Positions ({len(open_pos)})",
        f"{'─' * 28}",
        f"Invested: {total_invested:.3f} SOL",
        f"Value: {total_value:.3f} SOL",
        f"{emoji} PnL: {total_pnl:+.4f} SOL ({pnl_pct:+.1f}%)",
        f"{'─' * 28}",
        "",
    ]

    for i, p in enumerate(open_pos, 1):
        pnl = p.get("pnl_pct", 0)
        em = "\U0001f7e2" if pnl > 0 else "\U0001f534" if pnl < 0 else "\u26aa"
        age_h = (time.time() - p.get("entry_time", time.time())) / 3600

        if age_h < 1:
            age_str = f"{age_h*60:.0f}m"
        elif age_h < 24:
            age_str = f"{age_h:.1f}h"
        else:
            age_str = f"{age_h/24:.1f}d"

        lines.append(
            f"{em} {p.get('symbol', '?')} | {pnl:+.1f}% | "
            f"{p.get('amount_sol', 0):.2f} SOL | {age_str}"
        )

    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (truncated)"
    return result


def format_position_detail(pos: dict) -> str:
    """Format single position detail."""
    if not pos:
        return "Position not found"

    entry = pos.get("entry_price", 0)
    current = pos.get("current_price", entry)
    pnl = pos.get("pnl_pct", 0)
    em = "\U0001f7e2" if pnl > 0 else "\U0001f534" if pnl < 0 else "\u26aa"
    age_h = (time.time() - pos.get("entry_time", time.time())) / 3600

    # Price formatting
    def fmt_price(p):
        if p >= 1: return f"${p:,.4f}"
        elif p >= 0.001: return f"${p:.6f}"
        else: return f"${p:.10f}"

    pnl_sol = pos.get("amount_sol", 0) * (pnl / 100)

    return (
        f"{em} {pos.get('name', '?')} (${pos.get('symbol', '?')})\n"
        f"{'─' * 28}\n"
        f"Entry: {fmt_price(entry)}\n"
        f"Now: {fmt_price(current)}\n"
        f"PnL: {pnl:+.1f}% ({pnl_sol:+.4f} SOL)\n"
        f"Size: {pos.get('amount_sol', 0):.3f} SOL\n"
        f"Peak: {pos.get('peak_pnl', 0):+.1f}% | Low: {pos.get('trough_pnl', 0):+.1f}%\n"
        f"Hold: {age_h:.1f}h\n"
        f"\nCA: {pos.get('address', '?')}"
    )


# ─── Buy/Sell (Paper Mode) ───

def execute_buy(token_info: dict, amount_sol: float, mode: str = "paper") -> Optional[dict]:
    """Execute a buy (paper or live).
    Returns position dict or None.
    """
    if mode != "paper":
        logger.warning("Live trading not yet implemented")
        return None

    if not token_info.get("address"):
        return None
    price = token_info.get("price_usd", 0)
    if price <= 0:
        return None

    # Check for existing position
    positions = get_positions()
    existing = None
    for p in positions:
        if p.get("address") == token_info.get("address") and p.get("status") == "open":
            existing = p
            break

    sol_price_est = _get_sol_price_sync()
    tokens_bought = (amount_sol * sol_price_est) / price if price > 0 else 0

    if existing:
        # Average into position: cost-weighted average price in USD
        old_cost_usd = existing.get("tokens", 0) * existing.get("entry_price", 0)
        new_cost_usd = tokens_bought * price
        total_tokens = existing.get("tokens", 0) + tokens_bought
        total_sol = existing.get("amount_sol", 0) + amount_sol
        avg_price = (old_cost_usd + new_cost_usd) / total_tokens if total_tokens > 0 else price

        existing["entry_price"] = avg_price
        existing["amount_sol"] = total_sol
        existing["tokens"] = total_tokens
        existing["last_buy_time"] = time.time()
        save_positions(positions)
        return existing

    # New position
    now = time.time()
    pos = {
        "id": f"pos_{int(now)}_{token_info.get('symbol', 'X')[:8]}",
        "address": token_info.get("address", ""),
        "symbol": token_info.get("symbol", "?"),
        "name": token_info.get("name", "?"),
        "chain": token_info.get("chain", "solana"),
        "pair_url": token_info.get("pair_url", ""),
        "entry_price": price,
        "current_price": price,
        "amount_sol": amount_sol,
        "tokens": round(tokens_bought, 2),
        "entry_time": now,
        "last_buy_time": now,
        "last_updated": now,
        "status": "open",
        "pnl_pct": 0,
        "current_value_sol": amount_sol,
        "peak_pnl": 0,
        "trough_pnl": 0,
        "tp_pct": get_settings().get("default_tp_pct"),
        "sl_pct": get_settings().get("default_sl_pct"),
        "mode": mode,
    }

    positions.append(pos)
    # Prune: keep last 500 total, remove oldest closed first
    if len(positions) > 500:
        closed = [p for p in positions if p.get("status") != "open"]
        open_pos = [p for p in positions if p.get("status") == "open"]
        keep_closed = closed[-(500 - len(open_pos)):] if len(open_pos) < 500 else []
        positions = open_pos + keep_closed
    save_positions(positions)

    logger.info(f"BUY {pos['symbol']} | {amount_sol} SOL @ ${price}")
    return pos


def execute_sell(address: str, pct: int = 100, mode: str = "paper") -> Optional[dict]:
    """Execute a sell (paper or live).
    pct: percentage of position to sell (25, 50, 75, 100).
    Returns updated/closed position or None.
    """
    if mode != "paper":
        logger.warning("Live trading not yet implemented")
        return None

    positions = get_positions()
    pos = None
    for p in positions:
        if p.get("address") == address and p.get("status") == "open":
            pos = p
            break

    if not pos:
        return None

    try:
        pct = int(pct)
    except (ValueError, TypeError):
        pct = 100
    pct = max(1, min(100, pct))
    sell_sol = pos.get("amount_sol", 0) * (pct / 100)
    sell_tokens = pos.get("tokens", 0) * (pct / 100)

    current_price = pos.get("current_price", pos.get("entry_price", 0))
    entry_price = pos.get("entry_price", 0)
    pnl_pct = ((current_price - entry_price) / entry_price * 100) if entry_price > 0 else 0
    pnl_sol = sell_sol * (pnl_pct / 100)

    if pct >= 100:
        pos["status"] = "closed"
        pos["close_time"] = time.time()
        pos["close_price"] = current_price
        pos["final_pnl_pct"] = round(pnl_pct, 2)
        pos["final_pnl_sol"] = round(pnl_sol, 4)
        pos["close_reason"] = "manual"
    else:
        pos["amount_sol"] = pos.get("amount_sol", 0) - sell_sol
        pos["tokens"] = pos.get("tokens", 0) - sell_tokens

    save_positions(positions)

    logger.info(f"SELL {pct}% {pos.get('symbol', '?')} | PnL: {pnl_pct:+.1f}% ({pnl_sol:+.4f} SOL)")
    return {
        "position": pos,
        "sold_pct": pct,
        "sold_sol": round(sell_sol, 4),
        "pnl_pct": round(pnl_pct, 2),
        "pnl_sol": round(pnl_sol, 4),
    }


def format_buy_result(pos: dict, amount_sol: float) -> str:
    """Format buy confirmation message."""
    return (
        f"\u2705 Buy Executed (Paper)\n"
        f"{'─' * 28}\n"
        f"Token: {pos.get('symbol', '?')} ({pos.get('name', '?')})\n"
        f"Price: ${pos.get('entry_price', 0):.10f}\n"
        f"Amount: {amount_sol} SOL (~{pos.get('tokens', 0):,.0f} tokens)\n"
        f"TP: +{pos.get('tp_pct', 100)}% | SL: {pos.get('sl_pct', -30)}%\n"
        f"\n{pos.get('pair_url', '')}"
    )


def format_sell_result(result: dict) -> str:
    """Format sell confirmation message."""
    if not result or not isinstance(result, dict):
        return "Sell failed — no result"
    pos = result.get("position", {})
    pnl = result.get("pnl_pct", 0)
    em = "\U0001f7e2" if pnl > 0 else "\U0001f534"
    status = "CLOSED" if pos.get("status") == "closed" else f"Sold {result.get('sold_pct', 0)}%"

    return (
        f"{em} Sell Executed (Paper)\n"
        f"{'─' * 28}\n"
        f"Token: {pos.get('symbol', '?')}\n"
        f"Status: {status}\n"
        f"PnL: {pnl:+.1f}% ({result.get('pnl_sol', 0):+.4f} SOL)\n"
        f"Sold: {result.get('sold_sol', 0):.4f} SOL"
    )


# ─── Settings Display ───

def format_settings() -> str:
    """Format settings for Telegram display."""
    s = get_settings()
    mev = "ON" if s.get("mev_protection") else "OFF"
    auto = "ON" if s.get("auto_approve") else "OFF"
    ts = s.get("trailing_stop_pct")
    ts_str = f"{ts}%" if ts else "OFF"

    return (
        f"\u2699\ufe0f Trading Settings\n"
        f"{'─' * 28}\n"
        f"\n"
        f"Buy Slippage: {s.get('buy_slippage_pct', 15)}%\n"
        f"Sell Slippage: {s.get('sell_slippage_pct', 20)}%\n"
        f"Priority Fee: {s.get('priority_fee_sol', 0.005)} SOL\n"
        f"Snipe Fee: {s.get('snipe_priority_sol', 0.02)} SOL\n"
        f"MEV Protection: {mev}\n"
        f"\n"
        f"Default Buy: {s.get('auto_buy_sol', 0.5)} SOL\n"
        f"Buy Buttons: {s.get('buy_buttons', [])}\n"
        f"Auto Confirm: {auto}\n"
        f"\n"
        f"Default TP: +{s.get('default_tp_pct', 100)}%\n"
        f"Default SL: {s.get('default_sl_pct', -30)}%\n"
        f"Trailing Stop: {ts_str}\n"
    )


# ─── Trading Stats ───

def get_trade_stats() -> dict:
    """Get overall trading statistics."""
    positions = get_positions()
    closed = [p for p in positions if p.get("status") == "closed"]
    open_pos = [p for p in positions if p.get("status") == "open"]

    if not closed:
        return {
            "total": 0, "open": len(open_pos),
            "wins": 0, "losses": 0, "win_rate": 0,
            "total_pnl_sol": 0, "avg_pnl_pct": 0,
            "best_pct": 0, "worst_pct": 0,
        }

    wins = [p for p in closed if (p.get("final_pnl_pct") or 0) > 0]
    pnls = [p.get("final_pnl_pct", 0) or 0 for p in closed]
    sol_pnls = [p.get("final_pnl_sol", 0) or 0 for p in closed]

    return {
        "total": len(closed),
        "open": len(open_pos),
        "wins": len(wins),
        "losses": len(closed) - len(wins),
        "win_rate": round(len(wins) / len(closed) * 100, 1) if closed else 0,
        "total_pnl_sol": round(sum(sol_pnls), 4),
        "avg_pnl_pct": round(sum(pnls) / len(pnls), 2) if pnls else 0,
        "best_pct": round(max(pnls), 2) if pnls else 0,
        "worst_pct": round(min(pnls), 2) if pnls else 0,
    }


def format_trade_stats() -> str:
    """Format stats for Telegram."""
    s = get_trade_stats()
    if s["total"] == 0:
        return "\U0001f4c8 No closed trades yet\nPaste a token CA to start!"

    em = "\U0001f7e2" if s["total_pnl_sol"] >= 0 else "\U0001f534"
    result = (
        f"\U0001f4c8 Trading Stats\n"
        f"{'─' * 28}\n"
        f"Trades: {s['total']} ({s['open']} open)\n"
        f"Win Rate: {s['win_rate']:.0f}% ({s['wins']}W/{s['losses']}L)\n"
        f"{em} Total PnL: {s['total_pnl_sol']:+.4f} SOL\n"
        f"Avg PnL: {s['avg_pnl_pct']:+.1f}%\n"
        f"Best: {s['best_pct']:+.1f}% | Worst: {s['worst_pct']:+.1f}%\n"
    )
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (truncated)"
    return result
