"""
Live Trader Module — Jupiter V6 swaps with strict risk controls.

Executes real trades on Solana via Jupiter DEX aggregator.
All transactions go through secure_wallet.py (swap-only signing).
"""

import os, json, logging, time, asyncio
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
POSITIONS_FILE = BASE_DIR / "_live_positions.json"
LIVE_CONFIG_FILE = BASE_DIR / "_live_config.json"

JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_URL = "https://quote-api.jup.ag/v6/swap"
JUPITER_PRICE_URL = "https://api.jup.ag/price/v2"
JUPITER_TOKEN_LIST = "https://token.jup.ag/strict"

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# ── Risk Control Defaults ──
DEFAULT_LIVE_CONFIG = {
    "enabled": False,
    "max_trade_pct": 15.0,       # max % of portfolio per trade
    "max_positions": 5,           # max concurrent positions
    "daily_loss_limit_pct": 10.0, # halt if daily loss exceeds this
    "stop_loss_pct": 3.0,         # per-trade stop loss
    "take_profit_pct": 8.0,       # per-trade take profit
    "min_liquidity_usd": 500_000, # minimum token liquidity
    "min_mcap_usd": 5_000_000,    # minimum market cap
    "max_slippage_bps": 100,      # max slippage in basis points (1%)
    "min_sol_reserve": 0.05,      # always keep this much SOL for gas
    "check_interval": 30,         # position check interval (seconds)
    "scan_interval": 900,         # strategy scan interval (seconds)
    "starting_balance_sol": 0,    # set on first start
    "daily_pnl_sol": 0,
    "daily_reset_ts": 0,
}


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG & PERSISTENCE
# ══════════════════════════════════════════════════════════════════════════════

def _load_config() -> dict:
    try:
        if LIVE_CONFIG_FILE.exists():
            with open(LIVE_CONFIG_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
                cfg = DEFAULT_LIVE_CONFIG.copy()
                cfg.update(saved)
                return cfg
    except Exception:
        pass
    return DEFAULT_LIVE_CONFIG.copy()


def _save_config(cfg: dict):
    try:
        tmp = str(LIVE_CONFIG_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(LIVE_CONFIG_FILE))
    except Exception as e:
        logger.error(f"Config save failed: {e}")


def _load_positions() -> list:
    try:
        if POSITIONS_FILE.exists():
            with open(POSITIONS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return []


def _save_positions(positions: list):
    try:
        tmp = str(POSITIONS_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(positions, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(POSITIONS_FILE))
    except Exception as e:
        logger.error(f"Positions save failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# JUPITER API
# ══════════════════════════════════════════════════════════════════════════════

async def _get_jupiter_quote(input_mint: str, output_mint: str, amount_lamports: int,
                              slippage_bps: int = 100) -> Optional[dict]:
    """Get swap quote from Jupiter V6."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(JUPITER_QUOTE_URL, params={
                "inputMint": input_mint,
                "outputMint": output_mint,
                "amount": str(amount_lamports),
                "slippageBps": slippage_bps,
                "onlyDirectRoutes": "false",
                "asLegacyTransaction": "false",
            })
            if resp.status_code != 200:
                logger.error(f"Jupiter quote failed: {resp.status_code} {resp.text[:200]}")
                return None
            return resp.json()
    except Exception as e:
        logger.error(f"Jupiter quote error: {e}")
        return None


async def _execute_jupiter_swap(quote: dict, user_pubkey: str) -> Optional[str]:
    """Execute swap via Jupiter V6 — returns transaction signature or None."""
    import secure_wallet

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(JUPITER_SWAP_URL, json={
                "quoteResponse": quote,
                "userPublicKey": user_pubkey,
                "wrapAndUnwrapSol": True,
                "dynamicComputeUnitLimit": True,
                "prioritizationFeeLamports": "auto",
            })
            if resp.status_code != 200:
                logger.error(f"Jupiter swap failed: {resp.status_code} {resp.text[:200]}")
                return None

            swap_data = resp.json()
            tx_base64 = swap_data.get("swapTransaction")
            if not tx_base64:
                logger.error("No swapTransaction in Jupiter response")
                return None

        # Decode and sign through secure wallet (swap-only validation)
        import base64 as b64
        tx_bytes = b64.b64decode(tx_base64)
        signed_bytes = secure_wallet.sign_swap_transaction(tx_bytes)
        if not signed_bytes:
            logger.error("Transaction rejected by secure wallet")
            return None

        # Submit signed transaction
        import base64 as b64
        signed_b64 = b64.b64encode(signed_bytes).decode()

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.mainnet-beta.solana.com",
                json={
                    "jsonrpc": "2.0", "id": 1,
                    "method": "sendTransaction",
                    "params": [
                        signed_b64,
                        {"encoding": "base64", "skipPreflight": False,
                         "preflightCommitment": "confirmed", "maxRetries": 3}
                    ]
                }
            )
            result = resp.json()
            if "error" in result:
                logger.error(f"TX submit error: {result['error']}")
                return None
            sig = result.get("result")
            logger.info(f"Swap TX submitted: {sig}")
            return sig

    except Exception as e:
        logger.error(f"Swap execution error: {e}")
        return None


async def _get_token_price_usd(mint: str) -> Optional[float]:
    """Get token price in USD from Jupiter Price API."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(JUPITER_PRICE_URL, params={"ids": mint})
            if resp.status_code == 200:
                data = resp.json()
                price_data = data.get("data", {}).get(mint, {})
                return float(price_data.get("price", 0))
    except Exception:
        pass
    return None


async def _get_sol_price_usd() -> float:
    """Get SOL price in USD."""
    price = await _get_token_price_usd(SOL_MINT)
    return price if price else 150.0  # fallback


async def _validate_token(mint: str, cfg: dict) -> tuple[bool, str]:
    """Check if token passes safety filters."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Check Jupiter strict list
            resp = await client.get(f"https://api.jup.ag/tokens/v1/strict")
            if resp.status_code == 200:
                tokens = resp.json()
                found = None
                for t in tokens:
                    if t.get("address") == mint:
                        found = t
                        break
                if not found:
                    return False, "Token not on Jupiter strict list (potential scam)"

            # Check liquidity via DexScreener
            resp = await client.get(f"https://api.dexscreener.com/latest/dex/tokens/{mint}")
            if resp.status_code == 200:
                pairs = resp.json().get("pairs") or []
                if not pairs:
                    return False, "No trading pairs found"

                best = max(pairs, key=lambda p: float((p.get("liquidity") or {}).get("usd", 0) or 0))
                liq = float((best.get("liquidity") or {}).get("usd", 0) or 0)
                mcap = float(best.get("marketCap", 0) or 0)

                min_liq = cfg.get("min_liquidity_usd", 500_000)
                min_mcap = cfg.get("min_mcap_usd", 5_000_000)

                if liq < min_liq:
                    return False, f"Liquidity ${liq:,.0f} < ${min_liq:,.0f} minimum"
                if mcap < min_mcap:
                    return False, f"Market cap ${mcap:,.0f} < ${min_mcap:,.0f} minimum"

                return True, f"OK (liq=${liq:,.0f}, mcap=${mcap:,.0f})"

    except Exception as e:
        return False, f"Validation error: {e}"
    return False, "Validation failed"


# ══════════════════════════════════════════════════════════════════════════════
# RISK CONTROLS
# ══════════════════════════════════════════════════════════════════════════════

async def _check_risk_controls(amount_sol: float, cfg: dict) -> tuple[bool, str]:
    """Pre-trade risk check. Returns (allowed, reason)."""
    import secure_wallet

    # 1. Daily loss limit
    now = time.time()
    if now - cfg.get("daily_reset_ts", 0) > 86400:
        cfg["daily_pnl_sol"] = 0
        cfg["daily_reset_ts"] = now
        _save_config(cfg)

    starting = cfg.get("starting_balance_sol", 2.0)
    daily_loss_pct = abs(min(cfg.get("daily_pnl_sol", 0), 0)) / max(starting, 0.01) * 100
    if daily_loss_pct >= cfg.get("daily_loss_limit_pct", 10.0):
        return False, f"Daily loss limit hit ({daily_loss_pct:.1f}% >= {cfg['daily_loss_limit_pct']}%)"

    # 2. Max positions
    positions = _load_positions()
    open_pos = [p for p in positions if p.get("status") == "open"]
    if len(open_pos) >= cfg.get("max_positions", 5):
        return False, f"Max positions reached ({len(open_pos)}/{cfg['max_positions']})"

    # 3. Max trade size
    balance = await secure_wallet.get_sol_balance()
    if balance is None:
        return False, "Cannot fetch balance"
    max_trade = balance * cfg.get("max_trade_pct", 15.0) / 100
    if amount_sol > max_trade:
        return False, f"Trade {amount_sol:.4f} SOL > max {max_trade:.4f} SOL ({cfg['max_trade_pct']}%)"

    # 4. Reserve
    min_reserve = cfg.get("min_sol_reserve", 0.05)
    if balance - amount_sol < min_reserve:
        return False, f"Would leave {balance - amount_sol:.4f} SOL < {min_reserve} reserve"

    return True, "OK"


# ══════════════════════════════════════════════════════════════════════════════
# TRADE EXECUTION
# ══════════════════════════════════════════════════════════════════════════════

async def buy_token(mint: str, amount_sol: float, symbol: str = "",
                    signal_data: dict = None) -> Optional[dict]:
    """
    Buy a token with SOL via Jupiter.
    Returns position dict or None on failure.
    """
    import secure_wallet
    cfg = _load_config()

    if not cfg.get("enabled"):
        return None

    # Risk check
    allowed, reason = await _check_risk_controls(amount_sol, cfg)
    if not allowed:
        logger.warning(f"Trade blocked: {reason}")
        return None

    # Token safety check
    valid, val_reason = await _validate_token(mint, cfg)
    if not valid:
        logger.warning(f"Token rejected: {val_reason}")
        return None

    # Get wallet
    pubkey = secure_wallet.get_public_key()
    if not pubkey:
        return None

    # Get quote
    amount_lamports = int(amount_sol * 1_000_000_000)
    slippage = cfg.get("max_slippage_bps", 100)
    quote = await _get_jupiter_quote(SOL_MINT, mint, amount_lamports, slippage)
    if not quote:
        return None

    # Check price impact
    price_impact = float(quote.get("priceImpactPct", 0) or 0)
    if price_impact > 2.0:
        logger.warning(f"Price impact too high: {price_impact:.2f}%")
        return None

    # Execute swap
    out_amount = int(quote.get("outAmount", 0))
    sig = await _execute_jupiter_swap(quote, pubkey)
    if not sig:
        return None

    # Record position
    sol_price = await _get_sol_price_usd()
    token_price = await _get_token_price_usd(mint)

    position = {
        "id": f"live_{int(time.time())}_{symbol or mint[:8]}",
        "status": "open",
        "mint": mint,
        "symbol": symbol or mint[:8],
        "direction": "long",
        "amount_sol": amount_sol,
        "amount_usd": amount_sol * sol_price,
        "out_amount_raw": out_amount,
        "entry_price_usd": token_price,
        "entry_sol_price": sol_price,
        "entry_time": time.time(),
        "tx_signature": sig,
        "stop_loss_pct": -cfg.get("stop_loss_pct", 3.0),
        "take_profit_pct": cfg.get("take_profit_pct", 8.0),
        "peak_pnl_pct": 0,
        "signal": signal_data or {},
    }

    positions = _load_positions()
    positions.append(position)
    _save_positions(positions)
    logger.info(f"Position opened: {symbol} | {amount_sol:.4f} SOL | TX: {sig}")
    return position


async def sell_token(position_id: str, reason: str = "manual") -> Optional[dict]:
    """Sell entire position back to SOL."""
    import secure_wallet

    positions = _load_positions()
    pos = None
    for p in positions:
        if p.get("id") == position_id and p.get("status") == "open":
            pos = p
            break
    if not pos:
        return None

    pubkey = secure_wallet.get_public_key()
    if not pubkey:
        return None

    # Get token balance for this mint
    token_balances = await secure_wallet.get_token_balances()
    token_bal = None
    for tb in token_balances:
        if tb["mint"] == pos["mint"]:
            token_bal = tb
            break

    if not token_bal or token_bal["amount"] <= 0:
        logger.error(f"No token balance for {pos['mint']}")
        pos["status"] = "closed"
        pos["close_reason"] = "no_balance"
        pos["close_time"] = time.time()
        _save_positions(positions)
        return pos

    # Sell all tokens back to SOL
    raw_amount = int(token_bal["amount"] * (10 ** token_bal["decimals"]))
    cfg = _load_config()
    quote = await _get_jupiter_quote(
        pos["mint"], SOL_MINT, raw_amount,
        cfg.get("max_slippage_bps", 100)
    )
    if not quote:
        return None

    sig = await _execute_jupiter_swap(quote, pubkey)
    if not sig:
        return None

    # Calculate PnL
    entry_price = pos.get("entry_price_usd", 0)
    current_price = await _get_token_price_usd(pos["mint"])
    if entry_price and current_price:
        pnl_pct = ((current_price - entry_price) / entry_price) * 100
    else:
        pnl_pct = 0

    sol_received = int(quote.get("outAmount", 0)) / 1_000_000_000
    pnl_sol = sol_received - pos.get("amount_sol", 0)

    pos["status"] = "closed"
    pos["close_reason"] = reason
    pos["close_time"] = time.time()
    pos["close_price_usd"] = current_price
    pos["pnl_pct"] = round(pnl_pct, 2)
    pos["pnl_sol"] = round(pnl_sol, 4)
    pos["sol_received"] = round(sol_received, 4)
    pos["close_tx"] = sig

    _save_positions(positions)

    # Update daily PnL
    cfg["daily_pnl_sol"] = cfg.get("daily_pnl_sol", 0) + pnl_sol
    _save_config(cfg)

    logger.info(f"Position closed: {pos['symbol']} | {reason} | PnL: {pnl_pct:+.1f}% ({pnl_sol:+.4f} SOL)")
    return pos


# ══════════════════════════════════════════════════════════════════════════════
# POSITION MONITORING
# ══════════════════════════════════════════════════════════════════════════════

async def check_positions(send_func=None) -> dict:
    """Monitor open positions, auto-close on TP/SL."""
    positions = _load_positions()
    open_pos = [p for p in positions if p.get("status") == "open"]
    closed_count = 0

    for pos in open_pos:
        current_price = await _get_token_price_usd(pos["mint"])
        if not current_price or current_price <= 0:
            continue

        entry_price = pos.get("entry_price_usd", 0)
        if entry_price <= 0:
            continue

        pnl_pct = ((current_price - entry_price) / entry_price) * 100

        # Track peak
        if pnl_pct > pos.get("peak_pnl_pct", 0):
            pos["peak_pnl_pct"] = pnl_pct

        # Check stop-loss
        close_reason = None
        sl = pos.get("stop_loss_pct", -3.0)
        tp = pos.get("take_profit_pct", 8.0)

        if pnl_pct <= sl:
            close_reason = "stop_loss"
        elif pnl_pct >= tp:
            close_reason = "take_profit"
        # Trailing stop: if peak >4% and drops back 50%
        elif pos.get("peak_pnl_pct", 0) >= 4.0:
            drawback = pos["peak_pnl_pct"] - pnl_pct
            if drawback >= pos["peak_pnl_pct"] * 0.5:
                close_reason = "trailing_stop"

        if close_reason:
            result = await sell_token(pos["id"], close_reason)
            if result:
                closed_count += 1
                if send_func:
                    emoji = "\U0001f7e2" if (result.get("pnl_pct", 0) or 0) > 0 else "\U0001f534"
                    reason_cn = {
                        "stop_loss": "\u6b62\u635f \u274c", "take_profit": "\u6b62\u76c8 \u2705",
                        "trailing_stop": "\u8ffd\u8e2a\u6b62\u635f \U0001f4c9",
                    }.get(close_reason, close_reason)
                    msg = (
                        f"{emoji} **LIVE TRADE \u5e73\u4ed3**\n\n"
                        f"Token: {result.get('symbol', '?')}\n"
                        f"\u539f\u56e0: {reason_cn}\n"
                        f"PnL: {result.get('pnl_pct', 0):+.1f}% ({result.get('pnl_sol', 0):+.4f} SOL)\n"
                        f"\u6536\u56de: {result.get('sol_received', 0):.4f} SOL"
                    )
                    try:
                        await send_func(msg)
                    except Exception:
                        pass

        await asyncio.sleep(0.5)  # Rate limit

    _save_positions(positions)
    return {"checked": len(open_pos), "closed": closed_count}


# ══════════════════════════════════════════════════════════════════════════════
# STATS & STATUS
# ══════════════════════════════════════════════════════════════════════════════

def get_live_stats() -> dict:
    """Get live trading statistics."""
    positions = _load_positions()
    cfg = _load_config()
    open_pos = [p for p in positions if p.get("status") == "open"]
    closed_pos = [p for p in positions if p.get("status") == "closed"]

    total_pnl = sum(p.get("pnl_sol", 0) or 0 for p in closed_pos)
    wins = sum(1 for p in closed_pos if (p.get("pnl_pct", 0) or 0) > 0)
    wr = (wins / len(closed_pos) * 100) if closed_pos else 0

    return {
        "enabled": cfg.get("enabled", False),
        "open_positions": len(open_pos),
        "closed_trades": len(closed_pos),
        "total_pnl_sol": round(total_pnl, 4),
        "win_rate": round(wr, 1),
        "daily_pnl_sol": round(cfg.get("daily_pnl_sol", 0), 4),
        "starting_balance": cfg.get("starting_balance_sol", 0),
    }


def format_live_status() -> str:
    """Format live trading status for Telegram."""
    s = get_live_stats()
    positions = _load_positions()
    open_pos = [p for p in positions if p.get("status") == "open"]

    status_em = "\U0001f7e2" if s["enabled"] else "\U0001f534"
    lines = [
        f"\U0001f4b0 LIVE TRADING {status_em}",
        f"\u2501" * 28,
        f"\u5f00\u59cb\u8d44\u91d1: {s['starting_balance']:.4f} SOL",
        f"\u603b PnL: {s['total_pnl_sol']:+.4f} SOL",
        f"\u4eca\u65e5 PnL: {s['daily_pnl_sol']:+.4f} SOL",
        f"\u80dc\u7387: {s['win_rate']:.0f}% ({s['closed_trades']} trades)",
        f"\u6301\u4ed3: {s['open_positions']} positions",
    ]

    if open_pos:
        lines.append(f"\n\U0001f4c8 \u5f53\u524d\u6301\u4ed3:")
        for p in open_pos:
            lines.append(f"  {p.get('symbol', '?')} | {p.get('amount_sol', 0):.4f} SOL")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
# LIVE TRADING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class LiveTrader:
    """Background engine: scans for signals and executes live trades."""

    def __init__(self, send_func=None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        if self._running:
            return
        cfg = _load_config()
        if not cfg.get("enabled"):
            cfg["enabled"] = True
            _save_config(cfg)

        # Set starting balance if first time
        if cfg.get("starting_balance_sol", 0) <= 0:
            import secure_wallet
            bal = await secure_wallet.get_sol_balance()
            if bal:
                cfg["starting_balance_sol"] = bal
                _save_config(cfg)

        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("LiveTrader started")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
        cfg = _load_config()
        cfg["enabled"] = False
        _save_config(cfg)
        logger.info("LiveTrader stopped")

    async def _loop(self):
        """Main trading loop: scan for signals → execute → monitor positions."""
        import pro_strategy

        while self._running:
            try:
                cfg = _load_config()
                if not cfg.get("enabled"):
                    await asyncio.sleep(10)
                    continue

                # 1. Check existing positions (TP/SL/trailing)
                await check_positions(self._send)

                # 2. Scan for new signals
                signals = await pro_strategy.scan_all_pro(cfg)

                for sig in signals:
                    if not self._running:
                        break

                    symbol = sig.get("symbol", "")
                    direction = sig.get("direction", "")
                    score = sig.get("combined_score", 0)

                    # Only take long signals for spot trading (can't short on DEX)
                    # For short signals: skip (or future: short via perps)
                    if direction != "long":
                        continue

                    # Score threshold for live trading (higher than paper)
                    if score < 40:
                        continue

                    # Need token mint address for Jupiter
                    # Map common symbols to mints
                    mint = _symbol_to_mint(symbol)
                    if not mint:
                        continue

                    # Calculate position size
                    import secure_wallet
                    balance = await secure_wallet.get_sol_balance()
                    if not balance:
                        continue

                    regime_mult = sig.get("regime_mult", 1.0)
                    confidence = min(score / 100, 1.0)
                    base_pct = cfg.get("max_trade_pct", 15.0) / 100
                    trade_sol = balance * base_pct * regime_mult * (0.5 + 0.5 * confidence)
                    trade_sol = min(trade_sol, balance - cfg.get("min_sol_reserve", 0.05))
                    trade_sol = max(trade_sol, 0.01)  # minimum trade

                    result = await buy_token(
                        mint, trade_sol, symbol,
                        signal_data={"score": score, "direction": direction}
                    )

                    if result and self._send:
                        msg = (
                            f"\U0001f7e2 **LIVE BUY** {symbol}\n"
                            f"\u91d1\u989d: {trade_sol:.4f} SOL\n"
                            f"\u8bc4\u5206: {score:.0f}/100\n"
                            f"TX: {result.get('tx_signature', '?')[:20]}..."
                        )
                        try:
                            await self._send(msg)
                        except Exception:
                            pass

                    await asyncio.sleep(2)  # Don't spam

                # Wait for next scan
                await asyncio.sleep(cfg.get("scan_interval", 900))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"LiveTrader loop error: {e}")
                await asyncio.sleep(30)

    @property
    def running(self):
        return self._running


# ── Symbol → Mint mapping (top Solana tokens) ──

_MINT_MAP = {
    "SOL-USDT": SOL_MINT,
    "SOL-USDC": SOL_MINT,
    # Top Solana ecosystem tokens
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "ORCA": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "JTO": "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
    "PYTH": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
    "RENDER": "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof",
    "HNT": "hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux",
    "TENSOR": "TNSRxcUxoT9xBG3de7PiJyTDYu7kskLqcpddxnEJAS6",
    "W": "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ",
    "MOBILE": "mb1eu7TzEc71KxDpsmsKoucSSuuoGLv1drys1oP2jh6",
}


def _symbol_to_mint(symbol: str) -> Optional[str]:
    """Map CEX-style symbol to Solana mint address."""
    # Direct lookup
    clean = symbol.replace("-USDT", "").replace("-USDC", "").replace("-USD", "")
    if clean in _MINT_MAP:
        return _MINT_MAP[clean]
    if symbol in _MINT_MAP:
        return _MINT_MAP[symbol]
    return None
