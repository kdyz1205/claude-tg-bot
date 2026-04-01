"""
Live Trader Module — Jupiter V6 swaps with strict risk controls.

Executes real trades on Solana via Jupiter DEX aggregator.
All transactions go through secure_wallet.py (swap-only signing).

Delta-neutral: ``asyncio.gather(DEX buy, OKX short, return_exceptions=True)`` (single-shot legs)
plus **immediate limping-leg fuse** — market-unwind the surviving leg; see ``limping_fuse_flatten_short``.
"""

import os, json, logging, time, asyncio
from pathlib import Path
from typing import Any, Optional

import httpx

import dex_trader

logger = logging.getLogger(__name__)


def _httpx_response_json(resp: httpx.Response):
    try:
        return resp.json()
    except json.JSONDecodeError:
        logger.warning(
            "JSON decode failed status=%s body=%s",
            resp.status_code,
            (resp.text or "")[:160],
        )
        return None


BASE_DIR = Path(__file__).parent
POSITIONS_FILE = BASE_DIR / "_live_positions.json"
LIVE_CONFIG_FILE = BASE_DIR / "_live_config.json"
POLY_EXECUTIONS_FILE = BASE_DIR / "_poly_executions.json"

JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_URL = "https://quote-api.jup.ag/v6/swap"
JUPITER_PRICE_URL = "https://api.jup.ag/price/v2"
JUPITER_TOKEN_LIST = "https://token.jup.ag/strict"

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# God / delta-neutral DEX hard rails (meme liquidity + slippage + gas headroom)
GOD_DEX_MIN_LIQUIDITY_USD = 10_000.0
GOD_MAX_JUPITER_SLIPPAGE_BPS = 200
GOD_GAS_RESERVE_SOL = 0.015

# ── Risk Control Defaults ──
DEFAULT_LIVE_CONFIG = {
    "enabled": False,
    "max_trade_pct": 15.0,       # max % of portfolio per trade
    # Hard cap per buy/hedge leg (SOL). None = only max_trade_pct applies.
    "max_trade_sol": None,
    "max_positions": 5,           # max concurrent positions
    "daily_loss_limit_pct": 10.0, # halt if daily loss exceeds this
    "stop_loss_pct": 3.0,         # per-trade stop loss
    "take_profit_pct": 8.0,       # per-trade take profit
    "min_liquidity_usd": 500_000, # minimum token liquidity
    "min_mcap_usd": 5_000_000,    # minimum market cap
    "max_slippage_bps": 100,      # max slippage in basis points (1%)
    "min_sol_reserve": 0.05,      # always keep this much SOL for gas
    "check_interval": 30,         # TP/SL 监控周期（秒）— 与 scan_interval 独立
    "scan_interval": 300,         # 新信号扫描周期（秒）；旧配置若仍为 900 则沿用文件值
    "starting_balance_sol": 0,    # set on first start
    "daily_pnl_sol": 0,
    "daily_reset_ts": 0,
    # Phase 11–12: neural → DEX buy + OKX perp hedge (off by default)
    "neural_execution_enabled": False,
    "neural_confidence_threshold": 0.85,
    "neural_okx_inst": "BTC-USDT-SWAP",
    "neural_hedge_symbol": "SOLUSDT",
    "neural_dex_mint": "",
    "neural_poll_sec": 45,
    # Deprecated: delta-neutral uses single-shot parallel gather + immediate limping fuse (no leg retries).
    "hedge_leg_retry_count": 8,
    "hedge_leg_retry_delay_sec": 0.35,
    "dex_leg_retry_count": 3,
    "limping_fuse_verify_rounds": 8,
    # OKX 极端正资金费 + Solana DEX 流动性 → Jupiter 现货多 + OKX 永续空（默认关闭）
    "funding_delta_execution_enabled": False,
    "funding_delta_cooldown_sec": 14400,
    "funding_delta_min_liquidity_override_usd": 50_000.0,
    # Polymarket：Gamma/CLOB 概率差扫描 + CLOB 处决（默认关闭）
    "poly_enabled": False,
    "poly_oracle_interval_sec": 3600,
    "poly_edge_threshold_pct": 15.0,
    "poly_bankroll_usd": 500.0,
    "poly_fractional_kelly": 0.25,
    "poly_max_stake_usd": 200.0,
    "poly_min_stake_usd": 5.0,
    "poly_order_type": "FOK",
    "poly_max_markets_scan": 80,
    "poly_min_liquidity_usd": 5000.0,
    "poly_require_unrestricted": True,
    "poly_dedupe_hours": 24,
    "poly_max_orders_per_scan": 1,
    # Kelly live sizing (see trading.portfolio_manager.PortfolioManager)
    "kelly_fractional_scale": 0.25,
    "kelly_min_edge_p": 0.52,
    "kelly_max_equity_fraction": 0.35,
    "kelly_signal_priors": {},
    "kelly_default_win_p": None,
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
            out = _httpx_response_json(resp)
            return out if isinstance(out, dict) else None
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

            swap_data = _httpx_response_json(resp)
            if not isinstance(swap_data, dict):
                return None
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
        signed_b64 = b64.b64encode(signed_bytes).decode()

        sig = await dex_trader.submit_signed_tx_base64(
            signed_b64,
            skip_preflight=False,
        )
        if sig:
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
                data = _httpx_response_json(resp)
                if not isinstance(data, dict):
                    return None
                price_data = data.get("data", {}).get(mint, {})
                return float(price_data.get("price", 0))
    except Exception:
        pass
    return None


_cached_sol_price = 83.0
_cached_sol_price_ts = 0

async def _get_sol_price_usd() -> float:
    """SOL/USD from OKX ticker WSS hub (60s cache); Jupiter HTTP only if hub empty."""
    global _cached_sol_price, _cached_sol_price_ts
    if time.time() - _cached_sol_price_ts < 60:
        return _cached_sol_price
    try:
        from trading import okx_ws_hub

        await okx_ws_hub.ensure_started()
        p = okx_ws_hub.get_last_price_usdt("SOL-USDT")
        if p > 0:
            _cached_sol_price = p
            _cached_sol_price_ts = time.time()
            return _cached_sol_price
    except Exception:
        pass
    price = await _get_token_price_usd(SOL_MINT)
    if price and price > 0:
        _cached_sol_price = price
        _cached_sol_price_ts = time.time()
    return _cached_sol_price


async def _validate_token(mint: str, cfg: dict) -> tuple[bool, str]:
    """Check if token passes safety filters."""
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # Check Jupiter strict list
            resp = await client.get(f"https://api.jup.ag/tokens/v1/strict")
            if resp.status_code == 200:
                tokens = _httpx_response_json(resp)
                if not isinstance(tokens, list):
                    tokens = []
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
                ds = _httpx_response_json(resp)
                pairs = ds.get("pairs") or [] if isinstance(ds, dict) else []
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

async def _check_risk_controls(
    amount_sol: float,
    cfg: dict,
    signal_data: dict | None = None,
) -> tuple[bool, str]:
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
    cap = cfg.get("max_trade_sol")
    if cap is not None:
        try:
            max_trade = min(max_trade, float(cap))
        except (TypeError, ValueError):
            pass
    try:
        from trading.kelly_sizing import clamped_kelly_max_sol

        kmax = clamped_kelly_max_sol(float(balance), cfg, signal_data=signal_data)
        if kmax is not None and kmax > 0:
            max_trade = min(max_trade, kmax)
    except Exception:
        pass
    if amount_sol > max_trade:
        return False, f"Trade {amount_sol:.4f} SOL > max {max_trade:.4f} SOL (pct {cfg.get('max_trade_pct', 15)}%{' + max_trade_sol cap' if cap is not None else ''})"

    # 4. Reserve
    min_reserve = cfg.get("min_sol_reserve", 0.05)
    if balance - amount_sol < min_reserve:
        return False, f"Would leave {balance - amount_sol:.4f} SOL < {min_reserve} reserve"

    return True, "OK"


async def _live_sol_equity_for_order() -> float:
    """Fresh tradable SOL: portfolio snapshot (wallet leg) with RPC fallback."""
    try:
        from trading.portfolio_snapshot import fetch_tradable_sol_balance

        bal, _src = await fetch_tradable_sol_balance()
        if bal > 0:
            return float(bal)
    except Exception as e:
        logger.debug("live equity via portfolio_snapshot: %s", e)
    import secure_wallet

    b = await secure_wallet.get_sol_balance()
    return float(b or 0)


async def _resolve_kelly_trade_sol(skill_id: str | None, cfg: dict) -> Optional[float]:
    """Equity fetch + Kelly absolute SOL; None = abort (logged)."""
    from trading.portfolio_manager import PortfolioManager, clamp_kelly_stake_to_balance

    equity = await _live_sol_equity_for_order()
    if equity <= 0:
        logger.warning("Kelly 仓位建议不足或已熔断，放弃开火。")
        return None
    raw = PortfolioManager.get_kelly_position_size(skill_id, equity, cfg=cfg)
    return clamp_kelly_stake_to_balance(raw, equity, cfg)


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

    from trading.portfolio_manager import KELLY_MIN_TRADE_SOL

    if amount_sol <= 0 or amount_sol < KELLY_MIN_TRADE_SOL:
        logger.warning("Kelly 仓位建议不足或已熔断，放弃开火。")
        return None

    try:
        from pipeline.god_orchestrator import is_god_hard_stop

        if is_god_hard_stop():
            logger.critical("buy_token blocked: god engine circuit breaker")
            return None
    except ImportError:
        pass

    # Risk check
    allowed, reason = await _check_risk_controls(amount_sol, cfg, signal_data)
    if not allowed:
        logger.warning(f"Trade blocked: {reason}")
        return None

    if signal_data and signal_data.get("llm_trade_directive_json"):
        try:
            import json as _json

            from dispatcher import sanitize_llm_trade_output

            raw = signal_data["llm_trade_directive_json"]
            st = raw if isinstance(raw, str) else _json.dumps(raw)
            safe = None
            try:
                import claude_agent as _ca

                _fn = getattr(_ca, "sanitize_llm_trade_output_with_retries", None)
                if _fn is not None:
                    safe = await _fn(st, max_retries=3)
            except Exception as ex:
                logger.debug("llm trade JSON retries skipped: %s", ex)
            if safe is None:
                safe = await sanitize_llm_trade_output(st)
            if safe is None:
                logger.warning("Trade blocked: LLM directive failed hallucination filter")
                return None
        except Exception as e:
            logger.warning("llm trade guard error: %s", e)
            return None

    val_cfg = dict(cfg)
    if signal_data and signal_data.get("funding_delta_carry"):
        floor_liq = float(
            cfg.get("funding_delta_min_liquidity_override_usd", 50_000.0) or 50_000.0
        )
        val_cfg["min_liquidity_usd"] = floor_liq
        val_cfg["min_mcap_usd"] = 0.0
    if signal_data and signal_data.get("god_engine"):
        val_cfg["min_liquidity_usd"] = max(
            float(val_cfg.get("min_liquidity_usd", 0) or 0),
            GOD_DEX_MIN_LIQUIDITY_USD,
        )
    if signal_data and signal_data.get("smart_money_copy"):
        try:
            val_cfg["min_liquidity_usd"] = float(
                os.environ.get("SMART_MONEY_COPY_MIN_LIQ_USD")
                or cfg.get("smart_money_copy_min_liq_usd", 25_000)
            )
        except (TypeError, ValueError):
            val_cfg["min_liquidity_usd"] = 25_000.0
        val_cfg["min_mcap_usd"] = float(
            cfg.get("smart_money_copy_min_mcap_usd", 0) or 0
        )

    # Token safety check
    valid, val_reason = await _validate_token(mint, val_cfg)
    if not valid:
        logger.warning(f"Token rejected: {val_reason}")
        return None

    # Get wallet
    pubkey = secure_wallet.get_public_key()
    if not pubkey:
        return None

    # Get quote
    amount_lamports = int(amount_sol * 1_000_000_000)
    cap_bps = int(cfg.get("max_slippage_bps", 100))
    if signal_data and signal_data.get("god_engine"):
        cap_bps = min(cap_bps, GOD_MAX_JUPITER_SLIPPAGE_BPS)
    bps = min(cap_bps, dex_trader.compute_dynamic_slippage_bps(0, 0))
    quote = await _get_jupiter_quote(SOL_MINT, mint, amount_lamports, bps)
    if not quote:
        return None

    # Check price impact
    price_impact = float(quote.get("priceImpactPct", 0) or 0)
    bps2 = min(cap_bps, dex_trader.compute_dynamic_slippage_bps(price_impact, 0))
    if bps2 != bps:
        quote = await _get_jupiter_quote(SOL_MINT, mint, amount_lamports, bps2)
        if not quote:
            return None
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
    if not token_price or token_price <= 0:
        # Estimate from quote: outAmount tokens for amount_sol SOL
        token_price = (amount_sol * sol_price) / max(out_amount / 1e6, 1e-12) if out_amount > 0 else 0

    position = {
        "id": f"live_{int(time.time())}_{int(time.time()*1000) % 1000}_{symbol or mint[:8]}",
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
    cap_bps = int(cfg.get("max_slippage_bps", 100))
    sbps = min(cap_bps, dex_trader.compute_dynamic_slippage_bps(0, 0))
    quote = await _get_jupiter_quote(pos["mint"], SOL_MINT, raw_amount, sbps)
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

    try:
        from pipeline.god_orchestrator import on_god_trade_closed

        await on_god_trade_closed(pos)
    except ImportError:
        pass
    except Exception as e:
        logger.debug("on_god_trade_closed: %s", e)

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

        entry_price = pos.get("entry_price_usd") or 0
        if not entry_price or entry_price <= 0:
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


def get_open_live_positions() -> list:
    """Open Jupiter-engine positions (file read only, no RPC)."""
    return [p for p in _load_positions() if p.get("status") == "open"]


def get_recent_closed_trades(limit: int = 5) -> list:
    closed = [p for p in _load_positions() if p.get("status") == "closed"]
    closed.sort(key=lambda x: x.get("close_time") or 0, reverse=True)
    return closed[: max(0, int(limit))]


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
# PHASE 12 — Delta-neutral: gather parallel legs + immediate limping fuse
# ══════════════════════════════════════════════════════════════════════════════

_mint_delta_locks: dict[str, asyncio.Lock] = {}
_neural_pipeline_lock = asyncio.Lock()
_shared_hedge_okx: Any = None
_last_funding_delta_exec_ts: dict[str, float] = {}


def _shared_hedge_okx_executor():
    """Single OKXExecutor for hedge legs — reuses aiohttp TCP pool across trades."""
    global _shared_hedge_okx
    from trading.okx_executor import OKXExecutor

    if _shared_hedge_okx is None:
        _shared_hedge_okx = OKXExecutor()
    _shared_hedge_okx.load_state()
    return _shared_hedge_okx


def _mint_delta_lock(mint: str) -> asyncio.Lock:
    if mint not in _mint_delta_locks:
        _mint_delta_locks[mint] = asyncio.Lock()
    return _mint_delta_locks[mint]


def _dex_leg_success(dex_r: Any) -> bool:
    return isinstance(dex_r, dict) and bool(dex_r.get("tx_signature"))


def _hedge_leg_success(hed_r: Any) -> bool:
    return isinstance(hed_r, dict) and hed_r.get("ok") is True


async def _run_delta_neutral_with_recovery(
    mint: str,
    amount_sol: float,
    symbol: str,
    hedge_symbol: str,
    signal_data: Optional[dict],
) -> dict:
    """
    **Parallel entry only** — no linear “hedge after DEX” sequencing.

    ``asyncio.gather(DEX buy, OKX short, return_exceptions=True)`` schedules both legs in the
    same event-loop tick (single-shot ``open_position`` / ``buy_token``, no retry loops inside).

    **Limping-leg fuse:** if exactly one leg succeeds, **immediately** market-unwind the other
    (Jupiter sell for DEX spot; ``limping_fuse_flatten_short`` = exchange reduce-only first,
    then verified flatten). No multi-second retry windows before rescue.
    """
    try:
        from pipeline.god_orchestrator import is_god_hard_stop

        if is_god_hard_stop():
            return {
                "ok": False,
                "reason": "god_circuit_breaker",
                "dex": None,
                "hedge": None,
                "notional_usd": 0.0,
            }
    except ImportError:
        pass

    from self_monitor import trigger_alert

    execu = _shared_hedge_okx_executor()
    cfg = _load_config()
    sig = signal_data or {}

    if sig.get("god_engine"):
        import secure_wallet as _sw

        bal = await _sw.get_sol_balance()
        if bal is None:
            return {
                "ok": False,
                "reason": "god_balance_unknown",
                "dex": None,
                "hedge": None,
                "notional_usd": 0.0,
            }
        reserve = max(float(cfg.get("min_sol_reserve", 0.05)), GOD_GAS_RESERVE_SOL)
        from trading.portfolio_manager import KELLY_MIN_TRADE_SOL as _kmin

        amount_sol = min(float(amount_sol), max(0.0, float(bal) - reserve))
        if amount_sol < _kmin:
            logger.warning(
                "delta-neutral god: insufficient SOL after reserve (bal=%.4f reserve=%.4f)",
                bal,
                reserve,
            )
            return {
                "ok": False,
                "reason": "god_insufficient_sol",
                "dex": None,
                "hedge": None,
                "notional_usd": 0.0,
            }

    sol_p = await _get_sol_price_usd()
    usd = max(1.0, float(amount_sol) * float(sol_p))

    async def _dex_leg() -> Any:
        return await buy_token(mint, amount_sol, symbol, signal_data=sig)

    async def _hedge_leg() -> Any:
        # Single market attempt — paired with DEX in one gather (not open_position_with_retry).
        return await execu.open_position(hedge_symbol, "short", usd)

    async def _parallel_entry() -> tuple[Any, Any]:
        return await asyncio.gather(
            _dex_leg(),
            _hedge_leg(),
            return_exceptions=True,
        )

    entry_task = asyncio.create_task(_parallel_entry(), name="delta_neutral_gather")
    try:
        from trading.hard_risk_kill import register_trading_task

        register_trading_task(entry_task)
    except Exception:
        pass

    raw = await entry_task
    dex_r, hed_r = raw[0], raw[1]

    if isinstance(dex_r, BaseException):
        logger.error("DEX leg raised: %s", dex_r, exc_info=True)
        dex_r = None
    if isinstance(hed_r, BaseException):
        logger.error("Hedge leg raised: %s", hed_r, exc_info=True)
        hed_r = {"ok": False, "reason": str(hed_r)}

    dex_ok = _dex_leg_success(dex_r)
    hedge_ok = _hedge_leg_success(hed_r)

    # ── Limping fuse: DEX filled, hedge dead → immediate Jupiter market unwind ──
    if dex_ok and not hedge_ok:
        pid = dex_r.get("id") if isinstance(dex_r, dict) else None
        logger.critical(
            "LIMPING_FUSE naked-long risk mint=%s hedge=%s — immediate DEX market unwind",
            mint[:12],
            hedge_symbol,
        )
        alert_long = trigger_alert(
            "NAKED_LONG_RISK",
            f"DEX filled but OKX short failed mint={mint[:12]}… "
            f"Immediate market flatten (Jupiter→SOL). hedge_err={hed_r!r}",
            severity="critical",
        )
        if pid:
            ga0 = await asyncio.gather(
                alert_long,
                sell_token(pid, "limping_fuse_hedge_failed"),
                return_exceptions=True,
            )
            if isinstance(ga0[0], BaseException):
                logger.error("trigger_alert naked long: %s", ga0[0], exc_info=True)
            flat_r = None if isinstance(ga0[1], BaseException) else ga0[1]
            if isinstance(ga0[1], BaseException):
                logger.error("limping fuse sell: %s", ga0[1], exc_info=True)
            for attempt in range(5):
                if flat_r:
                    break
                await asyncio.sleep(0.12 * (attempt + 1))
                flat_r = await sell_token(pid, "limping_fuse_hedge_failed")
            if not flat_r:
                await trigger_alert(
                    "EMERGENCY_DEX_UNWIND_FAILED",
                    f"Could not market-close DEX position_id={pid} mint={mint[:12]}",
                    severity="critical",
                )
        else:
            try:
                await alert_long
            except Exception as e:
                logger.error("trigger_alert: %s", e, exc_info=True)
        return {
            "ok": False,
            "reason": "naked_long_rolled_back",
            "dex": dex_r,
            "hedge": hed_r,
            "notional_usd": usd,
            "fuse": "dex_unwind",
        }

    # ── Limping fuse: hedge on exchange, DEX dead → immediate OKX market reduce + verify ──
    if hedge_ok and not dex_ok:
        logger.critical(
            "LIMPING_FUSE naked-short risk hedge=%s mint=%s — immediate OKX fuse flatten",
            hedge_symbol,
            mint[:12],
        )
        alert_coro = trigger_alert(
            "NAKED_SHORT_RISK",
            f"OKX short on {hedge_symbol} but DEX buy failed mint={mint[:12]}… "
            f"Immediate market fuse + verified flatten. dex_err={dex_r!r}",
            severity="critical",
        )
        flat_coro = execu.limping_fuse_flatten_short(
            hedge_symbol,
            reason="limping_fuse_dex_failed",
            max_verify_rounds=int(cfg.get("limping_fuse_verify_rounds", 8)),
        )
        ga = await asyncio.gather(alert_coro, flat_coro, return_exceptions=True)
        flat = ga[1] if len(ga) > 1 else {"ok": False, "reason": "gather_short"}
        if isinstance(flat, BaseException):
            logger.error("limping_fuse_flatten_short raised: %s", flat, exc_info=True)
            flat = {"ok": False, "reason": str(flat), "verified": False}
        if isinstance(ga[0], BaseException):
            logger.error("trigger_alert raised: %s", ga[0], exc_info=True)
        if not (isinstance(flat, dict) and flat.get("verified")):
            await trigger_alert(
                "OKX_UNWIND_UNVERIFIED",
                f"Hedge {hedge_symbol} may still be open after limping fuse: {flat!r}",
                severity="critical",
            )
        return {
            "ok": False,
            "reason": "naked_short_flattened",
            "dex": dex_r,
            "hedge": hed_r,
            "flatten": flat,
            "notional_usd": usd,
            "fuse": "okx_limping_fuse",
        }

    if dex_ok and hedge_ok:
        return {
            "ok": True,
            "dex": dex_r,
            "hedge": hed_r,
            "notional_usd": usd,
        }

    await trigger_alert(
        "DELTA_NEUTRAL_ABORT",
        f"Both legs failed mint={mint[:8]} hedge={hedge_symbol}",
        severity="warning",
    )
    return {
        "ok": False,
        "reason": "both_legs_failed",
        "dex": dex_r,
        "hedge": hed_r,
        "notional_usd": usd,
    }


async def execute_delta_neutral_buy(
    mint: str,
    amount_sol: float,
    symbol: str,
    hedge_symbol: str,
    signal_data: Optional[dict] = None,
) -> dict:
    """
    Per-mint mutex: no concurrent duplicate delta-neutral stacks on same mint.
    """
    async with _mint_delta_lock(mint):
        return await _run_delta_neutral_with_recovery(
            mint, amount_sol, symbol, hedge_symbol, signal_data
        )


class LiveExecutionGateway:
    """
    Optional facade: neural-style routing with position lock (one flight per mint).
    """

    def __init__(self):
        self._active_mints: set[str] = set()
        self._gate = asyncio.Lock()

    async def execute_atomic_hedge(
        self,
        mint: str,
        amount_sol: float,
        symbol: str,
        hedge_symbol: str,
        signal_data: Optional[dict] = None,
    ) -> dict:
        async with self._gate:
            if mint in self._active_mints:
                return {"ok": False, "reason": "mint_already_in_flight"}
            self._active_mints.add(mint)
        try:
            return await execute_delta_neutral_buy(
                mint, amount_sol, symbol, hedge_symbol, signal_data
            )
        finally:
            self._active_mints.discard(mint)


_live_execution_gateway: Optional[LiveExecutionGateway] = None


def get_live_execution_gateway() -> LiveExecutionGateway:
    global _live_execution_gateway
    if _live_execution_gateway is None:
        _live_execution_gateway = LiveExecutionGateway()
    return _live_execution_gateway


async def strategy_brain_signal_listener_loop(
    send_func=None,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """
    Dedicated asyncio task: polls StrategyBrain ``live_predict`` on a tensor window.
    When confidence ≥ configured threshold (default 85%) and action is ``long``,
    fires ``asyncio.gather``-style atomic hedge via ``execute_delta_neutral_buy``
    (DEX Jupiter buy ∥ OKX perp short), with legged recovery inside that path.
    """
    ev = stop_event or asyncio.Event()
    while not ev.is_set():
        cfg = _load_config()
        if not cfg.get("neural_execution_enabled"):
            try:
                await asyncio.wait_for(ev.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                pass
            continue
        try:
            await _neural_delta_neutral_once(send_func)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("strategy_brain_signal_listener_loop tick failed")
        poll = max(5.0, float(cfg.get("neural_poll_sec", 45)))
        try:
            await asyncio.wait_for(ev.wait(), timeout=poll)
        except asyncio.TimeoutError:
            pass


async def _neural_delta_neutral_once(send_func=None) -> None:
    """StrategyBrain.live_predict; global mutex prevents stacked signals."""
    cfg = _load_config()
    if not cfg.get("neural_execution_enabled"):
        return

    async with _neural_pipeline_lock:
        mint = (cfg.get("neural_dex_mint") or "").strip()
        if not mint or len(mint) < 32:
            return

        try:
            from trading.live_tensor_stream import ensure_stream_started
            from trading.strategy_brain import get_default_strategy_brain
        except ImportError as e:
            logger.debug("Neural bridge import failed: %s", e)
            return

        brain = get_default_strategy_brain()
        await brain.reload_singularity_weights(force=False)
        seq_len = 64
        if brain._singularity_bundle:
            seq_len = int(brain._singularity_bundle.get("seq_len", 64))

        inst = cfg.get("neural_okx_inst") or "BTC-USDT-SWAP"
        stream = await ensure_stream_started(inst, window=max(512, seq_len * 4))
        tens = await stream.build_model_tensor(seq_len=seq_len)
        if tens is None:
            return

        pred = await brain.live_predict(tens)
        if not pred:
            return
        thr = float(cfg.get("neural_confidence_threshold", 0.85))
        if pred.get("confidence", 0) < thr or pred.get("action") != "long":
            return

        trade_sol = await _resolve_kelly_trade_sol("strategy_brain_neural", cfg)
        if trade_sol is None:
            return
        hedge_sym = cfg.get("neural_hedge_symbol") or "SOLUSDT"
        sigd = {
            "source": "strategy_brain_neural",
            "confidence": pred.get("confidence"),
            "prob_up": pred.get("prob_up"),
            "neural": pred,
        }
        gw = get_live_execution_gateway()
        out = await gw.execute_atomic_hedge(
            mint, trade_sol, mint[:8], hedge_sym, signal_data=sigd,
        )
        logger.info("Delta-neutral neural exec: %s", out)
        if send_func:
            try:
                await send_func(
                    f"🧠 Neural Δ-neutral ok={out.get('ok')}\n{out.get('reason', '')}\n"
                    f"USD~{out.get('notional_usd', 0):.0f}"
                )
            except Exception:
                pass


# ══════════════════════════════════════════════════════════════════════════════
# POLYMARKET — sk_poly_oracle + poly_executor
# ══════════════════════════════════════════════════════════════════════════════


def _poly_kelly_stake_usd(
    belief_p: float,
    ask_price: float,
    bankroll_usd: float,
    fractional_kelly: float,
    max_stake_usd: float,
    min_stake_usd: float,
) -> float:
    """二元合约买入：在价 ask 支付，真概率 belief_p；全额凯利 f=(p-π)/(1-π)，再乘 fractional。"""
    p = max(0.001, min(0.999, float(belief_p)))
    pi = max(0.001, min(0.999, float(ask_price)))
    if p <= pi:
        return 0.0
    denom = 1.0 - pi
    if denom <= 1e-9:
        return 0.0
    f_full = (p - pi) / denom
    f_full = max(0.0, min(1.0, f_full))
    stake = float(fractional_kelly) * f_full * float(bankroll_usd)
    stake = min(stake, float(max_stake_usd))
    if stake < float(min_stake_usd):
        return 0.0
    return stake


def _load_poly_executions() -> list:
    try:
        if POLY_EXECUTIONS_FILE.exists():
            with open(POLY_EXECUTIONS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else []
    except Exception:
        pass
    return []


def _save_poly_executions(rows: list):
    try:
        tmp = str(POLY_EXECUTIONS_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rows[-500:], f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(POLY_EXECUTIONS_FILE))
    except Exception as e:
        logger.error("Poly executions save failed: %s", e)


def _poly_token_recently_executed(token_id: str, dedupe_hours: float) -> bool:
    cutoff = time.time() - max(1.0, float(dedupe_hours)) * 3600.0
    for row in _load_poly_executions():
        if not isinstance(row, dict):
            continue
        if str(row.get("token_id")) != str(token_id):
            continue
        ts = float(row.get("ts") or 0)
        if ts >= cutoff:
            return True
    return False


async def _poly_oracle_scan_and_execute(send_func=None) -> None:
    cfg = _load_config()
    if not cfg.get("poly_enabled"):
        return

    try:
        from skills.sk_poly_oracle import scan_probability_edges
        from trading.poly_executor import PolyExecutor
    except ImportError as e:
        logger.warning("Polymarket modules unavailable: %s", e)
        return

    thr = float(cfg.get("poly_edge_threshold_pct", 15.0))
    max_mk = int(cfg.get("poly_max_markets_scan", 80))
    min_liq = float(cfg.get("poly_min_liquidity_usd", 5000.0))
    unrestricted = bool(cfg.get("poly_require_unrestricted", True))

    try:
        opportunities = await scan_probability_edges(
            min_edge_pct=thr,
            max_markets=max_mk,
            min_liquidity_usd=min_liq,
            require_unrestricted=unrestricted,
        )
    except Exception as e:
        logger.exception("Poly oracle scan failed: %s", e)
        return

    if not opportunities:
        logger.info("Poly oracle: no edges ≥ %.1f%%", thr)
        return

    bankroll = float(cfg.get("poly_bankroll_usd", 500.0))
    fk = float(cfg.get("poly_fractional_kelly", 0.25))
    cap = float(cfg.get("poly_max_stake_usd", 200.0))
    floor = float(cfg.get("poly_min_stake_usd", 5.0))
    otype = str(cfg.get("poly_order_type") or "FOK").strip().upper()
    dedupe_h = float(cfg.get("poly_dedupe_hours", 24))
    max_n = max(1, int(cfg.get("poly_max_orders_per_scan", 1)))

    execu = PolyExecutor()
    done = 0
    for opp in opportunities:
        if done >= max_n:
            break
        if not isinstance(opp, dict):
            continue
        tid = str(opp.get("token_id") or "")
        if not tid:
            continue
        if _poly_token_recently_executed(tid, dedupe_h):
            continue

        p = float(opp.get("oracle_prob") or 0)
        ask = float(opp.get("entry_ask") or 0)
        min_sz = float(opp.get("order_min_size") or 5)

        stake_usd = _poly_kelly_stake_usd(p, ask, bankroll, fk, cap, floor)
        if stake_usd <= 0:
            continue

        size_shares = stake_usd / max(ask, 1e-6)
        if size_shares < min_sz:
            size_shares = float(min_sz)
            stake_usd = size_shares * ask

        if stake_usd > cap:
            size_shares = cap / max(ask, 1e-6)
            stake_usd = cap

        result = await execu.open_position(
            token_id=tid,
            price=ask,
            size_shares=size_shares,
            order_type=otype,
        )

        rows = _load_poly_executions()
        rows.append(
            {
                "ts": time.time(),
                "token_id": tid,
                "condition_id": opp.get("condition_id"),
                "edge_pct": opp.get("edge_pct"),
                "stake_usd": round(stake_usd, 2),
                "ok": bool(result.get("ok")),
                "order_id": result.get("order_id"),
            }
        )
        _save_poly_executions(rows)

        if result.get("ok") and send_func:
            q = (opp.get("question") or "")[:120]
            msg = (
                "⚔️ **Polymarket 战报**\n"
                f"市场: {q}\n"
                f"结果: {opp.get('outcome_name', '?')}\n"
                f"概率差: {opp.get('edge_pct')}% (Γ {p:.3f} vs 盘口 ~{opp.get('market_mid', 0):.3f})\n"
                f"凯利名义: ~${stake_usd:.2f} | 份额 {size_shares:.2f} @ {ask:.3f}\n"
                f"订单: `{result.get('order_id') or 'ok'}`"
            )
            try:
                await send_func(msg)
            except Exception:
                pass

        if result.get("ok"):
            done += 1
        else:
            logger.warning("Poly order failed: %s", result.get("reason", result))


# ══════════════════════════════════════════════════════════════════════════════
# LIVE TRADING ENGINE
# ══════════════════════════════════════════════════════════════════════════════

class LiveTrader:
    """Background engine: scans for signals and executes live trades."""

    def __init__(self, send_func=None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_pos_check = 0.0
        self._last_sig_scan = 0.0
        self._last_poly_oracle_ts = 0.0
        self._brain_stop: Optional[asyncio.Event] = None
        self._brain_task: Optional[asyncio.Task] = None

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
        self._last_pos_check = 0.0
        self._last_sig_scan = 0.0
        self._last_poly_oracle_ts = 0.0
        self._brain_stop = asyncio.Event()
        self._brain_stop.clear()
        self._task = asyncio.create_task(self._loop())
        # Always run listener: when neural_execution_enabled is off it idles cheaply;
        # toggling config does not require LiveTrader restart.
        self._brain_task = asyncio.create_task(
            strategy_brain_signal_listener_loop(self._send, self._brain_stop)
        )
        logger.info("LiveTrader started")

    async def stop(self):
        self._running = False
        if self._brain_stop is not None:
            self._brain_stop.set()
        if self._brain_task:
            self._brain_task.cancel()
            try:
                await self._brain_task
            except asyncio.CancelledError:
                pass
            self._brain_task = None
        self._brain_stop = None
        if self._task:
            self._task.cancel()
            self._task = None
        cfg = _load_config()
        cfg["enabled"] = False
        _save_config(cfg)
        logger.info("LiveTrader stopped")

    async def _signal_scan_and_execute(self):
        """Scan pro + alpha + on-chain filter, then attempt Jupiter buys (real TX)."""
        import pro_strategy

        cfg = _load_config()
        signals = await pro_strategy.scan_all_pro(cfg)
        if not isinstance(signals, list):
            signals = list(signals) if signals else []

        if cfg.get("funding_delta_execution_enabled"):
            try:
                from arbitrage_engine import scan_funding_delta_neutral_signals

                funding_rows = await scan_funding_delta_neutral_signals()
            except Exception as e:
                logger.warning("funding delta neutral scan failed: %s", e)
                funding_rows = []
            for fr in funding_rows:
                if not isinstance(fr, dict) or not fr.get("execute_delta_neutral_buy_compat"):
                    continue
                sm = fr.get("solana_mint")
                if not sm or len(str(sm)) < 32:
                    continue
                hs = (fr.get("hedge_symbol") or "").strip()
                if not hs:
                    continue
                base = (fr.get("base_asset") or "").strip() or "?"
                signals.insert(
                    0,
                    {
                        "symbol": f"{base}-USDT" if base else "?",
                        "direction": "long",
                        "combined_score": 100.0,
                        "mint_address": str(sm),
                        "source": "funding_delta_positive",
                        "execute_delta_neutral_buy_compat": True,
                        "hedge_symbol": hs,
                        "funding_annualized_pct": fr.get("annualized_rate"),
                        "dex_spot_liquidity_usd": fr.get("dex_spot_liquidity_usd"),
                        "funding_delta_carry": True,
                        "okx_inst_id": fr.get("okx_inst_id"),
                    },
                )

        try:
            from alpha_engine import scan_alpha, scan_onchain_filter

            alpha_result = await scan_alpha()
            if alpha_result:
                for token in (alpha_result if isinstance(alpha_result, list) else []):
                    addr = token.get("address", "")
                    if addr and len(addr) >= 32 and not addr.startswith("0x"):
                        signals.append({
                            "symbol": token.get("symbol", "?"),
                            "direction": "long",
                            "combined_score": float(token.get("score", 0) or 0),
                            "mint_address": addr,
                            "source": "alpha_engine",
                        })

            oc_res = await scan_onchain_filter()
            if oc_res:
                seen = {s.get("mint_address") or s.get("address") for s in signals if s.get("mint_address") or s.get("address")}
                for token in (oc_res if isinstance(oc_res, list) else []):
                    addr = token.get("address", "")
                    if not addr or len(addr) < 32 or addr.startswith("0x") or addr in seen:
                        continue
                    seen.add(addr)
                    signals.append({
                        "symbol": token.get("symbol", "?"),
                        "direction": "long",
                        "combined_score": float(token.get("score", 0) or 0),
                        "mint_address": addr,
                        "source": "onchain_filter",
                    })
        except Exception as e:
            logger.debug("Alpha/onchain scan in live loop: %s", e)

        for sig in signals:
            if not self._running:
                break

            if _is_poly_market_signal(sig):
                if not cfg.get("poly_execution_enabled"):
                    logger.debug("POLY_MARKET signal skipped: poly_execution_enabled=False")
                    continue
                try:
                    from trading.poly_executor import execute_poly_market_signal_async

                    poly_r = await execute_poly_market_signal_async(sig, cfg)
                except Exception as e:
                    logger.error("Polymarket execution error: %s", e)
                    continue
                if poly_r.get("ok") and self._send:
                    msg = (
                        f"\U0001f3af **POLY CLOB**\n"
                        f"token: `{str(poly_r.get('token_id', ''))[:24]}…`\n"
                        f"~${poly_r.get('usdc_approx', 0):.2f} @ {poly_r.get('price', 0):.4f}\n"
                        f"resp: `{str(poly_r.get('response', ''))[:180]}`"
                    )
                    try:
                        await self._send(msg)
                    except Exception:
                        pass
                elif not poly_r.get("ok"):
                    logger.warning("Polymarket signal not executed: %s", poly_r.get("reason"))
                await asyncio.sleep(2)
                continue

            if sig.get("execute_delta_neutral_buy_compat") and sig.get("mint_address"):
                mint_dn = sig["mint_address"]
                hedge_sym = (sig.get("hedge_symbol") or "").strip()
                if not hedge_sym:
                    continue
                cooldown = float(cfg.get("funding_delta_cooldown_sec", 14400))
                now_ts = time.time()
                if now_ts - _last_funding_delta_exec_ts.get(mint_dn, 0) < cooldown:
                    continue

                skill_dn = (
                    str(sig.get("skill_id") or "").strip()
                    or str(sig.get("source") or "funding_delta_positive").strip()
                )
                trade_sol = await _resolve_kelly_trade_sol(skill_dn or "funding_delta_positive", cfg)
                if trade_sol is None:
                    continue

                sym_short = (sig.get("symbol") or "?").replace("-USDT", "").replace(
                    "-USDC", ""
                )
                dn_result = await execute_delta_neutral_buy(
                    mint_dn,
                    trade_sol,
                    sym_short,
                    hedge_sym,
                    signal_data=sig,
                )
                if dn_result.get("ok"):
                    _last_funding_delta_exec_ts[mint_dn] = now_ts
                if self._send:
                    try:
                        ann = sig.get("funding_annualized_pct", 0)
                        await self._send(
                            f"\U0001f4b5 **FUNDING \u0394-neutral** ok={dn_result.get('ok')}\n"
                            f"{sym_short}  hedge `{hedge_sym}`  ~${dn_result.get('notional_usd', 0):.0f}\n"
                            f"\u5e74\u5316\u8d44\u91d1\u8d39(OKX): **{ann}%**\n"
                            f"reason: `{dn_result.get('reason', '')}`"
                        )
                    except Exception:
                        pass
                await asyncio.sleep(2)
                continue

            symbol = sig.get("symbol", "")
            direction = sig.get("direction", "")
            score = float(sig.get("combined_score", 0) or 0)

            if direction != "long":
                continue

            if score < 40:
                continue

            mint = sig.get("mint_address") or _symbol_to_mint(symbol)
            if not mint:
                continue

            skill_scan = (
                str(sig.get("skill_id") or "").strip()
                or str(sig.get("source") or "live_scan").strip()
            )
            trade_sol = await _resolve_kelly_trade_sol(skill_scan or "live_scan", cfg)
            if trade_sol is None:
                continue

            result = await buy_token(
                mint, trade_sol, symbol,
                signal_data={
                    "score": score,
                    "direction": direction,
                    "source": sig.get("source"),
                    "skill_id": skill_scan or None,
                },
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

            await asyncio.sleep(2)

    async def _loop(self):
        """TP/SL 按 check_interval；新单扫描按 scan_interval（互不阻塞）。"""
        while self._running:
            try:
                cfg = _load_config()
                if not cfg.get("enabled"):
                    await asyncio.sleep(10)
                    continue

                now = time.time()
                ci = max(10.0, float(cfg.get("check_interval", 30)))
                si = max(60.0, float(cfg.get("scan_interval", 300)))
                poly_int = max(300.0, float(cfg.get("poly_oracle_interval_sec", 3600)))

                if cfg.get("poly_enabled") and (
                    self._last_poly_oracle_ts == 0
                    or (now - self._last_poly_oracle_ts) >= poly_int
                ):
                    self._last_poly_oracle_ts = time.time()
                    await _poly_oracle_scan_and_execute(self._send)

                if self._last_pos_check == 0 or (now - self._last_pos_check) >= ci:
                    self._last_pos_check = time.time()
                    await check_positions(self._send)

                now = time.time()
                if self._last_sig_scan == 0 or (now - self._last_sig_scan) >= si:
                    self._last_sig_scan = time.time()
                    await self._signal_scan_and_execute()

                now = time.time()
                next_p = self._last_pos_check + ci
                next_s = self._last_sig_scan + si
                next_poly = (
                    self._last_poly_oracle_ts + poly_int if cfg.get("poly_enabled") else 1e18
                )
                wait = min(next_p, next_s, next_poly) - now
                wait = max(1.0, min(wait, 120.0))
                await asyncio.sleep(wait)

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
    # Pixels (PIXEL) — verify mint in your wallet / Jupiter before large size
    "PIXEL": "Di4B2JSRykk27QcD9oe9sjqff1kTW4mf23bfDePwEKLu",
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


def _is_poly_market_signal(sig: dict) -> bool:
    """Route to Polymarket CLOB when signal is explicitly tagged."""
    if sig.get("poly_market") is True:
        return True
    t = (sig.get("type") or sig.get("venue") or sig.get("market_type") or "")
    return str(t).upper() == "POLY_MARKET"


def _symbol_to_mint(symbol: str) -> Optional[str]:
    """Map CEX-style symbol to Solana mint address."""
    # Direct lookup
    clean = symbol.replace("-USDT", "").replace("-USDC", "").replace("-USD", "")
    if clean in _MINT_MAP:
        return _MINT_MAP[clean]
    if symbol in _MINT_MAP:
        return _MINT_MAP[symbol]
    return None


def install_smart_money_copy_trade_bridge() -> None:
    """
    Register the async handler for ``trading.smart_money_copy_hook`` so consensus
    signals can call :func:`buy_token` when ``SMART_MONEY_COPY_TRADE_ENABLED=1``.
    Safe to call multiple times (re-registers the same handler).
    """
    from trading.smart_money_copy_hook import register_copy_trade_handler

    async def _on_smart_money_consensus(
        *,
        contract: str,
        token: str,
        buys: list,
        source: str = "",
        **_: Any,
    ) -> None:
        mint = (contract or "").strip()
        if not mint:
            logger.warning("smart_money_copy: empty contract/mint, skip")
            return
        try:
            amt = float(os.environ.get("SMART_MONEY_COPY_TRADE_SOL") or "0.1")
        except (TypeError, ValueError):
            amt = 0.1
        if amt <= 0:
            logger.debug("smart_money_copy: SMART_MONEY_COPY_TRADE_SOL <= 0, skip")
            return
        sig: dict[str, Any] = {
            "smart_money_copy": True,
            "source": source,
            "smart_money_buys": buys,
            "kelly_win_rate": float(os.environ.get("SMART_MONEY_COPY_KELLY_P") or 0.55),
            "kelly_b": float(os.environ.get("SMART_MONEY_COPY_KELLY_B") or 1.2),
        }
        await buy_token(mint, amt, symbol=(token or "")[:32], signal_data=sig)

    register_copy_trade_handler(_on_smart_money_consensus)
    logger.info("smart_money_copy_trade_bridge: handler installed")
