"""
paper_trader.py — Paper Trading System

Phase 1 of the auto-trading pipeline:
1. Receive signals from onchain filter / alpha engine
2. Simulate buy at current price
3. Track price at intervals (5m, 15m, 1h, 4h, 24h)
4. Auto-close: take profit +50%, stop loss -20%, time stop 24h
5. Calculate real win rate, avg return, max drawdown
6. Only graduate to real trading when win rate > 55% over 50+ trades

Data file: _paper_trades.json
"""

import asyncio
import json
import logging
import os
import time
import tempfile
from typing import Optional

logger = logging.getLogger(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TRADES_FILE = os.path.join(BASE_DIR, "_paper_trades.json")
CONFIG_FILE = os.path.join(BASE_DIR, "_paper_config.json")

# ─── Default Configuration ───
DEFAULT_CONFIG = {
    "enabled": True,
    "mode": "paper",           # "paper" | "live" (future)
    "max_position_sol": 0.5,   # Max per trade in SOL
    "max_total_sol": 2.0,      # Max total exposure
    "daily_loss_limit_sol": 0.3,
    "take_profit_pct": 50.0,   # +50% take profit
    "stop_loss_pct": -20.0,    # -20% stop loss
    "time_stop_hours": 24,     # Close after 24h
    "min_liquidity": 20000,    # Min USD liquidity to enter
    "min_mcap": 15000,
    "max_mcap": 8000000,
    "min_alpha_score": 70,     # Min score from alpha engine
    "graduation_trades": 50,   # Need 50+ trades before live
    "graduation_winrate": 55,  # Need 55%+ win rate before live
    "check_interval": 60,      # Check prices every 60 seconds
}

# ─── Trade Schema ───
# {
#   "id": "paper_1711234567_SOL123",
#   "symbol": "TOKEN",
#   "name": "Token Name",
#   "address": "So1ana...",
#   "chain": "solana",
#   "pair_url": "https://dexscreener.com/...",
#   "entry_price": 0.00123,
#   "entry_mcap": 500000,
#   "entry_liq": 30000,
#   "entry_time": 1711234567.0,
#   "position_sol": 0.5,
#   "position_tokens": 406.5,
#   "status": "open" | "closed",
#   "close_reason": null | "take_profit" | "stop_loss" | "time_stop" | "manual",
#   "close_price": null | 0.00185,
#   "close_time": null | 1711238167.0,
#   "pnl_pct": null | 50.4,
#   "pnl_sol": null | 0.25,
#   "price_history": [
#     {"time": 1711234627, "price": 0.00125, "pnl_pct": 1.6},
#     ...
#   ],
#   "peak_pnl_pct": 0.0,    # highest unrealized PnL
#   "trough_pnl_pct": 0.0,  # lowest unrealized PnL (max drawdown per trade)
#   "alpha_score": 85,
#   "signal_source": "onchain_filter",
# }


def _load_trades() -> list:
    try:
        with open(TRADES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_trades(trades: list):
    try:
        tmp = TRADES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, TRADES_FILE)
    except Exception as e:
        logger.warning(f"paper_trader save failed: {e}")


def _load_config() -> dict:
    cfg = dict(DEFAULT_CONFIG)
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            user_cfg = json.load(f)
            cfg.update(user_cfg)
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return cfg


def _save_config(cfg: dict):
    try:
        tmp = CONFIG_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, CONFIG_FILE)
    except Exception as e:
        logger.warning(f"paper_trader config save failed: {e}")


# ─── Entry Logic ───

def can_open_trade(token: dict, cfg: dict = None) -> tuple:
    """Check if we should open a paper trade for this token.
    Returns (allowed: bool, reason: str).
    """
    if cfg is None:
        cfg = _load_config()

    if not cfg.get("enabled", True):
        return False, "paper trading disabled"

    trades = _load_trades()

    # Check max open positions
    open_trades = [t for t in trades if t.get("status") == "open"]
    total_exposure = sum(t.get("position_sol", 0) for t in open_trades)
    if total_exposure >= cfg.get("max_total_sol", 2.0):
        return False, f"max exposure reached ({total_exposure:.2f} SOL)"

    # Check daily loss limit
    today_start = (time.time() // 86400) * 86400
    today_closed = [t for t in trades
                    if t.get("status") == "closed"
                    and t.get("close_time", 0) >= today_start]
    daily_loss = sum(t.get("pnl_sol", 0) for t in today_closed if t.get("pnl_sol", 0) < 0)
    if abs(daily_loss) >= cfg.get("daily_loss_limit_sol", 0.3):
        return False, f"daily loss limit hit ({daily_loss:.3f} SOL)"

    # Check duplicate (same address already open)
    addr = token.get("address", "")
    for t in open_trades:
        if t.get("address") == addr:
            return False, f"already have open position in {token.get('symbol', '?')}"

    # Check liquidity
    liq = token.get("liquidity_usd", 0) or token.get("entry_liq", 0)
    if liq < cfg.get("min_liquidity", 20000):
        return False, f"liquidity too low (${liq:,.0f})"

    # Check mcap
    mcap = token.get("market_cap_usd", 0) or token.get("entry_mcap", 0) or token.get("mcap", 0)
    if mcap < cfg.get("min_mcap", 15000):
        return False, f"mcap too low (${mcap:,.0f})"
    if mcap > cfg.get("max_mcap", 8000000):
        return False, f"mcap too high (${mcap:,.0f})"

    return True, "ok"


def open_paper_trade(token: dict, cfg: dict = None) -> Optional[dict]:
    """Open a simulated paper trade.
    token: dict from scan_onchain_filter or alpha_engine.
    Returns the trade record or None if rejected.
    """
    if cfg is None:
        cfg = _load_config()

    allowed, reason = can_open_trade(token, cfg)
    if not allowed:
        logger.info(f"Paper trade rejected for {token.get('symbol', '?')}: {reason}")
        return None

    price = float(token.get("price_usd", 0) or token.get("entry_price", 0) or 0)
    if price <= 0:
        logger.warning(f"Invalid price for {token.get('symbol', '?')}: {price}")
        return None

    position_sol = cfg.get("max_position_sol", 0.5)
    # Estimate SOL price (~$150, will be fetched live in future)
    sol_price_est = 150.0
    position_usd = position_sol * sol_price_est
    position_tokens = position_usd / price

    now = time.time()
    trade = {
        "id": f"paper_{int(now)}_{token.get('symbol', 'UNK')[:10]}",
        "symbol": token.get("symbol", "?"),
        "name": token.get("name", "?"),
        "address": token.get("address", ""),
        "chain": token.get("chain", "solana"),
        "pair_url": token.get("pair_url", ""),
        "entry_price": price,
        "entry_mcap": token.get("market_cap_usd", 0) or token.get("mcap", 0),
        "entry_liq": token.get("liquidity_usd", 0) or token.get("entry_liq", 0),
        "entry_time": now,
        "position_sol": position_sol,
        "position_tokens": round(position_tokens, 2),
        "status": "open",
        "close_reason": None,
        "close_price": None,
        "close_time": None,
        "pnl_pct": None,
        "pnl_sol": None,
        "price_history": [],
        "peak_pnl_pct": 0.0,
        "trough_pnl_pct": 0.0,
        "alpha_score": token.get("score", 0),
        "signal_source": token.get("source", "onchain_filter"),
    }

    trades = _load_trades()
    trades.append(trade)
    # Keep last 500 trades
    if len(trades) > 500:
        trades = trades[-500:]
    _save_trades(trades)

    logger.info(f"Paper trade opened: {trade['symbol']} @ ${price:.8f} ({position_sol} SOL)")
    return trade


def close_paper_trade(trade_id: str, current_price: float, reason: str) -> Optional[dict]:
    """Close a paper trade and record P&L."""
    trades = _load_trades()
    for t in trades:
        if t.get("id") == trade_id and t.get("status") == "open":
            entry = t.get("entry_price", 0)
            if entry <= 0:
                entry = 1e-12

            pnl_pct = ((current_price - entry) / entry) * 100
            pnl_sol = t.get("position_sol", 0) * (pnl_pct / 100)

            t["status"] = "closed"
            t["close_reason"] = reason
            t["close_price"] = current_price
            t["close_time"] = time.time()
            t["pnl_pct"] = round(pnl_pct, 2)
            t["pnl_sol"] = round(pnl_sol, 4)

            _save_trades(trades)
            logger.info(f"Paper trade closed: {t['symbol']} | {reason} | PnL: {pnl_pct:+.1f}% ({pnl_sol:+.4f} SOL)")
            return t
    return None


# ─── Price Monitoring Loop ───

async def _fetch_current_price(address: str) -> Optional[float]:
    """Fetch current price from DexScreener."""
    try:
        import httpx
        url = f"https://api.dexscreener.com/latest/dex/tokens/{address}"
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return None
            data = resp.json()
            pairs = data.get("pairs") or []
            if not pairs:
                return None
            # Pick pair with most liquidity
            best = max(pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0) or 0))
            return float(best.get("priceUsd", 0) or 0)
    except Exception as e:
        logger.debug(f"Price fetch failed for {address}: {e}")
        return None


async def check_and_update_trades(send_func=None) -> dict:
    """Main monitoring loop iteration.
    Checks all open trades, updates prices, auto-closes on TP/SL/time stop.
    Returns summary dict.
    """
    cfg = _load_config()
    trades = _load_trades()
    open_trades = [t for t in trades if t.get("status") == "open"]

    if not open_trades:
        return {"checked": 0, "closed": 0}

    closed_count = 0
    now = time.time()

    for trade in open_trades:
        address = trade.get("address", "")
        if not address:
            continue

        # Rate limit: 0.3s between API calls
        await asyncio.sleep(0.3)

        price = await _fetch_current_price(address)
        if price is None or price <= 0:
            continue

        entry = trade.get("entry_price", 0)
        if entry <= 0:
            continue

        pnl_pct = ((price - entry) / entry) * 100
        age_hours = (now - trade.get("entry_time", now)) / 3600

        # Update price history (keep last 100 entries per trade)
        history = trade.get("price_history", [])
        history.append({
            "time": now,
            "price": price,
            "pnl_pct": round(pnl_pct, 2),
        })
        if len(history) > 100:
            history = history[-100:]
        trade["price_history"] = history

        # Update peak/trough
        trade["peak_pnl_pct"] = max(trade.get("peak_pnl_pct", 0), pnl_pct)
        trade["trough_pnl_pct"] = min(trade.get("trough_pnl_pct", 0), pnl_pct)

        # Check take profit
        tp = cfg.get("take_profit_pct", 50.0)
        sl = cfg.get("stop_loss_pct", -20.0)
        ts = cfg.get("time_stop_hours", 24)

        close_reason = None
        if pnl_pct >= tp:
            close_reason = "take_profit"
        elif pnl_pct <= sl:
            close_reason = "stop_loss"
        elif age_hours >= ts:
            close_reason = "time_stop"

        if close_reason:
            # Close in-memory (NOT via close_paper_trade which reloads from disk
            # and would be overwritten by our _save_trades below — classic lost update)
            if entry <= 0:
                entry = 1e-12
            pnl_sol = trade.get("position_sol", 0) * (pnl_pct / 100)
            trade["status"] = "closed"
            trade["close_reason"] = close_reason
            trade["close_price"] = price
            trade["close_time"] = time.time()
            trade["pnl_pct"] = round(pnl_pct, 2)
            trade["pnl_sol"] = round(pnl_sol, 4)
            closed_count += 1
            logger.info(f"Paper trade closed: {trade.get('symbol', '?')} | {close_reason} | PnL: {pnl_pct:+.1f}%")

            # Send notification
            if send_func:
                emoji = "\U0001f7e2" if pnl_pct > 0 else "\U0001f534"
                reason_cn = {
                    "take_profit": "\u6b62\u76c8 \u2705",
                    "stop_loss": "\u6b62\u635f \u274c",
                    "time_stop": "\u65f6\u95f4\u6b62\u635f \u23f0",
                }.get(close_reason, close_reason)

                msg = (
                    f"{emoji} Paper Trade \u5e73\u4ed3\n\n"
                    f"Token: {trade.get('symbol', '?')} ({trade.get('name', '?')})\n"
                    f"\u539f\u56e0: {reason_cn}\n"
                    f"\u5165\u573a: ${entry:.8f}\n"
                    f"\u51fa\u573a: ${price:.8f}\n"
                    f"PnL: {pnl_pct:+.1f}% ({pnl_sol:+.4f} SOL)\n"
                    f"\u6301\u4ed3\u65f6\u95f4: {age_hours:.1f}h\n"
                    f"\u6700\u9ad8: {trade.get('peak_pnl_pct', 0):+.1f}% | \u6700\u4f4e: {trade.get('trough_pnl_pct', 0):+.1f}%\n"
                    f"\n\U0001f4ca \u5f53\u524d\u6218\u7ee9: {format_stats_brief()}"
                )
                try:
                    await send_func(msg)
                except Exception:
                    pass

    # Save ALL updates (price histories + closures) in one atomic write
    _save_trades(trades)

    return {"checked": len(open_trades), "closed": closed_count}


# ─── Statistics ───

def compute_stats() -> dict:
    """Compute overall paper trading statistics."""
    trades = _load_trades()
    closed = [t for t in trades if t.get("status") == "closed"]
    open_trades = [t for t in trades if t.get("status") == "open"]

    if not closed:
        return {
            "total_trades": 0,
            "open_count": len(open_trades),
            "win_rate": 0,
            "avg_pnl_pct": 0,
            "total_pnl_sol": 0,
            "best_trade_pct": 0,
            "worst_trade_pct": 0,
            "avg_hold_hours": 0,
            "max_drawdown_pct": 0,
            "by_reason": {},
            "by_source": {},
            "ready_for_live": False,
            "graduation_progress": "0/50 trades",
        }

    wins = [t for t in closed if (t.get("pnl_pct") or 0) > 0]
    losses = [t for t in closed if (t.get("pnl_pct") or 0) <= 0]

    pnls = [t.get("pnl_pct", 0) or 0 for t in closed]
    sol_pnls = [t.get("pnl_sol", 0) or 0 for t in closed]

    hold_times = []
    for t in closed:
        entry_t = t.get("entry_time", 0)
        close_t = t.get("close_time", 0)
        if entry_t and close_t:
            hold_times.append((close_t - entry_t) / 3600)

    # By close reason
    by_reason = {}
    for t in closed:
        r = t.get("close_reason", "unknown")
        if r not in by_reason:
            by_reason[r] = {"count": 0, "wins": 0, "total_pnl": 0}
        by_reason[r]["count"] += 1
        if (t.get("pnl_pct") or 0) > 0:
            by_reason[r]["wins"] += 1
        by_reason[r]["total_pnl"] += t.get("pnl_sol", 0) or 0

    # By signal source
    by_source = {}
    for t in closed:
        s = t.get("signal_source", "unknown")
        if s not in by_source:
            by_source[s] = {"count": 0, "wins": 0, "total_pnl": 0}
        by_source[s]["count"] += 1
        if (t.get("pnl_pct") or 0) > 0:
            by_source[s]["wins"] += 1
        by_source[s]["total_pnl"] += t.get("pnl_sol", 0) or 0

    # Max drawdown (peak-to-trough across all trades)
    troughs = [t.get("trough_pnl_pct", 0) for t in closed]
    max_dd = min(troughs) if troughs else 0

    # Graduation check
    total = len(closed)
    wr = (len(wins) / total * 100) if total > 0 else 0
    cfg = _load_config()
    grad_trades = cfg.get("graduation_trades", 50)
    grad_wr = cfg.get("graduation_winrate", 55)
    ready = total >= grad_trades and wr >= grad_wr

    return {
        "total_trades": total,
        "open_count": len(open_trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(wr, 1),
        "avg_pnl_pct": round(sum(pnls) / total, 2),
        "total_pnl_sol": round(sum(sol_pnls), 4),
        "best_trade_pct": round(max(pnls), 2) if pnls else 0,
        "worst_trade_pct": round(min(pnls), 2) if pnls else 0,
        "avg_hold_hours": round(sum(hold_times) / len(hold_times), 1) if hold_times else 0,
        "max_drawdown_pct": round(max_dd, 2),
        "by_reason": by_reason,
        "by_source": by_source,
        "ready_for_live": ready,
        "graduation_progress": f"{total}/{grad_trades} trades, {wr:.0f}%/{grad_wr}% WR",
    }


def format_stats_brief() -> str:
    """One-line stats summary."""
    s = compute_stats()
    if s["total_trades"] == 0:
        return "\u6682\u65e0\u4ea4\u6613\u8bb0\u5f55"
    return (
        f"W/L: {s['wins']}/{s['losses']} "
        f"({s['win_rate']:.0f}%) | "
        f"PnL: {s['total_pnl_sol']:+.4f} SOL | "
        f"Open: {s['open_count']}"
    )


def format_stats_full() -> str:
    """Full stats report for /paper command."""
    s = compute_stats()
    cfg = _load_config()

    lines = [
        "\U0001f4ca Paper Trading \u6a21\u62df\u4ea4\u6613\u62a5\u544a",
        "\u2550" * 30,
        "",
    ]

    if s["total_trades"] == 0:
        lines.append("\u6682\u65e0\u4ea4\u6613\u8bb0\u5f55\u3002\u7b49\u5f85\u4fe1\u53f7\u89e6\u53d1\u81ea\u52a8\u5f00\u4ed3...")
        lines.append(f"\n\u2699\ufe0f \u914d\u7f6e:")
        lines.append(f"  \u6a21\u5f0f: {'\u7eb8\u76d8' if cfg.get('mode') == 'paper' else '\u5b9e\u76d8'}")
        lines.append(f"  \u5355\u7b14: {cfg.get('max_position_sol', 0.5)} SOL")
        lines.append(f"  \u6b62\u76c8: +{cfg.get('take_profit_pct', 50)}%")
        lines.append(f"  \u6b62\u635f: {cfg.get('stop_loss_pct', -20)}%")
        lines.append(f"  \u65f6\u95f4\u6b62\u635f: {cfg.get('time_stop_hours', 24)}h")
        return "\n".join(lines)

    # Overview
    status_emoji = "\U0001f7e2" if s["total_pnl_sol"] >= 0 else "\U0001f534"
    grad_emoji = "\u2705" if s["ready_for_live"] else "\u23f3"

    lines.extend([
        f"{status_emoji} \u603b\u6536\u76ca: {s['total_pnl_sol']:+.4f} SOL",
        f"\U0001f4c8 \u80dc\u7387: {s['win_rate']:.1f}% ({s['wins']}W / {s['losses']}L)",
        f"\U0001f4c9 \u5e73\u5747PnL: {s['avg_pnl_pct']:+.2f}%",
        f"\U0001f3c6 \u6700\u4f73: {s['best_trade_pct']:+.2f}% | \u6700\u5dee: {s['worst_trade_pct']:+.2f}%",
        f"\u23f1 \u5e73\u5747\u6301\u4ed3: {s['avg_hold_hours']:.1f}h",
        f"\U0001f4c9 \u6700\u5927\u56de\u64a4: {s['max_drawdown_pct']:.2f}%",
        f"\U0001f513 \u5f53\u524d\u6301\u4ed3: {s['open_count']}",
        "",
    ])

    # By close reason
    if s["by_reason"]:
        lines.append("\U0001f4cb \u5e73\u4ed3\u539f\u56e0:")
        for reason, data in s["by_reason"].items():
            reason_cn = {
                "take_profit": "\u6b62\u76c8",
                "stop_loss": "\u6b62\u635f",
                "time_stop": "\u65f6\u95f4\u6b62\u635f",
                "manual": "\u624b\u52a8",
            }.get(reason, reason)
            wr = (data["wins"] / data["count"] * 100) if data["count"] > 0 else 0
            lines.append(f"  {reason_cn}: {data['count']}\u6b21 | \u80dc\u7387{wr:.0f}% | PnL: {data['total_pnl']:.4f} SOL")
        lines.append("")

    # Graduation status
    lines.extend([
        f"{grad_emoji} \u6bd5\u4e1a\u8fdb\u5ea6: {s['graduation_progress']}",
    ])
    if s["ready_for_live"]:
        lines.append("\U0001f393 \u7b56\u7565\u5df2\u8fbe\u6807\uff01\u53ef\u4ee5\u8003\u8651\u5207\u6362\u5230\u5b9e\u76d8\u6a21\u5f0f")
    else:
        lines.append("\U0001f4dd \u7ee7\u7eed\u79ef\u7d2f\u4ea4\u6613\u6570\u636e...")

    # Open positions
    trades = _load_trades()
    open_trades = [t for t in trades if t.get("status") == "open"]
    if open_trades:
        lines.extend(["", "\U0001f4c2 \u5f53\u524d\u6301\u4ed3:"])
        for t in open_trades:
            entry = t.get("entry_price", 0)
            age_h = (time.time() - t.get("entry_time", time.time())) / 3600
            # Show last known PnL from price_history
            history = t.get("price_history", [])
            if history:
                last_pnl = history[-1].get("pnl_pct", 0)
                emoji = "\U0001f7e2" if last_pnl > 0 else "\U0001f534"
                lines.append(f"  {emoji} {t.get('symbol', '?')}: {last_pnl:+.1f}% | {age_h:.1f}h | ${entry:.8f}")
            else:
                lines.append(f"  \u23f3 {t.get('symbol', '?')}: \u7b49\u5f85\u4ef7\u683c\u66f4\u65b0 | {age_h:.1f}h")

    result = "\n".join(lines)
    if len(result) > 4000:
        result = result[:3950] + "\n\n... (截断，内容过长)"
    return result


# ─── Signal Integration ───

async def on_signal_detected(tokens: list, send_func=None) -> list:
    """Called when onchain filter or alpha engine finds tokens.
    Automatically opens paper trades for qualifying tokens.
    Returns list of opened trades.
    """
    cfg = _load_config()
    if not cfg.get("enabled", True):
        return []

    opened = []
    for token in tokens:
        trade = open_paper_trade(token, cfg)
        if trade:
            opened.append(trade)
            if send_func:
                msg = (
                    f"\U0001f4dd Paper Trade \u5f00\u4ed3\n\n"
                    f"Token: {trade['symbol']} ({trade['name']})\n"
                    f"Chain: {trade['chain']}\n"
                    f"\u5165\u573a\u4ef7: ${trade['entry_price']:.8f}\n"
                    f"\u4ed3\u4f4d: {trade['position_sol']} SOL (~{trade['position_tokens']:.0f} tokens)\n"
                    f"MCap: ${trade['entry_mcap']:,.0f}\n"
                    f"Liq: ${trade['entry_liq']:,.0f}\n"
                    f"\u6b62\u76c8: +{cfg.get('take_profit_pct', 50)}% | \u6b62\u635f: {cfg.get('stop_loss_pct', -20)}%\n"
                    f"\n{trade.get('pair_url', '')}"
                )
                try:
                    await send_func(msg)
                except Exception:
                    pass

    return opened


# ─── Background Service ───

class PaperTrader:
    """Background service that monitors open paper trades."""

    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._send = None
        self.running = False

    async def start(self, send_func=None):
        if self.running:
            return
        self._send = send_func
        self.running = True
        self._task = asyncio.create_task(self._loop())
        self._task.add_done_callback(self._on_done)
        logger.info("PaperTrader started")

    async def stop(self):
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("PaperTrader stopped")

    def _on_done(self, task: asyncio.Task):
        self.running = False
        try:
            exc = task.exception()
            if exc:
                logger.error(f"PaperTrader crashed: {exc}")
        except asyncio.CancelledError:
            pass

    async def _loop(self):
        cfg = _load_config()
        interval = cfg.get("check_interval", 60)

        while self.running:
            try:
                result = await check_and_update_trades(self._send)
                if result.get("closed", 0) > 0:
                    logger.info(f"PaperTrader: closed {result['closed']} trades")
            except Exception as e:
                logger.error(f"PaperTrader loop error: {e}")

            await asyncio.sleep(interval)


# Module-level singleton
paper_trader = PaperTrader()
