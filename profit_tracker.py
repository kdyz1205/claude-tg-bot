"""
profit_tracker.py — Signal performance tracker.

Records signals from proactive_monitor, tracks price performance at
1h / 4h / 24h after signal, computes win rate and returns.

Data files:
  _signal_history.json    — all signals with performance data
  _performance_stats.json — aggregated statistics cache
"""

import json
import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Callable, Coroutine

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIGNAL_HISTORY_FILE = os.path.join(BASE_DIR, "_signal_history.json")
PERFORMANCE_STATS_FILE = os.path.join(BASE_DIR, "_performance_stats.json")
CHART_FILE = os.path.join(BASE_DIR, "_performance_chart.png")
RISK_EVENTS_FILE = os.path.join(BASE_DIR, "_risk_events.jsonl")

# Hours after signal at which we sample price
CHECK_INTERVALS_H = [1, 4, 24]
# Maximum signals to keep
MAX_SIGNALS = 500
TG_MSG_LIMIT = 4096


# ── File helpers ──────────────────────────────────────────────────────────────

def _load_signals() -> list:
    if not os.path.exists(SIGNAL_HISTORY_FILE):
        return []
    try:
        with open(SIGNAL_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save_signals(signals: list) -> None:
    try:
        _tmp = SIGNAL_HISTORY_FILE + ".tmp"
        with open(_tmp, "w", encoding="utf-8") as f:
            json.dump(signals, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(_tmp, SIGNAL_HISTORY_FILE)
    except Exception as e:
        logger.warning("profit_tracker: _save_signals failed: %s", e)


# ── Public API ────────────────────────────────────────────────────────────────

def record_arb_signal(pair: str, buy_exchange: str, sell_exchange: str,
                      spread_pct: float, buy_price: float) -> str:
    """Record a cross-exchange arbitrage signal for win-rate tracking.

    The signal is treated as a 'long' on the buy side: a win means the
    buy-exchange price rose after entry (confirming the arb direction).
    signal_type is set to 'arb_spread' so stats appear in by_type breakdown.
    """
    sig_id = record_signal(
        symbol=pair,
        direction="long",
        signal_type="arb_spread",
        entry_price=buy_price,
    )
    # Annotate the stored record with arb-specific metadata
    signals = _load_signals()
    for s in signals:
        if s.get("id") == sig_id:
            s["arb_buy_exchange"] = buy_exchange
            s["arb_sell_exchange"] = sell_exchange
            s["arb_spread_pct"] = round(spread_pct, 4)
            break
    _save_signals(signals[-MAX_SIGNALS:])
    return sig_id


def record_risk_kill_event(kind: str, payload: dict) -> None:
    """Append a risk / hard-kill event for audit (independent of signal PnL tracking)."""
    row = {"ts": time.time(), "kind": kind, **payload}
    try:
        with open(RISK_EVENTS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
    except Exception as e:
        logger.warning("profit_tracker: risk event log failed: %s", e)


def compute_arb_stats() -> dict:
    """Return win-rate stats for arb_spread signals only."""
    signals = _load_signals()
    arb_sigs = [s for s in signals if s.get("signal_type") == "arb_spread"]
    stats = compute_stats(arb_sigs)
    stats["signal_label"] = "套利信号 (arb_spread)"
    return stats


def format_arb_stats(stats: dict = None) -> str:
    """Format arb-specific win-rate stats for Telegram display."""
    if stats is None:
        stats = compute_arb_stats()
    lines = ["📊 套利信号胜率统计", ""]
    total = stats.get("total_signals", 0)
    resolved = stats.get("resolved", 0)
    pending = stats.get("pending", 0)
    if total == 0:
        lines.append("暂无已记录套利信号。")
        lines.append("每次检测到套利机会时自动记录，24h后统计胜率。")
        return "\n".join(lines)
    lines.append(f"总记录: {total}  已结算: {resolved}  待定: {pending}")
    if stats.get("win_rate") is not None:
        lines.append(f"胜率: **{stats['win_rate']}%**  均收益: {stats.get('avg_pnl_pct', 0):+.2f}%")
    wk = stats.get("this_week", {})
    if wk.get("total", 0) > 0:
        lines.append(f"本周: {wk['total']}信号  胜率{wk.get('win_rate', 'N/A')}%  均{wk.get('avg_pnl', 0):+.2f}%")
    best = stats.get("best_signal")
    if best:
        ts_b = datetime.fromtimestamp(best.get("timestamp", 0)).strftime("%m-%d %H:%M")
        lines.append(f"最佳: {best.get('symbol', '?')} 入场${best.get('entry_price', 0):.4f} 收益{best.get('final_pnl_pct', 0):+.2f}% [{ts_b}]")
    result = "\n".join(lines)
    return result[:TG_MSG_LIMIT] if len(result) > TG_MSG_LIMIT else result


def record_signal(symbol: str, direction: str, signal_type: str, entry_price: float) -> str:
    """Record a new signal. Returns signal ID.

    direction: "long" (profit when price rises) or "short" (profit when falls)
    signal_type: e.g. "breakout_high", "breakout_low", "momentum_1h"
    """
    signals = _load_signals()
    sig_id = f"{symbol}_{signal_type}_{int(time.time())}"
    signals.append({
        "id": sig_id,
        "symbol": symbol,
        "direction": direction,
        "signal_type": signal_type,
        "entry_price": entry_price,
        "timestamp": time.time(),
        "check_times": {str(h): time.time() + h * 3600 for h in CHECK_INTERVALS_H},
        "checked_prices": {},
        "pnl_pct": {},
        "status": "pending",
    })
    _save_signals(signals[-MAX_SIGNALS:])
    return sig_id


async def update_pending_signals() -> None:
    """Fetch prices and update pnl for pending signals whose check time has passed."""
    signals = _load_signals()
    now = time.time()
    changed = False

    for sig in signals:
        if sig.get("status") != "pending":
            continue

        for h_str, check_time in sig.get("check_times", {}).items():
            if h_str in sig.get("checked_prices", {}):
                continue
            if now < check_time:
                continue

            price = await _fetch_price(sig.get("symbol", ""))
            if price is None:
                continue

            entry = sig.get("entry_price", 0)
            direction = sig.get("direction", "long")
            if not entry or entry <= 0:
                continue
            pnl_pct = (price - entry) / entry * 100
            if direction == "short":
                pnl_pct = -pnl_pct

            sig.setdefault("checked_prices", {})[h_str] = price
            sig.setdefault("pnl_pct", {})[h_str] = round(pnl_pct, 3)
            changed = True

        # Finalise when all intervals are checked
        if len(sig.get("checked_prices", {})) == len(CHECK_INTERVALS_H):
            # Use 24h result as final verdict
            pnl_dict = sig.get("pnl_pct", {})
            if "24" in pnl_dict:
                final_pnl = pnl_dict["24"]
            elif pnl_dict:
                try:
                    final_pnl = pnl_dict[max(pnl_dict.keys(), key=int)]
                except (ValueError, KeyError):
                    final_pnl = 0
            else:
                final_pnl = 0
            sig["final_pnl_pct"] = round(final_pnl, 3)
            sig["status"] = "win" if final_pnl > 0 else "loss"
            changed = True

    if changed:
        _save_signals(signals)


def compute_stats(signals: list = None) -> dict:
    """Compute aggregated performance statistics."""
    if signals is None:
        signals = _load_signals()

    resolved = [s for s in signals if s.get("status") in ("win", "loss")]
    pending = [s for s in signals if s.get("status") == "pending"]

    base = {
        "total_signals": len(signals),
        "resolved": len(resolved),
        "pending": len(pending),
        "win_rate": None,
        "avg_pnl_pct": None,
        "best_signal": None,
        "worst_signal": None,
        "yesterday": {},
        "this_week": {},
        "by_type": {},
    }

    if not resolved:
        return base

    wins = [s for s in resolved if s.get("status") == "win"]
    pnls = [s.get("final_pnl_pct", 0) for s in resolved]

    base["win_rate"] = round(len(wins) / len(resolved) * 100, 1) if resolved else 0
    base["avg_pnl_pct"] = round(sum(pnls) / len(pnls), 3) if pnls else 0
    base["best_signal"] = max(resolved, key=lambda s: s.get("final_pnl_pct", 0))
    base["worst_signal"] = min(resolved, key=lambda s: s.get("final_pnl_pct", 0))

    # Yesterday window
    yd_end = time.time() - 86400
    yd_start = yd_end - 86400
    yesterday = [s for s in resolved if yd_start <= s.get("timestamp", 0) <= yd_end]
    yd_wins = [s for s in yesterday if s.get("status") == "win"]
    yd_pnls = [s.get("final_pnl_pct", 0) for s in yesterday]
    base["yesterday"] = {
        "total": len(yesterday),
        "wins": len(yd_wins),
        "win_rate": round(len(yd_wins) / len(yesterday) * 100, 1) if yesterday else None,
        "avg_pnl": round(sum(yd_pnls) / len(yd_pnls), 3) if yd_pnls else None,
    }

    # This week window
    week_start = time.time() - 86400 * 7
    this_week = [s for s in resolved if s.get("timestamp", 0) >= week_start]
    wk_wins = [s for s in this_week if s.get("status") == "win"]
    wk_pnls = [s.get("final_pnl_pct", 0) for s in this_week]
    base["this_week"] = {
        "total": len(this_week),
        "wins": len(wk_wins),
        "win_rate": round(len(wk_wins) / len(this_week) * 100, 1) if this_week else None,
        "avg_pnl": round(sum(wk_pnls) / len(wk_pnls), 3) if wk_pnls else None,
    }

    # By signal type
    by_type: dict = {}
    for s in resolved:
        st = s.get("signal_type", "unknown")
        by_type.setdefault(st, {"wins": 0, "total": 0, "pnls": []})
        by_type[st]["total"] += 1
        by_type[st]["pnls"].append(s.get("final_pnl_pct", 0))
        if s.get("status") == "win":
            by_type[st]["wins"] += 1
    # Remove raw pnl lists before returning (keep summary)
    for st in by_type:
        raw = by_type[st].pop("pnls", [])
        by_type[st]["avg_pnl"] = round(sum(raw) / len(raw), 3) if raw else 0
    base["by_type"] = by_type

    return base


def save_performance_stats(stats: dict) -> None:
    """Persist stats to _performance_stats.json."""
    serialisable = json.loads(json.dumps(stats, default=str))
    _tmp = PERFORMANCE_STATS_FILE + ".tmp"
    with open(_tmp, "w", encoding="utf-8") as f:
        json.dump(serialisable, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(_tmp, PERFORMANCE_STATS_FILE)


def format_report(stats: dict, title: str = "信号表现报告") -> str:
    """Convert stats dict into a human-readable Telegram message."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [f"📊 {title}", f"生成时间: {ts}", ""]

    if stats["resolved"] == 0:
        lines.append("暂无已完成信号数据。")
        lines.append(f"待追踪信号: {stats['pending']} 个")
        lines.append("\n信号由市场预警自动记录（breakout / momentum）。")
        return "\n".join(lines)

    lines.append("📈 总体表现")
    lines.append(f"  总信号: {stats['total_signals']}  (已完成: {stats['resolved']}, 待定: {stats['pending']})")
    lines.append(f"  胜率:   {stats['win_rate']}%")
    lines.append(f"  均收益: {stats['avg_pnl_pct']:+.2f}%")
    lines.append("")

    yd = stats.get("yesterday", {})
    if yd.get("total", 0) > 0:
        wr = yd.get("win_rate")
        avg = yd.get("avg_pnl")
        lines.append("📅 昨日表现")
        lines.append(f"  信号: {yd['total']}  胜率: {wr}%  均收益: {avg:+.2f}%" if (wr is not None and avg is not None) else f"  信号: {yd['total']}")
        lines.append("")

    wk = stats.get("this_week", {})
    if wk.get("total", 0) > 0:
        wr = wk.get("win_rate")
        avg = wk.get("avg_pnl")
        lines.append("📆 本周累计")
        lines.append(f"  信号: {wk['total']}  胜率: {wr}%  均收益: {avg:+.2f}%" if (wr is not None and avg is not None) else f"  信号: {wk['total']}")
        lines.append("")

    best = stats.get("best_signal")
    worst = stats.get("worst_signal")
    if best:
        ts_b = datetime.fromtimestamp(best.get("timestamp", 0)).strftime("%m-%d %H:%M")
        lines.append(f"🏆 最佳  {best['symbol']} {best.get('signal_type','')} ({ts_b})")
        lines.append(f"       入场: ${best.get('entry_price', 0):.2f}  收益: {best.get('final_pnl_pct', 0):+.2f}%")
    if worst:
        ts_w = datetime.fromtimestamp(worst.get("timestamp", 0)).strftime("%m-%d %H:%M")
        lines.append(f"📉 最差  {worst['symbol']} {worst.get('signal_type','')} ({ts_w})")
        lines.append(f"       入场: ${worst.get('entry_price', 0):.2f}  收益: {worst.get('final_pnl_pct', 0):+.2f}%")
    if best or worst:
        lines.append("")

    by_type = stats.get("by_type", {})
    if by_type:
        lines.append("🔍 按类型")
        for st, d in sorted(by_type.items()):
            wr = round(d["wins"] / d["total"] * 100, 1) if d["total"] else 0
            lines.append(f"  {st}: {d['wins']}/{d['total']} ({wr}%胜率  均{d['avg_pnl']:+.2f}%)")

    result = "\n".join(lines)
    return result[:TG_MSG_LIMIT] if len(result) > TG_MSG_LIMIT else result


def generate_chart() -> Optional[str]:
    """Generate win-rate trend + cumulative PnL chart. Returns PNG path or None."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        logger.warning("matplotlib not installed — skipping chart generation")
        return None

    signals = _load_signals()
    resolved = sorted(
        [s for s in signals if s.get("status") in ("win", "loss")],
        key=lambda s: s.get("timestamp", 0),
    )

    if len(resolved) < 3:
        return None

    window = min(10, len(resolved))

    # Rolling win rate
    win_rates = []
    dates_wr = []
    for i in range(window - 1, len(resolved)):
        batch = resolved[i - window + 1: i + 1]
        win_rates.append(sum(1 for s in batch if s.get("status") == "win") / window * 100)
        dates_wr.append(datetime.fromtimestamp(resolved[i].get("timestamp", 0)))

    # Cumulative PnL
    pnls = [s.get("final_pnl_pct", 0) for s in resolved]
    cum_pnl = []
    total = 0.0
    for p in pnls:
        total += p
        cum_pnl.append(total)
    dates_pnl = [datetime.fromtimestamp(s.get("timestamp", 0)) for s in resolved]

    bg_dark = "#1a1a2e"
    bg_mid = "#16213e"

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8))
    fig.patch.set_facecolor(bg_dark)

    for ax in (ax1, ax2):
        ax.set_facecolor(bg_mid)
        ax.tick_params(colors="white")
        for spine in ax.spines.values():
            spine.set_edgecolor("#444")

    # Top: rolling win rate
    ax1.plot(dates_wr, win_rates, color="#00d4ff", linewidth=2)
    ax1.axhline(50, color="#ff6b6b", linestyle="--", alpha=0.7, label="50%")
    ax1.fill_between(dates_wr, win_rates, 50,
                     where=[w > 50 for w in win_rates], alpha=0.18, color="#00d4ff")
    ax1.fill_between(dates_wr, win_rates, 50,
                     where=[w <= 50 for w in win_rates], alpha=0.18, color="#ff6b6b")
    ax1.set_ylim(0, 100)
    ax1.set_ylabel("胜率 %", color="white")
    ax1.set_title(f"滚动胜率 (近{window}信号)", color="white")
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))
    ax1.legend(facecolor="#333", labelcolor="white", framealpha=0.7)

    # Bottom: cumulative PnL
    ax2.plot(dates_pnl, cum_pnl, color="#ffd700", linewidth=2)
    ax2.axhline(0, color="white", linestyle="-", alpha=0.3)
    ax2.fill_between(dates_pnl, cum_pnl, 0,
                     where=[c > 0 for c in cum_pnl], alpha=0.18, color="#ffd700")
    ax2.fill_between(dates_pnl, cum_pnl, 0,
                     where=[c <= 0 for c in cum_pnl], alpha=0.18, color="#ff6b6b")
    ax2.set_ylabel("累计收益 %", color="white")
    ax2.set_title("累计收益曲线", color="white")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d"))

    plt.tight_layout(pad=2.0)
    try:
        plt.savefig(CHART_FILE, dpi=120, bbox_inches="tight", facecolor=fig.get_facecolor())
        return CHART_FILE
    finally:
        plt.close(fig)


# ── Async price helper ────────────────────────────────────────────────────────

async def _fetch_price(symbol: str) -> Optional[float]:
    try:
        import httpx
        # Normalize symbol for OKX: "BTCUSDT" → "BTC-USDT"
        if symbol and "-" not in symbol:
            # Try common quote currencies
            for quote in ("USDT", "USDC", "USD", "BTC", "ETH"):
                if symbol.upper().endswith(quote):
                    symbol = symbol[:-len(quote)] + "-" + quote
                    break
        url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            data = resp.json()
            if data.get("code") == "0" and data.get("data"):
                val = data["data"][0].get("last")
                return float(val) if val is not None else None
    except Exception as e:
        logger.warning("profit_tracker: failed to fetch %s: %s", symbol, e)
    return None


# ── Background service class ──────────────────────────────────────────────────

class ProfitTracker:
    """Background service: updates pending signals every 10 min, sends daily report at 09:00."""

    def __init__(self):
        self._send: Optional[Callable[..., Coroutine]] = None
        self._send_photo: Optional[Callable[..., Coroutine]] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="profit_tracker")
        self._task.add_done_callback(self._on_done)
        logger.info("ProfitTracker started")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _on_done(self, task: asyncio.Task) -> None:
        if not task.cancelled():
            try:
                task.result()
            except Exception as e:
                logger.error("ProfitTracker loop crashed: %s", e, exc_info=True)

    async def _loop(self) -> None:
        await asyncio.sleep(30)
        next_daily = self._next_daily_ts(9, 0)

        while self._running:
            try:
                await update_pending_signals()
                stats = compute_stats()
                save_performance_stats(stats)

                if time.time() >= next_daily:
                    await self._send_daily_report()
                    next_daily = self._next_daily_ts(9, 0)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("ProfitTracker loop error: %s", e)

            try:
                await asyncio.sleep(600)  # 10 minutes
            except asyncio.CancelledError:
                break

    @staticmethod
    def _next_daily_ts(hour: int, minute: int) -> float:
        now = datetime.now()
        target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        return target.timestamp()

    async def _send_daily_report(self) -> None:
        if self._send is None:
            return
        try:
            stats = compute_stats()
            save_performance_stats(stats)
            text = format_report(stats, title="每日信号报告 (09:00)")
            await self._send(text)

            chart_path = generate_chart()
            if chart_path and os.path.exists(chart_path) and self._send_photo:
                await self._send_photo(chart_path)
        except Exception as e:
            logger.error("ProfitTracker daily report failed: %s", e)

    async def get_report_and_chart(self) -> tuple:
        """Generate on-demand report text + chart path. Returns (text, chart_path_or_None)."""
        await update_pending_signals()
        stats = compute_stats()
        save_performance_stats(stats)
        text = format_report(stats)
        chart_path = generate_chart()
        return text, chart_path


# Module-level singleton
profit_tracker = ProfitTracker()
