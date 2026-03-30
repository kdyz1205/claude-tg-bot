"""
proactive_monitor.py — Autonomous crypto market monitoring.

Monitors BTC/ETH/SOL via OKX public API, no auth required.
Alerts:
  - Price breaks above 24h high or below 24h low
  - Price changes > 3% within the last hour
Runs as async background tasks inside the bot's event loop.
Controlled via /market on|off|status command.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Callable, Coroutine

logger = logging.getLogger(__name__)

SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
CHECK_INTERVAL = 300          # 5 minutes between checks
PRICE_CHANGE_THRESHOLD = 0.03  # 3% in 1 hour triggers alert
# Cooldown per (symbol, alert_type) to avoid spam (seconds)
ALERT_COOLDOWN = 1800  # 30 minutes


class MarketMonitor:
    """Background market monitor: breakout + momentum alerts."""

    def __init__(self, send_func: Callable[..., Coroutine] | None = None):
        self._send = send_func
        self._running = False
        self._task: asyncio.Task | None = None
        # Track last alert time per (symbol, alert_type) for cooldown
        self._last_alert: dict[str, float] = {}
        # Cache previous prices for 1h change calculation
        # {symbol: [(timestamp, price), ...]} — rolling window
        self._price_history: dict[str, list[tuple[float, float]]] = {s: [] for s in SYMBOLS}

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        if self._running:
            logger.warning("MarketMonitor already running")
            return
        self._running = True
        self._task = asyncio.create_task(self._monitor_loop(), name="market_monitor")
        self._task.add_done_callback(self._on_done)
        logger.info("MarketMonitor started (interval=%ds)", CHECK_INTERVAL)

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("MarketMonitor stopped")

    def _on_done(self, task: asyncio.Task) -> None:
        if not task.cancelled():
            try:
                task.result()
            except Exception as e:
                logger.error("MarketMonitor loop crashed: %s", e, exc_info=True)

    def status(self) -> str:
        state = "running" if self._running else "stopped"
        return f"MarketMonitor: {state} | symbols: {', '.join(SYMBOLS)} | interval: {CHECK_INTERVAL}s"

    # ------------------------------------------------------------------ #
    # Main loop                                                            #
    # ------------------------------------------------------------------ #

    async def _monitor_loop(self) -> None:
        logger.info("MarketMonitor loop starting")
        # Stagger first check by 15s to let bot fully initialise
        await asyncio.sleep(15)
        while self._running:
            try:
                await self._check_all()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("MarketMonitor check error: %s", e)
            try:
                await asyncio.sleep(CHECK_INTERVAL)
            except asyncio.CancelledError:
                break

    async def _check_all(self) -> None:
        """Fetch tickers for all symbols and evaluate alert conditions."""
        try:
            import httpx
        except ImportError:
            logger.error("httpx not installed — market monitor cannot fetch prices")
            return

        now = time.time()
        async with httpx.AsyncClient(timeout=10.0) as client:
            for symbol in SYMBOLS:
                try:
                    await self._check_symbol(client, symbol, now)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("MarketMonitor: error checking %s: %s", symbol, e)
                # Small delay between requests to be polite to the API
                await asyncio.sleep(0.5)

    async def _check_symbol(self, client, symbol: str, now: float) -> None:
        """Fetch OKX ticker for one symbol, update history, fire alerts."""
        url = f"https://www.okx.com/api/v5/market/ticker?instId={symbol}"
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

        if data.get("code") != "0" or not data.get("data"):
            logger.warning("OKX API bad response for %s: %s", symbol, data.get("msg", "?"))
            return

        ticker = data["data"][0]
        last  = float(ticker.get("last", 0))    # current price
        high24 = float(ticker.get("high24h", 0)) # 24h high
        low24  = float(ticker.get("low24h", 0))  # 24h low
        if not last:
            return

        base = symbol.split("-")[0]  # BTC, ETH, SOL

        # --- Breakout alerts ---
        if last >= high24:
            await self._maybe_alert(
                symbol, "breakout_high",
                f"🚀 {base} 突破24h高点！\n"
                f"当前价: ${last:,.2f}\n"
                f"24h高点: ${high24:,.2f}",
                now,
            )
        elif last <= low24:
            await self._maybe_alert(
                symbol, "breakout_low",
                f"📉 {base} 跌破24h低点！\n"
                f"当前价: ${last:,.2f}\n"
                f"24h低点: ${low24:,.2f}",
                now,
            )

        # --- Update 1h price history ---
        history = self._price_history[symbol]
        history.append((now, last))
        # Keep only entries within the last 70 minutes
        cutoff = now - 4200
        self._price_history[symbol] = [(t, p) for t, p in history if t >= cutoff]

        # --- 1h momentum alert ---
        one_hour_ago = now - 3600
        old_entries = [(t, p) for t, p in self._price_history[symbol] if t <= one_hour_ago]
        if old_entries:
            # Use the most recent entry that is at least 1h old
            _, old_price = max(old_entries, key=lambda x: x[0])
            if old_price <= 0:
                return
            change = (last - old_price) / old_price
            if abs(change) >= PRICE_CHANGE_THRESHOLD:
                direction = "涨" if change > 0 else "跌"
                emoji = "📈" if change > 0 else "📉"
                await self._maybe_alert(
                    symbol, "momentum_1h",
                    f"{emoji} {base} 1小时{direction}幅超3%！\n"
                    f"1h前: ${old_price:,.2f}\n"
                    f"当前: ${last:,.2f}\n"
                    f"变化: {change:+.2%}",
                    now,
                )

    # ------------------------------------------------------------------ #
    # Alert helpers                                                        #
    # ------------------------------------------------------------------ #

    async def _maybe_alert(self, symbol: str, alert_type: str, message: str, now: float) -> None:
        """Send alert only if cooldown has elapsed for this (symbol, type) pair."""
        key = f"{symbol}:{alert_type}"
        last_sent = self._last_alert.get(key, 0.0)
        if now - last_sent < ALERT_COOLDOWN:
            return
        self._last_alert[key] = now
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        full_msg = f"[市场预警 {ts}]\n{message}"
        await self._emit(full_msg)
        # Record signal for performance tracking
        try:
            import profit_tracker as _pt
            # Determine direction from alert type
            direction = "long" if alert_type in ("breakout_high", "momentum_1h_up") else "short"
            if alert_type == "momentum_1h":
                direction = "long" if "涨" in message else "short"
            # Extract current price from cached history
            history = self._price_history.get(symbol, [])
            entry_price = history[-1][1] if history else 0.0
            if entry_price > 0:
                _pt.record_signal(symbol, direction, alert_type, entry_price)
        except Exception:
            pass

    async def _emit(self, text: str) -> None:
        if self._send is None:
            logger.info("MarketMonitor alert (no send_func): %s", text[:80])
            return
        try:
            await self._send(text)
        except Exception as e:
            logger.error("MarketMonitor: failed to send alert: %s", e)


# Module-level singleton
market_monitor = MarketMonitor()
