"""
arbitrage_engine.py — Cross-exchange arbitrage monitor.

WebSocket feeds (OKX + Bybit + Binance) for real-time prices on core pairs.
REST scan every 5 minutes for full Top-50 market coverage with volume filter.
Fee + slippage adjusted net profit — only positive-EV signals emitted.

Signal flow:
  1. WS price tick → _evaluate_arb → if spread > MIN_SPREAD_PCT
                                       AND volume > MIN_VOLUME_USDT
                                       AND net_profit > 0 → record + alert
  2. REST scanner (5 min) → top-50 by volume → same threshold checks → record + alert
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, date
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# ── Static symbol list for WebSocket streams ──────────────────────────────────
SYMBOLS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "BNB-USDT", "XRP-USDT",
    "DOGE-USDT", "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT",
    "MATIC-USDT", "UNI-USDT", "LTC-USDT", "ATOM-USDT", "NEAR-USDT",
]

# ── Thresholds & cost model ───────────────────────────────────────────────────
MIN_SPREAD_PCT      = 0.50        # minimum gross spread % to consider
MIN_VOLUME_USDT     = 5_000_000   # 24h quote volume filter (5M USDT)
FEE_RATE_PCT        = 0.10        # taker fee per leg (%)
SLIPPAGE_PCT        = 0.05        # estimated slippage per leg (%)
ROUND_TRIP_COST_PCT = (FEE_RATE_PCT + SLIPPAGE_PCT) * 2   # 0.30% total

# ── Operational settings ──────────────────────────────────────────────────────
TRADE_SIZE_USDT  = 1_000.0        # notional for profit estimate
SIGNAL_MAX_AGE   = 60             # seconds before WS cached signal expires
RECONNECT_DELAY  = 5              # seconds between WS reconnect attempts
SCAN_INTERVAL    = 60             # REST scan period (60 seconds) — Phase3 Task26
MIN_NET_PROFIT_PCT = 0.30         # minimum net profit % to alert (after fees+slippage)
ALERT_COOLDOWN   = 600            # 10-min cooldown per pair+exchange combo
HISTORY_FILE     = ".arbitrage_history.json"
ARB_SIGNALS_FILE = ".arbitrage_log.jsonl"        # JSONL log of all arb signals (Phase3 Task26)
MAX_HISTORY_DAYS = 30             # days of history to retain
SMART_WALLETS_FILE = ".smart_wallets.json"
MAX_WALLETS_ARB_SIGNALS = 50      # max arb signals kept in .smart_wallets.json


# ── Symbol format helpers ─────────────────────────────────────────────────────

def _to_bybit(pair: str) -> str:
    """BTC-USDT → BTCUSDT"""
    return pair.replace("-", "")


def _to_binance(pair: str) -> str:
    """BTC-USDT → btcusdt"""
    return pair.replace("-", "").lower()


# Pre-build reverse maps
_BYBIT_TO_CANONICAL   = {_to_bybit(s): s   for s in SYMBOLS}
_BINANCE_TO_CANONICAL = {_to_binance(s): s  for s in SYMBOLS}


# ── Main engine ───────────────────────────────────────────────────────────────

class ArbEngine:
    """
    Background engine: real-time WS price monitoring + periodic REST market scan.
    Emits Telegram alerts for arbitrage windows with positive net profit.
    """

    def __init__(self, send_func=None):
        self._send = send_func
        self._prices: dict[str, dict[str, float]] = {}    # pair → {exchange: price}
        self._volumes: dict[str, float] = {}              # pair → 24h USDT volume
        self._running = False
        self._tasks: list[asyncio.Task] = []
        self._signals: list[dict] = []                    # live WS signals cache
        self._alert_cooldown: dict[str, float] = {}       # cooldown_key → last_ts
        self._history: list[dict] = []                    # all recorded opportunities
        self._scan_stats = {"scanned": 0, "qualified": 0, "alerted": 0}
        self._load_history()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._okx_ws(),           name="arb_okx"),
            asyncio.create_task(self._bybit_ws(),         name="arb_bybit"),
            asyncio.create_task(self._binance_ws(),       name="arb_binance"),
            asyncio.create_task(self._periodic_scanner(), name="arb_scanner"),
        ]
        for t in self._tasks:
            t.add_done_callback(self._on_task_done)
        logger.info("ArbEngine started (OKX + Bybit + Binance WS + REST scanner)")

    async def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        logger.info("ArbEngine stopped")

    def _on_task_done(self, task: asyncio.Task) -> None:
        if not task.cancelled():
            try:
                task.result()
            except Exception as e:
                logger.error("ArbEngine task %s crashed: %s", task.get_name(), e)

    @property
    def running(self) -> bool:
        return self._running

    # ── Price update & spread calculation (WS path) ───────────────────────────

    def _update_price(self, pair: str, exchange: str, price: float) -> None:
        if pair not in self._prices:
            self._prices[pair] = {}
        self._prices[pair][exchange] = price
        self._evaluate_arb(pair)

    def _evaluate_arb(self, pair: str) -> None:
        """Check all exchange-pair combos; cache signals and alert on new windows."""
        prices = self._prices.get(pair, {})
        if len(prices) < 2:
            return

        # Volume gate — skipped until REST scan has populated cache
        vol = self._volumes.get(pair, 0)
        if vol > 0 and vol < MIN_VOLUME_USDT:
            return

        exchanges = list(prices.items())
        new_signals: list[dict] = []
        now = time.time()

        for i in range(len(exchanges)):
            for j in range(i + 1, len(exchanges)):
                ex_a, price_a = exchanges[i]
                ex_b, price_b = exchanges[j]

                if price_a <= 0 or price_b <= 0:
                    continue

                if price_a < price_b:
                    buy_ex, buy_price = ex_a, price_a
                    sell_ex, sell_price = ex_b, price_b
                else:
                    buy_ex, buy_price = ex_b, price_b
                    sell_ex, sell_price = ex_a, price_a

                if buy_price <= 0:
                    continue
                spread_pct = (sell_price - buy_price) / buy_price * 100
                if spread_pct < MIN_SPREAD_PCT:
                    continue

                net_profit_pct = spread_pct - ROUND_TRIP_COST_PCT
                if net_profit_pct < MIN_NET_PROFIT_PCT:
                    continue

                quantity = TRADE_SIZE_USDT / buy_price
                net_profit_usdt = (
                    quantity * (sell_price - buy_price)
                    - TRADE_SIZE_USDT * ROUND_TRIP_COST_PCT / 100
                )

                new_signals.append({
                    "pair": pair,
                    "buy_exchange": buy_ex,
                    "sell_exchange": sell_ex,
                    "buy_price": round(buy_price, 6),
                    "sell_price": round(sell_price, 6),
                    "spread_pct": round(spread_pct, 4),
                    "net_profit_pct": round(net_profit_pct, 4),
                    "net_profit_usdt": round(net_profit_usdt, 4),
                    "volume_24h_usdt": round(vol) if vol > 0 else None,
                    "timestamp": now,
                    "source": "websocket",
                })

                # Alert + record only when cooldown permits
                cooldown_key = f"{pair}:{buy_ex}:{sell_ex}"
                if now - self._alert_cooldown.get(cooldown_key, 0) >= ALERT_COOLDOWN:
                    self._alert_cooldown[cooldown_key] = now
                    self._record_opportunity(new_signals[-1])

        if new_signals:
            self._signals = [s for s in self._signals if s["pair"] != pair]
            self._signals.extend(new_signals)
            self._signals.sort(key=lambda x: x["spread_pct"], reverse=True)
            self._signals = self._signals[:100]

    # ── REST periodic scanner ─────────────────────────────────────────────────

    async def _periodic_scanner(self) -> None:
        """Full market REST scan every SCAN_INTERVAL seconds."""
        await asyncio.sleep(30)   # let WS connections establish first
        while self._running:
            try:
                await self._scan_market_rest()
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("ArbEngine periodic scanner error: %s", e)
            try:
                await asyncio.sleep(SCAN_INTERVAL)
            except asyncio.CancelledError:
                return

    async def _scan_market_rest(self) -> None:
        """
        Fetch Top-50 USDT pairs by 24h volume from OKX, compare prices across
        OKX/Binance/Bybit, emit alerts for positive-EV arbitrage windows.
        """
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            ) as session:
                # 1. OKX: all spot tickers (price + 24h USDT volume)
                okx_tickers = await self._fetch_okx_tickers(session)
                if not okx_tickers:
                    logger.warning("ArbEngine REST scan: OKX returned no tickers")
                    return

                # 2. Volume filter + update cache
                self._volumes.update({p: d["vol_usdt"] for p, d in okx_tickers.items()})
                qualified = {
                    p: d for p, d in okx_tickers.items()
                    if d["vol_usdt"] >= MIN_VOLUME_USDT
                }
                top50 = sorted(
                    qualified.keys(),
                    key=lambda p: qualified[p]["vol_usdt"],
                    reverse=True,
                )[:50]

                self._scan_stats["scanned"] += len(top50)
                if not top50:
                    return

                # 3. Fetch Binance + Bybit prices for top 50
                binance_prices = await self._fetch_binance_prices_batch(session, top50)
                bybit_prices   = await self._fetch_bybit_prices_batch(session, top50)

                # 4. Evaluate spreads and collect alerts
                now = time.time()
                to_record: list[dict] = []

                for pair in top50:
                    okx_price   = okx_tickers[pair]["price"]
                    vol_usdt    = qualified[pair]["vol_usdt"]
                    bnb_price   = binance_prices.get(pair)
                    bybt_price  = bybit_prices.get(pair)

                    combos = []
                    if bnb_price:
                        combos.append(("OKX", okx_price, "Binance", bnb_price))
                    if bybt_price:
                        combos.append(("OKX", okx_price, "Bybit", bybt_price))
                    if bnb_price and bybt_price:
                        combos.append(("Binance", bnb_price, "Bybit", bybt_price))

                    for ex_a, price_a, ex_b, price_b in combos:
                        if price_a <= 0 or price_b <= 0:
                            continue
                        if price_a < price_b:
                            buy_ex, buy_price, sell_ex, sell_price = ex_a, price_a, ex_b, price_b
                        else:
                            buy_ex, buy_price, sell_ex, sell_price = ex_b, price_b, ex_a, price_a

                        if buy_price <= 0:
                            continue
                        spread_pct = (sell_price - buy_price) / buy_price * 100
                        if spread_pct < MIN_SPREAD_PCT:
                            continue

                        self._scan_stats["qualified"] += 1
                        net_profit_pct = spread_pct - ROUND_TRIP_COST_PCT
                        if net_profit_pct < MIN_NET_PROFIT_PCT:
                            continue

                        cooldown_key = f"{pair}:{buy_ex}:{sell_ex}"
                        if now - self._alert_cooldown.get(cooldown_key, 0) < ALERT_COOLDOWN:
                            continue

                        quantity = TRADE_SIZE_USDT / buy_price
                        net_profit_usdt = (
                            quantity * (sell_price - buy_price)
                            - TRADE_SIZE_USDT * ROUND_TRIP_COST_PCT / 100
                        )

                        sig = {
                            "pair": pair,
                            "buy_exchange": buy_ex,
                            "sell_exchange": sell_ex,
                            "buy_price": round(buy_price, 6),
                            "sell_price": round(sell_price, 6),
                            "spread_pct": round(spread_pct, 4),
                            "net_profit_pct": round(net_profit_pct, 4),
                            "net_profit_usdt": round(net_profit_usdt, 4),
                            "volume_24h_usdt": round(vol_usdt),
                            "timestamp": now,
                            "source": "rest_scan",
                        }
                        self._alert_cooldown[cooldown_key] = now
                        to_record.append(sig)
                        self._scan_stats["alerted"] += 1

                # 5. Batch-record + send alerts
                if to_record:
                    today = datetime.fromtimestamp(now).strftime("%Y-%m-%d")
                    for sig in to_record:
                        sig["date"] = today
                        self._history.append(sig)
                    if len(self._history) > 10_000:
                        self._history = self._history[-5_000:]
                    self._save_history()

                    for sig in to_record:
                        self._append_signal_jsonl(sig)
                        self._write_arb_to_wallets(sig)
                        self._record_to_profit_tracker(sig)

                    for sig in to_record:
                        if self._send:
                            try:
                                await self._send(_format_rest_alert(sig))
                            except Exception as e:
                                logger.error("ArbEngine alert send error: %s", e)

                    logger.info(
                        "ArbEngine REST scan: %d alerts | top50 of %d qualified",
                        len(to_record), len(qualified),
                    )

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("ArbEngine _scan_market_rest error: %s", e)

    # ── REST data fetchers ────────────────────────────────────────────────────

    async def _fetch_okx_tickers(
        self, session: aiohttp.ClientSession
    ) -> dict[str, dict]:
        """Return {pair: {price, vol_usdt}} for all OKX USDT spot pairs."""
        url = "https://www.okx.com/api/v5/market/tickers?instType=SPOT"
        try:
            async with session.get(url) as resp:
                data = await resp.json()
            result: dict[str, dict] = {}
            for item in data.get("data", []):
                inst_id = item.get("instId", "")
                if not inst_id.endswith("-USDT"):
                    continue
                try:
                    price   = float(item["last"])
                    # volCcy24h = 24h volume in quote currency (USDT for *-USDT pairs)
                    vol_usdt = float(item.get("volCcy24h") or 0)
                    if price > 0:
                        result[inst_id] = {"price": price, "vol_usdt": vol_usdt}
                except (KeyError, TypeError, ValueError):
                    continue
            return result
        except Exception as e:
            logger.warning("OKX ticker fetch error: %s", e)
            return {}

    async def _fetch_binance_prices_batch(
        self, session: aiohttp.ClientSession, pairs: list[str]
    ) -> dict[str, float]:
        """Return {canonical_pair: price} from Binance for the given pairs."""
        url = "https://api.binance.com/api/v3/ticker/price"
        try:
            async with session.get(url) as resp:
                data = await resp.json()
            bnb_map: dict[str, float] = {}
            for item in data:
                try:
                    bnb_map[item["symbol"].lower()] = float(item["price"])
                except (KeyError, TypeError, ValueError):
                    continue
            result: dict[str, float] = {}
            for pair in pairs:
                key = _to_binance(pair)
                if key in bnb_map and bnb_map[key] > 0:
                    result[pair] = bnb_map[key]
            return result
        except Exception as e:
            logger.warning("Binance price batch fetch error: %s", e)
            return {}

    async def _fetch_bybit_prices_batch(
        self, session: aiohttp.ClientSession, pairs: list[str]
    ) -> dict[str, float]:
        """Return {canonical_pair: price} from Bybit for the given pairs."""
        url = "https://api.bybit.com/v5/market/tickers?category=spot"
        try:
            async with session.get(url) as resp:
                data = await resp.json()
            bybit_map: dict[str, float] = {}
            for item in data.get("result", {}).get("list", []):
                try:
                    price = float(item.get("lastPrice") or 0)
                    if price > 0:
                        bybit_map[item["symbol"]] = price
                except (KeyError, TypeError, ValueError):
                    continue
            result: dict[str, float] = {}
            for pair in pairs:
                key = _to_bybit(pair)
                if key in bybit_map:
                    result[pair] = bybit_map[key]
            return result
        except Exception as e:
            logger.warning("Bybit price batch fetch error: %s", e)
            return {}

    # ── History management ────────────────────────────────────────────────────

    def _load_history(self) -> None:
        try:
            if os.path.exists(HISTORY_FILE):
                with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                    self._history = json.load(f)
                # Prune to MAX_HISTORY_DAYS
                all_dates = sorted(set(o.get("date", "") for o in self._history))
                if len(all_dates) > MAX_HISTORY_DAYS:
                    keep = set(all_dates[-MAX_HISTORY_DAYS:])
                    self._history = [o for o in self._history if o.get("date") in keep]
        except Exception as e:
            logger.warning("ArbEngine history load error: %s", e)
            self._history = []

    def _save_history(self) -> None:
        tmp = str(HISTORY_FILE) + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._history, f, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, str(HISTORY_FILE))
        except Exception as e:
            logger.error("ArbEngine history save error: %s", e)
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def _record_opportunity(self, sig: dict) -> None:
        """Record a WS-detected opportunity to history."""
        record = dict(sig)
        record["date"] = datetime.fromtimestamp(sig.get("timestamp", time.time())).strftime("%Y-%m-%d")
        self._history.append(record)
        if len(self._history) > 10_000:
            self._history = self._history[-5_000:]
        self._save_history()
        self._append_signal_jsonl(record)
        self._write_arb_to_wallets(record)
        self._record_to_profit_tracker(record)

    def _write_arb_to_wallets(self, sig: dict) -> None:
        """Append arb signal to .smart_wallets.json under 'arb_signals' key."""
        try:
            if os.path.exists(SMART_WALLETS_FILE):
                with open(SMART_WALLETS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = {}
            arb_list = data.get("arb_signals", [])
            entry = {
                "pair": sig.get("pair", "?"),
                "buy_exchange": sig.get("buy_exchange", "?"),
                "sell_exchange": sig.get("sell_exchange", "?"),
                "spread_pct": sig.get("spread_pct", 0),
                "net_profit_pct": sig.get("net_profit_pct", 0),
                "net_profit_usdt": sig.get("net_profit_usdt", 0),
                "buy_price": sig.get("buy_price", 0),
                "sell_price": sig.get("sell_price", 0),
                "timestamp": sig.get("timestamp", time.time()),
                "date": sig.get("date", ""),
                "source": sig.get("source", "websocket"),
            }
            arb_list.append(entry)
            data["arb_signals"] = arb_list[-MAX_WALLETS_ARB_SIGNALS:]
            tmp = str(SMART_WALLETS_FILE) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, str(SMART_WALLETS_FILE))
        except Exception as e:
            logger.warning("ArbEngine _write_arb_to_wallets error: %s", e)

    @staticmethod
    def _append_signal_jsonl(sig: dict) -> None:
        """Append a single arb signal as a JSONL line to ARB_SIGNALS_FILE."""
        try:
            sig_ts = sig.get("timestamp", time.time())
            entry = {
                "ts": sig_ts,
                "date": sig.get("date", datetime.fromtimestamp(sig_ts).strftime("%Y-%m-%d")),
                "pair": sig.get("pair", "?"),
                "buy_exchange": sig.get("buy_exchange", "?"),
                "sell_exchange": sig.get("sell_exchange", "?"),
                "spread_pct": sig.get("spread_pct", 0),
                "net_profit_pct": sig.get("net_profit_pct", 0),
                "buy_price": sig.get("buy_price", 0),
                "sell_price": sig.get("sell_price", 0),
                "source": sig.get("source", "websocket"),
            }
            with open(ARB_SIGNALS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            # Truncate to last 5000 lines to prevent unbounded growth
            try:
                with open(ARB_SIGNALS_FILE, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                if len(lines) > 5000:
                    with open(ARB_SIGNALS_FILE, "w", encoding="utf-8") as f:
                        f.writelines(lines[-5000:])
            except Exception:
                pass
        except Exception as e:
            logger.warning("ArbEngine _append_signal_jsonl error: %s", e)

    @staticmethod
    def _record_to_profit_tracker(sig: dict) -> None:
        """Record arb signal into profit_tracker for win-rate tracking."""
        try:
            from profit_tracker import record_arb_signal
            record_arb_signal(
                pair=sig.get("pair", "?"),
                buy_exchange=sig.get("buy_exchange", "?"),
                sell_exchange=sig.get("sell_exchange", "?"),
                spread_pct=sig.get("spread_pct", 0),
                buy_price=sig.get("buy_price", 0),
            )
        except Exception as e:
            logger.debug("ArbEngine profit_tracker record skipped: %s", e)

    # ── Query interface ───────────────────────────────────────────────────────

    def get_top_spreads(self, n: int = 5) -> list[dict]:
        """Return top-n live arb signals (within SIGNAL_MAX_AGE seconds)."""
        now = time.time()
        recent = [s for s in self._signals if now - s["timestamp"] < SIGNAL_MAX_AGE]
        return recent[:n]

    def get_today_summary(self) -> dict:
        """Return today's arbitrage opportunity stats for /arb command."""
        today = date.today().isoformat()
        today_opps = [o for o in self._history if o.get("date") == today]
        if not today_opps:
            return {
                "date": today,
                "count": 0,
                "avg_spread_pct": 0.0,
                "avg_net_profit_pct": 0.0,
                "total_estimated_profit_usdt": 0.0,
                "opportunities": [],
            }
        avg_spread = sum(o["spread_pct"] for o in today_opps) / len(today_opps)
        avg_net    = sum(o.get("net_profit_pct", 0) for o in today_opps) / len(today_opps)
        total_pnl  = sum(o.get("net_profit_usdt", 0) for o in today_opps)
        top10      = sorted(today_opps, key=lambda x: x["spread_pct"], reverse=True)[:10]
        return {
            "date": today,
            "count": len(today_opps),
            "avg_spread_pct": round(avg_spread, 4),
            "avg_net_profit_pct": round(avg_net, 4),
            "total_estimated_profit_usdt": round(total_pnl, 2),
            "opportunities": top10,
        }

    def get_all_prices(self) -> dict[str, dict[str, float]]:
        return {k: dict(v) for k, v in self._prices.items()}

    def exchange_count(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for prices in self._prices.values():
            for ex in prices:
                counts[ex] = counts.get(ex, 0) + 1
        return counts

    # ── OKX WebSocket ─────────────────────────────────────────────────────────

    async def _okx_ws(self) -> None:
        url  = "wss://ws.okx.com:8443/ws/v5/public"
        args = [{"channel": "tickers", "instId": sym} for sym in SYMBOLS]
        _okx_delay = RECONNECT_DELAY

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=25) as ws:
                        await ws.send_json({"op": "subscribe", "args": args})
                        logger.debug("ArbEngine: OKX WS connected")
                        _okx_delay = RECONNECT_DELAY  # reset on successful connection
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle_okx(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("ArbEngine OKX WS error: %s", e)
                _okx_delay = min(_okx_delay * 2, 300)
            if self._running:
                await asyncio.sleep(_okx_delay)

    def _handle_okx(self, raw: str) -> None:
        try:
            data = json.loads(raw)
            for item in data.get("data", []):
                inst_id = item.get("instId", "")
                last    = item.get("last")
                if inst_id in set(SYMBOLS) and last:
                    self._update_price(inst_id, "OKX", float(last))
        except Exception:
            pass

    # ── Bybit WebSocket ───────────────────────────────────────────────────────

    async def _bybit_ws(self) -> None:
        url  = "wss://stream.bybit.com/v5/public/spot"
        args = [f"tickers.{_to_bybit(sym)}" for sym in SYMBOLS]
        _bybit_delay = RECONNECT_DELAY

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=20) as ws:
                        await ws.send_json({"op": "subscribe", "args": args})
                        logger.debug("ArbEngine: Bybit WS connected")
                        _bybit_delay = RECONNECT_DELAY  # reset on successful connection
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle_bybit(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("ArbEngine Bybit WS error: %s", e)
                _bybit_delay = min(_bybit_delay * 2, 300)
            if self._running:
                await asyncio.sleep(_bybit_delay)

    def _handle_bybit(self, raw: str) -> None:
        try:
            data   = json.loads(raw)
            topic  = data.get("topic", "")
            if not topic.startswith("tickers."):
                return
            ticker     = data.get("data", {})
            symbol     = ticker.get("symbol", "")
            last_price = ticker.get("lastPrice")
            canonical  = _BYBIT_TO_CANONICAL.get(symbol)
            if canonical and last_price:
                self._update_price(canonical, "Bybit", float(last_price))
        except Exception:
            pass

    # ── Binance WebSocket ─────────────────────────────────────────────────────

    async def _binance_ws(self) -> None:
        streams = "/".join(f"{_to_binance(sym)}@miniTicker" for sym in SYMBOLS)
        url     = f"wss://stream.binance.com:9443/stream?streams={streams}"
        _binance_delay = RECONNECT_DELAY

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url, heartbeat=30) as ws:
                        logger.debug("ArbEngine: Binance WS connected")
                        _binance_delay = RECONNECT_DELAY  # reset on successful connection
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                self._handle_binance(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("ArbEngine Binance WS error: %s", e)
                _binance_delay = min(_binance_delay * 2, 300)
            if self._running:
                await asyncio.sleep(_binance_delay)

    def _handle_binance(self, raw: str) -> None:
        try:
            data        = json.loads(raw)
            stream_data = data.get("data", {})
            symbol      = stream_data.get("s", "").lower()
            last_price  = stream_data.get("c")
            canonical   = _BINANCE_TO_CANONICAL.get(symbol)
            if canonical and last_price:
                self._update_price(canonical, "Binance", float(last_price))
        except Exception:
            pass


# ── Formatting ────────────────────────────────────────────────────────────────

def _format_rest_alert(sig: dict) -> str:
    """Telegram alert for a REST-scan arbitrage signal."""
    vol_str = ""
    if sig.get("volume_24h_usdt"):
        vol_m = sig["volume_24h_usdt"] / 1_000_000
        vol_str = f"\n  成交量: ${vol_m:.1f}M (24h)"
    return (
        f"🔀 套利信号 [{sig.get('source', 'unknown').replace('_',' ')}]\n"
        f"**{sig.get('pair', '?')}**  价差: **{sig.get('spread_pct', 0):.3f}%**\n"
        f"  买入 {sig.get('buy_exchange', '?')} @ ${sig.get('buy_price', 0):,.4f}\n"
        f"  卖出 {sig.get('sell_exchange', '?')} @ ${sig.get('sell_price', 0):,.4f}\n"
        f"  净利润: **{sig.get('net_profit_pct', 0):.3f}%** (手续费+滑点后)\n"
        f"  预估利润 (1000U): **${sig.get('net_profit_usdt', 0):.2f}**"
        f"{vol_str}"
    )


def format_arb_signal(sig: dict) -> str:
    """Format a single arb signal for display."""
    age     = int(time.time() - sig.get("timestamp", time.time()))
    net_str = (
        f"  净利润: **{sig.get('net_profit_pct', 0):.3f}%** | ${sig.get('net_profit_usdt', 0):.2f}\n"
        if "net_profit_pct" in sig else ""
    )
    return (
        f"💹 **{sig.get('pair', '?')}**  |  价差: **{sig.get('spread_pct', 0):.3f}%**\n"
        f"  买入 {sig.get('buy_exchange', '?')} @ ${sig.get('buy_price', 0):,.4f}\n"
        f"  卖出 {sig.get('sell_exchange', '?')} @ ${sig.get('sell_price', 0):,.4f}\n"
        f"{net_str}"
        f"  更新: {age}s 前"
    )


def format_arb_top5(signals: list[dict]) -> str:
    if not signals:
        return "📊 暂无实时套利机会 (价差 < 0.3% 或净利润为负)\n\n数据来自 OKX / Bybit / Binance 实时行情。"
    lines = [f"🔀 实时套利信号 Top {len(signals)}\n"]
    for i, sig in enumerate(signals, 1):
        lines.append(f"{i}. {format_arb_signal(sig)}")
    return "\n".join(lines)


def format_arb_top10(signals: list[dict]) -> str:
    if not signals:
        return "📊 暂无实时套利机会 (价差 < 0.3% 或净利润为负)\n\n数据来自 OKX / Bybit / Binance 实时行情。"
    lines = [f"🔀 实时套利信号 Top {len(signals)}\n"]
    for i, sig in enumerate(signals, 1):
        lines.append(f"{i}. {format_arb_signal(sig)}")
    return "\n".join(lines)


def format_arb_today(summary: dict) -> str:
    """Format today's arbitrage summary for /arb command."""
    if summary["count"] == 0:
        return (
            f"📅 今日套利汇总 ({summary['date']})\n\n"
            "暂无套利机会被记录。\n"
            "引擎每5分钟扫描 Top-50 交易对，价差>0.3% 且净利润>0 才记录。"
        )

    lines = [
        f"📅 今日套利汇总 ({summary['date']})",
        f"发现机会: **{summary['count']}** 次",
        f"平均价差: **{summary['avg_spread_pct']:.3f}%**",
        f"平均净利润率: **{summary['avg_net_profit_pct']:.3f}%**",
        f"总预估利润 (1000U): **${summary['total_estimated_profit_usdt']:.2f}**",
        "",
        "🏆 今日 Top 机会:",
    ]
    for i, opp in enumerate(summary["opportunities"], 1):
        t = datetime.fromtimestamp(opp["timestamp"]).strftime("%H:%M")
        vol_str = ""
        if opp.get("volume_24h_usdt"):
            vol_str = f" | 量${opp['volume_24h_usdt']/1_000_000:.1f}M"
        lines.append(
            f"{i}. {opp['pair']} | {opp['buy_exchange']}→{opp['sell_exchange']}"
            f" | 价差{opp['spread_pct']:.3f}% | 净{opp.get('net_profit_pct', 0):.3f}%"
            f"{vol_str} [{t}]"
        )
    return "\n".join(lines)


# ── Module-level singleton ────────────────────────────────────────────────────

arb_engine = ArbEngine()
