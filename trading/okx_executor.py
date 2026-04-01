"""
OKX Perpetual Contract Executor — Paper & Live modes.

Adapted from crypto-analysis okx_trader.py for the telegram bot.
Paper mode simulates fills at market price. Live mode uses OKX REST API v5
with HMAC-SHA256 signing. The agent starts in paper mode; switching to live
requires API keys in .env and explicit activation.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import httpx

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore

try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass

log = logging.getLogger(__name__)

OKX_REST_BASE = "https://www.okx.com"
OKX_DEMO_TRADING = os.environ.get("OKX_DEMO_TRADING", "false").lower() == "true"

STATE_FILE = Path(__file__).resolve().parent.parent / "agent_state.json"


@dataclass
class Position:
    symbol: str
    side: str          # 'long' | 'short'
    size: float        # USD notional
    entry_price: float
    entry_time: float  # unix ts
    unrealized_pnl: float = 0.0
    peak_pnl: float = 0.0


@dataclass
class TradeRecord:
    symbol: str
    side: str
    entry_price: float
    exit_price: float
    size: float
    pnl_pct: float
    pnl_usd: float
    entry_time: float
    exit_time: float
    reason: str


@dataclass
class RiskLimits:
    max_position_pct: float = 0.05
    max_total_exposure_pct: float = 0.15
    max_daily_loss_pct: float = 0.02
    max_drawdown_pct: float = 0.05
    max_positions: int = 3
    cooldown_seconds: int = 3600


@dataclass
class AgentState:
    mode: str = "paper"
    equity: float = 10_000.0
    peak_equity: float = 10_000.0
    cash: float = 10_000.0
    positions: dict = field(default_factory=dict)
    trade_history: list = field(default_factory=list)
    daily_pnl: float = 0.0
    daily_trades: int = 0
    last_daily_reset: float = 0.0
    generation: int = 0
    total_trades: int = 0
    total_pnl_usd: float = 0.0
    win_count: int = 0
    loss_count: int = 0
    is_alive: bool = True
    shutdown_reason: str = ""
    last_trade_time: dict = field(default_factory=dict)
    strategy_params: dict = field(default_factory=lambda: {
        "ma5_len": 5, "ma8_len": 8, "ema21_len": 21, "ma55_len": 55,
        "bb_length": 21, "bb_std_dev": 2.5,
        "dist_ma5_ma8": 1.5, "dist_ma8_ema21": 2.5, "dist_ema21_ma55": 4.0,
        "slope_len": 3, "slope_threshold": 0.1, "atr_period": 14,
    })

    def to_dict(self) -> dict:
        return {
            "mode": self.mode,
            "equity": round(self.equity, 2),
            "peak_equity": round(self.peak_equity, 2),
            "cash": round(self.cash, 2),
            "positions": {
                k: {
                    "symbol": v.symbol, "side": v.side,
                    "size": round(v.size, 2),
                    "entry_price": v.entry_price,
                    "unrealized_pnl": round(v.unrealized_pnl, 2),
                }
                for k, v in self.positions.items()
            },
            "daily_pnl": round(self.daily_pnl, 2),
            "daily_trades": self.daily_trades,
            "generation": self.generation,
            "total_trades": self.total_trades,
            "total_pnl_usd": round(self.total_pnl_usd, 2),
            "win_rate": round(
                self.win_count / max(self.total_trades, 1) * 100, 1
            ),
            "is_alive": self.is_alive,
            "shutdown_reason": self.shutdown_reason,
            "strategy_params": self.strategy_params,
            "recent_trades": [
                {
                    "symbol": t.symbol, "side": t.side,
                    "pnl_pct": round(t.pnl_pct, 2),
                    "pnl_usd": round(t.pnl_usd, 2),
                    "reason": t.reason,
                    "entry_price": t.entry_price,
                    "exit_price": t.exit_price,
                }
                for t in self.trade_history[-20:]
            ],
        }


class OKXExecutor:
    """Full OKX trading interface with paper and live modes."""

    def __init__(
        self,
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
    ):
        self.api_key = api_key or os.environ.get("OKX_API_KEY", "")
        self.api_secret = api_secret or os.environ.get("OKX_SECRET", "")
        self.passphrase = passphrase or os.environ.get("OKX_PASSPHRASE", "")
        self.state = AgentState()
        self.risk = RiskLimits()
        self._price_cache: dict[str, float] = {}
        self._price_cache_ts: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._aio_session: Optional["aiohttp.ClientSession"] = None
        self._aio_session_lock = asyncio.Lock()
        self._okx_http_spacing_lock = asyncio.Lock()
        self._last_okx_http_ts: float = 0.0
        self._min_http_interval_sec: float = 0.095
        if self.api_key:
            log.info("OKX API key loaded — live trading available")

    def has_api_keys(self) -> bool:
        return bool(self.api_key and self.api_secret and self.passphrase)

    async def _get_aio_session(self) -> Optional["aiohttp.ClientSession"]:
        if aiohttp is None:
            return None
        async with self._aio_session_lock:
            if self._aio_session is None or self._aio_session.closed:
                connector = aiohttp.TCPConnector(
                    limit_per_host=24,
                    keepalive_timeout=60,
                    enable_cleanup_closed=True,
                )
                timeout = aiohttp.ClientTimeout(total=20, connect=10)
                self._aio_session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                )
            return self._aio_session

    async def aclose(self) -> None:
        async with self._aio_session_lock:
            if self._aio_session and not self._aio_session.closed:
                await self._aio_session.close()
            self._aio_session = None

    async def _okx_http_spacing(self) -> None:
        async with self._okx_http_spacing_lock:
            gap = time.time() - self._last_okx_http_ts
            if gap < self._min_http_interval_sec:
                await asyncio.sleep(self._min_http_interval_sec - gap)
            self._last_okx_http_ts = time.time()

    # ── Price data ────────────────────────────────────────────────────────

    async def get_price(self, symbol: str) -> float | None:
        now = time.time()
        if symbol in self._price_cache and now - self._price_cache_ts.get(symbol, 0) < 5:
            return self._price_cache[symbol]
        inst_id = self._inst_id(symbol)
        try:
            session = await self._get_aio_session()
            if session is not None:
                await self._okx_http_spacing()
                url = f"{OKX_REST_BASE}/api/v5/market/ticker"
                async with session.get(url, params={"instId": inst_id}) as resp:
                    data = await resp.json()
            else:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(
                        f"{OKX_REST_BASE}/api/v5/market/ticker",
                        params={"instId": inst_id},
                    )
                    data = resp.json()
            if data.get("code") == "0" and data.get("data"):
                price = float(data["data"][0]["last"])
                self._price_cache[symbol] = price
                self._price_cache_ts[symbol] = now
                return price
        except Exception as e:
            log.warning("Price fetch error for %s: %s", symbol, e)
        return self._price_cache.get(symbol)

    async def get_ohlcv(
        self, symbol: str, bar: str = "4H", limit: int = 300
    ) -> list[list]:
        """Fetch OHLCV candles from OKX public API.

        Returns list of [ts, open, high, low, close, vol] with floats.
        """
        inst_id = self._inst_id(symbol)
        all_candles: list[list] = []
        after = ""
        remaining = limit

        async with httpx.AsyncClient(timeout=15.0) as client:
            while remaining > 0:
                params: dict[str, Any] = {
                    "instId": inst_id,
                    "bar": bar,
                    "limit": str(min(remaining, 300)),
                }
                if after:
                    params["after"] = after

                resp = await client.get(
                    f"{OKX_REST_BASE}/api/v5/market/candles", params=params
                )
                data = resp.json()
                if data.get("code") != "0" or not data.get("data"):
                    break

                rows = data["data"]
                if not rows:
                    break

                for r in rows:
                    all_candles.append([
                        float(r[0]),    # ts
                        float(r[1]),    # open
                        float(r[2]),    # high
                        float(r[3]),    # low
                        float(r[4]),    # close
                        float(r[5]),    # vol
                    ])

                after = rows[-1][0]
                remaining -= len(rows)
                if len(rows) < 100:
                    break

        all_candles.sort(key=lambda c: c[0])
        return all_candles

    # ── Risk checks ───────────────────────────────────────────────────────

    def check_daily_reset(self):
        import datetime as _dt
        now = time.time()
        now_utc = _dt.datetime.fromtimestamp(now, tz=_dt.timezone.utc)
        if self.state.last_daily_reset > 0:
            last_utc = _dt.datetime.fromtimestamp(
                self.state.last_daily_reset, tz=_dt.timezone.utc
            )
        else:
            last_utc = None
        if last_utc is None or now_utc.date() > last_utc.date():
            self.state.daily_pnl = 0.0
            self.state.daily_trades = 0
            self.state.last_daily_reset = now

    def can_trade(self, symbol: str) -> tuple[bool, str]:
        if not self.state.is_alive:
            return False, f"Agent shutdown: {self.state.shutdown_reason}"
        self.check_daily_reset()
        if self.state.daily_pnl < -self.risk.max_daily_loss_pct * self.state.peak_equity:
            self.state.is_alive = False
            self.state.shutdown_reason = f"Daily loss limit hit: {self.state.daily_pnl:.2f}"
            return False, self.state.shutdown_reason
        if self.state.peak_equity <= 0:
            self.state.peak_equity = max(self.state.equity, 1.0)
        dd = (self.state.peak_equity - self.state.equity) / self.state.peak_equity
        if dd > self.risk.max_drawdown_pct:
            self.state.is_alive = False
            self.state.shutdown_reason = f"Max drawdown hit: {dd * 100:.1f}%"
            return False, self.state.shutdown_reason
        if len(self.state.positions) >= self.risk.max_positions and symbol not in self.state.positions:
            return False, f"Max positions ({self.risk.max_positions}) reached"
        total_exp = sum(p.size for p in self.state.positions.values())
        max_exp = self.risk.max_total_exposure_pct * self.state.equity
        if total_exp >= max_exp:
            return False, f"Max total exposure reached ({total_exp:.0f}/{max_exp:.0f})"
        last = self.state.last_trade_time.get(symbol, 0)
        if time.time() - last < self.risk.cooldown_seconds:
            remaining = int(self.risk.cooldown_seconds - (time.time() - last))
            return False, f"Cooldown: {remaining}s remaining for {symbol}"
        return True, "OK"

    # ── Open / Close ──────────────────────────────────────────────────────

    async def open_position(self, symbol: str, side: str, size_usd: float) -> dict:
        async with self._lock:
            can, reason = self.can_trade(symbol)
            if not can:
                return {"ok": False, "reason": reason}
            max_size = self.state.equity * self.risk.max_position_pct
            size_usd = min(size_usd, max_size)
            current_exp = sum(p.size for p in self.state.positions.values())
            remaining = self.risk.max_total_exposure_pct * self.state.equity - current_exp
            if remaining <= 0:
                return {"ok": False, "reason": "Max total exposure reached"}
            size_usd = min(size_usd, remaining)
            if size_usd < 1.0:
                return {"ok": False, "reason": f"Position size too small: ${size_usd:.2f}"}

            price = await self.get_price(symbol)
            if price is None:
                return {"ok": False, "reason": f"Cannot get price for {symbol}"}

            if self.state.mode == "live":
                result = await self._place_order_live(symbol, side, size_usd, price)
                if not result.get("ok"):
                    return result
            else:
                if self.state.cash < size_usd:
                    return {"ok": False, "reason": f"Insufficient cash: {self.state.cash:.2f} < {size_usd:.2f}"}

            pos = Position(
                symbol=symbol, side=side, size=size_usd,
                entry_price=price, entry_time=time.time(),
            )
            self.state.positions[symbol] = pos
            self.state.cash -= size_usd
            self.state.equity = self.state.cash + sum(p.size for p in self.state.positions.values())
            self.state.last_trade_time[symbol] = time.time()
            return {"ok": True, "price": price, "size": size_usd, "side": side}

    async def close_position(self, symbol: str, reason: str = "SIGNAL") -> dict:
        async with self._lock:
            if symbol not in self.state.positions:
                return {"ok": False, "reason": f"No position for {symbol}"}
            pos = self.state.positions[symbol]
            price = await self.get_price(symbol)
            if price is None:
                return {"ok": False, "reason": f"Cannot get price for {symbol}"}
            if pos.entry_price <= 0:
                return {"ok": False, "reason": f"Invalid entry price for {symbol}"}
            if pos.side == "long":
                pnl_pct = (price - pos.entry_price) / pos.entry_price * 100
            else:
                pnl_pct = (pos.entry_price - price) / pos.entry_price * 100
            pnl_usd = pos.size * pnl_pct / 100

            if self.state.mode == "live":
                live_result = await self._close_order_live(symbol, pos.side)
                if not live_result.get("ok"):
                    return {"ok": False, "reason": f"Exchange close failed: {live_result.get('reason')}"}

            record = TradeRecord(
                symbol=symbol, side=pos.side,
                entry_price=pos.entry_price, exit_price=price,
                size=pos.size, pnl_pct=pnl_pct, pnl_usd=pnl_usd,
                entry_time=pos.entry_time, exit_time=time.time(),
                reason=reason,
            )
            self.state.trade_history.append(record)
            if len(self.state.trade_history) > 500:
                self.state.trade_history = self.state.trade_history[-500:]
            self.state.total_trades += 1
            self.state.total_pnl_usd += pnl_usd
            self.state.daily_pnl += pnl_usd
            self.state.daily_trades += 1
            if pnl_usd > 0:
                self.state.win_count += 1
            elif pnl_usd < 0:
                self.state.loss_count += 1
            del self.state.positions[symbol]
            self.state.cash += pos.size + pnl_usd
            self.state.equity = self.state.cash + sum(p.size for p in self.state.positions.values())
            self.state.peak_equity = max(self.state.peak_equity, self.state.equity)
            self.state.last_trade_time[symbol] = time.time()
            return {
                "ok": True, "price": price,
                "pnl_pct": round(pnl_pct, 2),
                "pnl_usd": round(pnl_usd, 2),
                "reason": reason,
                "exit_price": price,
            }

    async def update_positions(self):
        for symbol, pos in list(self.state.positions.items()):
            price = await self.get_price(symbol)
            if price is None or pos.entry_price <= 0:
                continue
            if pos.side == "long":
                pos.unrealized_pnl = (price - pos.entry_price) / pos.entry_price * 100
            else:
                pos.unrealized_pnl = (pos.entry_price - price) / pos.entry_price * 100
            pos.peak_pnl = max(pos.peak_pnl, pos.unrealized_pnl)
        total_unrealized = sum(
            p.size * p.unrealized_pnl / 100
            for p in self.state.positions.values()
        )
        self.state.equity = self.state.cash + sum(
            p.size for p in self.state.positions.values()
        ) + total_unrealized
        self.state.peak_equity = max(self.state.peak_equity, self.state.equity)

    # ── OKX authenticated requests ───────────────────────────────────────

    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        message = timestamp + method + path + body
        mac = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode()

    def _make_headers(self, timestamp: str, method: str, path: str, body: str = "") -> dict:
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if OKX_DEMO_TRADING:
            headers["x-simulated-trading"] = "1"
        return headers

    def _inst_id(self, symbol: str) -> str:
        base = symbol.upper().replace("USDT", "").replace("-", "")
        return f"{base}-USDT-SWAP"

    def _spot_inst_id(self, symbol: str) -> str:
        base = symbol.upper().replace("USDT", "").replace("-", "")
        return f"{base}-USDT"

    async def _okx_request(self, method: str, path: str, body: str = "") -> dict:
        if not self.has_api_keys():
            return {"code": "-1", "msg": "No API keys configured"}
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())
        headers = self._make_headers(timestamp, method, path, body)
        url = f"{OKX_REST_BASE}{path}"
        try:
            session = await self._get_aio_session()
            if session is not None:
                await self._okx_http_spacing()
                if method == "GET":
                    async with session.get(url, headers=headers) as resp:
                        return await resp.json()
                async with session.post(url, headers=headers, data=body) as resp:
                    return await resp.json()
            async with httpx.AsyncClient(timeout=15.0) as client:
                if method == "GET":
                    resp = await client.get(url, headers=headers)
                else:
                    resp = await client.post(url, headers=headers, content=body)
                return resp.json()
        except asyncio.TimeoutError:
            return {"code": "-1", "msg": "timeout"}
        except Exception as e:
            return {"code": "-1", "msg": str(e)}

    async def get_account_balance(self) -> dict:
        data = await self._okx_request("GET", "/api/v5/account/balance")
        if data.get("code") == "0" and data.get("data"):
            acct = data["data"][0]
            details = acct.get("details", [])
            usdt = next((d for d in details if d.get("ccy") == "USDT"), None)
            return {
                "ok": True,
                "total_equity": float(acct.get("totalEq", 0)),
                "usdt_available": float(usdt.get("availBal", 0)) if usdt else 0,
            }
        return {"ok": False, "reason": data.get("msg", "Unknown error")}

    async def get_exchange_positions(self) -> list[dict]:
        data = await self._okx_request("GET", "/api/v5/account/positions")
        if data.get("code") == "0" and data.get("data"):
            return data["data"]
        return []

    async def _get_contract_size(self, inst_id: str) -> float:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{OKX_REST_BASE}/api/v5/public/instruments",
                    params={"instType": "SWAP", "instId": inst_id},
                )
                data = resp.json()
            if data.get("code") == "0" and data.get("data"):
                return float(data["data"][0].get("ctVal", 1))
        except Exception as e:
            log.warning("Failed to get contract size for %s: %s", inst_id, e)
        return 1.0

    async def _place_order_live(self, symbol: str, side: str, size_usd: float, price: float) -> dict:
        if not self.has_api_keys():
            return {"ok": False, "reason": "No API keys configured"}
        inst_id = self._inst_id(symbol)
        ct_val = await self._get_contract_size(inst_id)
        if price <= 0 or ct_val <= 0:
            return {"ok": False, "reason": f"Invalid price ({price}) or contract value ({ct_val})"}
        n_contracts = max(1, int(size_usd / (price * ct_val)))
        path = "/api/v5/trade/order"
        body = json.dumps({
            "instId": inst_id,
            "tdMode": "cross",
            "side": "buy" if side == "long" else "sell",
            "posSide": "long" if side == "long" else "short",
            "ordType": "market",
            "sz": str(n_contracts),
        })
        data = await self._okx_request("POST", path, body)
        if data.get("code") == "0" and data.get("data"):
            ord_id = data["data"][0].get("ordId", "")
            log.info("OKX order placed: %s %s x%d ordId=%s", side, inst_id, n_contracts, ord_id)
            return {"ok": True, "orderId": ord_id, "price": price, "contracts": n_contracts}
        msg = data.get("msg", "")
        if not msg and data.get("data"):
            msg = data["data"][0].get("sMsg", "Unknown error")
        log.warning("OKX order failed: %s", msg)
        return {"ok": False, "reason": msg}

    async def _close_order_live(self, symbol: str, side: str = "") -> dict:
        if not self.has_api_keys():
            return {"ok": False, "reason": "No API keys configured"}
        inst_id = self._inst_id(symbol)
        path = "/api/v5/trade/close-position"
        close_body: dict[str, str] = {"instId": inst_id, "mgnMode": "cross"}
        if side:
            close_body["posSide"] = side
        body = json.dumps(close_body)
        data = await self._okx_request("POST", path, body)
        if data.get("code") == "0":
            log.info("OKX position closed: %s", inst_id)
            return {"ok": True}
        msg = data.get("msg", "Unknown error")
        log.warning("OKX close failed: %s — trying market order fallback", msg)
        pos_data = await self._okx_request("GET", f"/api/v5/account/positions?instId={inst_id}")
        if pos_data.get("code") == "0" and pos_data.get("data"):
            pos = pos_data["data"][0]
            pos_amt = abs(float(pos.get("pos", 0)))
            if pos_amt > 0:
                close_side = "sell" if float(pos.get("pos", 0)) > 0 else "buy"
                pos_side = "long" if float(pos.get("pos", 0)) > 0 else "short"
                fb_body = json.dumps({
                    "instId": inst_id, "tdMode": "cross",
                    "side": close_side, "posSide": pos_side,
                    "ordType": "market", "sz": str(pos_amt),
                    "reduceOnly": "true",
                })
                close_data = await self._okx_request("POST", "/api/v5/trade/order", fb_body)
                if close_data.get("code") == "0":
                    return {"ok": True}
        return {"ok": False, "reason": msg}

    # ── State persistence ─────────────────────────────────────────────────

    def save_state(self):
        try:
            STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "mode": self.state.mode,
                "equity": self.state.equity,
                "peak_equity": self.state.peak_equity,
                "cash": self.state.cash,
                "generation": self.state.generation,
                "total_trades": self.state.total_trades,
                "total_pnl_usd": self.state.total_pnl_usd,
                "win_count": self.state.win_count,
                "loss_count": self.state.loss_count,
                "is_alive": self.state.is_alive,
                "shutdown_reason": self.state.shutdown_reason,
                "strategy_params": self.state.strategy_params,
            }
            STATE_FILE.write_text(json.dumps(data, indent=2))
        except Exception as e:
            log.warning("Failed to save state: %s", e)

    def load_state(self):
        if not STATE_FILE.exists():
            return
        try:
            data = json.loads(STATE_FILE.read_text())
            s = self.state
            s.mode = data.get("mode", "paper")
            s.equity = data.get("equity", 10_000.0)
            s.peak_equity = data.get("peak_equity", 10_000.0)
            s.cash = data.get("cash", 10_000.0)
            s.generation = data.get("generation", 0)
            s.total_trades = data.get("total_trades", 0)
            s.total_pnl_usd = data.get("total_pnl_usd", 0.0)
            s.win_count = data.get("win_count", 0)
            s.loss_count = data.get("loss_count", 0)
            s.is_alive = data.get("is_alive", True)
            s.shutdown_reason = data.get("shutdown_reason", "")
            if "strategy_params" in data:
                s.strategy_params.update(data["strategy_params"])
            log.info("Loaded state: gen=%d equity=%.2f trades=%d",
                     s.generation, s.equity, s.total_trades)
        except Exception as e:
            log.warning("Failed to load state: %s", e)

    def revive(self):
        self.state.is_alive = True
        self.state.shutdown_reason = ""
        self.state.daily_pnl = 0.0
        self.state.daily_trades = 0
        self.state.last_daily_reset = time.time()

    async def open_position_with_retry(
        self,
        symbol: str,
        side: str,
        size_usd: float,
        max_retries: int = 4,
    ) -> dict:
        """REST order placement with exponential backoff (Phase 12)."""
        delay = 0.4
        last: dict = {"ok": False, "reason": "no_attempt"}
        for attempt in range(max(1, max_retries)):
            last = await self.open_position(symbol, side, size_usd)
            if last.get("ok"):
                return last
            log.warning(
                "OKX open retry %s/%s %s %s: %s",
                attempt + 1,
                max_retries,
                side,
                symbol,
                last.get("reason"),
            )
            await asyncio.sleep(delay)
            delay = min(delay * 1.85, 12.0)
        return last


class OKXDeltaNeutralExecutor:
    """
    Facade for hedge legs (delta-neutral). Reuses OKXExecutor connection pool + signing.
    Compatible with live_trader emergency flatten paths.
    """

    def __init__(self, executor: OKXExecutor | None = None):
        self._ex = executor or OKXExecutor()
        self._ex.load_state()

    @property
    def inner(self) -> OKXExecutor:
        return self._ex

    async def execute_hedge_short(self, symbol: str, notional_usd: float) -> dict[str, Any]:
        r = await self._ex.open_position_with_retry(symbol, "short", notional_usd)
        if r.get("ok"):
            return {"status": "SUCCESS", "detail": r}
        return {"status": "FAILED", "error": r.get("reason", "unknown")}

    async def emergency_flatten_short(self, symbol: str, reason: str = "emergency") -> dict[str, Any]:
        if symbol not in self._ex.state.positions:
            return {"ok": True, "reason": "no_local_position"}
        pos = self._ex.state.positions[symbol]
        if pos.side != "short":
            return {"ok": False, "reason": f"position_not_short:{pos.side}"}
        return await self._ex.close_position(symbol, reason=reason)

    async def close(self) -> None:
        await self._ex.aclose()
