"""
Funding Rate Arbitrage Scanner for OKX perpetual futures.

Targets **extreme positive funding** (longs pay shorts): delta-neutral harvest is
**DEX spot long + OKX perp short**, collecting periodic funding from the crowded long side.

The scanner:
  1. Fetches current and predicted funding rates for a configurable symbol list.
  2. Filters to **positive** funding only; computes annualised rate (longs pay shorts).
  3. Analyses historical persistence, trend, and mean-reversion risk.
  4. Estimates expected PnL net of round-trip trading costs (incl. slippage model).
  5. When Solana mint is known and DexScreener best-pool liquidity ≥ min threshold,
     sets ``execute_delta_neutral_buy_compat: True`` for ``live_trader`` routing.

Alt mode (``fetch_usdt_swap_inst_ids`` / ``fetch_funding_rates_concurrent`` /
``scan_extreme_negative_funding``): discover all OKX USDT perpetuals (optional
altcoin-only), pull funding concurrently, and filter deeply negative rates for
use by ``arbitrage_engine`` funding-carry tasks.

Designed for production use: proper session management, rate limiting,
graceful error handling, structured logging, and full type coverage.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Collection
from datetime import datetime, timezone
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# OKX public API configuration
# ---------------------------------------------------------------------------
OKX_BASE_URL = "https://www.okx.com"
FUNDING_RATE_PATH = "/api/v5/public/funding-rate"
FUNDING_HISTORY_PATH = "/api/v5/public/funding-rate-history"
MARK_PRICE_PATH = "/api/v5/public/mark-price"
INSTRUMENTS_PATH = "/api/v5/public/instruments"

# Bases excluded when scanning “altcoin” USDT swaps (majors).
DEFAULT_ALTCOIN_EXCLUDE_BASES: frozenset[str] = frozenset({"BTC", "ETH"})

# ---------------------------------------------------------------------------
# Default instrument list -- USDT-margined perpetual swaps (OKX instId)
# Includes majors + Solana-mapped bases for DEX+perp delta-neutral execute path.
# ---------------------------------------------------------------------------
DEFAULT_SYMBOLS: list[str] = [
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "DOGE-USDT-SWAP",
    "XRP-USDT-SWAP",
    "AVAX-USDT-SWAP",
    "LINK-USDT-SWAP",
    "ADA-USDT-SWAP",
    "JUP-USDT-SWAP",
    "RAY-USDT-SWAP",
    "ORCA-USDT-SWAP",
    "BONK-USDT-SWAP",
    "WIF-USDT-SWAP",
    "JTO-USDT-SWAP",
    "PYTH-USDT-SWAP",
    "RENDER-USDT-SWAP",
    "HNT-USDT-SWAP",
    "TNSR-USDT-SWAP",
    "W-USDT-SWAP",
    "MOBILE-USDT-SWAP",
]

# Round-trip: taker both legs + conservative per-leg slippage (delta-neutral = 4 fills over life).
DEFAULT_TAKER_FEE_FRAC = 0.0005   # 5 bps per leg (taker)
DEFAULT_SLIPPAGE_FRAC = 0.00025  # 2.5 bps per leg (slippage model)
DEFAULT_ROUND_TRIP_COST = (DEFAULT_TAKER_FEE_FRAC + DEFAULT_SLIPPAGE_FRAC) * 2

# OKX funds every 8 h → 3 periods per day.
FUNDING_PERIODS_PER_DAY = 3

# Minimum DEX (Solana) spot pool liquidity (USD) to arm automated delta-neutral execute.
MIN_DEX_SPOT_POOL_USD_DEFAULT = 50_000.0

# OKX SWAP base asset → Solana mint (Jupiter / live_trader hedge universe). Unknown → no auto-exec flag.
OKX_BASE_TO_SOLANA_MINT: dict[str, str] = {
    "SOL": "So11111111111111111111111111111111111111112",
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "ORCA": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",
    "PIXEL": "Di4B2JSRykk27QcD9oe9sjqff1kTW4mf23bfDePwEKLu",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "JTO": "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
    "PYTH": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
    "RENDER": "rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof",
    "HNT": "hntyVP6YFm1Hg25TN9WGLqM12b8TQmcknKrdu1oxWux",
    "TNSR": "TNSRxcUxoT9xBG3de7PiJyTDYu7kskLqcpddxnEJAS6",
    "W": "85VBFQZC9TZkfaptBWjvUw7YbZjy52A6mjtPGjstQAmQ",
    "MOBILE": "mb1eu7TzEc71KxDpsmsKoucSSuuoGLv1drys1oP2jh6",
}


def okx_swap_inst_to_base(inst_id: str) -> str:
    """BTC-USDT-SWAP → BTC."""
    s = inst_id.upper().strip()
    for suf in ("-USDT-SWAP", "-SWAP"):
        if s.endswith(suf):
            s = s[: -len(suf)]
            break
    return s.split("-")[0] if "-" in s else s


def okx_swap_inst_to_hedge_symbol(inst_id: str) -> str:
    """BTC-USDT-SWAP → BTCUSDT (OKXExecutor / open_position)."""
    return f"{okx_swap_inst_to_base(inst_id)}USDT"


class FundingRateScanner:
    """Async scanner for funding-rate arbitrage on OKX perpetual swaps.

    Parameters
    ----------
    symbols:
        OKX instrument IDs to monitor.  Defaults to *DEFAULT_SYMBOLS*.
    min_rate_threshold:
        Minimum **positive** funding rate (in %) per period to flag.
        Default ``0.01`` means 0.01 %.
    annualized_threshold:
        Minimum **positive** annualised rate (in %) for an opportunity to be included.
        Default ``10.0`` %.
    min_dex_spot_pool_usd:
        DexScreener best-pool USD liquidity required to set
        ``execute_delta_neutral_buy_compat`` (default 50_000).
    require_positive_funding:
        If True (default), ignore negative funding (shorts-pay-longs) regimes.
    max_rps:
        Maximum HTTP requests per second (rate-limiter).
    session_timeout:
        Per-request timeout in seconds.
    """

    def __init__(
        self,
        symbols: list[str] | None = None,
        min_rate_threshold: float = 0.01,
        annualized_threshold: float = 10.0,
        *,
        min_dex_spot_pool_usd: float = MIN_DEX_SPOT_POOL_USD_DEFAULT,
        require_positive_funding: bool = True,
        max_rps: int = 10,
        session_timeout: float = 10.0,
    ) -> None:
        self.symbols = symbols or list(DEFAULT_SYMBOLS)
        self.min_rate_threshold = min_rate_threshold
        self.annualized_threshold = annualized_threshold
        self.min_dex_spot_pool_usd = float(min_dex_spot_pool_usd)
        self.require_positive_funding = require_positive_funding

        self._max_rps = max_rps
        self._min_interval = 1.0 / max(max_rps, 1)
        self._last_request_ts: float = 0.0
        self._rate_lock: asyncio.Lock | None = None  # lazily created in event loop

        self._session_timeout = aiohttp.ClientTimeout(total=session_timeout)
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Return the shared *aiohttp* session, creating it if needed."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                base_url=OKX_BASE_URL,
                timeout=self._session_timeout,
                headers={"Accept": "application/json"},
            )
        return self._session

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def __aenter__(self) -> "FundingRateScanner":
        await self._ensure_session()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Rate-limited HTTP helper
    # ------------------------------------------------------------------

    async def _throttle(self) -> None:
        """Enforce *max_rps* across all concurrent tasks."""
        if self._rate_lock is None:
            self._rate_lock = asyncio.Lock()
        async with self._rate_lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last_request_ts)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_ts = time.monotonic()

    async def _get(self, path: str, params: dict[str, str] | None = None) -> dict:
        """Perform a rate-limited GET and return the parsed JSON body.

        Raises
        ------
        RuntimeError
            If the OKX API returns a non-zero ``code`` or the HTTP status is
            not 2xx.
        """
        session = await self._ensure_session()
        await self._throttle()

        try:
            async with session.get(path, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(
                        f"OKX API HTTP {resp.status} for {path}: {text[:300]}"
                    )
                body: dict = await resp.json()
        except asyncio.TimeoutError:
            raise RuntimeError(f"OKX API timeout for {path} params={params}")
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"OKX API client error for {path}: {exc}") from exc

        code = body.get("code", "0")
        if code != "0":
            msg = body.get("msg", "unknown error")
            raise RuntimeError(f"OKX API error code={code} msg={msg} path={path}")

        return body

    # ------------------------------------------------------------------
    # Data fetchers
    # ------------------------------------------------------------------

    async def get_funding_rate(self, symbol: str) -> dict:
        """Fetch the current and predicted next funding rate for *symbol*.

        Returns
        -------
        dict
            Keys: ``fundingRate``, ``nextFundingRate``, ``fundingTime``,
            ``nextFundingTime`` -- all as native Python types.
        """
        body = await self._get(FUNDING_RATE_PATH, {"instId": symbol})
        data = body.get("data", [])
        if not data:
            raise RuntimeError(f"No funding rate data for {symbol}")

        rec = data[0]

        def _f(x: Any, default: float = 0.0) -> float:
            if x is None or x == "":
                return default
            return float(x)

        return {
            "fundingRate": _f(rec.get("fundingRate"), 0.0),
            "nextFundingRate": _f(rec.get("nextFundingRate"), 0.0),
            "fundingTime": rec.get("fundingTime", ""),
            "nextFundingTime": rec.get("nextFundingTime", ""),
        }

    async def get_funding_history(
        self, symbol: str, limit: int = 100
    ) -> list[dict]:
        """Fetch up to *limit* historical funding rate records.

        Returns
        -------
        list[dict]
            Each dict has ``fundingRate`` (float) and ``fundingTime`` (str ms
            epoch).  Ordered newest-first (as returned by OKX).
        """
        body = await self._get(
            FUNDING_HISTORY_PATH,
            {"instId": symbol, "limit": str(min(limit, 100))},
        )
        rows: list[dict] = []
        for rec in body.get("data", []):
            fr = rec.get("fundingRate", 0)
            try:
                fr_f = float(fr) if fr not in (None, "") else 0.0
            except (TypeError, ValueError):
                fr_f = 0.0
            rows.append(
                {
                    "fundingRate": fr_f,
                    "fundingTime": rec.get("fundingTime", ""),
                }
            )
        return rows

    async def get_mark_price(self, symbol: str) -> float:
        """Return the current mark price for *symbol*."""
        body = await self._get(
            MARK_PRICE_PATH,
            {"instId": symbol, "instType": "SWAP"},
        )
        for rec in body.get("data", []):
            if rec.get("instId") == symbol:
                return float(rec["markPx"])

        raise RuntimeError(f"Mark price not found for {symbol}")

    async def fetch_dex_spot_liquidity_usd(self, mint: str) -> float:
        """Best DexScreener-reported USD liquidity for *mint* (largest pool)."""
        if not mint or len(mint) < 32:
            return 0.0
        session = await self._ensure_session()
        await self._throttle()
        url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return 0.0
                body: dict = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            logger.warning("DexScreener fetch failed mint=%s… err=%s", mint[:8], exc)
            return 0.0
        pairs = body.get("pairs") or []
        if not isinstance(pairs, list) or not pairs:
            return 0.0
        best = 0.0
        for p in pairs:
            if not isinstance(p, dict):
                continue
            liq = (p.get("liquidity") or {}) if isinstance(p.get("liquidity"), dict) else {}
            try:
                u = float(liq.get("usd", 0) or 0)
            except (TypeError, ValueError):
                u = 0.0
            if u > best:
                best = u
        return best

    async def fetch_usdt_swap_inst_ids(
        self,
        *,
        exclude_bases: Collection[str] | None = None,
        altcoins_only: bool = True,
    ) -> list[str]:
        """List OKX USDT-margined perpetual ``instId`` values (e.g. ``DOGE-USDT-SWAP``).

        When *altcoins_only* is True, bases in :data:`DEFAULT_ALTCOIN_EXCLUDE_BASES`
        are dropped unless *exclude_bases* overrides the effective exclusion set.
        """
        body = await self._get(INSTRUMENTS_PATH, {"instType": "SWAP"})
        exclude: set[str]
        if exclude_bases is not None:
            exclude = {b.upper() for b in exclude_bases}
        elif altcoins_only:
            exclude = set(DEFAULT_ALTCOIN_EXCLUDE_BASES)
        else:
            exclude = set()

        out: list[str] = []
        for rec in body.get("data", []):
            iid = rec.get("instId") or ""
            if not iid.endswith("-USDT-SWAP"):
                continue
            state = rec.get("state", "")
            if state and state != "live":
                continue
            base = iid.replace("-USDT-SWAP", "")
            if base.upper() in exclude:
                continue
            out.append(iid)
        return out

    @staticmethod
    def base_from_swap_inst(inst_id: str) -> str:
        """``DOGE-USDT-SWAP`` → ``DOGE``."""
        return inst_id.replace("-USDT-SWAP", "")

    async def fetch_funding_rates_concurrent(
        self,
        symbols: list[str],
        *,
        max_concurrency: int = 40,
    ) -> list[dict[str, Any]]:
        """Concurrent current funding snapshot for many ``instId``s (one HTTP GET each).

        Failed symbols are skipped (logged at debug).
        """
        sem = asyncio.Semaphore(max(1, max_concurrency))

        async def _one(inst_id: str) -> dict[str, Any] | None:
            async with sem:
                try:
                    return await self.get_funding_rate(inst_id)
                except Exception:
                    logger.debug("funding fetch failed for %s", inst_id, exc_info=True)
                    return None

        tasks = [_one(sym) for sym in symbols]
        raw = await asyncio.gather(*tasks)
        rows: list[dict[str, Any]] = []
        for inst_id, fr in zip(symbols, raw, strict=True):
            if fr is None:
                continue
            rows.append({"instId": inst_id, **fr})
        return rows

    async def scan_extreme_negative_funding(
        self,
        *,
        min_negative_abs_pct: float = 0.05,
        symbols: list[str] | None = None,
        altcoins_only: bool = True,
        max_concurrency: int = 40,
    ) -> list[dict[str, Any]]:
        """All (or given) USDT swaps concurrently; keep *severely negative* funding.

        *min_negative_abs_pct* is the minimum |rate| in **percent per funding period**
        (e.g. ``0.05`` → funding must be ≤ ``-0.05`` %).

        Returns rows sorted by most negative rate first, with ``annualized_rate``,
        ``rate_pct``, ``base``, and timing fields for downstream orchestration.
        """
        syms = symbols
        if syms is None:
            syms = await self.fetch_usdt_swap_inst_ids(altcoins_only=altcoins_only)
        if not syms:
            return []

        snapshots = await self.fetch_funding_rates_concurrent(
            syms, max_concurrency=max_concurrency
        )

        qualified: list[dict[str, Any]] = []
        for row in snapshots:
            inst_id = row["instId"]
            raw_frac = float(row.get("fundingRate", 0))
            rate_pct = raw_frac * 100.0
            if rate_pct >= -min_negative_abs_pct:
                continue

            pred = float(row.get("nextFundingRate", 0))
            ann = self.compute_annualized_rate(rate_pct)
            nft_raw = row.get("nextFundingTime", "")
            try:
                nft_iso = datetime.fromtimestamp(
                    int(nft_raw) / 1000, tz=timezone.utc
                ).isoformat()
            except (ValueError, TypeError, OSError):
                nft_iso = str(nft_raw)

            qualified.append(
                {
                    "symbol": inst_id,
                    "base": self.base_from_swap_inst(inst_id),
                    "funding_rate_frac": raw_frac,
                    "rate_pct": round(rate_pct, 8),
                    "predicted_rate_pct": round(pred * 100.0, 8),
                    "annualized_rate": round(ann, 2),
                    "next_funding_time": nft_iso,
                }
            )

        qualified.sort(key=lambda x: x["rate_pct"])
        logger.info(
            "Extreme negative funding: %d / %d symbols (threshold -%.4f%% / period)",
            len(qualified),
            len(syms),
            min_negative_abs_pct,
        )
        return qualified

    # ------------------------------------------------------------------
    # Analytics
    # ------------------------------------------------------------------

    @staticmethod
    def compute_annualized_rate(
        rate: float, periods_per_day: int = FUNDING_PERIODS_PER_DAY
    ) -> float:
        """Annualise a single-period funding rate.

        Parameters
        ----------
        rate:
            Funding rate expressed as a *percentage* for one period
            (e.g. 0.01 means 0.01 %).
        periods_per_day:
            Number of funding settlements per day (default 3 for 8-h).

        Returns
        -------
        float
            Annualised rate in percent.
        """
        return rate * max(periods_per_day, 1) * 365

    @staticmethod
    def assess_persistence(history: list[dict]) -> dict:
        """Evaluate how persistent the funding rate has been.

        Parameters
        ----------
        history:
            List of dicts each having a ``fundingRate`` key (float, raw
            fraction -- *not* percentage).

        Returns
        -------
        dict
            Structured persistence assessment.
        """
        if not history:
            return {
                "avg_rate": 0.0,
                "std_rate": 0.0,
                "positive_pct": 0.0,
                "above_threshold_pct": 0.0,
                "trend": "stable",
                "persistence_score": 0.0,
                "mean_reversion_risk": 0.5,
            }

        rates = [h.get("fundingRate", 0) * 100 for h in history]  # → percentage
        n = len(rates)

        avg = sum(rates) / n if n else 0
        variance = sum((r - avg) ** 2 for r in rates) / n if n else 0
        std = variance ** 0.5

        positive_count = sum(1 for r in rates if r > 0)
        positive_pct = positive_count / n * 100 if n else 0

        # "Above threshold" = absolute value exceeds a common minimum (0.01 %)
        above_threshold_count = sum(1 for r in rates if abs(r) >= 0.01)
        above_threshold_pct = above_threshold_count / n * 100 if n else 0

        # ---- Trend detection (simple linear regression slope sign) ----
        # history is newest-first; reverse so index 0 = oldest.
        ordered = list(reversed(rates))
        if n >= 5:
            x_mean = (n - 1) / 2
            y_mean = avg
            num = sum((i - x_mean) * (ordered[i] - y_mean) for i in range(n))
            den = sum((i - x_mean) ** 2 for i in range(n))
            slope = num / den if den else 0.0

            if slope > 0.0005:
                trend = "increasing"
            elif slope < -0.0005:
                trend = "decreasing"
            else:
                trend = "stable"
        else:
            trend = "stable"

        # ---- Persistence score (0-1) ----
        # High if |avg| is large relative to std and above-threshold pct is high.
        consistency = 1.0 - min(std / (abs(avg) + 1e-9), 1.0)
        threshold_factor = above_threshold_pct / 100
        persistence_score = round(
            0.5 * consistency + 0.5 * threshold_factor, 4
        )
        persistence_score = max(0.0, min(1.0, persistence_score))

        # ---- Mean-reversion risk (0-1) ----
        # If the rate has been extreme for many consecutive recent periods
        # the chance of a snap-back is higher.
        recent = rates[:21]  # newest 7 days (~21 periods)
        if recent:
            recent_avg_abs = sum(abs(r) for r in recent) / len(recent)
            overall_avg_abs = sum(abs(r) for r in rates) / n if n else 0
            ratio = recent_avg_abs / (overall_avg_abs + 1e-9)
            # If recent magnitude is >1.5x overall, reversion risk is elevated.
            mean_reversion_risk = round(
                max(0.0, min(1.0, (ratio - 0.5) / 1.5)), 4
            )
        else:
            mean_reversion_risk = 0.5

        return {
            "avg_rate": round(avg, 6),
            "std_rate": round(std, 6),
            "positive_pct": round(positive_pct, 2),
            "above_threshold_pct": round(above_threshold_pct, 2),
            "trend": trend,
            "persistence_score": persistence_score,
            "mean_reversion_risk": mean_reversion_risk,
        }

    @staticmethod
    def estimate_pnl(
        rate: float,
        notional: float = 10_000,
        days: int = 7,
        periods_per_day: int = FUNDING_PERIODS_PER_DAY,
        round_trip_cost: float = DEFAULT_ROUND_TRIP_COST,
    ) -> dict:
        """Project funding-rate earnings for a delta-neutral position.

        Parameters
        ----------
        rate:
            Funding rate as a percentage per period (e.g. 0.03 → 0.03 %).
        notional:
            Position size in USD.
        days:
            Holding period.
        periods_per_day:
            Funding settlements per day.
        round_trip_cost:
            Total entry + exit cost as a fraction (default 0.1 %).

        Returns
        -------
        dict
            ``gross_pnl``, ``cost``, ``net_pnl``, ``annualized_return_pct``.
        """
        total_periods = days * max(periods_per_day, 1)
        rate_frac = abs(rate) / 100  # convert percentage to fraction
        gross = notional * rate_frac * total_periods
        cost = notional * round_trip_cost
        net = gross - cost
        ann_return = (net / max(notional, 1e-12)) * (365 / max(days, 1)) * 100

        return {
            "gross_pnl": round(gross, 2),
            "cost": round(cost, 2),
            "net_pnl": round(net, 2),
            "annualized_return_pct": round(ann_return, 2),
        }

    def analyze_opportunity(
        self,
        current_rate: float,
        history: list[dict],
        mark_price: float,
    ) -> dict:
        """Combine all analytics into a single opportunity assessment.

        Parameters
        ----------
        current_rate:
            Current funding rate as a *raw fraction* (e.g. 0.0003 for 0.03 %).
        history:
            Raw history list from :meth:`get_funding_history`.
        mark_price:
            Current mark price in USD.

        Returns
        -------
        dict
            Merged analytics suitable for the scan result schema.
        """
        rate_pct = current_rate * 100  # → percentage
        annualized = self.compute_annualized_rate(rate_pct)
        persistence = self.assess_persistence(history)
        pnl = self.estimate_pnl(rate_pct)

        # Direction logic
        if rate_pct > 0:
            direction = "short_perp"  # shorts receive funding
        else:
            direction = "long_perp"  # longs receive funding

        # Risk classification
        p_score = persistence["persistence_score"]
        mr_risk = persistence["mean_reversion_risk"]
        if p_score >= 0.6 and mr_risk <= 0.4:
            risk_level = "low"
        elif p_score >= 0.35 or mr_risk <= 0.65:
            risk_level = "medium"
        else:
            risk_level = "high"

        # Human-readable recommendation
        rec_parts: list[str] = []
        if abs(annualized) >= self.annualized_threshold:
            rec_parts.append(
                f"Annualised {abs(annualized):.1f}% via {direction.replace('_', ' ')}"
            )
        if persistence["trend"] == "increasing" and rate_pct > 0:
            rec_parts.append("positive rate trending higher — larger long crowding / carry")
        elif persistence["trend"] == "decreasing" and rate_pct > 0:
            rec_parts.append("positive rate cooling — carry may compress")
        if mr_risk >= 0.6:
            rec_parts.append("elevated mean-reversion risk -- consider smaller size")
        if risk_level == "high":
            rec_parts.append("high risk -- monitor closely")
        recommendation = ". ".join(rec_parts) if rec_parts else "Below threshold."

        return {
            "current_rate": round(rate_pct, 6),
            "annualized_rate": round(annualized, 2),
            "direction": direction,
            "persistence_score": p_score,
            "avg_rate_7d": persistence["avg_rate"],
            "std_rate_7d": persistence["std_rate"],
            "estimated_weekly_pnl_usd": pnl["net_pnl"],
            "risk_level": risk_level,
            "recommendation": recommendation,
            "mark_price": mark_price,
        }

    # ------------------------------------------------------------------
    # Top-level scan
    # ------------------------------------------------------------------

    async def scan(self) -> list[dict]:
        """Scan all configured symbols and return **positive** funding opportunities.

        Returns
        -------
        list[dict]
            Longs-pay-shorts regimes only, sorted by annualised rate descending.
            ``execute_delta_neutral_buy_compat`` is True when a Solana mint is mapped
            and DexScreener spot liquidity ≥ ``min_dex_spot_pool_usd``.
        """
        logger.info(
            "Starting extreme-positive funding scan for %d symbols", len(self.symbols)
        )

        results: list[dict] = []

        async def _process(symbol: str) -> dict | None:
            try:
                funding, history, mark = await asyncio.gather(
                    self.get_funding_rate(symbol),
                    self.get_funding_history(symbol),
                    self.get_mark_price(symbol),
                )
            except Exception:
                logger.exception("Failed to fetch data for %s", symbol)
                return None

            current_rate = funding["fundingRate"]
            predicted_rate = funding["nextFundingRate"]

            if self.require_positive_funding:
                if current_rate <= 0:
                    return None
                if current_rate * 100 < self.min_rate_threshold:
                    logger.debug(
                        "%s positive rate %.6f%% below threshold",
                        symbol,
                        current_rate * 100,
                    )
                    return None
            else:
                if abs(current_rate * 100) < self.min_rate_threshold:
                    return None

            analysis = self.analyze_opportunity(current_rate, history, mark)

            ann = analysis["annualized_rate"]
            if self.require_positive_funding:
                if ann < self.annualized_threshold:
                    return None
            else:
                if abs(ann) < self.annualized_threshold:
                    return None

            base = okx_swap_inst_to_base(symbol)
            sol_mint = OKX_BASE_TO_SOLANA_MINT.get(base, "") or ""
            hedge_symbol = okx_swap_inst_to_hedge_symbol(symbol)
            dex_liq = 0.0
            execute_delta_neutral_buy_compat = False
            if sol_mint:
                dex_liq = await self.fetch_dex_spot_liquidity_usd(sol_mint)
                if dex_liq >= self.min_dex_spot_pool_usd:
                    execute_delta_neutral_buy_compat = True

            # Derive next funding time as ISO string
            nft_raw = funding.get("nextFundingTime", "")
            try:
                nft_iso = datetime.fromtimestamp(
                    int(nft_raw) / 1000, tz=timezone.utc
                ).isoformat()
            except (ValueError, TypeError, OSError):
                nft_iso = nft_raw

            return {
                "symbol": symbol,
                "okx_inst_id": symbol,
                "base_asset": base,
                "hedge_symbol": hedge_symbol,
                "solana_mint": sol_mint or None,
                "dex_spot_liquidity_usd": round(dex_liq, 2),
                "execute_delta_neutral_buy_compat": execute_delta_neutral_buy_compat,
                "predicted_rate": round(predicted_rate * 100, 6),
                "next_funding_time": nft_iso,
                **analysis,
            }

        tasks = [_process(sym) for sym in self.symbols]
        raw = await asyncio.gather(*tasks, return_exceptions=True)

        for entry in raw:
            if isinstance(entry, Exception):
                logger.warning("Task failed: %s", entry)
                continue
            if entry is not None:
                results.append(entry)

        results.sort(key=lambda x: x["annualized_rate"], reverse=True)

        logger.info(
            "Scan complete: %d positive-funding opportunities from %d symbols "
            "(%d armed for delta-neutral execute)",
            len(results),
            len(self.symbols),
            sum(1 for r in results if r.get("execute_delta_neutral_buy_compat")),
        )
        return results
