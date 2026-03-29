"""
Funding Rate Arbitrage Scanner for OKX perpetual futures.

Scans OKX USDT-margined perpetual swaps for funding-rate arbitrage
opportunities.  When the funding rate deviates meaningfully from zero a
delta-neutral strategy (short perp + long spot, or vice-versa) can harvest
the periodic funding payments.

The scanner:
  1. Fetches current and predicted funding rates for a configurable symbol list.
  2. Computes annualised rates and filters by a minimum threshold.
  3. Analyses historical persistence, trend, and mean-reversion risk.
  4. Estimates expected PnL net of round-trip trading costs.
  5. Returns opportunities ranked by annualised rate.

Designed for production use: proper session management, rate limiting,
graceful error handling, structured logging, and full type coverage.
"""

from __future__ import annotations

import asyncio
import logging
import time
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

# ---------------------------------------------------------------------------
# Default instrument list -- major USDT-margined perpetual swaps
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
]

# Assumed round-trip trading cost (taker fee both legs) as a fraction.
DEFAULT_ROUND_TRIP_COST = 0.001  # 0.1%

# OKX funds every 8 h → 3 periods per day.
FUNDING_PERIODS_PER_DAY = 3


class FundingRateScanner:
    """Async scanner for funding-rate arbitrage on OKX perpetual swaps.

    Parameters
    ----------
    symbols:
        OKX instrument IDs to monitor.  Defaults to *DEFAULT_SYMBOLS*.
    min_rate_threshold:
        Minimum absolute funding rate (in %) per period to flag.
        Default ``0.01`` means 0.01 %.
    annualized_threshold:
        Minimum annualised rate (in %) for an opportunity to be included.
        Default ``10.0`` %.
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
        max_rps: int = 10,
        session_timeout: float = 10.0,
    ) -> None:
        self.symbols = symbols or list(DEFAULT_SYMBOLS)
        self.min_rate_threshold = min_rate_threshold
        self.annualized_threshold = annualized_threshold

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
        return {
            "fundingRate": float(rec.get("fundingRate", 0)),
            "nextFundingRate": float(rec.get("nextFundingRate", 0)),
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
            rows.append(
                {
                    "fundingRate": float(rec.get("fundingRate", 0)),
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

        rates = [h["fundingRate"] * 100 for h in history]  # → percentage
        n = len(rates)

        avg = sum(rates) / n
        variance = sum((r - avg) ** 2 for r in rates) / n
        std = variance ** 0.5

        positive_count = sum(1 for r in rates if r > 0)
        positive_pct = positive_count / n * 100

        # "Above threshold" = absolute value exceeds a common minimum (0.01 %)
        above_threshold_count = sum(1 for r in rates if abs(r) >= 0.01)
        above_threshold_pct = above_threshold_count / n * 100

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
            overall_avg_abs = sum(abs(r) for r in rates) / n
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
            rec_parts.append("rate trending higher -- favorable")
        elif persistence["trend"] == "decreasing" and rate_pct < 0:
            rec_parts.append("rate trending more negative -- favorable")
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
        """Scan all configured symbols and return ranked opportunities.

        Returns
        -------
        list[dict]
            Opportunities sorted descending by absolute annualised rate.
            Only includes entries whose annualised rate meets the threshold.
        """
        logger.info(
            "Starting funding-rate scan for %d symbols", len(self.symbols)
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

            # Quick threshold gate (raw fraction → pct)
            if abs(current_rate * 100) < self.min_rate_threshold:
                logger.debug(
                    "%s rate %.6f%% below threshold", symbol, current_rate * 100
                )
                return None

            analysis = self.analyze_opportunity(current_rate, history, mark)

            if abs(analysis["annualized_rate"]) < self.annualized_threshold:
                return None

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

        results.sort(key=lambda x: abs(x["annualized_rate"]), reverse=True)

        logger.info(
            "Scan complete: %d opportunities from %d symbols",
            len(results),
            len(self.symbols),
        )
        return results
