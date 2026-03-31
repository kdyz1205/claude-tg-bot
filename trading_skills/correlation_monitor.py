"""
Correlation Hedge Monitor — production-grade portfolio correlation analyser.

Tracks rolling pairwise correlations across crypto positions (BTCUSDT,
ETHUSDT, SOLUSDT, HYPEUSDT), computes correlation-adjusted risk metrics,
detects regime shifts in inter-asset relationships, and suggests hedging
trades when portfolio concentration exceeds safe thresholds.

Dependencies: numpy (no TA-Lib).
Expected price input: 1-D numpy arrays of close prices per symbol.
Position format: {symbol: {"side": "long"|"short", "size": float, "entry_price": float}}
"""

from __future__ import annotations

import logging
import warnings
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SUPPORTED_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "HYPEUSDT")
_MARKET_PROXY = "BTCUSDT"

# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _log_returns(prices: np.ndarray) -> np.ndarray:
    """Compute log returns from a 1-D price array.

    Returns an array of length ``len(prices) - 1``.  Leading NaN / zero /
    negative values are silently clamped to avoid ``log(0)`` errors.
    """
    prices = np.asarray(prices, dtype=np.float64)
    if prices.ndim != 1 or len(prices) < 2:
        raise ValueError("prices must be a 1-D array with at least 2 elements")
    # Replace NaN with forward-fill, then clamp non-positive
    mask = np.isnan(prices)
    if mask.any():
        prices = prices.copy()
        for i in range(len(prices)):
            if mask[i] and i > 0:
                prices[i] = prices[i - 1]
    # Guard against non-positive prices (bad data)
    safe = np.clip(prices, 1e-12, None)
    return np.diff(np.log(safe))


def _ewm_weights(length: int, span: int) -> np.ndarray:
    """Exponential weights vector (most recent = highest weight).

    Mirrors pandas ``ewm(span=span)`` decay: ``alpha = 2 / (span + 1)``.
    """
    alpha = 2.0 / (span + 1)
    w = np.power(1 - alpha, np.arange(length - 1, -1, -1, dtype=np.float64))
    return w / w.sum()


def _weighted_corr(
    x: np.ndarray, y: np.ndarray, weights: np.ndarray
) -> float:
    """Weighted Pearson correlation between *x* and *y*."""
    if len(x) != len(y) or len(x) != len(weights):
        raise ValueError("x, y, and weights must have the same length")
    if len(x) < 2:
        return 0.0
    # Filter out NaN entries
    valid = np.isfinite(x) & np.isfinite(y) & np.isfinite(weights)
    if valid.sum() < 2:
        return 0.0
    x, y, weights = x[valid], y[valid], weights[valid]
    w_sum = weights.sum()
    if w_sum == 0:
        return 0.0
    w = weights / w_sum
    mx = np.dot(w, x)
    my = np.dot(w, y)
    dx = x - mx
    dy = y - my
    cov = np.dot(w, dx * dy)
    sx = np.sqrt(np.dot(w, dx * dx))
    sy = np.sqrt(np.dot(w, dy * dy))
    denom = sx * sy
    if denom < 1e-15:
        return 0.0
    return float(np.clip(cov / denom, -1.0, 1.0))


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------


class CorrelationHedgeMonitor:
    """Portfolio correlation tracker and hedge adviser.

    Parameters
    ----------
    lookback : int
        Number of bars used for rolling correlation (default 90).
    high_corr_threshold : float
        Correlation above which two positions are considered "highly correlated"
        (default 0.75).
    rebalance_threshold : float
        Minimum change in portfolio weight before a rebalance suggestion is
        emitted (default 0.15).
    """

    def __init__(
        self,
        lookback: int = 90,
        high_corr_threshold: float = 0.75,
        rebalance_threshold: float = 0.15,
    ) -> None:
        if lookback < 10:
            raise ValueError("lookback must be >= 10")
        if not 0.0 < high_corr_threshold < 1.0:
            raise ValueError("high_corr_threshold must be in (0, 1)")

        self.lookback = lookback
        self.high_corr_threshold = high_corr_threshold
        self.rebalance_threshold = rebalance_threshold

        # {symbol: np.ndarray of close prices}
        self._prices: Dict[str, np.ndarray] = {}
        self._max_symbols: int = 50  # prevent unbounded symbol accumulation
        self._max_price_bars: int = max(lookback * 3, 500)  # trim stored prices
        # Cached correlation matrix {(sym_a, sym_b): float}
        self._corr_matrix: Dict[Tuple[str, str], float] = {}
        # Historical correlation snapshots for regime-shift detection
        # {(sym_a, sym_b): list[float]}  — most recent appended last
        self._corr_history: Dict[Tuple[str, str], List[float]] = {}
        self._max_corr_pairs: int = 200  # prevent unbounded pair accumulation

    # ------------------------------------------------------------------
    # Price ingestion
    # ------------------------------------------------------------------

    def update_prices(self, symbol: str, prices: np.ndarray) -> None:
        """Feed latest close prices for *symbol*.

        Parameters
        ----------
        symbol : str
            Trading pair (e.g. ``"BTCUSDT"``).
        prices : np.ndarray
            1-D array of close prices, chronologically ordered.  At least
            ``lookback + 1`` bars are recommended for meaningful statistics.
        """
        prices = np.asarray(prices, dtype=np.float64).ravel()
        if len(prices) < 2:
            logger.warning(
                "update_prices: %s received only %d price(s) — need >= 2",
                symbol,
                len(prices),
            )
            return
        # Trim stored prices to prevent unbounded memory growth
        if len(prices) > self._max_price_bars:
            prices = prices[-self._max_price_bars:]
        # Prevent unbounded symbol accumulation
        if symbol not in self._prices and len(self._prices) >= self._max_symbols:
            logger.warning(
                "update_prices: symbol limit (%d) reached, ignoring %s",
                self._max_symbols, symbol,
            )
            return
        self._prices[symbol] = prices
        # Invalidate cached matrix so next access recomputes
        self._corr_matrix.clear()

    # ------------------------------------------------------------------
    # Correlation matrix
    # ------------------------------------------------------------------

    def compute_correlation_matrix(self) -> Dict[Tuple[str, str], float]:
        """Compute full pairwise rolling correlation matrix.

        Uses log returns over the most recent ``lookback`` bars with
        exponential weighting (span = lookback).

        Returns
        -------
        dict
            Mapping ``(symbol_a, symbol_b) -> correlation`` for every
            unordered pair.  Self-correlations are omitted.
        """
        if self._corr_matrix:
            return dict(self._corr_matrix)

        symbols = sorted(self._prices.keys())
        if len(symbols) < 2:
            logger.debug("compute_correlation_matrix: fewer than 2 symbols loaded")
            return {}

        # Pre-compute log returns trimmed to common length
        returns_map: Dict[str, np.ndarray] = {}
        min_len = None
        for sym in symbols:
            lr = _log_returns(self._prices[sym])
            returns_map[sym] = lr
            if min_len is None or len(lr) < min_len:
                min_len = len(lr)

        if min_len is None or min_len < 2:
            return {}

        # Trim to common window (most recent bars)
        window = min(min_len, self.lookback)
        weights = _ewm_weights(window, span=self.lookback)

        matrix: Dict[Tuple[str, str], float] = {}
        for i, sym_a in enumerate(symbols):
            ra = returns_map[sym_a][-window:]
            for sym_b in symbols[i + 1 :]:
                rb = returns_map[sym_b][-window:]
                corr = _weighted_corr(ra, rb, weights)
                key = (sym_a, sym_b)
                matrix[key] = corr
                # Store history for regime-shift detection (bounded to prevent memory leak)
                if key not in self._corr_history and len(self._corr_history) >= self._max_corr_pairs:
                    # Evict oldest pair history to prevent unbounded dict growth
                    oldest_key = next(iter(self._corr_history))
                    del self._corr_history[oldest_key]
                hist = self._corr_history.setdefault(key, [])
                hist.append(corr)
                if len(hist) > 500:
                    self._corr_history[key] = hist[-500:]

        self._corr_matrix = matrix
        return dict(matrix)

    # ------------------------------------------------------------------
    # Portfolio analysis
    # ------------------------------------------------------------------

    def analyze_portfolio(self, positions: Dict[str, dict]) -> Dict[str, Any]:
        """Assess portfolio concentration and correlation risk.

        Parameters
        ----------
        positions : dict
            ``{symbol: {"side": "long"|"short", "size": float, "entry_price": float}}``

        Returns
        -------
        dict
            Keys: ``correlation_matrix``, ``concentration_risk``,
            ``effective_bets``, ``directional_bias``, ``corr_adjusted_var``,
            ``warnings``, ``risk_level``.
        """
        if not positions:
            return {
                "correlation_matrix": {},
                "concentration_risk": 0.0,
                "effective_bets": 0.0,
                "directional_bias": 0.0,
                "corr_adjusted_var": 0.0,
                "warnings": ["No open positions"],
                "risk_level": "low",
            }

        corr = self.compute_correlation_matrix()
        warnings_list: List[str] = []

        # --- Position weights (absolute dollar size) ---
        total_size = sum(abs(p.get("size", 0)) for p in positions.values())
        if total_size == 0:
            return {
                "correlation_matrix": corr,
                "concentration_risk": 0.0,
                "effective_bets": 0.0,
                "directional_bias": 0.0,
                "corr_adjusted_var": 0.0,
                "warnings": ["All positions have zero size"],
                "risk_level": "low",
            }

        weights: Dict[str, float] = {
            sym: abs(p.get("size", 0)) / total_size for sym, p in positions.items()
        }

        # --- Effective number of bets = 1 / sum(w_i^2) ---
        hhi = sum(w ** 2 for w in weights.values())
        effective_bets = 1.0 / hhi if hhi > 0 else 0.0

        # --- Concentration risk (0 = perfect diversification, 1 = single bet) ---
        n = len(positions)
        if n > 1:
            # HHI normalised: (HHI - 1/n) / (1 - 1/n)
            concentration_risk = float(
                np.clip((hhi - 1.0 / n) / (1.0 - 1.0 / n), 0.0, 1.0)
            )
        else:
            concentration_risk = 1.0

        # --- Directional bias: net signed exposure / gross ---
        signed_total = sum(
            p.get("size", 0) * (1.0 if p.get("side") == "long" else -1.0)
            for p in positions.values()
        )
        directional_bias = float(np.clip(signed_total / total_size, -1.0, 1.0))

        # --- Correlation-adjusted variance ---
        # V = sum_i sum_j w_i * w_j * sigma_i * sigma_j * rho_ij * d_i * d_j
        # where d_i = +1 for long, -1 for short
        # We approximate sigma_i from log-return std of available data.
        syms = sorted(positions.keys())
        m = len(syms)
        vol = np.ones(m, dtype=np.float64) * 0.02  # default 2% daily vol
        dirs = np.ones(m, dtype=np.float64)
        w_vec = np.zeros(m, dtype=np.float64)

        for idx, sym in enumerate(syms):
            w_vec[idx] = weights[sym]
            dirs[idx] = 1.0 if positions[sym].get("side") == "long" else -1.0
            if sym in self._prices and len(self._prices[sym]) > 2:
                lr = _log_returns(self._prices[sym])
                vol[idx] = float(np.std(lr[-self.lookback :]))

        # Build correlation sub-matrix
        rho = np.eye(m, dtype=np.float64)
        for i in range(m):
            for j in range(i + 1, m):
                key = tuple(sorted((syms[i], syms[j])))
                c = corr.get(key, 0.0)
                rho[i, j] = c
                rho[j, i] = c

        # Signed weight vector: w_i * d_i * vol_i
        sw = w_vec * dirs * vol
        corr_adj_var = float(sw @ rho @ sw)

        # --- Warnings ---
        if concentration_risk > 0.7:
            warnings_list.append(
                f"High concentration risk ({concentration_risk:.2f}) — "
                f"portfolio dominated by few positions"
            )
        if abs(directional_bias) > 0.8:
            direction = "long" if directional_bias > 0 else "short"
            warnings_list.append(
                f"Extreme directional bias ({directional_bias:+.2f}) — "
                f"heavily {direction}"
            )

        # Flag highly correlated same-direction pairs
        for i in range(m):
            for j in range(i + 1, m):
                key = tuple(sorted((syms[i], syms[j])))
                c = corr.get(key, 0.0)
                same_dir = dirs[i] * dirs[j] > 0
                if c > self.high_corr_threshold and same_dir:
                    warnings_list.append(
                        f"{syms[i]}/{syms[j]} correlation {c:.2f} with same "
                        f"direction — effectively one bet"
                    )

        if corr_adj_var > 0.05:
            warnings_list.append(
                f"Correlation-adjusted portfolio variance is elevated "
                f"({corr_adj_var:.4f})"
            )

        # --- Risk level ---
        risk_score = (
            0.3 * concentration_risk
            + 0.3 * abs(directional_bias)
            + 0.4 * min(corr_adj_var / 0.05, 1.0)
        )
        if risk_score > 0.75:
            risk_level = "critical"
        elif risk_score > 0.50:
            risk_level = "high"
        elif risk_score > 0.25:
            risk_level = "medium"
        else:
            risk_level = "low"

        return {
            "correlation_matrix": corr,
            "concentration_risk": round(concentration_risk, 4),
            "effective_bets": round(effective_bets, 2),
            "directional_bias": round(directional_bias, 4),
            "corr_adjusted_var": round(corr_adj_var, 6),
            "warnings": warnings_list,
            "risk_level": risk_level,
        }

    # ------------------------------------------------------------------
    # Hedge suggestions
    # ------------------------------------------------------------------

    def suggest_hedge(
        self, positions: Dict[str, dict]
    ) -> List[Dict[str, Any]]:
        """Suggest hedging trades to reduce portfolio correlation risk.

        Parameters
        ----------
        positions : dict
            Current positions (same format as :meth:`analyze_portfolio`).

        Returns
        -------
        list[dict]
            Each entry: ``{"action", "symbol", "reason", "urgency"}``.
            ``urgency`` is 0-1 (1 = immediately required).
        """
        analysis = self.analyze_portfolio(positions)
        corr = analysis["correlation_matrix"]
        suggestions: List[Dict[str, Any]] = []

        if not positions:
            return suggestions

        total_size = sum(abs(p.get("size", 0)) for p in positions.values())
        if total_size == 0:
            return suggestions

        # Determine dominant direction
        net_signed = sum(
            p.get("size", 0) * (1.0 if p.get("side") == "long" else -1.0)
            for p in positions.values()
        )
        dominant_side = "long" if net_signed >= 0 else "short"
        hedge_action = "short" if dominant_side == "long" else "long"

        # All tracked symbols, including those not in portfolio
        all_syms = set(_SUPPORTED_SYMBOLS) | set(positions.keys())
        held_syms = set(positions.keys())

        # 1. Suggest counter-positions on un-held symbols with low / negative
        #    correlation to the portfolio
        for candidate in sorted(all_syms - held_syms):
            # Average correlation of candidate with held symbols
            corrs_with_held = []
            for held in held_syms:
                key = tuple(sorted((candidate, held)))
                if key in corr:
                    corrs_with_held.append(corr[key])
            if not corrs_with_held:
                continue
            avg_corr = float(np.mean(corrs_with_held))
            if avg_corr < 0:
                urgency = min(
                    1.0,
                    abs(analysis["directional_bias"]) * 0.9 + 0.1,
                )
                suggestions.append(
                    {
                        "action": dominant_side,  # same direction amplifies hedge
                        "symbol": candidate,
                        "reason": (
                            f"Negative avg correlation ({avg_corr:.2f}) — "
                            f"natural hedge candidate"
                        ),
                        "urgency": round(urgency, 2),
                    }
                )
            elif avg_corr < 0.3:
                urgency = min(1.0, analysis["concentration_risk"] * 0.8 + 0.2)
                suggestions.append(
                    {
                        "action": hedge_action,
                        "symbol": candidate,
                        "reason": (
                            f"Low avg correlation ({avg_corr:.2f}) with portfolio — "
                            f"adds diversification"
                        ),
                        "urgency": round(urgency, 2),
                    }
                )

        # 2. If highly correlated same-direction pairs exist, suggest reducing
        #    one leg or shorting one of them
        syms = sorted(positions.keys())
        for i in range(len(syms)):
            for j in range(i + 1, len(syms)):
                key = tuple(sorted((syms[i], syms[j])))
                c = corr.get(key, 0.0)
                pi, pj = positions[syms[i]], positions[syms[j]]
                same_dir = (pi.get("side") == pj.get("side"))
                if c > self.high_corr_threshold and same_dir:
                    # Suggest shorting the smaller position's symbol
                    smaller = syms[i] if pi.get("size", 0) <= pj.get("size", 0) else syms[j]
                    counter = "short" if pi.get("side") == "long" else "long"
                    urgency = min(1.0, (c - self.high_corr_threshold) / 0.25 * 0.6 + 0.4)
                    suggestions.append(
                        {
                            "action": counter,
                            "symbol": smaller,
                            "reason": (
                                f"Highly correlated ({c:.2f}) with "
                                f"{syms[j] if smaller == syms[i] else syms[i]} — "
                                f"reduce redundant exposure"
                            ),
                            "urgency": round(min(urgency, 1.0), 2),
                        }
                    )

        # 3. If extreme directional bias, suggest counter-directional trade
        #    on lowest-beta asset
        if abs(analysis["directional_bias"]) > 0.7:
            betas = self._compute_betas()
            # Pick the held asset with the lowest absolute beta
            candidates = [
                (sym, abs(betas.get(sym, 1.0))) for sym in held_syms
                if sym != _MARKET_PROXY
            ]
            if candidates:
                candidates.sort(key=lambda x: x[1])
                best_sym = candidates[0][0]
                urgency = min(1.0, abs(analysis["directional_bias"]) * 0.8)
                suggestions.append(
                    {
                        "action": hedge_action,
                        "symbol": best_sym,
                        "reason": (
                            f"Extreme directional bias "
                            f"({analysis['directional_bias']:+.2f}) — "
                            f"{best_sym} has lowest beta ({candidates[0][1]:.2f})"
                        ),
                        "urgency": round(urgency, 2),
                    }
                )

        # Deduplicate by (action, symbol), keep highest urgency
        seen: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for s in suggestions:
            k = (s.get("action", ""), s.get("symbol", ""))
            if k not in seen or s.get("urgency", 0) > seen[k].get("urgency", 0):
                seen[k] = s
        suggestions = sorted(seen.values(), key=lambda x: -x["urgency"])

        return suggestions

    # ------------------------------------------------------------------
    # Regime-shift detection
    # ------------------------------------------------------------------

    def detect_regime_shift(
        self, symbol_pair: Tuple[str, str]
    ) -> Dict[str, Any]:
        """Detect correlation regime shift for a symbol pair.

        Compares the most recent 20-bar correlation against the full
        historical lookback.  A shift is flagged when ``|delta| > 0.3``.

        Parameters
        ----------
        symbol_pair : tuple[str, str]
            E.g. ``("BTCUSDT", "ETHUSDT")``.

        Returns
        -------
        dict
            Keys: ``pair``, ``current_corr``, ``historical_corr``, ``delta``,
            ``is_shift``, ``shift_type`` (``"breakdown"`` | ``"convergence"``
            | ``None``).
        """
        key = tuple(sorted(symbol_pair))
        result: Dict[str, Any] = {
            "pair": key,
            "current_corr": None,
            "historical_corr": None,
            "delta": 0.0,
            "is_shift": False,
            "shift_type": None,
        }

        sym_a, sym_b = key
        if sym_a not in self._prices or sym_b not in self._prices:
            logger.debug(
                "detect_regime_shift: missing price data for %s/%s", sym_a, sym_b
            )
            return result

        lr_a = _log_returns(self._prices[sym_a])
        lr_b = _log_returns(self._prices[sym_b])
        common = min(len(lr_a), len(lr_b))
        if common < 25:
            logger.debug(
                "detect_regime_shift: only %d common bars for %s/%s (need >= 25)",
                common,
                sym_a,
                sym_b,
            )
            return result

        # Recent window (20 bars)
        recent_window = 20
        weights_recent = _ewm_weights(recent_window, span=recent_window)
        current = _weighted_corr(
            lr_a[-recent_window:], lr_b[-recent_window:], weights_recent
        )

        # Historical window (full lookback)
        hist_window = min(common, self.lookback)
        weights_hist = _ewm_weights(hist_window, span=self.lookback)
        historical = _weighted_corr(
            lr_a[-hist_window:], lr_b[-hist_window:], weights_hist
        )

        delta = current - historical

        is_shift = abs(delta) > 0.3
        shift_type: Optional[str] = None
        if is_shift:
            shift_type = "breakdown" if delta < 0 else "convergence"

        result.update(
            {
                "current_corr": round(current, 4),
                "historical_corr": round(historical, 4),
                "delta": round(delta, 4),
                "is_shift": is_shift,
                "shift_type": shift_type,
            }
        )

        if is_shift:
            logger.info(
                "Regime shift detected: %s/%s  Δρ = %+.3f (%s)",
                sym_a,
                sym_b,
                delta,
                shift_type,
            )

        return result

    # ------------------------------------------------------------------
    # Diversification score
    # ------------------------------------------------------------------

    def get_diversification_score(
        self, positions: Dict[str, dict]
    ) -> float:
        """Compute a portfolio diversification score in [0, 1].

        Combines three factors:
        - Effective number of bets (weight dispersion)
        - Average pairwise correlation penalty
        - Directional balance

        A score of 1.0 = perfectly diversified; 0.0 = fully concentrated.

        Parameters
        ----------
        positions : dict
            Same format as :meth:`analyze_portfolio`.

        Returns
        -------
        float
        """
        if not positions:
            return 0.0

        n = len(positions)
        total_size = sum(abs(p.get("size", 0)) for p in positions.values())
        if total_size == 0:
            return 0.0

        weights = {
            sym: abs(p.get("size", 0)) / total_size for sym, p in positions.items()
        }

        # Factor 1: weight entropy score  (1 when equal-weighted)
        hhi = sum(w ** 2 for w in weights.values())
        max_hhi = 1.0  # single position
        min_hhi = 1.0 / n if n > 0 else 1.0
        if max_hhi - min_hhi > 0:
            weight_score = 1.0 - (hhi - min_hhi) / (max_hhi - min_hhi)
        else:
            weight_score = 1.0

        # Factor 2: correlation penalty
        corr = self.compute_correlation_matrix()
        syms = sorted(positions.keys())
        dirs = {
            sym: (1.0 if positions[sym].get("side") == "long" else -1.0)
            for sym in syms
        }

        if len(syms) >= 2 and corr:
            pair_penalties = []
            for i in range(len(syms)):
                for j in range(i + 1, len(syms)):
                    key = tuple(sorted((syms[i], syms[j])))
                    c = corr.get(key, 0.0)
                    # Same direction + high positive corr → bad
                    # Opposite direction + high positive corr → good (natural hedge)
                    effective_corr = c * dirs[syms[i]] * dirs[syms[j]]
                    # effective_corr in [-1, 1]; 1 = maximally redundant
                    pair_penalties.append(effective_corr)
            avg_eff_corr = float(np.mean(pair_penalties))
            # Transform: -1 (perfect hedge) → 1.0 score, +1 (clone) → 0.0
            corr_score = float(np.clip((1.0 - avg_eff_corr) / 2.0, 0.0, 1.0))
        else:
            corr_score = 0.0  # single position = no diversification

        # Factor 3: directional balance
        net_signed = sum(
            p.get("size", 0) * (1.0 if p.get("side") == "long" else -1.0)
            for p in positions.values()
        )
        dir_balance = 1.0 - abs(net_signed / total_size)

        # Weighted combination
        score = 0.35 * weight_score + 0.40 * corr_score + 0.25 * dir_balance
        return round(float(np.clip(score, 0.0, 1.0)), 4)

    # ------------------------------------------------------------------
    # Beta calculation (vs BTC)
    # ------------------------------------------------------------------

    def _compute_betas(self) -> Dict[str, float]:
        """Compute beta of each loaded symbol vs BTC (market proxy).

        Beta = Cov(r_asset, r_market) / Var(r_market).

        Returns
        -------
        dict
            ``{symbol: beta}``.  BTC itself has beta = 1.0 by definition.
        """
        if _MARKET_PROXY not in self._prices:
            return {}

        lr_market = _log_returns(self._prices[_MARKET_PROXY])
        window = min(len(lr_market), self.lookback)
        if window < 5:
            return {}

        market_slice = lr_market[-window:]
        var_market = float(np.var(market_slice))
        if var_market < 1e-15:
            return {}

        betas: Dict[str, float] = {_MARKET_PROXY: 1.0}
        weights = _ewm_weights(window, span=self.lookback)

        for sym, prices in self._prices.items():
            if sym == _MARKET_PROXY:
                continue
            lr = _log_returns(prices)
            common = min(len(lr), window)
            if common < 5:
                continue
            asset_slice = lr[-common:]
            mkt = market_slice[-common:]
            w = weights[-common:]
            w = w / w.sum()

            ma = np.dot(w, asset_slice)
            mm = np.dot(w, mkt)
            cov = np.dot(w, (asset_slice - ma) * (mkt - mm))
            var_m = np.dot(w, (mkt - mm) ** 2)
            if var_m < 1e-15:
                continue
            betas[sym] = round(float(cov / var_m), 4)

        return betas

    def get_betas(self) -> Dict[str, float]:
        """Public accessor for per-asset betas vs BTC.

        Returns
        -------
        dict
            ``{symbol: beta}``.
        """
        return self._compute_betas()

    # ------------------------------------------------------------------
    # Repr
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        loaded = list(self._prices.keys())
        return (
            f"CorrelationHedgeMonitor(lookback={self.lookback}, "
            f"symbols={loaded})"
        )
