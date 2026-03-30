"""
strategy_hypothesis_loop.py — Layer 3+4 End-to-End Demo

完整闭环:
  1. 跑 baseline 回测 (MA Ribbon, 默认参数) → Sharpe
  2. Claude 生成假设 ("MA周期从X改到Y因为...")
  3. 跑 experiment 回测 (修改后参数) → Sharpe
  4. 比较两次分数 → 证实/否决假设
  5. 结晶为 Skill (Layer 3) + 记录假设 (Layer 4)

Usage:  python strategy_hypothesis_loop.py
"""

import asyncio
import json
import os
import sys
import time
import math
from pathlib import Path
from datetime import datetime

# ── paths ──
BOT_DIR = Path(__file__).parent
CRYPTO_DIR = Path(r"C:\Users\alexl\Desktop\crypto-analysis-")
if CRYPTO_DIR.is_dir():
    sys.path.insert(0, str(CRYPTO_DIR))
sys.path.insert(0, str(BOT_DIR))

# ── imports from existing code ──
try:
    import numpy as np
except ImportError:
    np = None  # Deferred: will fail with clear message at first use

# ═══════════════════════════════════════════════════════════════════════════════
# STEP 0: Shared utilities (copied from ma_ribbon_backtest.py to be self-contained)
# ═══════════════════════════════════════════════════════════════════════════════

try:
    import httpx
except ImportError:
    httpx = None  # Deferred: will fail with clear message at first use


def fetch_top_symbols(n=5):
    """Fetch top N USDT-SWAP symbols by 24h volume from OKX."""
    if httpx is None:
        raise ImportError("httpx is required for strategy_hypothesis_loop: pip install httpx")
    r = httpx.get("https://www.okx.com/api/v5/market/tickers?instType=SWAP", timeout=15)
    r.raise_for_status()
    resp = r.json()
    data = resp.get("data")
    if not data:
        raise ValueError(f"OKX API returned no data: {resp.get('msg', 'unknown error')}")
    usdt = [d for d in data if d["instId"].endswith("-USDT-SWAP")]
    usdt.sort(key=lambda x: float(x.get("volCcy24h", 0) or 0), reverse=True)
    return [d["instId"] for d in usdt[:n]]


def fetch_ohlcv(inst_id, bar="1H", n_pages=4):
    """Fetch OHLCV from OKX. Returns (open, high, low, close, volume) numpy arrays."""
    rows = []
    after = ""
    for _ in range(n_pages):
        params = {"instId": inst_id, "bar": bar, "limit": "100"}
        if after:
            params["after"] = after
        try:
            r = httpx.get("https://www.okx.com/api/v5/market/history-candles",
                          params=params, timeout=15)
            chunk = r.json().get("data", [])
        except Exception:
            break
        if not chunk:
            break
        rows.extend(chunk)
        after = chunk[-1][0]
        time.sleep(0.05)
    if not rows:
        try:
            r = httpx.get("https://www.okx.com/api/v5/market/candles",
                          params={"instId": inst_id, "bar": bar, "limit": "300"}, timeout=15)
            rows = r.json().get("data", [])
        except Exception:
            return None
    if not rows:
        return None
    rows.sort(key=lambda x: int(x[0]))
    arr = np.array([[float(x[1]), float(x[2]), float(x[3]), float(x[4]), float(x[5])] for x in rows])
    return arr[:, 0], arr[:, 1], arr[:, 2], arr[:, 3], arr[:, 4]


# ── Indicators ──

def _check_numpy():
    if np is None:
        raise ImportError("numpy is required for strategy_hypothesis_loop: pip install numpy")

def sma(x, n):
    _check_numpy()
    out = np.full(len(x), np.nan)
    for i in range(n - 1, len(x)):
        out[i] = np.mean(x[i - n + 1:i + 1])
    return out

def ema(x, n):
    a = 2.0 / (n + 1)
    out = np.full(len(x), np.nan)
    out[n - 1] = np.mean(x[:n])
    for i in range(n, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out

def atr(h, l, c, n=14):
    prev_c = np.empty_like(c)
    prev_c[0] = c[0]
    prev_c[1:] = c[:-1]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    tr[0] = h[0] - l[0]
    return sma(tr, n)

def adx_calc(h, l, c, n=14):
    prev_c = np.empty_like(c)
    prev_c[0] = c[0]
    prev_c[1:] = c[:-1]
    prev_h = np.empty_like(h)
    prev_h[0] = h[0]
    prev_h[1:] = h[:-1]
    prev_l = np.empty_like(l)
    prev_l[0] = l[0]
    prev_l[1:] = l[:-1]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
    tr[0] = h[0] - l[0]
    dmp = np.where((h - prev_h) > (prev_l - l), np.maximum(h - prev_h, 0), 0.0)
    dmp[0] = 0
    dmn = np.where((prev_l - l) > (h - prev_h), np.maximum(prev_l - l, 0), 0.0)
    dmn[0] = 0
    atr14 = sma(tr, n)
    dip = 100 * sma(dmp, n) / (atr14 + 1e-12)
    din = 100 * sma(dmn, n) / (atr14 + 1e-12)
    dx = 100 * np.abs(dip - din) / (dip + din + 1e-12)
    return sma(dx, n)


# ═══════════════════════════════════════════════════════════════════════════════
# MA RIBBON BACKTEST ENGINE (parameterized)
# ═══════════════════════════════════════════════════════════════════════════════

def run_ribbon_backtest(o, h, l, c, v, params: dict) -> dict:
    """
    Run MA Ribbon backtest with given parameters.

    params keys:
        ma_fast (default 5), ma_mid (default 8), ema_slow (default 21),
        ma_trend (default 55), adx_min (default 20), atr_mult (default 2.0),
        rr (default 2.0), vol_filter (default True), vol_mult (default 1.2)

    Returns: dict with sharpe, total_return, win_rate, trades, max_dd, score
    """
    n = len(c)
    ma_fast_n  = params.get("ma_fast", 5)
    ma_mid_n   = params.get("ma_mid", 8)
    ema_slow_n = params.get("ema_slow", 21)
    ma_trend_n = params.get("ma_trend", 55)
    adx_min    = params.get("adx_min", 20)
    atr_mult   = params.get("atr_mult", 2.0)
    rr         = params.get("rr", 2.0)
    vol_filter = params.get("vol_filter", True)
    vol_mult   = params.get("vol_mult", 1.2)
    fee        = 0.0005

    ma5  = sma(c, ma_fast_n)
    ma8  = sma(c, ma_mid_n)
    e21  = ema(c, ema_slow_n)
    ma55 = sma(c, ma_trend_n)
    atr14 = atr(h, l, c, 14)
    adx14 = adx_calc(h, l, c, 14)
    vol_avg = sma(v, 20)

    bull = (c > ma5) & (ma5 > ma8) & (ma8 > e21) & (e21 > ma55)
    bear = (c < ma5) & (ma5 < ma8) & (ma8 < e21) & (e21 < ma55)
    adx_ok = adx14 > adx_min
    vol_ok = (v > vol_mult * vol_avg) if vol_filter else np.ones(n, bool)

    # Generate signals
    signals = np.zeros(n)
    for i in range(1, n):
        if bull[i] and not bull[i - 1] and adx_ok[i] and vol_ok[i]:
            signals[i] = 1
        elif bear[i] and not bear[i - 1] and adx_ok[i] and vol_ok[i]:
            signals[i] = -1

    # Run backtest
    pos = 0; entry = 0.0; sl = 0.0; tp = 0.0
    equity = 1.0; peak_eq = 1.0; max_dd = 0.0
    returns = []; wins = 0; trades = 0

    for i in range(1, n):
        if pos != 0:
            hit_sl = (pos == 1 and c[i] <= sl) or (pos == -1 and c[i] >= sl)
            hit_tp = (pos == 1 and c[i] >= tp) or (pos == -1 and c[i] <= tp)
            if hit_tp:
                net = abs(tp - entry) / entry - fee * 2
                equity *= (1 + net); returns.append(net); wins += 1; trades += 1; pos = 0
            elif hit_sl:
                net = -abs(sl - entry) / entry - fee * 2
                equity *= (1 + net); returns.append(net); trades += 1; pos = 0
            if pos == 1:
                new_sl = c[i] - atr_mult * atr14[i] if not np.isnan(atr14[i]) else sl
                sl = max(sl, new_sl)
            elif pos == -1:
                new_sl = c[i] + atr_mult * atr14[i] if not np.isnan(atr14[i]) else sl
                sl = min(sl, new_sl)
            peak_eq = max(peak_eq, equity)
            max_dd = max(max_dd, (peak_eq - equity) / peak_eq)
        if pos == 0 and signals[i] != 0 and not np.isnan(atr14[i]):
            pos = int(signals[i])
            entry = c[i]
            sl_dist = atr_mult * atr14[i]
            sl = entry - sl_dist if pos == 1 else entry + sl_dist
            tp = entry + rr * sl_dist if pos == 1 else entry - rr * sl_dist

    # Metrics
    sharpe = 0.0
    if len(returns) >= 2:
        r = np.array(returns)
        std_r = float(np.std(r))
        if std_r < 1e-10:
            sharpe = 0.0
        else:
            sharpe = float(np.mean(r) / std_r * np.sqrt(252 * 24))
    total_return = equity - 1.0
    win_rate = wins / trades * 100 if trades else 0

    # Composite score
    trade_factor = min(trades / 20.0, 1.0)
    dd_penalty = max(0, max_dd * 100 - 10.0) * 0.05
    score = sharpe * math.sqrt(trade_factor) - dd_penalty if trades >= 3 else -999.0

    return {
        "sharpe": round(sharpe, 4),
        "total_return_pct": round(total_return * 100, 2),
        "win_rate": round(win_rate, 1),
        "trades": trades,
        "max_dd_pct": round(max_dd * 100, 2),
        "score": round(score, 4),
        "params": params.copy(),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: Run baseline backtest
# ═══════════════════════════════════════════════════════════════════════════════

DEFAULT_PARAMS = {
    "ma_fast": 5, "ma_mid": 8, "ema_slow": 21, "ma_trend": 55,
    "adx_min": 20, "atr_mult": 2.0, "rr": 2.0,
    "vol_filter": True, "vol_mult": 1.2,
}

def run_multi_symbol_backtest(symbols: list, params: dict, bar="1H") -> dict:
    """Run backtest across multiple symbols, return aggregate metrics."""
    results = []
    for sym in symbols:
        data = fetch_ohlcv(sym, bar=bar)
        if data is None:
            print(f"  [SKIP] {sym}: no data")
            continue
        o, h, l, c, v = data
        if len(c) < 100:
            print(f"  [SKIP] {sym}: only {len(c)} bars")
            continue
        r = run_ribbon_backtest(o, h, l, c, v, params)
        r["symbol"] = sym
        results.append(r)
        print(f"  {sym}: sharpe={r['sharpe']:.3f} ret={r['total_return_pct']:.1f}% "
              f"wr={r['win_rate']:.0f}% trades={r['trades']} dd={r['max_dd_pct']:.1f}%")

    if not results:
        return {"avg_sharpe": 0, "avg_score": -999, "n_symbols": 0, "results": []}

    avg_sharpe = np.mean([r["sharpe"] for r in results])
    avg_score = np.mean([r["score"] for r in results])
    avg_return = np.mean([r["total_return_pct"] for r in results])
    avg_wr = np.mean([r["win_rate"] for r in results])
    total_trades = sum(r["trades"] for r in results)

    return {
        "avg_sharpe": round(float(avg_sharpe), 4),
        "avg_score": round(float(avg_score), 4),
        "avg_return_pct": round(float(avg_return), 2),
        "avg_win_rate": round(float(avg_wr), 1),
        "total_trades": total_trades,
        "n_symbols": len(results),
        "results": results,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2: Generate hypothesis via Claude
# ═══════════════════════════════════════════════════════════════════════════════

async def generate_hypothesis(baseline: dict, params: dict) -> dict | None:
    """Use Claude to generate a testable hypothesis for parameter improvement."""
    from claude_agent import _run_claude_raw

    prompt = f"""你是一个量化策略研究员。分析以下 MA Ribbon 策略回测结果，生成一个可测试的假设。

当前参数:
  MA快线={params['ma_fast']}, MA中线={params['ma_mid']}, EMA慢线={params['ema_slow']}, MA趋势={params['ma_trend']}
  ADX阈值={params['adx_min']}, ATR倍数={params['atr_mult']}, 风险收益比={params['rr']}

回测结果 ({baseline['n_symbols']}个交易对, 1H K线):
  平均Sharpe: {baseline['avg_sharpe']}
  平均收益: {baseline['avg_return_pct']}%
  平均胜率: {baseline['avg_win_rate']}%
  总交易数: {baseline['total_trades']}

每个交易对的表现:
{chr(10).join(f"  {r['symbol']}: sharpe={r['sharpe']} ret={r['total_return_pct']}% wr={r['win_rate']}% trades={r['trades']}" for r in baseline['results'][:8])}

请生成一个具体的参数修改假设。严格输出JSON:
{{
  "observation": "从数据中观察到的现象（如：交易次数太少/胜率低/回撤大）",
  "hypothesis": "如果把参数X从A改到B，预测Sharpe会提高，因为...",
  "param_changes": {{"参数名": 新值}},
  "predicted_direction": "higher_sharpe" 或 "lower_dd" 或 "more_trades",
  "confidence": 0.6
}}

规则:
- param_changes 的 key 必须是: ma_fast, ma_mid, ema_slow, ma_trend, adx_min, atr_mult, rr, vol_filter, vol_mult
- 每次只改1-2个参数，不要大改
- 基于观察到的具体问题提出针对性修改
- 只输出JSON"""

    try:
        raw = await _run_claude_raw(prompt=prompt, model="claude-haiku-4-5-20251001", timeout=30)
        if not raw:
            return None
        # Parse JSON from response using robust brace-matching
        from skill_library import _parse_json_from_response
        data = _parse_json_from_response(raw)
        if not data:
            return None
        if "param_changes" not in data or "hypothesis" not in data:
            return None
        return data
    except Exception as e:
        print(f"  [ERROR] Hypothesis generation failed: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 3-5: Run experiment, compare, record
# ═══════════════════════════════════════════════════════════════════════════════

RESULTS_FILE = BOT_DIR / ".strategy_experiments.jsonl"


def record_experiment(experiment: dict):
    """Append experiment to JSONL log."""
    with open(RESULTS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(experiment, ensure_ascii=False, default=str) + "\n")
        f.flush()
        os.fsync(f.fileno())


async def run_hypothesis_loop(n_hypotheses: int = 3):
    """
    Main loop:
      1. Baseline → 2. Hypothesis → 3. Experiment → 4. Compare → 5. Record
    Repeat n_hypotheses times, each building on prior knowledge.
    """
    print("=" * 70)
    print("  STRATEGY HYPOTHESIS LOOP — Layer 3+4 End-to-End")
    print("=" * 70)

    # Fetch symbols once
    print("\n[1/5] Fetching top symbols from OKX...")
    symbols = fetch_top_symbols(5)
    print(f"  Symbols: {', '.join(symbols)}")

    # Run baseline
    print(f"\n[2/5] Running BASELINE backtest (default params)...")
    print(f"  Params: {DEFAULT_PARAMS}")
    baseline = run_multi_symbol_backtest(symbols, DEFAULT_PARAMS, bar="1H")
    print(f"\n  >>> BASELINE: Sharpe={baseline['avg_sharpe']:.4f} | "
          f"Score={baseline['avg_score']:.4f} | "
          f"Return={baseline['avg_return_pct']:.1f}% | "
          f"WR={baseline['avg_win_rate']:.0f}% | "
          f"Trades={baseline['total_trades']}")

    all_experiments = []
    current_best_params = DEFAULT_PARAMS.copy()
    current_best_sharpe = baseline["avg_sharpe"]

    for h_idx in range(n_hypotheses):
        print(f"\n{'─' * 70}")
        print(f"  HYPOTHESIS #{h_idx + 1}/{n_hypotheses}")
        print(f"{'─' * 70}")

        # Generate hypothesis
        print(f"\n[3/5] Claude generating hypothesis...")
        hypothesis = await generate_hypothesis(baseline, current_best_params)

        if not hypothesis:
            print("  [FAIL] Could not generate hypothesis, skipping")
            continue

        print(f"  Observation: {hypothesis.get('observation', '?')}")
        print(f"  Hypothesis:  {hypothesis.get('hypothesis', '?')}")
        print(f"  Changes:     {hypothesis.get('param_changes', {})}")
        print(f"  Prediction:  {hypothesis.get('predicted_direction', '?')}")

        # Apply parameter changes
        experiment_params = current_best_params.copy()
        for k, v in hypothesis.get("param_changes", {}).items():
            if k in experiment_params:
                experiment_params[k] = v
                print(f"  {k}: {current_best_params.get(k)} → {v}")

        # Run experiment
        print(f"\n[4/5] Running EXPERIMENT backtest...")
        experiment_result = run_multi_symbol_backtest(symbols, experiment_params, bar="1H")
        print(f"\n  >>> EXPERIMENT: Sharpe={experiment_result['avg_sharpe']:.4f} | "
              f"Score={experiment_result['avg_score']:.4f} | "
              f"Return={experiment_result['avg_return_pct']:.1f}% | "
              f"WR={experiment_result['avg_win_rate']:.0f}% | "
              f"Trades={experiment_result['total_trades']}")

        # Compare
        sharpe_delta = experiment_result["avg_sharpe"] - baseline["avg_sharpe"]
        score_delta = experiment_result["avg_score"] - baseline["avg_score"]
        improved = experiment_result["avg_sharpe"] > baseline["avg_sharpe"]

        verdict = "CONFIRMED" if improved else "REJECTED"

        print(f"\n[5/5] VERDICT: {verdict}")
        print(f"  Sharpe: {baseline['avg_sharpe']:.4f} → {experiment_result['avg_sharpe']:.4f} "
              f"({'📈 +' if sharpe_delta >= 0 else '📉 '}{sharpe_delta:.4f})")
        print(f"  Score:  {baseline['avg_score']:.4f} → {experiment_result['avg_score']:.4f} "
              f"({'📈 +' if score_delta >= 0 else '📉 '}{score_delta:.4f})")

        # Record to file
        experiment_record = {
            "timestamp": datetime.now().isoformat(),
            "hypothesis_id": f"SH-{h_idx + 1:04d}",
            "observation": hypothesis.get("observation", ""),
            "hypothesis": hypothesis.get("hypothesis", ""),
            "param_changes": hypothesis.get("param_changes", {}),
            "baseline_params": current_best_params,
            "experiment_params": experiment_params,
            "baseline_sharpe": baseline["avg_sharpe"],
            "experiment_sharpe": experiment_result["avg_sharpe"],
            "baseline_score": baseline["avg_score"],
            "experiment_score": experiment_result["avg_score"],
            "sharpe_delta": round(sharpe_delta, 4),
            "verdict": verdict,
        }
        record_experiment(experiment_record)
        all_experiments.append(experiment_record)

        # Also record to the bot's hypothesis engine
        try:
            import auto_research
            h_record = {
                "id": f"SH-{h_idx + 1:04d}",
                "observation": hypothesis.get("observation", ""),
                "prediction": hypothesis.get("hypothesis", ""),
                "prompt_diff": json.dumps(hypothesis.get("param_changes", {})),
                "baseline_score": baseline["avg_sharpe"],
                "result_score": experiment_result["avg_sharpe"],
                "status": "confirmed" if improved else "rejected",
                "evidence": f"sharpe_delta={sharpe_delta:.4f}",
                "timestamp": datetime.now().isoformat(),
            }
            auto_research._save_hypothesis(h_record)
            print(f"  → Recorded to hypothesis engine")
        except Exception as e:
            print(f"  → [WARN] Could not save to hypothesis engine: {e}")

        # If improved, adopt as new best
        if improved:
            current_best_params = experiment_params.copy()
            current_best_sharpe = experiment_result["avg_sharpe"]
            print(f"  → Adopting new params as baseline for next round")

            # Try to crystallize as skill (Layer 3)
            try:
                import skill_library
                skill_text = f"""MA Ribbon 参数优化:
改动: {hypothesis.get('param_changes', {})}
原因: {hypothesis.get('observation', '')}
效果: Sharpe {baseline['avg_sharpe']:.4f} → {experiment_result['avg_sharpe']:.4f} (+{sharpe_delta:.4f})
最终参数: {experiment_params}"""
                new_skill_id = await skill_library.maybe_extract_skill(
                    f"MA Ribbon backtest parameter optimization",
                    skill_text,
                    score={"overall": 0.85},
                )
                if new_skill_id:
                    print(f"  → Crystallized as skill: {new_skill_id}")
            except Exception as e:
                print(f"  → [WARN] Skill crystallization: {e}")

    # ── Final Summary ──
    print(f"\n{'=' * 70}")
    print(f"  FINAL SUMMARY")
    print(f"{'=' * 70}")
    confirmed = [e for e in all_experiments if e["verdict"] == "CONFIRMED"]
    rejected = [e for e in all_experiments if e["verdict"] == "REJECTED"]
    print(f"  Hypotheses tested: {len(all_experiments)}")
    print(f"  Confirmed: {len(confirmed)}")
    print(f"  Rejected:  {len(rejected)}")
    print(f"\n  Original params:  {DEFAULT_PARAMS}")
    print(f"  Best params:      {current_best_params}")
    print(f"  Original Sharpe:  {baseline['avg_sharpe']:.4f}")
    print(f"  Best Sharpe:      {current_best_sharpe:.4f}")
    improvement = current_best_sharpe - baseline["avg_sharpe"]
    print(f"  Improvement:      {'📈 +' if improvement >= 0 else '📉 '}{improvement:.4f}")

    for e in all_experiments:
        status = "✅" if e["verdict"] == "CONFIRMED" else "❌"
        print(f"\n  {status} {e['hypothesis_id']}: {e['hypothesis'][:80]}")
        print(f"     Changes: {e['param_changes']} | Sharpe: {e['sharpe_delta']:+.4f}")

    return {
        "baseline_sharpe": baseline["avg_sharpe"],
        "best_sharpe": current_best_sharpe,
        "best_params": current_best_params,
        "experiments": all_experiments,
    }


# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    result = asyncio.run(run_hypothesis_loop(n_hypotheses=3))
    print(f"\n[DONE] Results saved to {RESULTS_FILE}")
