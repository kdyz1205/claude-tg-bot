"""
Offline OHLCV store for backtests — WSS/live snapshots write here; evolver reads disk only.

Avoids HTTP storms from ProcessPoolExecutor / tight backtest loops hitting OKX.
"""

from __future__ import annotations

import logging
import os
import threading
from pathlib import Path

import numpy as np

log = logging.getLogger(__name__)

_CACHE_DIR = Path(__file__).resolve().parent.parent / "_data_cache"
_CACHE_DIR.mkdir(parents=True, exist_ok=True)

_LOCK = threading.Lock()

# 4h bucket in ms (OKX candle ts)
_MS_4H = 4 * 3600 * 1000


def cache_dir() -> Path:
    return _CACHE_DIR


def write_live_1m_snapshot(inst_id_okx: str, ohlcv_6: np.ndarray) -> None:
    """
    Persist current WSS ring buffer as ``live_{inst_id}_1m.npy`` for offline resampling.
    ``ohlcv_6`` shape (N, 6) ts,o,h,l,c,vol.
    """
    arr = np.asarray(ohlcv_6, dtype=np.float64)
    if arr.ndim != 2 or arr.shape[1] < 6 or len(arr) < 10:
        return
    safe = inst_id_okx.replace("/", "_")
    path = _CACHE_DIR / f"live_{safe}_1m.npy"
    tmp = str(path) + ".tmp"
    try:
        with _LOCK:
            np.save(tmp, arr)
            os.replace(tmp, str(path))
        log.debug("tensor cache: wrote %s rows → %s", len(ohlcv_6), path.name)
    except OSError as e:
        log.warning("tensor cache write failed: %s", e)


def resample_1m_to_4h(arr_1m: np.ndarray) -> np.ndarray:
    """Bucket 1m OHLCV into 4H bars. Returns (M, 6) same column order."""
    if arr_1m is None or len(arr_1m) < 4:
        return np.zeros((0, 6), dtype=np.float64)
    a = np.asarray(arr_1m, dtype=np.float64)
    ts = a[:, 0].astype(np.int64)
    bucket = ts // _MS_4H
    order = np.argsort(bucket)
    a = a[order]
    bucket = bucket[order]
    out_rows: list[list[float]] = []
    i = 0
    n = len(a)
    while i < n:
        b = bucket[i]
        j = i
        while j < n and bucket[j] == b:
            j += 1
        chunk = a[i:j]
        t0 = float(chunk[0, 0])
        o0 = float(chunk[0, 1])
        h = float(np.max(chunk[:, 2]))
        l = float(np.min(chunk[:, 3]))
        c = float(chunk[-1, 4])
        v = float(np.sum(chunk[:, 5]))
        out_rows.append([t0, o0, h, l, c, v])
        i = j
    if not out_rows:
        return np.zeros((0, 6), dtype=np.float64)
    return np.array(out_rows, dtype=np.float64)


def _ohlcv_from_dataframe(df) -> np.ndarray | None:
    """Build (N, 6) float array ts,o,h,l,c,vol from a pandas DataFrame."""
    try:
        import pandas as pd
    except ImportError:
        return None
    if df is None or len(df) < 4:
        return None
    if not isinstance(df, pd.DataFrame):
        return None
    cols = {c.lower(): c for c in df.columns}
    def col(*names: str):
        for n in names:
            if n in cols:
                return df[cols[n]]
        return None
    ts = col("ts", "timestamp", "time")
    o = col("open", "o")
    h = col("high", "h")
    lo = col("low", "l")
    c = col("close", "c")
    v = col("vol", "volume", "v")
    if ts is None or o is None or h is None or lo is None or c is None:
        return None
    if v is None:
        v = np.ones(len(df))
    out = np.column_stack(
        (
            ts.astype("float64"),
            o.astype("float64"),
            h.astype("float64"),
            lo.astype("float64"),
            c.astype("float64"),
            v.astype("float64"),
        )
    )
    return out


def load_offline_ohlcv(inst_id: str, bar: str, limit: int) -> np.ndarray | None:
    """
    Load OHLCV without network. Tries:
    1) Parquet ``{inst_id}_{bar}_{limit}.parquet`` (pandas)
    2) Exact cache ``{inst_id}_{bar}_{limit}.npy``
    3) From ``live_*_1m.npy`` resampled to 4H when ``bar == '4H'``
    """
    inst_id = inst_id.strip()
    pq_path = _CACHE_DIR / f"{inst_id}_{bar}_{limit}.parquet"
    if pq_path.exists():
        try:
            import pandas as pd

            arr = _ohlcv_from_dataframe(pd.read_parquet(pq_path))
            if arr is not None and len(arr) >= min(100, limit // 2):
                return arr[-limit:] if len(arr) > limit else arr
        except Exception as e:
            log.debug("offline parquet load: %s", e)

    cache_file = _CACHE_DIR / f"{inst_id}_{bar}_{limit}.npy"
    if cache_file.exists():
        try:
            data = np.load(str(cache_file))
            if len(data) >= min(100, limit // 2):
                return data[-limit:] if len(data) > limit else data
        except OSError as e:
            log.debug("offline npy load: %s", e)

    if bar.upper() == "4H":
        live = _CACHE_DIR / f"live_{inst_id.replace('/', '_')}_1m.npy"
        if live.exists():
            try:
                m1 = np.load(str(live))
                h4 = resample_1m_to_4h(m1)
                if len(h4) >= min(80, limit // 2):
                    return h4[-limit:] if len(h4) > limit else h4
            except OSError as e:
                log.debug("offline live resample: %s", e)
    return None
