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


def load_offline_ohlcv(inst_id: str, bar: str, limit: int) -> np.ndarray | None:
    """
    Load OHLCV without network. Tries:
    1) Exact cache ``{inst_id}_{bar}_{limit}.npy``
    2) From ``live_*_1m.npy`` resampled to 4H when ``bar == '4H'``
    """
    inst_id = inst_id.strip()
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
