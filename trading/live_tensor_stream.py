"""
Live tensor stream — OKX / Binance WebSocket → sliding OHLCV window → z-scored [1, T, 5] tensor
aligned with singularity training contract (open, high, low, close, vol) per bar.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from typing import Any, Optional

import numpy as np

log = logging.getLogger(__name__)

OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"
BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"

_streams: dict[str, "WebSocketTensorStream"] = {}


def _zscore_window(ohlcv5: np.ndarray) -> np.ndarray:
    """ohlcv5: (T, 5) float — same feature order as singularity mock."""
    mu = ohlcv5.mean(axis=0, keepdims=True)
    sd = ohlcv5.std(axis=0, keepdims=True) + 1e-6
    return ((ohlcv5 - mu) / sd).astype(np.float32)


class WebSocketTensorStream:
    """
    Maintains a ring buffer of completed 1m candles + in-progress bar.
    Heavy numpy work is offloaded with asyncio.to_thread when T>128.
    """

    def __init__(
        self,
        inst_id_okx: str = "BTC-USDT-SWAP",
        window: int = 512,
        bar: str = "candle1m",
    ):
        self.inst_id = inst_id_okx
        self.window = max(64, int(window))
        self.bar = bar
        self._rows: deque[list[float]] = deque(maxlen=self.window)
        self._partial: Optional[dict[str, float]] = None
        self._funding_rate: float = 0.0
        self._bid_sz: float = 0.0
        self._ask_sz: float = 0.0
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()
        self._last_ts: float = 0.0

    @property
    def running(self) -> bool:
        return self._running and self._task is not None and not self._task.done()

    async def start(self) -> None:
        if self.running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_okx(), name=f"okx_ws_{self.inst_id}")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def _append_candle(self, ts: float, o: float, h: float, l: float, c: float, v: float) -> None:
        self._rows.append([ts, o, h, l, c, v])
        self._last_ts = ts

    async def _handle_okx_msg(self, data: list) -> None:
        async with self._lock:
            for row in data:
                if len(row) < 8:
                    continue
                ts, o, h, l, c, vol, _, _ = row[:8]
                self._append_candle(float(ts), float(o), float(h), float(l), float(c), float(vol))

    async def _handle_book(self, data: list) -> None:
        async with self._lock:
            for book in data:
                bids = book.get("bids") or []
                asks = book.get("asks") or []
                if bids:
                    self._bid_sz = sum(float(x[1]) for x in bids[:5])
                if asks:
                    self._ask_sz = sum(float(x[1]) for x in asks[:5])

    async def _handle_funding(self, data: list) -> None:
        async with self._lock:
            for fr in data:
                self._funding_rate = float(fr.get("fundingRate", 0) or 0)

    async def _run_okx(self) -> None:
        try:
            import aiohttp
        except ImportError:
            log.error("aiohttp required for WebSocketTensorStream")
            return

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(OKX_WS_PUBLIC, heartbeat=25) as ws:
                        sub = {
                            "op": "subscribe",
                            "args": [
                                {"channel": self.bar, "instId": self.inst_id},
                                {"channel": "books5", "instId": self.inst_id},
                                {"channel": "funding-rate", "instId": self.inst_id},
                            ],
                        }
                        await ws.send_str(json.dumps(sub))
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    payload = json.loads(msg.data)
                                except json.JSONDecodeError:
                                    continue
                                if payload.get("event") == "error":
                                    log.warning("OKX WS error: %s", payload)
                                    continue
                                arg = (payload.get("arg") or {})
                                ch = arg.get("channel", "")
                                data = payload.get("data") or []
                                if ch == self.bar and data:
                                    await self._handle_okx_msg(data)
                                elif ch == "books5" and data:
                                    await self._handle_book(data)
                                elif ch == "funding-rate" and data:
                                    await self._handle_funding(data)
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSE,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning("OKX WS disconnected (%s), reconnect in 3s", e)
                await asyncio.sleep(3)

    def get_ohlcv_numpy(self) -> np.ndarray:
        """[N, 6] ts, o, h, l, c, vol — snapshot under caller sync (use lock outside)."""
        if not self._rows:
            return np.zeros((0, 6), dtype=np.float64)
        return np.array(list(self._rows), dtype=np.float64)

    async def snapshot_ohlcv(self) -> np.ndarray:
        async with self._lock:
            return self.get_ohlcv_numpy()

    def get_aux_vector(self) -> np.ndarray:
        """Funding + order-book imbalance (for future models). Shape (3,)"""
        tot = self._bid_sz + self._ask_sz + 1e-9
        imb = (self._bid_sz - self._ask_sz) / tot
        return np.array([self._funding_rate, self._bid_sz, imb], dtype=np.float32)

    async def build_model_tensor(self, seq_len: int = 64) -> Optional[np.ndarray]:
        """
        Returns float32 array (1, seq_len, 5) z-scored over the last seq_len bars, or None if insufficient data.
        """
        arr = await self.snapshot_ohlcv()
        if len(arr) < seq_len:
            return None
        slice_ = arr[-seq_len:, 1:6].astype(np.float32, copy=False)

        def _norm(x: np.ndarray) -> np.ndarray:
            return _zscore_window(x)[np.newaxis, ...]

        if seq_len > 128:
            return await asyncio.to_thread(_norm, slice_)
        return _norm(slice_)


def get_shared_stream(inst_id: str = "BTC-USDT-SWAP", window: int = 512) -> WebSocketTensorStream:
    key = f"{inst_id}:{window}"
    if key not in _streams:
        _streams[key] = WebSocketTensorStream(inst_id_okx=inst_id, window=window)
    return _streams[key]


async def ensure_stream_started(inst_id: str = "BTC-USDT-SWAP", window: int = 512) -> WebSocketTensorStream:
    s = get_shared_stream(inst_id, window)
    await s.start()
    return s
