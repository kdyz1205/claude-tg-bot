"""
onchain_tracker.py — On-chain smart money / whale tracker.

Monitors known whale addresses on ETH, BSC, and Solana using free public APIs:
- Etherscan (free tier, optional API key from ETHERSCAN_API_KEY env var)
- BSCScan  (free tier, optional API key from BSCSCAN_API_KEY env var)
- Solscan  public API (no key required)
- CoinGecko public API for prices (no key required)

Scans every 10 minutes for large transfers (>$100k USD).
Emits high-confidence signals when whale activity aligns with technical indicators.
Signals saved to .whale_signals.json.
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIGNALS_FILE = os.path.join(BASE_DIR, ".whale_signals.json")
ADDRESSES_FILE = os.path.join(BASE_DIR, ".whale_addresses.json")

MIN_TRANSFER_USD = 100_000   # $100k threshold
SCAN_INTERVAL = 600          # 10 minutes
LOOKBACK_SECONDS = 700       # how far back to check per scan

# ── Default whale addresses ───────────────────────────────────────────────────

DEFAULT_ADDRESSES = {
    "0x28C6c06298d514Db089934071355E5743bf21d60": {
        "label": "Binance Hot-14",
        "network": "eth",
    },
    "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8": {
        "label": "Binance Cold-7",
        "network": "eth",
    },
    "0xF977814e90dA44bFA03b6295A0616a897441aceC": {
        "label": "Binance Hot-8",
        "network": "eth",
    },
    "0xcbB98864Ef56E9042e7d2efef76141f15731B82f": {
        "label": "Binance BSC Hot",
        "network": "bsc",
    },
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {
        "label": "Solana Whale-1",
        "network": "sol",
    },
}


def _detect_network(address: str) -> str:
    """Infer network from address format."""
    if re.match(r"^0x[a-fA-F0-9]{40}$", address):
        return "eth"
    return "sol"


def _load_whale_memory() -> dict:
    """Try to parse whale addresses embedded in .bot_memory.md."""
    addresses = {}
    mem_file = os.path.join(BASE_DIR, ".bot_memory.md")
    if not os.path.exists(mem_file):
        return addresses
    try:
        with open(mem_file, "r", encoding="utf-8") as f:
            content = f.read()
        for addr in re.findall(r"0x[a-fA-F0-9]{40}", content):
            addresses[addr] = {"label": f"Custom-{addr[:6]}", "network": "eth"}
        for addr in re.findall(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b", content):
            if addr not in addresses:
                addresses[addr] = {"label": f"Sol-{addr[:6]}", "network": "sol"}
    except Exception as e:
        logger.debug("whale_memory parse failed: %s", e)
    return addresses


# ── Price cache ───────────────────────────────────────────────────────────────

_PRICE_CACHE: dict = {}
_PRICE_CACHE_TTL = 300  # 5 min


async def _get_prices() -> dict:
    """Fetch ETH/BNB/SOL spot prices from CoinGecko (free, no key)."""
    now = time.time()
    if _PRICE_CACHE.get("_ts", 0) + _PRICE_CACHE_TTL > now:
        return _PRICE_CACHE
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": "ethereum,binancecoin,solana", "vs_currencies": "usd"}
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
        _PRICE_CACHE.update({
            "ETH": data.get("ethereum", {}).get("usd", 3000),
            "BNB": data.get("binancecoin", {}).get("usd", 400),
            "SOL": data.get("solana", {}).get("usd", 150),
            "_ts": now,
        })
    except Exception as e:
        logger.debug("_get_prices failed: %s", e)
        if "ETH" not in _PRICE_CACHE:
            _PRICE_CACHE.update({"ETH": 3000, "BNB": 400, "SOL": 150, "_ts": 0})
    return _PRICE_CACHE


# ── Blockchain API calls ──────────────────────────────────────────────────────

async def _etherscan_txlist(address: str, api_key: str, base_url: str) -> list:
    params = {
        "module": "account", "action": "txlist",
        "address": address, "sort": "desc", "offset": 20, "page": 1,
    }
    if api_key:
        params["apikey"] = api_key
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(base_url, params=params)
            data = resp.json()
        if data.get("status") == "1":
            min_ts = int(time.time()) - LOOKBACK_SECONDS
            return [tx for tx in data.get("result", [])
                    if int(tx.get("timeStamp", 0)) >= min_ts]
    except Exception as e:
        logger.debug("txlist %s: %s", address[:8], e)
    return []


async def _etherscan_tokentx(address: str, api_key: str, base_url: str) -> list:
    params = {
        "module": "account", "action": "tokentx",
        "address": address, "sort": "desc", "offset": 20, "page": 1,
    }
    if api_key:
        params["apikey"] = api_key
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(base_url, params=params)
            data = resp.json()
        if data.get("status") == "1":
            min_ts = int(time.time()) - LOOKBACK_SECONDS
            return [tx for tx in data.get("result", [])
                    if int(tx.get("timeStamp", 0)) >= min_ts]
    except Exception as e:
        logger.debug("tokentx %s: %s", address[:8], e)
    return []


async def _solscan_txs(address: str) -> list:
    try:
        url = "https://public-api.solscan.io/account/transactions"
        params = {"account": address, "limit": 20}
        headers = {"accept": "application/json"}
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                min_ts = int(time.time()) - LOOKBACK_SECONDS
                return [tx for tx in resp.json()
                        if tx.get("blockTime", 0) >= min_ts]
    except Exception as e:
        logger.debug("solscan %s: %s", address[:8], e)
    return []


# ── Transaction classifiers ───────────────────────────────────────────────────

_STABLECOINS = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "FRAX", "LUSD", "USDD"}


def _classify_native(tx: dict, address: str, prices: dict, network: str) -> Optional[dict]:
    value_wei = int(tx.get("value", "0"))
    if value_wei == 0:
        return None
    symbol = "ETH" if network == "eth" else "BNB"
    amount_usd = (value_wei / 1e18) * prices.get(symbol, 0)
    if amount_usd < MIN_TRANSFER_USD:
        return None
    addr_l = address.lower()
    action = "receive" if tx.get("to", "").lower() == addr_l else "send"
    return {
        "token": symbol, "amount_usd": round(amount_usd),
        "action": action, "tx_hash": tx.get("hash", ""),
        "timestamp": int(tx.get("timeStamp", time.time())),
    }


def _classify_erc20(tx: dict, address: str) -> Optional[dict]:
    token = tx.get("tokenSymbol", "UNKNOWN")
    if token not in _STABLECOINS:
        return None  # no free per-token price source; skip
    try:
        decimals = max(0, min(int(tx.get("tokenDecimal", 18)), 18))
        amount_usd = int(tx.get("value", "0")) / (10 ** decimals)
    except Exception:
        return None
    if amount_usd < MIN_TRANSFER_USD:
        return None
    addr_l = address.lower()
    action = "receive" if tx.get("to", "").lower() == addr_l else "send"
    return {
        "token": token, "amount_usd": round(amount_usd),
        "action": action, "tx_hash": tx.get("hash", ""),
        "timestamp": int(tx.get("timeStamp", time.time())),
    }


def _classify_sol(tx: dict, address: str, prices: dict) -> Optional[dict]:
    try:
        lamports = abs(int(tx.get("lamport", 0)))
        amount_usd = (lamports / 1e9) * prices.get("SOL", 150)
        if amount_usd < MIN_TRANSFER_USD:
            return None
        signer = tx.get("signer", [])
        if isinstance(signer, list):
            signer = signer[0] if signer else ""
        action = "send" if signer == address else "receive"
        return {
            "token": "SOL", "amount_usd": round(amount_usd),
            "action": action, "tx_hash": tx.get("txHash", ""),
            "timestamp": int(tx.get("blockTime", time.time())),
        }
    except Exception:
        return None


# ── Dual confirmation ─────────────────────────────────────────────────────────

def _get_tech_signal(token: str) -> Optional[dict]:
    """Check signal_engine's latest signals for alignment."""
    try:
        import signal_engine as _se
        token_base = token.replace("-USDT", "").upper()
        for sig in _se.signal_engine.get_last_signals():
            if sig.get("symbol", "").replace("-USDT", "").upper() == token_base:
                return sig
    except Exception as e:
        logger.debug("_get_tech_signal: %s", e)
    return None


def _dual_confirm(action: str, token: str) -> Optional[dict]:
    """
    Returns high-confidence dict if whale action aligns with technical signal.
      action="receive" (whale buys) + bullish → HIGH confidence LONG
      action="send"    (whale sells) + bearish → HIGH confidence SHORT
    """
    tech = _get_tech_signal(token)
    if not tech or tech.get("direction") == "neutral":
        return None
    tech_dir = tech["direction"]
    if action == "receive" and tech_dir == "long":
        return {"direction": "long", "confidence": "HIGH", "tech_score": tech.get("score", 0)}
    if action == "send" and tech_dir == "short":
        return {"direction": "short", "confidence": "HIGH", "tech_score": tech.get("score", 0)}
    return None


# ── OnchainTracker class ──────────────────────────────────────────────────────

class OnchainTracker:
    """Background monitor: scans whale addresses every 10 min for large moves."""

    def __init__(self, send_func=None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._signals: list = []
        self._addresses: dict = {}
        self._etherscan_key = os.getenv("ETHERSCAN_API_KEY", "")
        self._bscscan_key = os.getenv("BSCSCAN_API_KEY", "")
        self._load_addresses()

    # ── Address management ────────────────────────────────────────────────

    def _load_addresses(self):
        if os.path.exists(ADDRESSES_FILE):
            try:
                with open(ADDRESSES_FILE, "r", encoding="utf-8") as f:
                    self._addresses = json.load(f)
                logger.info("OnchainTracker: loaded %d addresses", len(self._addresses))
                return
            except Exception as e:
                logger.warning("OnchainTracker: address file error: %s", e)

        mem_addrs = _load_whale_memory()
        if mem_addrs:
            self._addresses = mem_addrs
            logger.info("OnchainTracker: %d addresses from memory", len(mem_addrs))
            return

        self._addresses = dict(DEFAULT_ADDRESSES)
        logger.info("OnchainTracker: using %d default addresses", len(self._addresses))

    def _save_addresses(self):
        try:
            _tmp = ADDRESSES_FILE + ".tmp"
            with open(_tmp, "w", encoding="utf-8") as f:
                json.dump(self._addresses, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(_tmp, ADDRESSES_FILE)
        except Exception as e:
            logger.warning("OnchainTracker: save addresses failed: %s", e)

    def add_address(self, address: str, label: str = "", network: str = "") -> bool:
        if address in self._addresses:
            return False
        net = network or _detect_network(address)
        self._addresses[address] = {
            "label": label or f"Custom-{address[:8]}",
            "network": net,
        }
        self._save_addresses()
        return True

    def remove_address(self, address: str) -> bool:
        if address in self._addresses:
            del self._addresses[address]
            self._save_addresses()
            return True
        return False

    # ── Signal persistence ────────────────────────────────────────────────

    def _load_signals(self):
        if os.path.exists(SIGNALS_FILE):
            try:
                with open(SIGNALS_FILE, "r", encoding="utf-8") as f:
                    self._signals = json.load(f)
                cutoff = time.time() - 86400
                self._signals = [s for s in self._signals if s.get("timestamp", 0) >= cutoff]
            except Exception:
                self._signals = []

    def _save_signals(self):
        cutoff = time.time() - 86400
        self._signals = [s for s in self._signals if s.get("timestamp", 0) >= cutoff]
        try:
            _tmp = SIGNALS_FILE + ".tmp"
            with open(_tmp, "w", encoding="utf-8") as f:
                json.dump(self._signals, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(_tmp, SIGNALS_FILE)
        except Exception as e:
            logger.warning("OnchainTracker: save signals failed: %s", e)

    def get_recent_signals(self, hours: int = 24) -> list:
        cutoff = time.time() - hours * 3600
        return [s for s in self._signals if s.get("timestamp", 0) >= cutoff]

    # ── Background loop ───────────────────────────────────────────────────

    async def start(self):
        if self._running:
            return
        self._load_signals()
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="onchain_tracker")
        self._task.add_done_callback(self._on_done)
        logger.info("OnchainTracker started, monitoring %d addresses", len(self._addresses))

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _on_done(self, task: asyncio.Task):
        if not task.cancelled():
            try:
                task.result()
            except Exception as e:
                logger.error("OnchainTracker loop crashed: %s", e, exc_info=True)

    async def _loop(self):
        await asyncio.sleep(30)  # initial warm-up delay
        while self._running:
            try:
                await self._scan_all()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("OnchainTracker scan error: %s", e)
            try:
                await asyncio.sleep(SCAN_INTERVAL)
            except asyncio.CancelledError:
                break

    # ── Scanning ──────────────────────────────────────────────────────────

    async def _scan_all(self):
        prices = await _get_prices()
        new_signals = []
        for address, meta in list(self._addresses.items()):
            try:
                sigs = await self._scan_address(address, meta, prices)
                new_signals.extend(sigs)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("scan_address %s error: %s", address[:8], e)
            await asyncio.sleep(1)  # rate-limit buffer

        if new_signals:
            # Dedup by tx_hash to avoid recording same transaction twice
            existing_hashes = {s.get("tx_hash") for s in self._signals if s.get("tx_hash")}
            new_signals = [s for s in new_signals if s.get("tx_hash") not in existing_hashes]
        if new_signals:
            self._signals.extend(new_signals)
            self._save_signals()
            for sig in new_signals:
                if self._send:
                    try:
                        await self._send(self._format_signal(sig))
                    except Exception:
                        pass

    async def _scan_address(self, address: str, meta: dict, prices: dict) -> list:
        network = meta.get("network", "eth")
        label = meta.get("label", address[:8])
        found = []

        if network == "sol":
            for tx in await _solscan_txs(address):
                c = _classify_sol(tx, address, prices)
                if c:
                    found.append(self._build_signal(c, address, label, "sol"))

        elif network in ("eth", "bsc"):
            eth_url = "https://api.etherscan.io/api"
            bsc_url = "https://api.bscscan.com/api"
            base_url = eth_url if network == "eth" else bsc_url
            api_key = self._etherscan_key if network == "eth" else self._bscscan_key

            for tx in await _etherscan_txlist(address, api_key, base_url):
                c = _classify_native(tx, address, prices, network)
                if c:
                    found.append(self._build_signal(c, address, label, network))

            await asyncio.sleep(0.5)
            for tx in await _etherscan_tokentx(address, api_key, base_url):
                c = _classify_erc20(tx, address)
                if c:
                    found.append(self._build_signal(c, address, label, network))

        return found

    def _build_signal(self, classified: dict, address: str, label: str, network: str) -> dict:
        token = classified["token"]
        action = classified["action"]
        sig = {
            "address": address,
            "address_label": label,
            "token": token,
            "amount_usd": classified["amount_usd"],
            "action": action,
            "tx_hash": classified["tx_hash"],
            "timestamp": classified["timestamp"],
            "network": network,
            "confidence": "NORMAL",
            "dual_confirmed": False,
        }
        dual = _dual_confirm(action, token)
        if dual:
            sig.update({
                "confidence": dual["confidence"],
                "dual_confirmed": True,
                "direction": dual["direction"],
                "tech_score": dual["tech_score"],
            })
        return sig

    # ── Formatting ────────────────────────────────────────────────────────

    def _format_signal(self, sig: dict) -> str:
        action_emoji = "🟢" if sig.get("action") == "receive" else "🔴"
        confidence_tag = " ⚡HIGH CONFIDENCE" if sig.get("dual_confirmed") else ""
        ts = datetime.fromtimestamp(sig.get("timestamp", 0)).strftime("%H:%M:%S")
        lines = [
            f"🐋 链上巨鲸动向{confidence_tag}",
            f"  地址: {sig.get('address_label', '?')} ({sig.get('address', '?')[:8]}...)",
            f"  {action_emoji} {sig.get('action', '?').upper()} {sig.get('token', '?')}",
            f"  金额: ${sig.get('amount_usd', 0):,.0f}",
            f"  时间: {ts}",
            f"  TxHash: {sig.get('tx_hash', '?')[:16]}...",
        ]
        if sig.get("dual_confirmed"):
            lines.append(
                f"  技术确认: {sig.get('direction', '?').upper()} "
                f"(score={sig.get('tech_score', '?')})"
            )
        return "\n".join(lines)

    def format_24h_report(self) -> str:
        signals = self.get_recent_signals(24)
        if not signals:
            return "📭 过去24小时无大额链上动向（>$100k）"

        lines = [f"🐋 24h 链上聪明钱动向 ({len(signals)} 笔)\n"]

        # Token summary
        token_stats: dict = {}
        for sig in signals:
            t = sig.get("token", "UNKNOWN")
            if t not in token_stats:
                token_stats[t] = {"buy": 0, "sell": 0, "count": 0}
            if sig.get("action") == "receive":
                token_stats[t]["buy"] += sig.get("amount_usd", 0)
            else:
                token_stats[t]["sell"] += sig.get("amount_usd", 0)
            token_stats[t]["count"] += 1

        for token, stats in sorted(
            token_stats.items(), key=lambda x: x[1]["buy"] + x[1]["sell"], reverse=True
        )[:10]:
            net = stats["buy"] - stats["sell"]
            emoji = "🟢" if net >= 0 else "🔴"
            lines.append(
                f"{emoji} {token}: 买${stats['buy']:,.0f} 卖${stats['sell']:,.0f} "
                f"净{'+' if net >= 0 else ''}{net:,.0f} ({stats['count']}笔)"
            )

        lines.append("\n最近5笔:")
        for sig in sorted(signals, key=lambda x: x.get("timestamp", 0), reverse=True)[:5]:
            ts = datetime.fromtimestamp(sig.get("timestamp", 0)).strftime("%H:%M")
            emoji = "🟢" if sig.get("action") == "receive" else "🔴"
            tag = " ⚡" if sig.get("dual_confirmed") else ""
            lines.append(
                f"  {emoji}{tag} {sig.get('address_label', '?')}: "
                f"{sig.get('action', '?')} {sig.get('token', '?')} ${sig.get('amount_usd', 0):,.0f} @{ts}"
            )

        lines.append(f"\n监控地址: {len(self._addresses)}个")
        return "\n".join(lines)

    def format_address_list(self) -> str:
        if not self._addresses:
            return "⚠️ 没有监控地址"
        lines = [f"👁 监控地址列表 ({len(self._addresses)})\n"]
        for addr, meta in self._addresses.items():
            lines.append(
                f"  [{meta['network'].upper()}] {meta['label']}: {addr[:12]}..."
            )
        return "\n".join(lines)

    @property
    def running(self) -> bool:
        return self._running


# Module-level singleton
whale_tracker = OnchainTracker()


# ── Smart Money Tracker ───────────────────────────────────────────────────────

SMART_WALLETS_FILE = os.path.join(BASE_DIR, ".smart_wallets.json")
SMART_MIN_BUY_USD = 50_000    # $50k threshold for ETH/BSC
SMART_MIN_SOL = 10            # 10 SOL minimum for Solana buys
SMART_SCAN_INTERVAL = 300     # 5 minutes
SMART_LOOKBACK_SECONDS = 360  # slightly more than scan interval
SMART_PERF_FILE = os.path.join(BASE_DIR, ".smart_signal_perf.json")

DEFAULT_SMART_WALLETS: dict = {
    # ETH smart money — publicly known high-profit addresses
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045": {"label": "Vitalik.eth", "network": "eth"},
    "0x28C6c06298d514Db089934071355E5743bf21d60": {"label": "Binance-Hot14", "network": "eth"},
    "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8": {"label": "Binance-Cold7", "network": "eth"},
    "0xF977814e90dA44bFA03b6295A0616a897441aceC": {"label": "Binance-Hot8", "network": "eth"},
    "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE": {"label": "Binance-Deposit", "network": "eth"},
    # SOL smart money — known high-volume Solana wallets
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {"label": "SOL-Whale-1", "network": "sol"},
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh": {"label": "SOL-SmartMoney-1", "network": "sol"},
    "7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE": {"label": "SOL-Alpha-1", "network": "sol"},
    "HN7cABqLq46Es1jh92dQQisAq662SmxELLLsHHe4YWrH": {"label": "SOL-Trader-1", "network": "sol"},
    "3tE3Hs7P2VbPEpBmAKvqEGMb1HzB6qRqjAnCjpfeFiLt": {"label": "SOL-Trader-2", "network": "sol"},
}


async def _dexscreener_token_info(contract_address: str) -> dict:
    """Fetch token price and info from Dexscreener (free, no key)."""
    if not contract_address:
        return {}
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{contract_address}"
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            data = resp.json()
        pairs = data.get("pairs") or []
        if pairs:
            p = pairs[0]
            return {
                "price_usd": float(p.get("priceUsd") or 0),
                "token_name": (p.get("baseToken") or {}).get("name", ""),
                "token_symbol": (p.get("baseToken") or {}).get("symbol", ""),
                "volume_24h": float((p.get("volume") or {}).get("h24") or 0),
                "liquidity_usd": float((p.get("liquidity") or {}).get("usd") or 0),
                "dex": p.get("dexId", ""),
            }
    except Exception as e:
        logger.debug("dexscreener %s: %s", contract_address[:10], e)
    return {}


async def _solscan_spl_transfers(address: str, lookback: int) -> list:
    """Fetch recent SPL token transfers for a Solana address."""
    try:
        url = "https://public-api.solscan.io/account/splTransfers"
        params = {"account": address, "limit": 25, "offset": 0}
        headers = {"accept": "application/json"}
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(url, params=params, headers=headers)
            if resp.status_code == 200:
                data = resp.json()
                items = (
                    data.get("data", []) if isinstance(data, dict)
                    else (data if isinstance(data, list) else [])
                )
                min_ts = int(time.time()) - lookback
                return [
                    tx for tx in items
                    if isinstance(tx, dict) and tx.get("blockTime", 0) >= min_ts
                ]
    except Exception as e:
        logger.debug("solscan_spl %s: %s", address[:8], e)
    return []


class SmartMoneyTracker:
    """Track known profitable wallets every 2 min. Alert on buys >$50k."""

    def __init__(self, send_func=None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._wallets: dict = {}
        self._recent_activity: list = []
        self._seen_hashes: set = set()
        self._perf_records: list = []   # [{tx_hash, token, entry_price, contract, timestamp, result}]
        self._etherscan_key = os.getenv("ETHERSCAN_API_KEY", "")
        self._bscscan_key = os.getenv("BSCSCAN_API_KEY", "")
        self._load_wallets()
        self._load_perf()

    # ── Wallet management ────────────────────────────────────────────────

    def _load_wallets(self):
        if os.path.exists(SMART_WALLETS_FILE):
            try:
                with open(SMART_WALLETS_FILE, "r", encoding="utf-8") as f:
                    self._wallets = json.load(f)
                logger.info("SmartMoneyTracker: loaded %d wallets", len(self._wallets))
                return
            except Exception as e:
                logger.warning("SmartMoneyTracker: load error: %s", e)
        self._wallets = dict(DEFAULT_SMART_WALLETS)
        self._save_wallets()
        logger.info("SmartMoneyTracker: using %d default wallets", len(self._wallets))

    def _save_wallets(self):
        try:
            with open(SMART_WALLETS_FILE, "w", encoding="utf-8") as f:
                json.dump(self._wallets, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("SmartMoneyTracker: save failed: %s", e)

    def add_wallet(self, address: str, label: str = "") -> bool:
        if address in self._wallets:
            return False
        net = _detect_network(address)
        self._wallets[address] = {
            "label": label or f"SmartMoney-{address[:8]}",
            "network": net,
            "added_at": int(time.time()),
        }
        self._save_wallets()
        logger.info("SmartMoneyTracker: added wallet %s", address[:12])
        return True

    def get_wallets(self) -> dict:
        return dict(self._wallets)

    # ── Activity management ───────────────────────────────────────────────

    def _prune_activity(self):
        cutoff = time.time() - 86400
        self._recent_activity = [a for a in self._recent_activity if a.get("timestamp", 0) >= cutoff]
        if len(self._seen_hashes) > 5000:
            self._seen_hashes = set(list(self._seen_hashes)[-2000:])

    def get_recent_activity(self, hours: int = 24) -> list:
        cutoff = time.time() - hours * 3600
        return [a for a in self._recent_activity if a.get("timestamp", 0) >= cutoff]

    # ── Performance tracking ──────────────────────────────────────────────

    def _load_perf(self):
        if os.path.exists(SMART_PERF_FILE):
            try:
                with open(SMART_PERF_FILE, "r", encoding="utf-8") as f:
                    self._perf_records = json.load(f)
                # Keep only last 30 days
                cutoff = time.time() - 30 * 86400
                self._perf_records = [r for r in self._perf_records if r.get("timestamp", 0) >= cutoff]
            except Exception:
                self._perf_records = []

    def _save_perf(self):
        try:
            with open(SMART_PERF_FILE, "w", encoding="utf-8") as f:
                json.dump(self._perf_records, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("SmartMoneyTracker: save perf failed: %s", e)

    def _register_signal_for_perf(self, sig: dict):
        """Register a new signal to be evaluated 24h later."""
        sig_ts = sig.get("timestamp", time.time())
        rec = {
            "tx_hash": sig.get("tx_hash", ""),
            "token": sig.get("token", "UNKNOWN"),
            "token_name": sig.get("token_name", sig.get("token", "UNKNOWN")),
            "contract_address": sig.get("contract_address", ""),
            "entry_price": sig.get("entry_price", 0),
            "amount_usd": sig.get("amount_usd", 0),
            "address_label": sig.get("address_label", ""),
            "timestamp": sig_ts,
            "check_at": sig_ts + 86400,  # evaluate 24h later
            "result": None,  # None=pending, True=profit, False=loss
            "exit_price": None,
            "pnl_pct": None,
        }
        self._perf_records.append(rec)
        self._save_perf()

    async def _evaluate_pending_signals(self):
        """Check signals that are >24h old and record price performance."""
        now = time.time()
        updated = False
        for rec in self._perf_records:
            if rec.get("result") is not None:
                continue
            if now < rec.get("check_at", now + 1):
                continue
            contract = rec.get("contract_address", "")
            if not contract:
                rec["result"] = "no_price"
                updated = True
                continue
            try:
                info = await _dexscreener_token_info(contract)
                current_price = info.get("price_usd", 0)
                entry_price = rec.get("entry_price", 0)
                if current_price and entry_price:
                    pnl_pct = (current_price - entry_price) / entry_price * 100
                    rec["exit_price"] = current_price
                    rec["pnl_pct"] = round(pnl_pct, 2)
                    rec["result"] = True if pnl_pct > 0 else False
                    updated = True
                    logger.info(
                        "SmartMoney perf: %s %s entry=%.6f exit=%.6f pnl=%.1f%%",
                        rec.get("address_label", "?"), rec.get("token", "?"), entry_price, current_price, pnl_pct
                    )
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.debug("evaluate_pending: %s", e)
        if updated:
            self._save_perf()

    def format_accuracy_report(self) -> str:
        evaluated = [r for r in self._perf_records if r.get("result") not in (None, "no_price")]
        pending = [r for r in self._perf_records if r.get("result") is None]
        if not evaluated and not pending:
            return "📊 尚无聪明钱信号表现数据"

        wins = [r for r in evaluated if r["result"] is True]
        losses = [r for r in evaluated if r["result"] is False]
        accuracy = len(wins) / len(evaluated) * 100 if evaluated else 0
        avg_pnl = sum(r["pnl_pct"] for r in evaluated) / len(evaluated) if evaluated else 0

        lines = [
            f"📊 聪明钱信号 24h 准确率报告",
            f"总信号: {len(evaluated)+len(pending)}  已评估: {len(evaluated)}  待评估: {len(pending)}",
            f"胜率: {accuracy:.1f}%  (✅{len(wins)} / ❌{len(losses)})",
            f"平均收益: {avg_pnl:+.1f}%",
        ]
        if evaluated:
            lines.append("\n最近5条记录:")
            for r in sorted(evaluated, key=lambda x: x["timestamp"], reverse=True)[:5]:
                ts = datetime.fromtimestamp(r["timestamp"]).strftime("%m/%d %H:%M")
                icon = "✅" if r["result"] else "❌"
                lines.append(
                    f"  {icon} {r['address_label']} {r['token']} "
                    f"入{r['entry_price']:.6g}→出{r.get('exit_price',0):.6g} "
                    f"{r['pnl_pct']:+.1f}% @{ts}"
                )
        return "\n".join(lines)

    # ── Background loop ───────────────────────────────────────────────────

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="smart_money_tracker")
        self._task.add_done_callback(self._on_done)
        logger.info("SmartMoneyTracker started, watching %d wallets", len(self._wallets))

    async def stop(self):
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _on_done(self, task: asyncio.Task):
        if not task.cancelled():
            try:
                task.result()
            except Exception as e:
                logger.error("SmartMoneyTracker loop crashed: %s", e, exc_info=True)

    async def _loop(self):
        await asyncio.sleep(15)  # short warm-up
        _perf_check_counter = 0
        while self._running:
            try:
                await self._scan_all()
                self._prune_activity()
                _perf_check_counter += 1
                # Evaluate 24h performance every 6 cycles (~30 min)
                if _perf_check_counter % 6 == 0:
                    await self._evaluate_pending_signals()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("SmartMoneyTracker scan error: %s", e)
            try:
                await asyncio.sleep(SMART_SCAN_INTERVAL)
            except asyncio.CancelledError:
                break

    # ── Scanning ──────────────────────────────────────────────────────────

    async def _scan_all(self):
        prices = await _get_prices()
        for address, meta in list(self._wallets.items()):
            try:
                signals = await self._scan_address(address, meta, prices)
                for sig in signals:
                    tx_hash = sig.get("tx_hash", "")
                    if tx_hash and tx_hash not in self._seen_hashes:
                        self._seen_hashes.add(tx_hash)
                        self._recent_activity.append(sig)
                        self._register_signal_for_perf(sig)
                        if self._send:
                            try:
                                await self._send(self._format_buy_signal(sig))
                            except Exception:
                                pass
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug("smart_scan %s: %s", address[:8], e)
            await asyncio.sleep(1.5)

    async def _scan_address(self, address: str, meta: dict, prices: dict) -> list:
        network = meta.get("network", "eth")
        label = meta.get("label", address[:8])
        found = []

        if network == "sol":
            for tx in await _solscan_spl_transfers(address, SMART_LOOKBACK_SECONDS):
                sig = await self._classify_sol_buy(tx, address, label, prices)
                if sig:
                    found.append(sig)


        elif network in ("eth", "bsc"):
            base_url = (
                "https://api.etherscan.io/api" if network == "eth"
                else "https://api.bscscan.com/api"
            )
            api_key = self._etherscan_key if network == "eth" else self._bscscan_key
            for tx in await _etherscan_tokentx(address, api_key, base_url):
                if tx.get("to", "").lower() != address.lower():
                    continue  # only receives (buys)
                sig = await self._classify_erc20_buy(tx, address, label, network, prices)
                if sig:
                    found.append(sig)

        return found

    async def _classify_erc20_buy(
        self, tx: dict, address: str, label: str, network: str, prices: dict
    ) -> Optional[dict]:
        try:
            token_symbol = tx.get("tokenSymbol", "UNKNOWN")
            token_name = tx.get("tokenName", token_symbol)
            contract_addr = tx.get("contractAddress", "")
            decimals = int(tx.get("tokenDecimal", 18))
            token_amount = int(tx.get("value", "0")) / (10 ** decimals)

            amount_usd = 0.0
            current_price = 0.0

            if token_symbol in _STABLECOINS:
                amount_usd = token_amount
                current_price = 1.0
            elif contract_addr:
                info = await _dexscreener_token_info(contract_addr)
                if info.get("price_usd"):
                    current_price = info["price_usd"]
                    amount_usd = token_amount * current_price
                    token_name = info.get("token_name") or token_name
                    token_symbol = info.get("token_symbol") or token_symbol
                else:
                    return None
            else:
                return None

            if amount_usd < SMART_MIN_BUY_USD:
                return None

            return {
                "address": address,
                "address_label": label,
                "address_masked": f"{address[:6]}...{address[-4:]}",
                "token": token_symbol,
                "token_name": token_name,
                "contract_address": contract_addr,
                "token_amount": round(token_amount, 4),
                "amount_usd": round(amount_usd),
                "current_price": current_price,
                "entry_price": current_price,
                "action": "buy",
                "tx_hash": tx.get("hash", ""),
                "timestamp": int(tx.get("timeStamp", time.time())),
                "network": network,
            }
        except Exception as e:
            logger.debug("classify_erc20_buy: %s", e)
            return None

    async def _classify_sol_buy(
        self, tx: dict, address: str, label: str, prices: dict
    ) -> Optional[dict]:
        try:
            dst = (
                tx.get("dst") or tx.get("toAddress") or tx.get("destinationOwner") or ""
            )
            if dst and dst != address:
                return None  # not a receive

            change_amount = tx.get("changeAmount") or tx.get("amount") or 0
            if not change_amount or int(change_amount) <= 0:
                return None

            decimals = int(tx.get("decimals") or 9)
            token_amount = abs(int(change_amount)) / (10 ** decimals)
            token_symbol = tx.get("tokenSymbol") or tx.get("symbol") or "UNKNOWN"
            token_name = tx.get("tokenName") or token_symbol
            token_address = tx.get("tokenAddress") or tx.get("mintAddress") or ""

            amount_usd = 0.0
            current_price = 0.0

            if token_address:
                info = await _dexscreener_token_info(token_address)
                if info.get("price_usd"):
                    current_price = info["price_usd"]
                    amount_usd = token_amount * current_price
                    token_name = info.get("token_name") or token_name
                    token_symbol = info.get("token_symbol") or token_symbol

            sol_price = prices.get("SOL", 150)
            min_usd = SMART_MIN_SOL * sol_price
            if amount_usd < min_usd:
                return None

            return {
                "address": address,
                "address_label": label,
                "address_masked": f"{address[:6]}...{address[-4:]}",
                "token": token_symbol,
                "token_name": token_name,
                "contract_address": token_address,
                "token_amount": round(token_amount, 4),
                "amount_usd": round(amount_usd),
                "current_price": current_price,
                "entry_price": current_price,
                "action": "buy",
                "tx_hash": tx.get("txHash") or tx.get("signature") or "",
                "timestamp": int(tx.get("blockTime") or time.time()),
                "network": "sol",
            }
        except Exception as e:
            logger.debug("classify_sol_buy: %s", e)
            return None

    # ── Formatting ────────────────────────────────────────────────────────

    def _format_buy_signal(self, sig: dict) -> str:
        ts = datetime.fromtimestamp(sig.get("timestamp", 0)).strftime("%H:%M:%S")
        network = sig.get("network", "?")
        net_emoji = {"eth": "🔷", "bsc": "🟡", "sol": "☀️"}.get(network, "🔗")
        addr_masked = sig.get("address_masked", sig.get("address", "?")[:10])
        token = sig.get("token", "UNKNOWN")
        amount_usd = sig.get("amount_usd", 0)

        p = sig.get("current_price", 0)
        if p >= 1:
            price_str = f"${p:,.4f}"
        elif p >= 0.0001:
            price_str = f"${p:.6f}"
        else:
            price_str = f"${p:.8f}" if p else "未知"

        sol_min = SMART_MIN_SOL * _PRICE_CACHE.get("SOL", 150)
        threshold = f"{SMART_MIN_SOL} SOL" if network == "sol" else f"${SMART_MIN_BUY_USD // 1000}k"
        return "\n".join([
            f"🚨 [聪明钱信号] {net_emoji}",
            f"地址{addr_masked} ({sig.get('address_label', '?')}) 买入 {token} ${amount_usd:,.0f}",
            f"数量: {sig.get('token_amount', 0):,.2f} {token}  现价: {price_str}",
            f"时间: {ts}  网络: {network.upper()}",
            f"| 跟单建议: 买入",
        ])

    def format_wallet_list(self) -> str:
        if not self._wallets:
            return "⚠️ 没有跟踪的聪明钱地址"
        lines = [f"👁 聪明钱跟踪列表 ({len(self._wallets)}个)\n"]
        for addr, meta in list(self._wallets.items())[:20]:
            net_emoji = {"eth": "🔷", "bsc": "🟡", "sol": "☀️"}.get(meta.get("network", ""), "🔗")
            added = ""
            if meta.get("added_at"):
                added = f" (added {datetime.fromtimestamp(meta['added_at']).strftime('%m/%d')})"
            lines.append(
                f"{net_emoji} {meta.get('label', '?')}: {addr[:8]}...{addr[-4:]}{added}"
            )
        return "\n".join(lines)

    def format_recent_activity(self, hours: int = 24) -> str:
        activity = self.get_recent_activity(hours)
        if not activity:
            return f"📭 过去{hours}小时无大额聪明钱买入(>${SMART_MIN_BUY_USD // 1000}k)"
        lines = [f"📊 近{hours}h聪明钱买入 ({len(activity)}笔)\n"]
        for sig in sorted(activity, key=lambda x: x.get("timestamp", 0), reverse=True)[:10]:
            ts = datetime.fromtimestamp(sig.get("timestamp", 0)).strftime("%H:%M")
            lines.append(
                f"🟢 {sig.get('address_label', '?')}: 买{sig.get('token', '?')} ${sig.get('amount_usd', 0):,.0f} @{ts}"
            )
        return "\n".join(lines)

    @property
    def running(self) -> bool:
        return self._running


# Module-level singleton
smart_tracker = SmartMoneyTracker()
