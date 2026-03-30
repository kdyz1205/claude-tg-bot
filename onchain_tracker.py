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
        for addr in re.findall(r"\b[1-9A-HJ-NP-Za-km-z]{40,44}\b", content):
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

# ── v2 Smart Money constants ────────────────────────────────────────────────
SMART_CACHE_FILE = os.path.join(BASE_DIR, ".smart_wallet_cache.json")
SMART_CACHE_SAVE_INTERVAL = 600     # persist cache every 10 min
WHALE_SOL_THRESHOLD = 50            # SOL amount for whale alert
CONSENSUS_MIN_WALLETS = 3           # min high-score wallets for consensus
HIGH_SCORE_THRESHOLD = 70           # wallet score >= 70 = high-score
MIN_TRADES_FOR_SCORE = 3            # need >= 3 trades before score is meaningful
OUTCOME_CHECK_DELAY = 4 * 3600      # 4h before checking outcome
OUTCOME_WIN_PCT = 20.0              # >=20% gain = win
CONSENSUS_WINDOW_SEC = 3600         # 1-hour consensus window
DISCOVERY_CHECKPOINTS = [3600, 4*3600, 24*3600]  # 1h, 4h, 24h follow-up

DEFAULT_SMART_WALLETS: dict = {
    # ETH smart money — publicly known high-profit addresses
    "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045": {"label": "Vitalik.eth", "network": "eth"},
    "0x28C6c06298d514Db089934071355E5743bf21d60": {"label": "Binance-Hot14", "network": "eth"},
    "0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8": {"label": "Binance-Cold7", "network": "eth"},
    "0xF977814e90dA44bFA03b6295A0616a897441aceC": {"label": "Binance-Hot8", "network": "eth"},
    "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE": {"label": "Binance-Deposit", "network": "eth"},
    # SOL smart money — known high-volume Solana wallets (expanded v2)
    "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM": {"label": "SOL-Whale-1", "network": "sol"},
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh": {"label": "SOL-SmartMoney-1", "network": "sol"},
    "7VHUFJHWu2CuExkJcJrzhQPJ2oygupTWkL2A2For4BmE": {"label": "SOL-Alpha-1", "network": "sol"},
    "HN7cABqLq46Es1jh92dQQisAq662SmxELLLsHHe4YWrH": {"label": "SOL-Trader-1", "network": "sol"},
    "3tE3Hs7P2VbPEpBmAKvqEGMb1HzB6qRqjAnCjpfeFiLt": {"label": "SOL-Trader-2", "network": "sol"},
    # v2 expanded: curated high-profit SOL wallets
    "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1": {"label": "SOL-Raydium-Authority", "network": "sol"},
    "2iZo6zrSQWgcVfFsmCXEcqQGPKySMfckJwHTTGCDatAy": {"label": "SOL-DeFi-Alpha-1", "network": "sol"},
    "FbGeZS8LiPCZiFpFwdUUeF2yxXtSsdfJoHTsVMvM8STh": {"label": "SOL-MEV-Trader-1", "network": "sol"},
    "Gx5dx4YEt7PLKYU62i1UWdxjNr3rmH4CAFGfVLLb2PJ4": {"label": "SOL-VC-Wallet-1", "network": "sol"},
    "AGNHGKiuZwrxMPDmpJsLyp3HtYDJCB2Cg5kxFLRFjhSs": {"label": "SOL-Sniper-Alpha", "network": "sol"},
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
        # ── v2 state ──
        self._token_price_cache: dict = {}          # {contract: {price, ts, info}}
        self._new_token_discoveries: dict = {}      # {contract: {wallets, first_seen, price_at_discovery, checkpoints}}
        self._token_buys_1h: dict = {}              # {contract: [{wallet, ts, amount_usd}]}
        self._emitted_consensus: set = set()        # hour-bucket keys to avoid dup alerts
        self._last_cache_save: float = 0.0
        self._load_wallets()
        self._load_perf()
        self._load_cache()

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
            _tmp = SMART_WALLETS_FILE + ".tmp"
            with open(_tmp, "w", encoding="utf-8") as f:
                json.dump(self._wallets, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(_tmp, SMART_WALLETS_FILE)
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
            self._seen_hashes = set(list(self._seen_hashes)[-2500:])

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
            _tmp = SMART_PERF_FILE + ".tmp"
            with open(_tmp, "w", encoding="utf-8") as f:
                json.dump(self._perf_records, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(_tmp, SMART_PERF_FILE)
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

    # ── v2: Cache load/save ──────────────────────────────────────────────

    def _load_cache(self):
        if os.path.exists(SMART_CACHE_FILE):
            try:
                with open(SMART_CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._token_price_cache = data.get("token_prices", {})
                self._new_token_discoveries = data.get("new_token_discoveries", {})
                # Rebuild emitted_consensus from saved set
                self._emitted_consensus = set(data.get("emitted_consensus", []))
                logger.info("SmartMoney v2 cache loaded")
            except Exception as e:
                logger.debug("v2 cache load failed: %s", e)

    def _save_cache(self):
        try:
            data = {
                "token_prices": self._token_price_cache,
                "new_token_discoveries": self._new_token_discoveries,
                "emitted_consensus": list(self._emitted_consensus)[-500:],
                "saved_at": time.time(),
            }
            _tmp = SMART_CACHE_FILE + ".tmp"
            with open(_tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(_tmp, SMART_CACHE_FILE)
            self._last_cache_save = time.time()
        except Exception as e:
            logger.warning("v2 cache save failed: %s", e)

    def _maybe_save_cache(self):
        if time.time() - self._last_cache_save >= SMART_CACHE_SAVE_INTERVAL:
            self._save_cache()

    # ── v2: Cached Dexscreener lookup ────────────────────────────────────

    async def _cached_token_info(self, contract_address: str) -> dict:
        """Dexscreener lookup with 5-min TTL cache."""
        if not contract_address:
            return {}
        cached = self._token_price_cache.get(contract_address)
        if cached and time.time() - cached.get("ts", 0) < 300:
            return cached.get("info", {})
        info = await _dexscreener_token_info(contract_address)
        if info:
            self._token_price_cache[contract_address] = {
                "info": info, "ts": time.time(),
            }
        return info

    # ── v2: Wallet scoring system ────────────────────────────────────────

    def _get_wallet_score(self, address: str) -> int:
        """Return wallet score 0-100. Default 50 for new wallets."""
        meta = self._wallets.get(address, {})
        return meta.get("score", 50)

    def _is_high_score(self, address: str) -> bool:
        """True if wallet has score >= 70 AND at least 3 trades."""
        meta = self._wallets.get(address, {})
        score = meta.get("score", 50)
        total = meta.get("total_trades", 0)
        return score >= HIGH_SCORE_THRESHOLD and total >= MIN_TRADES_FOR_SCORE

    def _record_pending_outcome(self, address: str, contract: str, entry_price: float, token: str):
        """Queue an outcome check for this buy signal."""
        meta = self._wallets.get(address)
        if not meta:
            return
        pending = meta.setdefault("pending_outcomes", [])
        pending.append({
            "contract": contract,
            "entry_price": entry_price,
            "token": token,
            "ts": time.time(),
            "check_at": time.time() + OUTCOME_CHECK_DELAY,
        })
        # Cap pending list
        if len(pending) > 50:
            meta["pending_outcomes"] = pending[-50:]

    async def _resolve_pending_outcomes(self):
        """Check pending outcomes that are past their check_at time."""
        now = time.time()
        updated = False
        for address, meta in self._wallets.items():
            pending = meta.get("pending_outcomes", [])
            still_pending = []
            for p in pending:
                if now < p.get("check_at", now + 1):
                    still_pending.append(p)
                    continue
                # Time to evaluate
                contract = p.get("contract", "")
                entry_price = p.get("entry_price", 0)
                if not contract or not entry_price:
                    continue
                try:
                    info = await self._cached_token_info(contract)
                    current_price = info.get("price_usd", 0)
                    if current_price and entry_price:
                        pnl_pct = (current_price - entry_price) / entry_price * 100
                        is_win = pnl_pct >= OUTCOME_WIN_PCT
                        meta["total_trades"] = meta.get("total_trades", 0) + 1
                        if is_win:
                            meta["win_trades"] = meta.get("win_trades", 0) + 1
                        # Recalculate score
                        total = meta["total_trades"]
                        wins = meta.get("win_trades", 0)
                        meta["score"] = int((wins / total) * 100) if total > 0 else 50
                        meta["last_score_update"] = now
                        updated = True
                        logger.info(
                            "Wallet score updated: %s score=%d (W%d/T%d) pnl=%.1f%%",
                            meta.get("label", address[:8]), meta["score"], wins, total, pnl_pct
                        )
                        # Also update discovery follow-up if applicable
                        await self._update_discovery_follow_results(contract, current_price)
                except Exception as e:
                    logger.debug("resolve_outcome %s: %s", address[:8], e)
                    still_pending.append(p)  # retry later
            meta["pending_outcomes"] = still_pending
        if updated:
            self._save_wallets()

    # ── v2: Multi-wallet consensus signal ────────────────────────────────

    def _update_consensus_window(self, contract: str, address: str, amount_usd: float, token: str):
        """Add a high-score wallet buy to the consensus window."""
        if not self._is_high_score(address):
            return
        now = time.time()
        buys = self._token_buys_1h.setdefault(contract, [])
        # Dedup: don't double-count same wallet in same window
        for b in buys:
            if b["wallet"] == address:
                return
        buys.append({
            "wallet": address,
            "label": self._wallets.get(address, {}).get("label", address[:8]),
            "ts": now,
            "amount_usd": amount_usd,
            "token": token,
        })
        # Prune entries older than 1h
        cutoff = now - CONSENSUS_WINDOW_SEC
        self._token_buys_1h[contract] = [b for b in buys if b["ts"] >= cutoff]

    async def _check_consensus(self, contract: str, token: str):
        """Check if consensus threshold is met and emit alert."""
        buys = self._token_buys_1h.get(contract, [])
        now = time.time()
        cutoff = now - CONSENSUS_WINDOW_SEC
        active_buys = [b for b in buys if b["ts"] >= cutoff]
        if len(active_buys) < CONSENSUS_MIN_WALLETS:
            return
        # Dedup key: hour bucket + contract
        hour_key = f"{int(now // 3600)}_{contract[:16]}"
        if hour_key in self._emitted_consensus:
            return
        self._emitted_consensus.add(hour_key)
        # Prune old keys
        if len(self._emitted_consensus) > 1000:
            self._emitted_consensus = set(list(self._emitted_consensus)[-500:])

        msg = self._format_consensus_signal(contract, token, active_buys)
        if self._send:
            try:
                await self._send(msg)
            except Exception:
                pass
        logger.info("Consensus signal emitted: %s (%d wallets)", token, len(active_buys))

    def _format_consensus_signal(self, contract: str, token: str, buys: list) -> str:
        total_usd = sum(b.get("amount_usd", 0) for b in buys)
        wallet_lines = []
        for b in buys:
            score = self._get_wallet_score(b["wallet"])
            wallet_lines.append(
                f"  • {b['label']} (评分{score}) ${b['amount_usd']:,.0f}"
            )
        return "\n".join([
            f"🔥🔥🔥 聪明钱共识买入信号",
            f"代币: {token}",
            f"合约: {contract[:12]}...{contract[-6:]}",
            f"1h内 {len(buys)} 个高分钱包独立买入！",
            f"总金额: ${total_usd:,.0f}",
            "",
            *wallet_lines,
            "",
            f"⚡ 强信号 — 多鲸鱼独立判断一致",
        ])

    # ── v2: Whale alert (large SOL new position) ────────────────────────

    def _is_new_position(self, address: str, contract: str) -> bool:
        """Check if wallet hasn't bought this token in last 7 days."""
        cutoff = time.time() - 7 * 86400
        for a in self._recent_activity:
            if (a.get("address") == address
                    and a.get("contract_address") == contract
                    and a.get("timestamp", 0) >= cutoff):
                return False
        # Also check perf records
        for r in self._perf_records:
            if (r.get("contract_address") == contract
                    and r.get("timestamp", 0) >= cutoff):
                # Check if this was from the same wallet (approximate — perf doesn't store address)
                pass
        return True

    def _format_whale_alert(self, sig: dict) -> str:
        """Format whale new-position alert."""
        score = self._get_wallet_score(sig.get("address", ""))
        p = sig.get("current_price", 0)
        price_str = f"${p:,.6f}" if p < 1 else f"${p:,.4f}" if p else "未知"
        return "\n".join([
            f"🚨🐳 鲸鱼新建仓预警",
            f"地址: {sig.get('address_label', '?')} (评分{score})",
            f"代币: {sig.get('token', '?')} ({sig.get('token_name', '')})",
            f"金额: ${sig.get('amount_usd', 0):,.0f}  数量: {sig.get('token_amount', 0):,.2f}",
            f"现价: {price_str}",
            f"网络: {sig.get('network', '?').upper()}",
            f"⚡ 首次建仓 — 重点关注",
        ])

    # ── v2: New token discovery tracking ─────────────────────────────────

    async def _handle_new_token_discovery(self, contract: str, token: str, address: str, price: float):
        """Track first-ever smart-money buy of a token."""
        if not contract or contract in self._new_token_discoveries:
            # Already discovered — just append wallet
            if contract in self._new_token_discoveries:
                wallets = self._new_token_discoveries[contract].setdefault("wallets", [])
                if address not in wallets:
                    wallets.append(address)
            return
        # First discovery!
        self._new_token_discoveries[contract] = {
            "token": token,
            "wallets": [address],
            "first_seen": time.time(),
            "price_at_discovery": price,
            "checkpoints": {},  # {3600: {price, pnl_pct, ts}, ...}
        }
        msg = self._format_new_token_alert(contract, token, address, price)
        if self._send:
            try:
                await self._send(msg)
            except Exception:
                pass
        logger.info("New token discovered: %s at $%.8f by %s", token, price, address[:8])

    def _format_new_token_alert(self, contract: str, token: str, address: str, price: float) -> str:
        label = self._wallets.get(address, {}).get("label", address[:8])
        score = self._get_wallet_score(address)
        price_str = f"${price:.8f}" if price < 0.001 else f"${price:.6f}" if price < 1 else f"${price:.4f}"
        return "\n".join([
            f"🆕 聪明钱首次发现新代币",
            f"代币: {token}",
            f"合约: {contract[:12]}...{contract[-6:]}",
            f"发现者: {label} (评分{score})",
            f"发现时价格: {price_str}",
            f"📌 已加入追踪 — 将在1h/4h/24h后回查表现",
        ])

    async def _update_discovery_follow_results(self, contract: str, current_price: float):
        """Append checkpoint data for discovered tokens."""
        disc = self._new_token_discoveries.get(contract)
        if not disc or not current_price:
            return
        now = time.time()
        first_seen = disc.get("first_seen", now)
        entry_price = disc.get("price_at_discovery", 0)
        if not entry_price:
            return
        for cp_seconds in DISCOVERY_CHECKPOINTS:
            cp_key = str(cp_seconds)
            if cp_key in disc.get("checkpoints", {}):
                continue
            if now - first_seen >= cp_seconds:
                pnl_pct = (current_price - entry_price) / entry_price * 100
                disc.setdefault("checkpoints", {})[cp_key] = {
                    "price": current_price,
                    "pnl_pct": round(pnl_pct, 2),
                    "ts": now,
                }
                label = {3600: "1h", 14400: "4h", 86400: "24h"}.get(cp_seconds, f"{cp_seconds}s")
                logger.info(
                    "Discovery checkpoint %s %s: %.8f → %.8f (%.1f%%)",
                    disc.get("token", "?"), label, entry_price, current_price, pnl_pct
                )
                # Send checkpoint alert if significant
                if abs(pnl_pct) >= 10 and self._send:
                    emoji = "🟢" if pnl_pct > 0 else "🔴"
                    try:
                        await self._send(
                            f"📊 新币追踪 {label}回查\n"
                            f"{emoji} {disc.get('token', '?')}: {pnl_pct:+.1f}%\n"
                            f"发现价 ${entry_price:.8g} → 现价 ${current_price:.8g}\n"
                            f"发现者: {len(disc.get('wallets', []))}个聪明钱"
                        )
                    except Exception:
                        pass

    async def _check_all_discoveries(self):
        """Periodically check price for all tracked discoveries."""
        for contract, disc in list(self._new_token_discoveries.items()):
            if len(disc.get("checkpoints", {})) >= len(DISCOVERY_CHECKPOINTS):
                continue  # all checkpoints done
            try:
                info = await self._cached_token_info(contract)
                if info.get("price_usd"):
                    await self._update_discovery_follow_results(contract, info["price_usd"])
            except Exception as e:
                logger.debug("discovery check %s: %s", contract[:10], e)
            await asyncio.sleep(0.5)

    # ── v2: Helius API integration ───────────────────────────────────────

    async def _fetch_helius_top_wallets(self) -> list:
        """Fetch top profitable wallets from Helius (if API key set)."""
        api_key = os.getenv("HELIUS_API_KEY", "")
        if not api_key:
            return []
        try:
            url = f"https://api.helius.xyz/v0/addresses/top-traders"
            params = {"api-key": api_key, "limit": 100}
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.get(url, params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    return [w.get("address", "") for w in data if w.get("address")]
        except Exception as e:
            logger.debug("helius top wallets: %s", e)
        return []

    async def _refresh_wallet_list(self):
        """Refresh wallet list from Helius + auto-promote discovered wallets."""
        # Try Helius first
        new_wallets = await self._fetch_helius_top_wallets()
        added = 0
        for addr in new_wallets[:50]:
            if addr not in self._wallets:
                self._wallets[addr] = {
                    "label": f"Helius-Top-{addr[:6]}",
                    "network": "sol",
                    "score": 50,
                    "source": "helius",
                    "added_at": int(time.time()),
                }
                added += 1
        if added:
            self._save_wallets()
            logger.info("Added %d wallets from Helius", added)

        # Auto-promote discovered wallets with good track records
        self._auto_promote_discovered_wallets()

    def _auto_promote_discovered_wallets(self):
        """Promote discovered wallets with >=5 trades and score >=60 to main list."""
        promoted = 0
        for addr, meta in list(self._wallets.items()):
            if meta.get("source") == "auto_discovered":
                continue
            # Check if any discovery wallets should be promoted
        # Look through discoveries for consistently appearing wallets
        wallet_appearances: dict = {}
        for disc in self._new_token_discoveries.values():
            for w in disc.get("wallets", []):
                wallet_appearances[w] = wallet_appearances.get(w, 0) + 1
        for addr, count in wallet_appearances.items():
            if addr in self._wallets:
                continue
            if count >= 5:
                self._wallets[addr] = {
                    "label": f"AutoDiscovered-{addr[:6]}",
                    "network": _detect_network(addr),
                    "score": 50,
                    "total_trades": count,
                    "source": "auto_discovered",
                    "added_at": int(time.time()),
                }
                promoted += 1
        if promoted:
            self._save_wallets()
            logger.info("Auto-promoted %d discovered wallets", promoted)

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
        self._save_cache()  # v2: persist cache on stop
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
        _wallet_refresh_counter = 0
        while self._running:
            try:
                await self._scan_all()
                self._prune_activity()
                self._maybe_save_cache()
                _perf_check_counter += 1
                _wallet_refresh_counter += 1
                # Evaluate outcomes + resolve wallet scores every 6 cycles (~30 min)
                if _perf_check_counter % 6 == 0:
                    await self._evaluate_pending_signals()
                    await self._resolve_pending_outcomes()
                    await self._check_all_discoveries()
                # Refresh wallet list every 72 cycles (~6h)
                if _wallet_refresh_counter % 72 == 0:
                    await self._refresh_wallet_list()
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

                        contract = sig.get("contract_address", "")
                        token = sig.get("token", "UNKNOWN")
                        entry_price = sig.get("entry_price", 0)
                        amount_usd = sig.get("amount_usd", 0)

                        # v2: Record pending outcome for wallet scoring
                        self._record_pending_outcome(address, contract, entry_price, token)

                        # v2: Update consensus window + check
                        self._update_consensus_window(contract, address, amount_usd, token)
                        await self._check_consensus(contract, token)

                        # v2: New token discovery tracking
                        await self._handle_new_token_discovery(contract, token, address, entry_price)

                        # v2: Whale alert for large SOL new positions
                        is_whale = False
                        if sig.get("network") == "sol":
                            sol_price = prices.get("SOL", 150)
                            sol_amount = amount_usd / sol_price if sol_price else 0
                            if sol_amount >= WHALE_SOL_THRESHOLD and self._is_new_position(address, contract):
                                is_whale = True

                        if self._send:
                            try:
                                if is_whale:
                                    await self._send(self._format_whale_alert(sig))
                                else:
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
                info = await self._cached_token_info(contract_addr)
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
                info = await self._cached_token_info(token_address)
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

        score = self._get_wallet_score(sig.get("address", ""))
        score_tag = f" ⭐评分{score}" if score >= HIGH_SCORE_THRESHOLD else f" 评分{score}"
        return "\n".join([
            f"🚨 [聪明钱信号] {net_emoji}",
            f"地址{addr_masked} ({sig.get('address_label', '?')}){score_tag}",
            f"买入 {token} ${amount_usd:,.0f}",
            f"数量: {sig.get('token_amount', 0):,.2f} {token}  现价: {price_str}",
            f"时间: {ts}  网络: {network.upper()}",
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
