"""
Real-time chain listeners via WebSocket.

- EVM (ETH / BSC): AsyncWeb3 + ``wss://`` WebSocketProvider, ``eth_subscribe`` on logs + newHeads.
- Solana: ``logs_subscribe`` (mentions) over WSS; tx bodies via Solana JSON-RPC (not used for spot quotes).

Each chain runner: **outer** ``while tracker._running`` + ``try/except`` + **exponential backoff**
(``_reconnect_sleep_s``) so a dropped socket or hub error cannot leave the radar idle forever.

Smart-money path: stablecoin/SOL/USDC/USDT only — no HTTP price APIs. Spot USD for majors comes from
``onchain_tracker._get_prices`` → OKX public **ticker WebSocket** hub.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from typing import Any

from hexbytes import HexBytes
from web3 import Web3

logger = logging.getLogger(__name__)

# keccak256("Transfer(address,address,uint256)") — match RPC log topics (0x-prefixed)
TRANSFER_EVENT_TOPIC = "0x" + Web3.keccak(text="Transfer(address,address,uint256)").hex()

_STABLE_CONTRACTS_ETH: dict[str, tuple[str, int]] = {
    "0xdac17f958d2ee523a2206206994597c13d831ec7": ("USDT", 6),
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": ("USDC", 6),
    "0x6b175474e89094c44da98b954eedeac495271d0f": ("DAI", 18),
    "0x4fabb145d64652a948d72533023f6e7a623c7c53": ("BUSD", 18),
}

_STABLE_CONTRACTS_BSC: dict[str, tuple[str, int]] = {
    "0x55d398326f99059ff775485246999027b3197955": ("USDT", 18),
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": ("USDC", 18),
    "0xe9e7cea3dedca5984780bafc599bd69add087d56": ("BUSD", 18),
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": ("DAI", 18),
}

_ERC20_DECIMALS_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "type": "function",
    }
]

_erc20_decimals_cache: dict[str, int] = {}


def _reconnect_sleep_s(attempt: int) -> float:
    return min(120.0, 2.0 ** min(attempt, 7)) + random.uniform(0, 2.0)


def _evm_stable_symbol_decimals(network: str, contract: str) -> tuple[str, int] | None:
    """Known main-net stables only — smart-money WS path skips all other ERC-20 (no HTTP quote)."""
    cmap = _STABLE_CONTRACTS_ETH if network == "eth" else _STABLE_CONTRACTS_BSC
    try:
        c = Web3.to_checksum_address(contract).lower()
    except Exception:
        c = (contract or "").lower()
    for addr, pair in cmap.items():
        if addr.lower() == c:
            return pair
    return None


def _pad_addr_topic(addr: str) -> str:
    raw = Web3.to_checksum_address(addr).lower().replace("0x", "")
    return "0x" + raw.rjust(64, "0")


def _topic_to_addr(topic_hex: str) -> str:
    if not topic_hex or topic_hex == "0x":
        return ""
    h = topic_hex.lower().replace("0x", "")
    return Web3.to_checksum_address("0x" + h[-40:])


def _log_tx_hash_hex(log: dict[str, Any]) -> str:
    th = log.get("transactionHash")
    if th is None:
        return ""
    if hasattr(th, "hex"):
        return th.hex()
    return str(th)


def _raw_transfer_amount(log: dict[str, Any]) -> int:
    data = log.get("data") or "0x"
    if data in ("0x", b""):
        return 0
    try:
        return int(HexBytes(data).hex(), 16)
    except Exception:
        return 0


async def _erc20_decimals(w3: Any, contract: str) -> int:
    key = contract.lower()
    if key in _erc20_decimals_cache:
        return _erc20_decimals_cache[key]
    c = Web3.to_checksum_address(contract)
    ctr = w3.eth.contract(address=c, abi=_ERC20_DECIMALS_ABI)
    d = int(await ctr.functions.decimals().call())
    _erc20_decimals_cache[key] = d
    return d


def _synthetic_erc20_tx(
    *,
    log: dict[str, Any],
    topics: list[str],
    token_symbol: str,
    decimals: int,
    raw_amount: int,
    contract: str,
) -> dict[str, Any]:
    if len(topics) < 3:
        return {}
    return {
        "hash": _log_tx_hash_hex(log),
        "tokenSymbol": token_symbol,
        "tokenDecimal": decimals,
        "value": str(raw_amount),
        "from": _topic_to_addr(topics[1]),
        "to": _topic_to_addr(topics[2]),
        "timeStamp": int(time.time()),
        "contractAddress": Web3.to_checksum_address(contract)
        if contract
        else contract,
    }


async def _run_evm_ws_whale(
    tracker: "OnchainTracker",
    network: str,
    wss_url: str,
) -> None:
    from web3 import AsyncWeb3
    from web3.providers.persistent import WebSocketProvider
    from web3.utils.subscriptions import LogsSubscription, NewHeadsSubscription

    stable_map = _STABLE_CONTRACTS_ETH if network == "eth" else _STABLE_CONTRACTS_BSC
    addrs = [a for a, m in tracker._addresses.items() if m.get("network") == network]
    if not addrs or not wss_url:
        logger.info("onchain_ws whale: skip %s (no addresses or empty WSS URL)", network)
        return

    topic_list_in = [_pad_addr_topic(a) for a in addrs]
    topic_list_out = topic_list_in.copy()
    watched_lower = {a.lower() for a in addrs}
    labels = {a.lower(): (a, tracker._addresses[a].get("label", a[:8])) for a in addrs}

    attempt = 0
    while tracker._running:
        try:
            async with AsyncWeb3(WebSocketProvider(wss_url)) as w3:
                attempt = 0
                w3.subscription_manager.parallelize = True

                async def on_incoming(ctx: Any) -> None:
                    try:
                        log = ctx.result
                        topics = [
                            t.hex() if hasattr(t, "hex") else str(t) for t in (log.get("topics") or [])
                        ]
                        if not topics or topics[0].lower() != TRANSFER_EVENT_TOPIC.lower():
                            return
                        c_raw = log.get("address")
                        contract = Web3.to_checksum_address(c_raw) if c_raw else ""
                        ck = contract.lower()
                        if ck not in stable_map:
                            return
                        sym, dec = stable_map[ck]
                        raw_amt = _raw_transfer_amount(log)
                        txd = _synthetic_erc20_tx(
                            log=log,
                            topics=topics,
                            token_symbol=sym,
                            decimals=dec,
                            raw_amount=raw_amt,
                            contract=contract,
                        )
                        if not txd:
                            return
                        from onchain_tracker import _classify_erc20

                        to_l = txd["to"].lower()
                        if to_l not in labels:
                            return
                        addr, label = labels[to_l]
                        c = _classify_erc20(txd, addr)
                        if not c:
                            return
                        await tracker._push_whale_signal_from_classified(c, addr, label, network)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug("whale on_incoming %s: %s", network, e)

                async def on_outgoing(ctx: Any) -> None:
                    try:
                        log = ctx.result
                        topics = [
                            t.hex() if hasattr(t, "hex") else str(t) for t in (log.get("topics") or [])
                        ]
                        if not topics or topics[0].lower() != TRANSFER_EVENT_TOPIC.lower():
                            return
                        c_raw = log.get("address")
                        contract = Web3.to_checksum_address(c_raw) if c_raw else ""
                        ck = contract.lower()
                        if ck not in stable_map:
                            return
                        sym, dec = stable_map[ck]
                        raw_amt = _raw_transfer_amount(log)
                        txd = _synthetic_erc20_tx(
                            log=log,
                            topics=topics,
                            token_symbol=sym,
                            decimals=dec,
                            raw_amount=raw_amt,
                            contract=contract,
                        )
                        if not txd:
                            return
                        from onchain_tracker import _classify_erc20

                        frm = _topic_to_addr(topics[1]).lower()
                        if frm not in labels:
                            return
                        addr, label = labels[frm]
                        c = _classify_erc20(txd, addr)
                        if not c:
                            return
                        await tracker._push_whale_signal_from_classified(c, addr, label, network)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug("whale on_outgoing %s: %s", network, e)

                async def on_head(ctx: Any) -> None:
                    try:
                        blk_id = ctx.result
                        block = await w3.eth.get_block(blk_id, full_transactions=True)
                        ts = int(block.get("timestamp", time.time()))
                        prices = getattr(tracker, "_prices_live", {}) or {}
                        for tx in block.get("transactions") or []:
                            try:
                                v = int(tx.get("value", 0) or 0)
                                if v == 0:
                                    continue
                                frm = (tx.get("from") or "").lower()
                                to = (tx.get("to") or "").lower()
                                th = tx.get("hash")
                                tx_hash = th.hex() if hasattr(th, "hex") else str(th)
                                if to in watched_lower:
                                    addr, label = labels[to]
                                    fake = {
                                        "value": str(v),
                                        "to": Web3.to_checksum_address(addr),
                                        "from": Web3.to_checksum_address(frm) if frm else "",
                                        "hash": tx_hash,
                                        "timeStamp": ts,
                                    }
                                    from onchain_tracker import _classify_native

                                    cl = _classify_native(fake, addr, prices, network)
                                    if cl:
                                        cl["timestamp"] = ts
                                        await tracker._push_whale_signal_from_classified(
                                            cl, addr, label, network
                                        )
                                if frm in watched_lower:
                                    addr, label = labels[frm]
                                    if not to:
                                        continue
                                    fake = {
                                        "value": str(v),
                                        "from": Web3.to_checksum_address(addr),
                                        "to": Web3.to_checksum_address(to),
                                        "hash": tx_hash,
                                        "timeStamp": ts,
                                    }
                                    from onchain_tracker import _classify_native

                                    cl = _classify_native(fake, addr, prices, network)
                                    if cl:
                                        cl["timestamp"] = ts
                                        await tracker._push_whale_signal_from_classified(
                                            cl, addr, label, network
                                        )
                            except asyncio.CancelledError:
                                raise
                            except Exception as e:
                                logger.debug("whale native tx %s: %s", network, e)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug("whale get_block %s: %s", network, e)

                inc = LogsSubscription(
                    topics=[TRANSFER_EVENT_TOPIC, None, topic_list_in],
                    handler=on_incoming,
                    label=f"whale-{network}-erc20-in",
                )
                out = LogsSubscription(
                    topics=[TRANSFER_EVENT_TOPIC, topic_list_out, None],
                    handler=on_outgoing,
                    label=f"whale-{network}-erc20-out",
                )
                heads = NewHeadsSubscription(
                    handler=on_head,
                    label=f"whale-{network}-heads",
                )
                await w3.subscription_manager.subscribe(inc)
                await w3.subscription_manager.subscribe(out)
                await w3.subscription_manager.subscribe(heads)
                logger.info("OnchainTracker whale WS connected (%s), subscriptions active", network)
                await w3.subscription_manager.handle_subscriptions(run_forever=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            delay = _reconnect_sleep_s(attempt)
            logger.warning(
                "OnchainTracker whale WS %s error (attempt %s): %s — reconnect in %.1fs",
                network,
                attempt,
                e,
                delay,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise


async def _run_evm_ws_smart(
    tracker: Any,
    network: str,
    wss_url: str,
) -> None:
    from web3 import AsyncWeb3
    from web3.providers.persistent import WebSocketProvider
    from web3.utils.subscriptions import LogsSubscription

    from onchain_tracker import _get_prices

    addrs = [a for a, m in tracker._wallets.items() if m.get("network") == network]
    if not addrs or not wss_url:
        logger.info("onchain_ws smart: skip %s (no wallets or empty WSS URL)", network)
        return

    topic_in = [_pad_addr_topic(a) for a in addrs]
    labels = {a.lower(): (a, tracker._wallets[a].get("label", a[:8])) for a in addrs}
    watched_in = {a.lower() for a in addrs}

    attempt = 0
    while tracker._running:
        try:
            async with AsyncWeb3(WebSocketProvider(wss_url)) as w3:
                attempt = 0
                w3.subscription_manager.parallelize = True

                async def on_in(ctx: Any) -> None:
                    try:
                        log = ctx.result
                        topics = [
                            t.hex() if hasattr(t, "hex") else str(t)
                            for t in (log.get("topics") or [])
                        ]
                        if not topics or topics[0].lower() != TRANSFER_EVENT_TOPIC.lower():
                            return
                        c_raw = log.get("address")
                        if not c_raw:
                            return
                        contract = Web3.to_checksum_address(c_raw)
                        to_a = _topic_to_addr(topics[2]).lower()
                        if to_a not in watched_in:
                            return
                        raw_amt = _raw_transfer_amount(log)
                        if raw_amt <= 0:
                            return
                        sd = _evm_stable_symbol_decimals(network, contract)
                        if not sd:
                            return
                        sym, dec = sd
                        txd = _synthetic_erc20_tx(
                            log=log,
                            topics=topics,
                            token_symbol=sym,
                            decimals=dec,
                            raw_amount=raw_amt,
                            contract=contract,
                        )
                        if not txd:
                            return
                        addr, label = labels[to_a]
                        prices = await _get_prices(None)
                        sig = await tracker._classify_erc20_buy(
                            txd, addr, label, network, prices
                        )
                        if sig:
                            await tracker._ingest_smart_signal(sig)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        logger.debug("smart on_in %s: %s", network, e)

                inc = LogsSubscription(
                    topics=[TRANSFER_EVENT_TOPIC, None, topic_in],
                    handler=on_in,
                    label=f"smart-{network}-erc20-in",
                )
                await w3.subscription_manager.subscribe(inc)
                logger.info("SmartMoneyTracker WS connected (%s)", network)
                await w3.subscription_manager.handle_subscriptions(run_forever=True)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            delay = _reconnect_sleep_s(attempt)
            logger.warning(
                "SmartMoney WS %s error (attempt %s): %s — reconnect in %.1fs",
                network,
                attempt,
                e,
                delay,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise


async def _native_sol_delta_for_wallet(resp: Any, wallet: str) -> int:
    """Return lamports balance delta for ``wallet`` (base58), or 0 if unknown."""
    if resp is None or resp.value is None:
        return 0
    try:
        txw = resp.value
        meta = txw.transaction.meta
        msg = txw.transaction.transaction.message
        if meta is None:
            return 0
        keys: list[str] = []
        for k in msg.account_keys:
            keys.append(str(k))
        if wallet not in keys:
            return 0
        idx = keys.index(wallet)
        pre = meta.pre_balances[idx]
        post = meta.post_balances[idx]
        return int(post) - int(pre)
    except Exception as e:
        logger.debug("sol native delta parse: %s", e)
        return 0


async def _run_solana_ws_whale(tracker: "OnchainTracker", wss_url: str, http_rpc: str) -> None:
    from solders.rpc.config import RpcTransactionLogsFilterMentions
    from solders.pubkey import Pubkey
    from solders.rpc.responses import LogsNotification, SubscriptionResult
    from solana.rpc.websocket_api import connect

    addrs = [a for a, m in tracker._addresses.items() if m.get("network") == "sol"]
    if not addrs or not wss_url or not http_rpc:
        logger.info("onchain_ws whale: skip sol (no addresses or URLs)")
        return

    sem = asyncio.Semaphore(6)
    attempt = 0

    while tracker._running:
        try:
            async with connect(wss_url) as ws:
                attempt = 0
                sub_id_to_addr: dict[int, str] = {}
                for addr in addrs:
                    await ws.logs_subscribe(
                        RpcTransactionLogsFilterMentions(Pubkey.from_string(addr))
                    )
                sub_idx = 0
                while sub_idx < len(addrs):
                    batch = await ws.recv()
                    for msg in batch:
                        if isinstance(msg, SubscriptionResult):
                            sub_id_to_addr[int(msg.result)] = addrs[sub_idx]
                            sub_idx += 1

                logger.info("OnchainTracker Solana WS connected, %d log subscriptions", len(addrs))

                while tracker._running:
                    batch = await ws.recv()
                    for msg in batch:
                        if isinstance(msg, LogsNotification):
                            sub_id = int(msg.subscription)
                            addr = sub_id_to_addr.get(sub_id)
                            if not addr:
                                continue
                            sig = msg.result.value.signature
                            sig_str = str(sig)
                            err = msg.result.value.err
                            if err is not None:
                                continue
                            asyncio.create_task(
                                _sol_whale_process_sig(tracker, addr, sig_str, http_rpc, sem),
                                name=f"sol-whale-{sig_str[:8]}",
                            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            delay = _reconnect_sleep_s(attempt)
            logger.warning(
                "OnchainTracker Solana WS error (attempt %s): %s — reconnect in %.1fs",
                attempt,
                e,
                delay,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise


async def _sol_whale_process_sig(
    tracker: Any,
    wallet: str,
    sig_str: str,
    http_rpc: str,
    sem: asyncio.Semaphore,
) -> None:
    from solders.signature import Signature
    from solana.rpc.async_api import AsyncClient
    from solana.rpc.commitment import Confirmed

    from onchain_tracker import _classify_sol

    async with sem:
        try:
            async with AsyncClient(http_rpc) as client:
                resp = await client.get_transaction(
                    Signature.from_string(sig_str),
                    commitment=Confirmed,
                    max_supported_transaction_version=0,
                )
        except Exception as e:
            logger.debug("sol whale get_tx %s: %s", sig_str[:12], e)
            return

    delta = await _native_sol_delta_for_wallet(resp, wallet)
    if delta == 0:
        return
    prices = getattr(tracker, "_prices_live", {}) or {}
    label = tracker._addresses.get(wallet, {}).get("label", wallet[:8])
    fake_tx = {
        "lamport": abs(delta),
        "signer": [wallet] if delta < 0 else [],
        "txHash": sig_str,
        "blockTime": int(time.time()),
    }
    c = _classify_sol(fake_tx, wallet, prices)
    if not c:
        return
    c["tx_hash"] = sig_str
    c["timestamp"] = int(time.time())
    await tracker._push_whale_signal_from_classified(c, wallet, label, "sol")


async def _run_solana_ws_smart(
    tracker: Any,
    wss_url: str,
    http_rpc: str,
) -> None:
    from solders.rpc.config import RpcTransactionLogsFilterMentions
    from solders.pubkey import Pubkey
    from solders.rpc.responses import LogsNotification, SubscriptionResult
    from solana.rpc.websocket_api import connect

    addrs = [a for a, m in tracker._wallets.items() if m.get("network") == "sol"]
    if not addrs or not wss_url or not http_rpc:
        logger.info("onchain_ws smart: skip sol")
        return

    sem = asyncio.Semaphore(6)
    attempt = 0

    while tracker._running:
        try:
            async with connect(wss_url) as ws:
                attempt = 0
                sub_id_to_addr: dict[int, str] = {}
                for addr in addrs:
                    await ws.logs_subscribe(
                        RpcTransactionLogsFilterMentions(Pubkey.from_string(addr))
                    )
                sub_idx = 0
                while sub_idx < len(addrs):
                    batch = await ws.recv()
                    for msg in batch:
                        if isinstance(msg, SubscriptionResult):
                            sub_id_to_addr[int(msg.result)] = addrs[sub_idx]
                            sub_idx += 1

                logger.info("SmartMoney Solana WS connected, %d log subscriptions", len(addrs))

                while tracker._running:
                    batch = await ws.recv()
                    for msg in batch:
                        if isinstance(msg, LogsNotification):
                            sub_id = int(msg.subscription)
                            addr = sub_id_to_addr.get(sub_id)
                            if not addr:
                                continue
                            if msg.result.value.err is not None:
                                continue
                            sig_str = str(msg.result.value.signature)
                            asyncio.create_task(
                                _sol_smart_process_sig(tracker, addr, sig_str, http_rpc, sem),
                                name=f"sol-smart-{sig_str[:8]}",
                            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            delay = _reconnect_sleep_s(attempt)
            logger.warning(
                "SmartMoney Solana WS error (attempt %s): %s — reconnect in %.1fs",
                attempt,
                e,
                delay,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise


async def _sol_smart_process_sig(
    tracker: Any,
    wallet: str,
    sig_str: str,
    http_rpc: str,
    sem: asyncio.Semaphore,
) -> None:
    from solders.signature import Signature
    from solana.rpc.async_api import AsyncClient
    from solana.rpc.commitment import Confirmed

    async with sem:
        try:
            async with AsyncClient(http_rpc) as client:
                resp = await client.get_transaction(
                    Signature.from_string(sig_str),
                    commitment=Confirmed,
                    max_supported_transaction_version=0,
                )
        except Exception as e:
            logger.debug("sol smart get_tx %s: %s", sig_str[:12], e)
            return

    from onchain_tracker import _get_prices

    prices = await _get_prices(None)
    label = tracker._wallets.get(wallet, {}).get("label", wallet[:8])

    pre_tb = []
    post_tb = []
    try:
        meta = resp.value.transaction.meta
        if meta:
            pre_tb = list(meta.pre_token_balances or [])
            post_tb = list(meta.post_token_balances or [])
    except Exception:
        pass

    from onchain_tracker import SMART_MIN_SOL

    native_delta = await _native_sol_delta_for_wallet(resp, wallet)
    sol_price = prices.get("SOL", 150)
    if native_delta > 0 and (native_delta / 1e9) >= SMART_MIN_SOL:
        fake = {
            "changeAmount": str(native_delta),
            "decimals": 9,
            "tokenSymbol": "SOL",
            "tokenName": "SOL",
            "tokenAddress": "",
            "txHash": sig_str,
            "blockTime": int(time.time()),
            "dst": wallet,
        }
        sig = await tracker._classify_sol_buy(fake, wallet, label, prices)
        if sig:
            await tracker._ingest_smart_signal(sig)

    for pb in post_tb:
        try:
            if pb.owner is None or str(pb.owner) != wallet:
                continue
            mint = str(pb.mint)
            post_raw = int(pb.ui_token_amount.amount)
            decimals = int(pb.ui_token_amount.decimals)
            pre_raw = 0
            for x in pre_tb:
                if x.owner and str(x.owner) == wallet and str(x.mint) == mint:
                    pre_raw = int(x.ui_token_amount.amount)
                    break
            d_raw = post_raw - pre_raw
            if d_raw <= 0:
                continue
            fake = {
                "changeAmount": str(d_raw),
                "decimals": decimals,
                "tokenSymbol": "UNKNOWN",
                "tokenName": "UNKNOWN",
                "tokenAddress": mint,
                "txHash": sig_str,
                "blockTime": int(time.time()),
                "dst": wallet,
            }
            sig2 = await tracker._classify_sol_buy(fake, wallet, label, prices)
            if sig2:
                await tracker._ingest_smart_signal(sig2)
        except Exception as e:
            logger.debug("sol smart spl row: %s", e)


async def _run_solana_ws_target_monitor(
    monitor: Any,
    wss_url: str,
    http_rpc: str,
) -> None:
    """Solana logs_subscribe (mentions) for TargetWalletMonitor — non-blocking per-signature tasks."""
    from solders.pubkey import Pubkey
    from solders.rpc.config import RpcTransactionLogsFilterMentions
    from solders.rpc.responses import LogsNotification, SubscriptionResult
    from solana.rpc.websocket_api import connect

    addrs = [a for a in monitor.targets]
    if not addrs or not wss_url or not http_rpc:
        logger.info("onchain_ws parasite: skip sol (no targets or URLs)")
        return

    sem = asyncio.Semaphore(6)
    attempt = 0

    while monitor._running:
        try:
            async with connect(wss_url) as ws:
                attempt = 0
                sub_id_to_addr: dict[int, str] = {}
                for addr in addrs:
                    await ws.logs_subscribe(
                        RpcTransactionLogsFilterMentions(Pubkey.from_string(addr))
                    )
                sub_idx = 0
                while sub_idx < len(addrs):
                    batch = await ws.recv()
                    for msg in batch:
                        if isinstance(msg, SubscriptionResult):
                            sub_id_to_addr[int(msg.result)] = addrs[sub_idx]
                            sub_idx += 1

                logger.info(
                    "TargetWalletMonitor Solana WS connected, %d log subscriptions",
                    len(addrs),
                )

                while monitor._running:
                    batch = await ws.recv()
                    for msg in batch:
                        if isinstance(msg, LogsNotification):
                            sub_id = int(msg.subscription)
                            addr = sub_id_to_addr.get(sub_id)
                            if not addr:
                                continue
                            if msg.result.value.err is not None:
                                continue
                            sig_str = str(msg.result.value.signature)
                            asyncio.create_task(
                                _sol_target_process_sig(monitor, addr, sig_str, http_rpc, sem),
                                name=f"sol-parasite-{sig_str[:8]}",
                            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            delay = _reconnect_sleep_s(attempt)
            logger.warning(
                "TargetWalletMonitor Solana WS error (attempt %s): %s — reconnect in %.1fs",
                attempt,
                e,
                delay,
            )
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                raise


async def _sol_target_process_sig(
    monitor: Any,
    wallet: str,
    sig_str: str,
    http_rpc: str,
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        try:
            await monitor.process_signature(wallet, sig_str, http_rpc)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.debug("sol parasite process %s: %s", sig_str[:12], e)


def _schedule_target_wallet_monitor(monitor: Any) -> list[asyncio.Task]:
    import config

    tasks: list[asyncio.Task] = []
    sol_u = getattr(config, "ONCHAIN_SOL_WSS", "") or ""
    sol_http = getattr(config, "SOLANA_RPC_HTTP", "") or ""
    if sol_u.strip() and sol_http.strip():
        tasks.append(
            asyncio.create_task(
                _run_solana_ws_target_monitor(monitor, sol_u.strip(), sol_http.strip()),
                name="target-wallet-monitor-sol",
            )
        )
    return tasks


def _schedule_ws_runners_whale(tracker: Any) -> list[asyncio.Task]:
    import config

    tasks = []
    eth_u = getattr(config, "ONCHAIN_ETH_WSS", "") or ""
    bsc_u = getattr(config, "ONCHAIN_BSC_WSS", "") or ""
    sol_u = getattr(config, "ONCHAIN_SOL_WSS", "") or ""
    sol_http = getattr(config, "SOLANA_RPC_HTTP", "") or ""

    if eth_u.strip():
        tasks.append(
            asyncio.create_task(
                _run_evm_ws_whale(tracker, "eth", eth_u.strip()),
                name="whale-ws-eth",
            )
        )
    if bsc_u.strip():
        tasks.append(
            asyncio.create_task(
                _run_evm_ws_whale(tracker, "bsc", bsc_u.strip()),
                name="whale-ws-bsc",
            )
        )
    if sol_u.strip() and sol_http.strip():
        tasks.append(
            asyncio.create_task(
                _run_solana_ws_whale(tracker, sol_u.strip(), sol_http.strip()),
                name="whale-ws-sol",
            )
        )
    return tasks


def _schedule_ws_runners_smart(tracker: Any) -> list[asyncio.Task]:
    import config

    tasks = []
    eth_u = getattr(config, "ONCHAIN_ETH_WSS", "") or ""
    bsc_u = getattr(config, "ONCHAIN_BSC_WSS", "") or ""
    sol_u = getattr(config, "ONCHAIN_SOL_WSS", "") or ""
    sol_http = getattr(config, "SOLANA_RPC_HTTP", "") or ""

    if eth_u.strip():
        tasks.append(
            asyncio.create_task(
                _run_evm_ws_smart(tracker, "eth", eth_u.strip()),
                name="smart-ws-eth",
            )
        )
    if bsc_u.strip():
        tasks.append(
            asyncio.create_task(
                _run_evm_ws_smart(tracker, "bsc", bsc_u.strip()),
                name="smart-ws-bsc",
            )
        )
    if sol_u.strip() and sol_http.strip():
        tasks.append(
            asyncio.create_task(
                _run_solana_ws_smart(tracker, sol_u.strip(), sol_http.strip()),
                name="smart-ws-sol",
            )
        )
    return tasks
