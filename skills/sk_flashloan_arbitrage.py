"""
skills/sk_flashloan_arbitrage.py — Flash Loan + DEX 价差监控与 Flashbots 提交骨架（教育/研究）

⚠️ 合规与安全
--------------
- 仅用于自有合约 + 自有资金；须审计后再上主网。不保证盈利；错误可导致全部损失。
- 不自动部署合约、不在仓库存私钥。Flashbots 为隐私提交，非「绝对防夹」。

原子流转（概念）
--------------
  EOA 签名 tx → Flashbots Relay eth_sendBundle
  → 区块内: Aave Pool.flashLoanSimple(WETH, amount, receiver, calldata)
  → receiver.executeOperation: SwapLow → SwapHigh → approve Pool → 还款成功则整笔成功，否则 revert。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

# Ethereum Mainnet（请自行核对）
AAVE_V3_POOL = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
UNISWAP_V3_QUOTER_V2 = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
UNI_FEE_500 = 500
SUSHI_ROUTER_V2 = "0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F"

QUOTER_V2_ABI = [
    {
        "name": "quoteExactInputSingle",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {
                "components": [
                    {"name": "tokenIn", "type": "address"},
                    {"name": "tokenOut", "type": "address"},
                    {"name": "amountIn", "type": "uint256"},
                    {"name": "fee", "type": "uint24"},
                    {"name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "name": "params",
                "type": "tuple",
            }
        ],
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "sqrtPriceX96After", "type": "uint160"},
            {"name": "initializedTicksCrossed", "type": "uint32"},
            {"name": "gasEstimate", "type": "uint256"},
        ],
    }
]

SUSHI_ROUTER_ABI = [
    {
        "name": "getAmountsOut",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "path", "type": "address[]"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    }
]

EXECUTOR_ABI = [
    {
        "name": "executeFlashArb",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "asset", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "buyOn", "type": "uint8"},
            {"name": "sellOn", "type": "uint8"},
            {"name": "minProfit", "type": "uint256"},
        ],
        "outputs": [],
    }
]

DEFAULT_FLASHBOTS_RELAY = "https://relay.flashbots.net"


@dataclass
class SpreadSnapshot:
    dex_a: str
    dex_b: str
    amount_in_wei: int
    amount_out_uni: int
    amount_out_sushi: int
    spread_bps: float
    better_on: str
    ts: float


async def monitor_weth_usdc_spread(
    rpc_url: str,
    amount_in_wei: int = 10**18,
    retries: int = 3,
) -> SpreadSnapshot:
    """Uniswap V3 QuoterV2 vs Sushiswap V2 getAmountsOut（WETH→USDC）。"""
    from web3 import AsyncWeb3
    from web3.providers import AsyncHTTPProvider

    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            quoter = w3.eth.contract(
                address=w3.to_checksum_address(UNISWAP_V3_QUOTER_V2),
                abi=QUOTER_V2_ABI,
            )
            params = (
                w3.to_checksum_address(WETH),
                w3.to_checksum_address(USDC),
                amount_in_wei,
                UNI_FEE_500,
                0,
            )
            out = await quoter.functions.quoteExactInputSingle(params).call()
            out_uni = int(out[0])

            router = w3.eth.contract(
                address=w3.to_checksum_address(SUSHI_ROUTER_V2),
                abi=SUSHI_ROUTER_ABI,
            )
            path = [w3.to_checksum_address(WETH), w3.to_checksum_address(USDC)]
            amounts = await router.functions.getAmountsOut(amount_in_wei, path).call()
            out_sushi = int(amounts[1])

            hi, lo = max(out_uni, out_sushi), min(out_uni, out_sushi)
            spread_bps = ((hi - lo) / lo) * 10000.0 if lo else 0.0
            better = "uniswap_v3" if out_uni > out_sushi else "sushiswap_v2"
            return SpreadSnapshot(
                dex_a="uniswap_v3",
                dex_b="sushiswap_v2",
                amount_in_wei=amount_in_wei,
                amount_out_uni=out_uni,
                amount_out_sushi=out_sushi,
                spread_bps=spread_bps,
                better_on=better,
                ts=time.time(),
            )
        except Exception as e:
            last_err = e
            logger.warning("monitor spread attempt %s failed: %s", attempt + 1, e)
            await asyncio.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"monitor_weth_usdc_spread failed after {retries} tries: {last_err}")


def rough_profit_covers_gas(
    snap: SpreadSnapshot,
    eth_price_usd: float,
    gas_price_gwei: float,
    gas_units: int = 900_000,
    safety: float = 1.35,
) -> Tuple[bool, Dict[str, float]]:
    hi, lo = max(snap.amount_out_uni, snap.amount_out_sushi), min(
        snap.amount_out_uni, snap.amount_out_sushi
    )
    edge_usdc = (hi - lo) / 1e6
    gas_eth = (gas_price_gwei * 1e-9) * gas_units
    gas_usd = gas_eth * eth_price_usd
    ok = edge_usdc > gas_usd * safety
    return ok, {
        "edge_usdc": edge_usdc,
        "gas_usd_est": gas_usd,
        "spread_bps": snap.spread_bps,
    }


def encode_execute_flash_arb_calldata(
    executor: str,
    asset: str,
    amount: int,
    buy_on: int,
    sell_on: int,
    min_profit: int,
) -> str:
    """编码调用已部署执行合约的 executeFlashArb（不含 flashLoan，仅单笔 call data）。"""
    from web3 import Web3

    w3 = Web3()
    c = w3.eth.contract(address=Web3.to_checksum_address(executor), abi=EXECUTOR_ABI)
    return c.encode_abi(
        "executeFlashArb",
        [
            Web3.to_checksum_address(asset),
            amount,
            buy_on,
            sell_on,
            min_profit,
        ],
    )


async def submit_flashbots_bundle(
    signed_txs_hex: List[str],
    target_block_hex: str,
    *,
    relay_url: Optional[str] = None,
    flashbots_private_key: Optional[str] = None,
    retries: int = 2,
) -> Dict[str, Any]:
    """
    eth_sendBundle。需 FLASHBOTS_SIGNER_KEY（不含 0x 的 secp256k1 密钥，专用于签名请求，非 EOA）。
    signed_txs_hex: 已 RLP 编码的 0x 交易串列表。
    """
    relay = relay_url or os.environ.get("FLASHBOTS_RELAY_URL", DEFAULT_FLASHBOTS_RELAY)
    fb_key = flashbots_private_key or os.environ.get("FLASHBOTS_SIGNER_KEY")
    if not fb_key:
        return {
            "ok": False,
            "error": "FLASHBOTS_SIGNER_KEY not set; bundle not sent",
        }

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_sendBundle",
        "params": [
            {
                "txs": signed_txs_hex,
                "blockNumber": target_block_hex,
            }
        ],
    }
    body_str = json.dumps(body, separators=(",", ":"), sort_keys=True)
    from eth_account import Account
    from eth_account.messages import encode_defunct

    sig = Account.sign_message(encode_defunct(text=body_str), private_key=fb_key).signature.hex()
    # 认证格式以 https://docs.flashbots.net 最新说明为准（部分版本为 personal_sign(body)）
    headers = {
        "Content-Type": "application/json",
        "X-Flashbots-Signature": f"{Account.from_key(fb_key).address}:0x{sig}",
    }

    last: Optional[Exception] = None
    async with httpx.AsyncClient(timeout=30.0) as client:
        for attempt in range(retries):
            try:
                r = await client.post(relay, content=body_str, headers=headers)
                data = r.json()
                return {"ok": r.is_success, "status": r.status_code, "relay_response": data}
            except Exception as e:
                last = e
                await asyncio.sleep(0.5 * (attempt + 1))
    return {"ok": False, "error": str(last)}


# ── Solidity 模板（未审计，仅作起点）──────────────────────────────────────────

FLASH_ARB_EXECUTOR_SOLIDITY = r"""
// SPDX-License-Identifier: MIT
// ⚠️ 未审计示例：部署前请专业审计；主网使用风险自负。
pragma solidity ^0.8.20;

interface IAavePool {
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;
}

interface IERC20 {
    function approve(address spender, uint256 v) external returns (bool);
    function balanceOf(address a) external view returns (uint256);
    function transfer(address to, uint256 v) external returns (bool);
}

/// @dev 简化 UniswapV2 Router（Sushi 兼容）
interface IV2Router {
    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address to,
        uint256 deadline
    ) external returns (uint256[] memory amounts);
}

/// @dev 简化：仅示意路径；真实套利需 SwapRouter02 + exactInput 或 V3 periphery
contract FlashArbExecutor {
    address public immutable pool;
    address public immutable v2Router;
    address public owner;

    uint8 public constant ROUTE_UNI_THEN_SUSHI = 0;
    uint8 public constant ROUTE_SUSHI_THEN_UNI = 1;

    constructor(address _aavePool, address _v2Router) {
        pool = _aavePool;
        v2Router = _v2Router;
        owner = msg.sender;
    }

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    /// @notice 入口：由链下监控发现价差后调用；内部发起闪电贷
    function executeFlashArb(
        address asset,
        uint256 amount,
        uint8 buyOn,
        uint8 sellOn,
        uint256 minProfit
    ) external onlyOwner {
        bytes memory params = abi.encode(buyOn, sellOn, minProfit);
        IAavePool(pool).flashLoanSimple(address(this), asset, amount, params, 0);
    }

    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool) {
        require(msg.sender == pool, "pool only");
        require(initiator == address(this), "init");
        (uint8 buyOn, uint8 sellOn, uint256 minProfit) = abi.decode(params, (uint8, uint8, uint256));

        // 收到 amount，须偿还 amount + premium。以下模板故意 revert，避免未补全路由即上主网。
        uint256 owed = amount + premium;
        if (minProfit > 0 && buyOn <= 1 && sellOn <= 1) {
            // 占位：真实逻辑中在此完成 DEX 循环套利，再 approve Pool
        }
        revert("FlashArbExecutor: implement swaps then approve pool and return true");
    }

    function rescue(address token, address to, uint256 amt) external onlyOwner {
        IERC20(token).transfer(to, amt);
    }
}
"""


async def run_skill(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    params:
      rpc_url: 以太坊 HTTP RPC（必填，用于监控）
      amount_in_wei: 报价基准（默认 1 WETH）
      eth_price_usd / gas_price_gwei: 粗算 Gas 覆盖
      min_spread_bps: 仅当 spread 超过此值时标记 actionable
      executor / ... : 若提供则生成 calldata 预览
    """
    params = params or {}
    rpc = params.get("rpc_url") or os.environ.get("ETH_RPC_URL") or os.environ.get("MAINNET_RPC_URL")
    if not rpc:
        return {"ok": False, "error": "rpc_url or ETH_RPC_URL required"}

    amount = int(params.get("amount_in_wei") or 10**18)
    eth_usd = float(params.get("eth_price_usd") or 3500)
    gwei = float(params.get("gas_price_gwei") or 30)
    min_bps = float(params.get("min_spread_bps") or 8)

    try:
        snap = await monitor_weth_usdc_spread(rpc, amount_in_wei=amount)
    except Exception as e:
        return {"ok": False, "error": str(e)}

    covers, est = rough_profit_covers_gas(snap, eth_usd, gwei)
    actionable = covers and snap.spread_bps >= min_bps

    out: Dict[str, Any] = {
        "ok": True,
        "snapshot": asdict(snap),
        "gas_estimate": est,
        "covers_gas_heuristic": covers,
        "actionable": actionable,
        "min_spread_bps_threshold": min_bps,
    }

    ex = params.get("executor_address")
    if ex and actionable:
        try:
            calldata = encode_execute_flash_arb_calldata(
                ex,
                params.get("asset", WETH),
                int(params.get("flash_amount", amount)),
                int(params.get("buy_on", 0)),
                int(params.get("sell_on", 1)),
                int(params.get("min_profit_wei", 0)),
            )
            out["execute_flash_arb_calldata"] = calldata
        except Exception as e:
            out["calldata_error"] = str(e)

    out["solidity_template_note"] = "See module constant FLASH_ARB_EXECUTOR_SOLIDITY (unaudited)."
    return out


def run_skill_sync(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_skill(params))
    raise RuntimeError("Use await run_skill(...) in async context")
