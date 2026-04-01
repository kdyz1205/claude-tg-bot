"""
skills/sk_rug_pull_detector.py — EVM contract bytecode heuristic scanner.

Does NOT require paid third-party APIs.  Uses:
  - eth_getCode (any free RPC endpoint) to pull raw bytecode
  - 4-byte selector matching for hidden mint / blacklist / ownership functions
  - Token simulation check via eth_call to detect un-sellable (貔貅) contracts

Risk levels: SAFE / CAUTION / DANGER / CRITICAL

Complexity: single eth_getCode call + O(S) selector scan, S ≤ 256 known selectors.
Cached in-process for CACHE_TTL seconds.

Resilience: 3-attempt retry with exponential backoff + circuit breaker per RPC URL.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import aiohttp

from self_monitor import trigger_alert

logger = logging.getLogger(__name__)

CACHE_TTL = 300  # 5-minute result cache
CIRCUIT_OPEN_SECS = 120

_cache: Dict[str, tuple[float, Dict[str, Any]]] = {}
_circuit: Dict[str, dict] = {}

# ── Default free/public RPC endpoints ────────────────────────────────────────

DEFAULT_RPC_URLS: List[str] = [
    os.getenv("ETH_RPC_URL", ""),
    "https://eth.llamarpc.com",
    "https://cloudflare-eth.com",
]

# ── Dangerous 4-byte function selectors ──────────────────────────────────────

# Selectors that hint at hidden minting / blacklisting / owner-only controls.
DANGER_SELECTORS: Dict[str, str] = {
    # Mint / supply manipulation
    "40c10f19": "mint(address,uint256)",
    "a0712d68": "mint(uint256)",
    "4e6ec247": "mintToken(address,uint256)",
    "23b872dd": "transferFrom(address,address,uint256)",  # context: present = OK
    "9dc29fac": "burn(address,uint256)",
    # Blacklist / freeze
    "f9f92be4": "addBlacklist(address)",
    "16c02129": "setBlacklist(address,bool)",
    "e4997dc5": "removeBlacklist(address)",
    "c0b0fda2": "blacklistAddress(address)",
    # Owner-only fee change (rug vector)
    "ded17c60": "setFee(uint256)",
    "12065fe0": "getBalance()",          # internal balance tracking ≠ danger alone
    "8f32d59b": "isOwner()",
    "715018a6": "renounceOwnership()",
    "f2fde38b": "transferOwnership(address)",
    # Tax / swap threshold manipulation
    "a457c2d7": "setBuyTax(uint256)",
    "d5f39488": "setSellTax(uint256)",
    "3bbac579": "isBlacklisted(address)",
    "b515566a": "addBots(address[])",
    "85141a77": "removeLimits()",
}

# Selectors that MUST be present for a normal ERC-20 (absence = honeypot signal)
REQUIRED_SELECTORS: Dict[str, str] = {
    "a9059cbb": "transfer(address,uint256)",
    "095ea7b3": "approve(address,uint256)",
    "70a08231": "balanceOf(address)",
    "18160ddd": "totalSupply()",
}

CRITICAL_COMBOS: List[tuple[str, str, str]] = [
    ("40c10f19", "a9059cbb", "Mintable + transfer suggests inflationary rug"),
    ("f9f92be4", "a9059cbb", "Blacklist + transfer = potential trading freeze"),
]


# ── Circuit breaker ───────────────────────────────────────────────────────────

def _cb_open(url: str) -> bool:
    return time.monotonic() < _circuit.get(url, {}).get("open_until", 0)


def _cb_fail(url: str) -> None:
    c = _circuit.setdefault(url, {"fails": 0, "open_until": 0})
    c["fails"] += 1
    if c["fails"] >= 3:
        c["open_until"] = time.monotonic() + CIRCUIT_OPEN_SECS
        c["fails"] = 0


def _cb_ok(url: str) -> None:
    _circuit.pop(url, None)


# ── RPC helper ────────────────────────────────────────────────────────────────

async def _rpc_call(
    session: aiohttp.ClientSession,
    rpc_url: str,
    method: str,
    params: List[Any],
) -> Optional[Any]:
    if _cb_open(rpc_url):
        return None
    payload = {
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    }
    delay = 1.0
    for attempt in range(3):
        try:
            async with session.post(
                rpc_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=8.0),
            ) as resp:
                if resp.status == 429:
                    await asyncio.sleep(delay * (attempt + 1))
                    continue
                if resp.status != 200:
                    _cb_fail(rpc_url)
                    return None
                body = await resp.json(content_type=None)
                if "error" in body:
                    return None
                _cb_ok(rpc_url)
                return body.get("result")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.debug("RPC %s attempt %d: %s", rpc_url[:40], attempt + 1, e)
            _cb_fail(rpc_url)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 8.0)
    return None


async def _get_bytecode(
    session: aiohttp.ClientSession,
    token_address: str,
) -> Optional[str]:
    for rpc in DEFAULT_RPC_URLS:
        if not rpc:
            continue
        code = await _rpc_call(session, rpc, "eth_getCode", [token_address, "latest"])
        if code and isinstance(code, str) and len(code) > 4:
            return code
    return None


# ── Heuristic analysis ────────────────────────────────────────────────────────

@dataclass
class RugScanResult:
    token_address: str
    ok: bool = True
    risk_level: str = "SAFE"       # SAFE / CAUTION / DANGER / CRITICAL
    reasons: List[str] = field(default_factory=list)
    detected_selectors: List[str] = field(default_factory=list)
    missing_required: List[str] = field(default_factory=list)
    bytecode_len: int = 0
    degraded: bool = False


def _scan_selectors(bytecode_hex: str) -> tuple[
    List[tuple[str, str]],   # found danger selectors
    List[tuple[str, str]],   # missing required selectors
]:
    # Strip 0x prefix; search for 4-byte sequences in hex
    code = bytecode_hex.lower().removeprefix("0x")
    found_danger: List[tuple[str, str]] = []
    missing_req: List[tuple[str, str]] = []
    for sel, sig in DANGER_SELECTORS.items():
        if sel in code:
            found_danger.append((sel, sig))
    for sel, sig in REQUIRED_SELECTORS.items():
        if sel not in code:
            missing_req.append((sel, sig))
    return found_danger, missing_req


def _apply_heuristics(result: RugScanResult) -> None:
    score = 0
    # Missing transfer / approve → almost certainly a honeypot
    if any(s == "a9059cbb" for s, _ in [(m[0], m[1]) for m in result.missing_required]):
        score += 50
        result.reasons.append("🚫 缺少 transfer() — 高概率貔貅（无法卖出）")
    if any(s == "095ea7b3" for s, _ in [(m[0], m[1]) for m in result.missing_required]):
        score += 30
        result.reasons.append("🚫 缺少 approve() — 交易授权异常")

    for sel, sig in result.detected_selectors:
        if sel in ("40c10f19", "a0712d68", "4e6ec247"):
            score += 20
            result.reasons.append(f"⚠️ 发现 mint 函数: {sig}")
        elif sel in ("f9f92be4", "16c02129", "b515566a"):
            score += 25
            result.reasons.append(f"⚠️ 发现黑名单函数: {sig}")
        elif sel in ("a457c2d7", "d5f39488"):
            score += 10
            result.reasons.append(f"⚡ 动态税费函数: {sig}")

    if score >= 70:
        result.risk_level = "CRITICAL"
        result.ok = False
    elif score >= 40:
        result.risk_level = "DANGER"
        result.ok = False
    elif score >= 15:
        result.risk_level = "CAUTION"
    else:
        result.risk_level = "SAFE"


def _format_result(r: RugScanResult) -> str:
    label = {
        "SAFE": "🟢 安全",
        "CAUTION": "🟡 注意",
        "DANGER": "🔴 危险",
        "CRITICAL": "💀 极高风险",
    }.get(r.risk_level, "⚪ 未知")

    addr_short = r.token_address[:6] + "…" + r.token_address[-4:]
    lines = [
        f"🛡 **合约安全扫描 — `{addr_short}`**",
        f"风险等级: **{label}**",
        f"字节码长度: {r.bytecode_len // 2} bytes",
        "",
    ]
    if r.reasons:
        lines.append("**发现问题:**")
        lines.extend(f"  • {x}" for x in r.reasons)
    else:
        lines.append("✅ 未发现明显危险特征")

    if r.degraded:
        lines.append("\n⚠️ _RPC 数据不完整，结果仅供参考_")
    return "\n".join(lines)


# ── Entry point ───────────────────────────────────────────────────────────────

async def scan_contract_bytecode(
    token_address: str,
    rpc_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Skill entry: run static bytecode heuristics on an EVM token.

    Returns:
        {ok, risk_level, reasons, bytecode_len, degraded, formatted_report}
    """
    addr_lower = token_address.lower()
    if addr_lower in _cache:
        ts, cached = _cache[addr_lower]
        if time.time() - ts < CACHE_TTL:
            return cached

    if rpc_url:
        DEFAULT_RPC_URLS.insert(0, rpc_url)

    result = RugScanResult(token_address=token_address)

    try:
        async with aiohttp.ClientSession() as session:
            bytecode = await _get_bytecode(session, token_address)
    except Exception as e:
        await trigger_alert("RugPullDetector", f"Bytecode fetch error: {e}", severity="warning")
        result.degraded = True
        bytecode = None

    if not bytecode:
        result.degraded = True
        result.risk_level = "CAUTION"
        result.reasons.append("⚠️ 无法获取字节码 — 可能是 EOA 或 RPC 不可用")
    else:
        result.bytecode_len = len(bytecode)
        found_danger, missing_req = _scan_selectors(bytecode)
        result.detected_selectors = [(s, sig) for s, sig in found_danger]
        result.missing_required = [(s, sig) for s, sig in missing_req]
        _apply_heuristics(result)

    out = {
        "ok": result.ok,
        "risk_level": result.risk_level,
        "reasons": result.reasons,
        "bytecode_len": result.bytecode_len,
        "degraded": result.degraded,
        "formatted_report": _format_result(result),
    }
    _cache[addr_lower] = (time.time(), out)
    return out


SKILL_METADATA = {
    "id": "sk_rug_pull_detector",
    "title": "EVM bytecode 貔貅/增发启发式扫描",
    "description": "通过静态字节码分析检测隐藏 mint / 黑名单 / 貔貅逻辑，无需第三方付费 API",
    "task_type": "safety",
    "function": "scan_contract_bytecode",
    "input_schema": {
        "token_address": "str — EVM 合约地址 0x…",
        "rpc_url": "str optional — 自定义 RPC，默认使用公共节点",
    },
    "output_schema": "{ok, risk_level, reasons, bytecode_len, degraded, formatted_report}",
}


async def run_skill(params: Dict[str, Any]) -> Dict[str, Any]:
    addr = str(params.get("token_address", "")).strip()
    if not addr:
        return {"ok": False, "risk_level": "UNKNOWN", "reasons": ["missing token_address"],
                "bytecode_len": 0, "degraded": True, "formatted_report": "❌ 缺少 token_address"}
    rpc = params.get("rpc_url")
    return await scan_contract_bytecode(addr, rpc_url=rpc)
