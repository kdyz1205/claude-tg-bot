"""
对手盘行为克隆 — 链上特征采集 + 交给 CLI 生成 ``skills/sk_clone_0x*.py``。

依赖 Etherscan HTTP API（需 ``ETHERSCAN_API_KEY``）与 DexScreener（无密钥）。
特征口径（每笔「疑似买入」= 地址收到的 ERC20 转入）：
  - 买入前约 5 分钟窗口内区块 baseFee 中位数 vs 该笔交易的 gasPrice → gas 倍率启发式
  - 当前池子流动性（DexScreener 最新快照，非历史精确值）
  - 代币合约是否在 Etherscan 有源码（开源/已验证）
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set

import aiohttp

logger = logging.getLogger(__name__)

_ETHERSCAN_API = "https://api.etherscan.io/api"
_DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens"

# ~5 min on Ethereum at ~12s/block
_PRE_BUY_WINDOW_BLOCKS = 23
_MAX_BUY_SAMPLES = 10
_MAX_PARALLEL_BLOCKS = 12

_EV_ADDR = re.compile(r"^0x[a-fA-F0-9]{40}$")


def normalize_wallet(addr: str) -> Optional[str]:
    a = (addr or "").strip()
    if not _EV_ADDR.match(a):
        return None
    return a.lower()


def clone_skill_filename(wallet: str) -> str:
    """``sk_clone_0x`` + 8 hex chars (lowercase), e.g. sk_clone_0xdeadbeef.py"""
    w = normalize_wallet(wallet) or wallet.lower()
    if not w.startswith("0x") or len(w) < 10:
        return "sk_clone_0x00000000.py"
    return f"sk_clone_0x{w[2:10]}.py"


def clone_skill_id(wallet: str) -> str:
    return clone_skill_filename(wallet)[:-3]


async def _http_get_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
) -> Any:
    try:
        async with session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=15.0)
        ) as resp:
            if resp.status != 200:
                logger.debug("wallet_clone HTTP %s %s", resp.status, url[:80])
                return None
            return await resp.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.debug("wallet_clone fetch: %s", e)
        return None


async def _etherscan(
    session: aiohttp.ClientSession,
    api_key: str,
    params: Dict[str, str],
) -> Any:
    p = dict(params)
    p["apikey"] = api_key
    data = await _http_get_json(session, _ETHERSCAN_API, params=p)
    if not data:
        return None
    res = data.get("result")
    st = str(data.get("status", ""))
    msg_l = str(data.get("message", "")).lower()
    if st == "1":
        return res
    # Empty history is still usable
    if isinstance(res, list) and not res:
        return res
    if "no transactions found" in msg_l and isinstance(res, list):
        return res
    if "no records found" in msg_l and isinstance(res, list):
        return res
    logger.debug(
        "etherscan non-ok: status=%s message=%s action=%s",
        st,
        data.get("message"),
        params.get("action"),
    )
    return None


async def _fetch_block_by_number(
    session: aiohttp.ClientSession,
    api_key: str,
    block_num: int,
    cache: Dict[int, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if block_num in cache:
        return cache[block_num]
    tag = hex(block_num)
    res = await _etherscan(
        session,
        api_key,
        {
            "module": "proxy",
            "action": "eth_getBlockByNumber",
            "tag": tag,
            "boolean": "false",
        },
    )
    if not res or not isinstance(res, dict):
        cache[block_num] = {}
        return None
    cache[block_num] = res
    return res


def _base_fee_wei(block: Dict[str, Any]) -> int:
    raw = block.get("baseFeePerGas")
    if raw is None:
        return 0
    if isinstance(raw, str) and raw.startswith("0x"):
        return int(raw, 16)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _tx_gas_price_wei(tx: Dict[str, Any]) -> int:
    try:
        return int(tx.get("gasPrice") or 0)
    except (TypeError, ValueError):
        return 0


async def _contract_verified(
    session: aiohttp.ClientSession, api_key: str, token_addr: str
) -> Optional[bool]:
    res = await _etherscan(
        session,
        api_key,
        {
            "module": "contract",
            "action": "getsourcecode",
            "address": token_addr,
        },
    )
    if not res or not isinstance(res, list) or not res[0]:
        return None
    src = (res[0].get("SourceCode") or "").strip()
    if not src:
        return False
    # Proxy contracts sometimes have minimal placeholder
    if src == "{{":
        return None
    return True


async def _dex_liquidity_usd(
    session: aiohttp.ClientSession, token_addr: str
) -> float:
    data = await _http_get_json(session, f"{_DEXSCREENER_TOKEN}/{token_addr}")
    if not data:
        return 0.0
    pairs = data.get("pairs") or []
    if not pairs:
        return 0.0
    best = max(
        pairs,
        key=lambda p: float((p.get("liquidity") or {}).get("usd") or 0),
    )
    try:
        return float((best.get("liquidity") or {}).get("usd") or 0)
    except (TypeError, ValueError):
        return 0.0


async def collect_wallet_clone_bundle(wallet: str) -> Dict[str, Any]:
    """
    Pull last ~100 normal txs + token transfers; enrich up to ``_MAX_BUY_SAMPLES``
    incoming token receives with pre-buy window baseFee stats, Dex liquidity, verified flag.
    """
    w = normalize_wallet(wallet)
    errors: List[str] = []
    out: Dict[str, Any] = {
        "wallet": w or wallet,
        "normal_tx_count": 0,
        "token_tx_sampled": 0,
        "buy_events": [],
        "aggregate": {},
        "errors": errors,
        "target_skill_file": clone_skill_filename(wallet) if w else None,
        "target_skill_id": clone_skill_id(wallet) if w else None,
    }
    if not w:
        errors.append("invalid_evm_address")
        return out

    api_key = (os.environ.get("ETHERSCAN_API_KEY") or "").strip()
    if not api_key:
        errors.append("missing_ETHERSCAN_API_KEY")
        return out

    block_cache: Dict[int, Dict[str, Any]] = {}

    async with aiohttp.ClientSession() as session:
        txlist = await _etherscan(
            session,
            api_key,
            {
                "module": "account",
                "action": "txlist",
                "address": w,
                "startblock": "0",
                "endblock": "99999999",
                "page": "1",
                "offset": "100",
                "sort": "desc",
            },
        )
        if txlist is None:
            errors.append("txlist_fetch_failed")
            txlist = []
        if isinstance(txlist, list):
            out["normal_tx_count"] = len(txlist)

        tokentx = await _etherscan(
            session,
            api_key,
            {
                "module": "account",
                "action": "tokentx",
                "address": w,
                "page": "1",
                "offset": "100",
                "sort": "desc",
            },
        )
        if tokentx is None:
            errors.append("tokentx_fetch_failed")
            tokentx = []
        incoming: List[Dict[str, Any]] = []
        if isinstance(tokentx, list):
            for row in tokentx:
                if str(row.get("to", "")).lower() != w:
                    continue
                try:
                    val = float(row.get("value") or 0)
                except (TypeError, ValueError):
                    val = 0.0
                if val <= 0:
                    continue
                incoming.append(row)
        out["token_tx_sampled"] = len(incoming)

        samples = incoming[:_MAX_BUY_SAMPLES]
        # Collect all block numbers needed
        needed: Set[int] = set()
        for row in samples:
            try:
                bn = int(row.get("blockNumber") or 0)
            except (TypeError, ValueError):
                continue
            if bn <= 0:
                continue
            lo = max(0, bn - _PRE_BUY_WINDOW_BLOCKS)
            for b in range(lo, bn + 1):
                needed.add(b)

        sem = asyncio.Semaphore(_MAX_PARALLEL_BLOCKS)

        async def _load(bn: int) -> None:
            async with sem:
                await _fetch_block_by_number(session, api_key, bn, block_cache)

        await asyncio.gather(*(_load(b) for b in sorted(needed)))

        buy_events: List[Dict[str, Any]] = []
        gas_ratios: List[float] = []
        liqs: List[float] = []
        verified_flags: List[bool] = []

        for row in samples:
            try:
                bn = int(row.get("blockNumber") or 0)
            except (TypeError, ValueError):
                bn = 0
            if bn <= 0:
                continue
            lo = max(0, bn - _PRE_BUY_WINDOW_BLOCKS)
            base_fees: List[int] = []
            for b in range(lo, bn + 1):
                blk = block_cache.get(b) or {}
                bf = _base_fee_wei(blk)
                if bf > 0:
                    base_fees.append(bf)
            median_bf = 0
            if base_fees:
                srt = sorted(base_fees)
                median_bf = srt[len(srt) // 2]
            gpw = _tx_gas_price_wei(row)
            gas_mult = 0.0
            if median_bf > 0 and gpw > 0:
                gas_mult = round(gpw / median_bf, 4)

            token = str(row.get("contractAddress") or "").lower()
            sym = row.get("tokenSymbol") or "?"

            verified = await _contract_verified(session, api_key, token)
            if verified is True:
                verified_flags.append(True)
            elif verified is False:
                verified_flags.append(False)

            liq = await _dex_liquidity_usd(session, token)
            if liq > 0:
                liqs.append(liq)

            if gas_mult > 0:
                gas_ratios.append(gas_mult)

            buy_events.append(
                {
                    "tx_hash": row.get("hash"),
                    "block": bn,
                    "token": token,
                    "token_symbol": sym,
                    "gas_price_wei": gpw,
                    "prebuy_window_median_base_fee_wei": median_bf,
                    "gas_to_median_base_fee_ratio": gas_mult,
                    "pool_liquidity_usd_snapshot": round(liq, 2),
                    "contract_verified": verified,
                }
            )

        out["buy_events"] = buy_events
        out["aggregate"] = {
            "buy_samples": len(buy_events),
            "median_gas_ratio": _median(gas_ratios),
            "p75_gas_ratio": _percentile(gas_ratios, 0.75),
            "median_pool_liquidity_usd": _median(liqs),
            "verified_token_share": (sum(1 for v in verified_flags if v) / len(verified_flags))
            if verified_flags
            else None,
            "prebuy_window_blocks": _PRE_BUY_WINDOW_BLOCKS,
        }

    return out


def _median(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    return round(s[len(s) // 2], 4)


def _percentile(xs: List[float], q: float) -> Optional[float]:
    if not xs:
        return None
    s = sorted(xs)
    idx = min(len(s) - 1, max(0, int(q * (len(s) - 1))))
    return round(s[idx], 4)


def build_wallet_clone_dev_prompt(wallet: str, bundle: Dict[str, Any]) -> str:
    """Strict CLI instructions: single new file under skills/, BaseSkill sniper factor."""
    w = bundle.get("wallet") or wallet
    fn = bundle.get("target_skill_file") or clone_skill_filename(w)
    sid = bundle.get("target_skill_id") or clone_skill_id(w)
    blob = json.dumps(bundle, ensure_ascii=False, indent=2)[:24000]

    return f"""你现在的任务是：根据「目标钱包」链上统计特征，编写一个**狙击因子** Python 技能模块（对手盘行为克隆）。

## 链上采集结果（JSON，由系统自动抓取，勿编造数据）
```json
{blob}
```

## 硬性要求
1. **只新增一个文件**：`skills/{fn}`（路径与文件名必须完全一致，禁止改其它文件）。
2. 必须继承 `skills.base_skill.BaseSkill`；先阅读 `skills/base_skill.py` 的契约（async run / _execute / 超时）。
3. 实现 `async def _execute(self, payload: dict) -> dict`，返回值必须至少包含：
   - `buy_confidence`: float ∈ [0,1] — 当观测到的链上/因子特征与上述统计规律一致时调高；
   - `sell_confidence`: float ∈ [0,1] — 离场或回避信号；
   可附加 `reason`, `metadata`（如阈值、是否要求合约已验证、流动性区间、gas 倍率区间）。
4. 在模块内定义 `SKILL_CLASS = YourSkillClass`，且 `skill_id = "{sid}"`（与文件名去掉 `.py` 一致）。
5. **逻辑应显式编码**从 JSON `aggregate` 与 `buy_events` 归纳出的区间阈值（例如 median_gas_ratio、median_pool_liquidity_usd、verified 偏好）；可用简单分段/打分，无需复现完整链上抓取（本模块在运行时只拿到 payload，不调用 Etherscan）。
6. `payload` 约定（供回测或上层填充）：可包含 `gas_to_median_base_fee_ratio`, `pool_liquidity_usd`, `contract_verified: bool`, `symbol` 等字段；若缺失则降低置信度而非崩溃。
7. 风格参考 `skills/sk_paper_to_alpha.py`、`skills/sk_smart_money_decoder.py` 的模块结构与 docstring。

## 目标
让指挥官无需理解原理，仅通过运行该 skill 获得与目标地址**统计行为相似**的买入/回避评分；在 docstring 中简短注明「克隆自钱包 {w}」。
"""


__all__ = [
    "build_wallet_clone_dev_prompt",
    "clone_skill_filename",
    "clone_skill_id",
    "collect_wallet_clone_bundle",
    "normalize_wallet",
]
