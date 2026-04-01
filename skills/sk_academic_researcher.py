"""
skills/sk_academic_researcher.py — 学术检索 + 注意力架构提炼（与 Multi-Agent / 奇点引擎兼容）。

- 优先从 arXiv 拉取与「Transformer + 高频/微观结构交易」相关的条目；
- 将摘要与标题融合为一份**可编码的注意力策略架构说明**（含伪创新机制名与张量形状约定）；
- 输出供 singularity_engine → codex_charger 使用的结构化载荷。

纯研究辅助；不构成投资建议。
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

ARXIV_API = "https://export.arxiv.org/api/query"
DEFAULT_QUERY = (
    'all:"transformer" AND (all:"high-frequency" OR all:"high frequency" '
    'OR all:"market microstructure" OR all:"algorithmic trading" OR all:"HFT")'
)


@dataclass
class ArxivEntry:
    arxiv_id: str
    title: str
    summary: str
    updated: str


def _parse_atom(xml_text: str) -> List[ArxivEntry]:
    root = ET.fromstring(xml_text)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    out: List[ArxivEntry] = []
    for entry in root.findall("a:entry", ns):
        id_el = entry.find("a:id", ns)
        title_el = entry.find("a:title", ns)
        summ_el = entry.find("a:summary", ns)
        upd_el = entry.find("a:updated", ns)
        rid = (id_el.text or "").strip()
        if "/abs/" in rid:
            rid = rid.split("/abs/")[-1]
        title = re.sub(r"\s+", " ", (title_el.text or "").strip()) if title_el is not None else ""
        summary = re.sub(r"\s+", " ", (summ_el.text or "").strip()) if summ_el is not None else ""
        updated = (upd_el.text or "")[:19] if upd_el is not None else ""
        out.append(ArxivEntry(arxiv_id=rid, title=title, summary=summary, updated=updated))
    return out


async def fetch_arxiv_hft_transformer(
    search_query: Optional[str] = None,
    max_results: int = 5,
) -> List[ArxivEntry]:
    """从 arXiv 获取与 Transformer / 高频交易相关的摘要列表。"""
    q = search_query or DEFAULT_QUERY
    params = {"search_query": q, "start": 0, "max_results": max(1, min(max_results, 10))}
    async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
        r = await client.get(ARXIV_API, params=params)
        r.raise_for_status()
        return _parse_atom(r.text)


def _mock_arxiv_entries() -> List[ArxivEntry]:
    """离线/失败时的确定性模拟摘要（仍贴合主题）。"""
    return [
        ArxivEntry(
            arxiv_id="mock.0001",
            title="Temporal Fragment Attention for Ultra-High-Frequency Limit Order Books",
            summary=(
                "We propose fragmenting the limit order book stream into non-overlapping temporal "
                "chunks and applying masked self-attention within each chunk, then cross-chunk "
                "aggregation via a lightweight transformer. Experiments on synthetic microstructure "
                "show improved directional accuracy under latency constraints."
            ),
            updated="2026-01-01T00:00:00",
        ),
        ArxivEntry(
            arxiv_id="mock.0002",
            title="Transformer Encoders with Tick-Time Positional Encoding for Crypto Markets",
            summary=(
                "Tick-time positional encodings replace wall-clock embeddings for irregular crypto "
                "trades. Coupled with dilated causal convolutions before self-attention, the model "
                "captures multi-scale volatility bursts relevant to short-horizon prediction."
            ),
            updated="2026-01-02T00:00:00",
        ),
    ]


def _fingerprint(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()[:12]


def synthesize_attention_architecture(papers: List[ArxivEntry]) -> Dict[str, Any]:
    """
    从多篇摘要中「蒸馏」出一种可实现的混合注意力架构说明（策略侧命名 + 模块清单）。
    非论文原文结论，而是供代码生成与沙盒实验的结构化规格。
    """
    blob = " || ".join(f"{p.title} :: {p.summary[:400]}" for p in papers)
    fp = _fingerprint(blob)

    # 由摘要关键词粗选超参（可替换为 LLM；此处保持确定性便于复现）
    long_text = blob.lower()
    d_model = 96 if "large" in long_text or "scale" in long_text else 64
    n_heads = 8 if d_model >= 96 else 4
    frag = 24 if "fragment" in long_text or "chunk" in long_text else 16
    n_layers = 3 if "deep" in long_text or "layer" in long_text else 2

    mechanism = (
        "Fragment-Blocked Self-Attention (FBSA): 序列按固定长度片段切分，片段内全连接自注意力，"
        "片段间通过可学习的 segment summary token 做二次 Transformer 融合；"
        "底层用双向 LSTM 注入局部 tick 惯性，再进入多头注意力栈。"
    )

    return {
        "schema_version": 1,
        "architecture_id": f"TFTA-HFT-{fp}",
        "display_name": "Temporal-Fragment Token Attention for HFT (TFTA-HFT)",
        "novelty_label": "FBSA + tick-LSTM hybrid backbone",
        "mechanism_description": mechanism,
        "tensor_contracts": {
            "raw_features": "[B, T, 5]  # open, high, low, close, volume (z-scored)",
            "lstm_out": "[B, T, d_model]",
            "after_fbsa": "[B, T, d_model]",
            "logits": "[B, 1]  # next-bar direction (binary)",
        },
        "hyperparams": {
            "d_model": d_model,
            "n_heads": n_heads,
            "fragment_len": frag,
            "transformer_layers": n_layers,
            "lstm_layers": 1,
            "dropout": 0.12,
            "seq_len": 64,
        },
        "backbone": "hybrid_lstm_transformer",
        "loss": "BCEWithLogitsLoss on directional label",
        "source_papers": [
            {"arxiv_id": p.arxiv_id, "title": p.title, "summary_preview": p.summary[:220] + "…"}
            for p in papers[:5]
        ],
        "generated_at": time.time(),
    }


def build_codex_prompt(architecture: Dict[str, Any]) -> str:
    """交给 codex_charger / CodexCharger.run_task 的代码生成指令。"""
    arch_json = json.dumps(architecture, indent=2, ensure_ascii=False)
    return f"""You are an expert PyTorch engineer for quantitative crypto.

Task: Write ONE complete, self-contained Python 3.11+ training script for Ethereum-like **mock** OHLCV data.

Requirements:
1. Read hyperparameters from a JSON file `arch.json` in the same directory as the script (do not hard-code architecture dict).
2. Implement a **hybrid LSTM + Transformer** model. Include a **fragment-blocked self-attention** block (attention only inside non-overlapping time fragments of length `fragment_len` from arch.json); then stack standard TransformerEncoderLayer(s).
3. Generate synthetic ETH-like series in-memory (GBM + volume noise); split train/val; train for a small number of epochs (e.g. 5–15) so it finishes quickly.
4. At the end, write `metrics.json` in the script directory with keys: `train_loss` (float), `val_loss` (float), `val_win_rate` (float 0–1), `epochs` (int), `architecture_id` (string from arch.json).
5. Use CPU by default; if CUDA available you may use it.
6. If `torch` is not installed, print clear error to stderr and exit code 2.

Architecture specification (for your design):
```json
{arch_json}
```

Output: ONLY the raw Python source inside a single ```python fenced block, no extra commentary outside the block.
"""


async def run_skill(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Skill 标准入口（异步），供 singularity_engine / 调度器 await。

    params:
      search_query: 覆盖默认 arXiv 查询
      max_results: 拉取条数
      force_mock: True 则跳过网络，仅用内置模拟论文
    """
    params = params or {}
    force_mock = bool(params.get("force_mock"))
    max_results = int(params.get("max_results") or 5)
    search_query = params.get("search_query")

    papers: List[ArxivEntry] = []
    source = "arxiv"

    if not force_mock:
        try:
            papers = await fetch_arxiv_hft_transformer(search_query, max_results=max_results)
        except Exception as e:
            logger.warning("arXiv fetch failed, using mock: %s", e)
            papers = _mock_arxiv_entries()
            source = "mock_fallback"
    else:
        papers = _mock_arxiv_entries()
        source = "mock"

    if not papers:
        papers = _mock_arxiv_entries()
        source = "mock_empty"

    architecture = synthesize_attention_architecture(papers)
    codex_prompt = build_codex_prompt(architecture)

    return {
        "ok": True,
        "source": source,
        "paper_count": len(papers),
        "papers": [p.__dict__ for p in papers],
        "architecture": architecture,
        "codex_prompt": codex_prompt,
        "skill": "sk_academic_researcher",
    }


def run_skill_sync(params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_skill(params))
    raise RuntimeError("在 async 上下文中请使用 await run_skill(...)")
