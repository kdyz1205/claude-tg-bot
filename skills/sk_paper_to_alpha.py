"""
skills/sk_paper_to_alpha.py — Paper-to-Alpha / Paper-to-Pseudocode 流水线骨架。

职责：
  - 从 arXiv（或本地文本）拉取/注入量化论文摘要或全文
  - 构造结构化 Prompt，供本地 Claude CLI / Codex CLI 将公式与算法提炼为可编码伪代码
  - 输出供 codex_charger / infinite_evolver / singularity_engine 消费的标准 JSON

不保证自动盈利；数学与实现之间需人工或沙盒回归验证。
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

ARXIV_API = "http://export.arxiv.org/api/query"


@dataclass
class PaperRecord:
    arxiv_id: str
    title: str
    summary: str
    pdf_hint: str


def _strip_ns(tag: str) -> str:
    return tag.split("}")[-1] if "}" in tag else tag


def parse_arxiv_atom(xml_text: str) -> List[PaperRecord]:
    """Parse arXiv Atom feed into PaperRecord list."""
    root = ET.fromstring(xml_text)
    ns = {"a": "http://www.w3.org/2005/Atom"}
    out: List[PaperRecord] = []
    for entry in root.findall("a:entry", ns):
        id_el = entry.find("a:id", ns)
        title_el = entry.find("a:title", ns)
        summ_el = entry.find("a:summary", ns)
        pid = (id_el.text or "").strip().split("/abs/")[-1] if id_el is not None else ""
        title = (title_el.text or "").strip().replace("\n", " ") if title_el is not None else ""
        summary = (summ_el.text or "").strip() if summ_el is not None else ""
        out.append(PaperRecord(arxiv_id=pid, title=title, summary=summary, pdf_hint=f"https://arxiv.org/pdf/{pid}.pdf"))
    return out


async def fetch_arxiv_papers(search_query: str, max_results: int = 5) -> List[PaperRecord]:
    params = {"search_query": search_query, "start": 0, "max_results": max(max_results, 1)}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(ARXIV_API, params=params)
        r.raise_for_status()
    return parse_arxiv_atom(r.text)


def build_codex_charger_payload(paper: PaperRecord, extra_constraints: str = "") -> Dict[str, Any]:
    """
    生成交给 codex_charger / CLI 的伪代码提取任务（非自动执行，仅结构化载荷）。
    """
    prompt = f"""你是量化+链上系统架构师。阅读以下论文条目，输出**可实现的伪代码与数学要点**。

## 论文
- ID: {paper.arxiv_id}
- 标题: {paper.title}
- 摘要:
{paper.summary[:12000]}

## 硬性要求
1. 用 Markdown 分节：「假设」「状态变量」「观测」「动作」「奖励/目标函数」「训练循环」「链上数据依赖」「风险」。
2. 所有**公式**用 LaTeX 行内或块写出，并给出**离散化/向量化**建议（如何变成张量）。
3. 给出 **PyTorch 风格** 的模块草图（类名、forward 输入输出形状），勿写完整训练脚本。
4. 标注哪些步骤必须在 **沙盒回测** 通过后才能接 live_trader / Jupiter。
5. {extra_constraints or "无额外约束。"}

## 输出末尾
单独一节 `CODEX_PSEUDOCODE_JSON`，内含一个 JSON 对象，键：
  "feature_map", "model_outline", "loss_or_reward", "data_sources", "sandbox_tests", "live_guards"
"""
    return {
        "kind": "paper_to_alpha",
        "arxiv_id": paper.arxiv_id,
        "title": paper.title,
        "pdf_hint": paper.pdf_hint,
        "cli_prompt": prompt,
        "target_modules": ["singularity_engine", "harness", "codex_charger"],
    }


def extract_json_from_llm_response(text: str) -> Optional[Dict[str, Any]]:
    """从 LLM 回复中抠出 JSON 块（宽松匹配）。"""
    m = re.search(r"CODEX_PSEUDOCODE_JSON\s*```(?:json)?\s*(\{{.*?\}})\s*```", text, re.DOTALL | re.IGNORECASE)
    if not m:
        m2 = re.search(r"\{[\s\S]*\"feature_map\"[\s\S]*\}", text)
        if not m2:
            return None
        blob = m2.group(0)
    else:
        blob = m.group(1)
    try:
        return json.loads(blob)
    except json.JSONDecodeError:
        return None


async def run_skill(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Skill 入口。

    params:
      search_query: str — arXiv 查询，如 "all:reinforcement learning AND all:portfolio"
      max_results: int
      paper_text: str — 可选，若提供则跳过 arXiv，直接打包 prompt
      title: str — 与 paper_text 搭配
      extra_constraints: str
    """
    extra = str(params.get("extra_constraints") or "")
    paper_text = params.get("paper_text")
    if paper_text:
        paper = PaperRecord(
            arxiv_id=str(params.get("arxiv_id") or "local"),
            title=str(params.get("title") or "User-supplied paper"),
            summary=paper_text[:120000],
            pdf_hint="",
        )
        payload = build_codex_charger_payload(paper, extra_constraints=extra)
        return {"ok": True, "source": "inline", "payload": payload}

    q = str(params.get("search_query") or "all:deep reinforcement learning AND all:trading")
    n = int(params.get("max_results") or 3)
    try:
        papers = await fetch_arxiv_papers(q, n)
    except Exception as e:
        logger.error("arxiv fetch failed: %s", e)
        return {"ok": False, "error": str(e)}

    if not papers:
        return {"ok": False, "error": "no papers"}

    payloads = [build_codex_charger_payload(p, extra_constraints=extra) for p in papers]
    return {"ok": True, "source": "arxiv", "count": len(payloads), "payloads": payloads, "papers": [p.__dict__ for p in papers]}


def run_skill_sync(params: Dict[str, Any]) -> Dict[str, Any]:
    """仅在无运行中 event loop 时使用（例如脚本入口）。"""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(run_skill(params))
    raise RuntimeError("在 async 上下文中请使用 await run_skill(...)")
