"""
agents/rag.py — Retrieval-Augmented Generation for the bot.

Stores past successful solutions and retrieves them when similar tasks appear.
This implements the RAG pattern from the NLAH paper:
- Index successful interactions by task type + keywords
- On new task, retrieve top-K similar past solutions
- Inject them into the prompt as examples

Storage: lightweight JSONL + keyword-based retrieval (no vector DB needed).
"""

import json
import logging
import os
import re
import time
from collections import Counter
from typing import Any

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SOLUTIONS_FILE = os.path.join(BOT_DIR, ".solutions_index.jsonl")
_MAX_SOLUTIONS = 300


def _extract_keywords(text: str) -> set[str]:
    """Extract keywords from text for matching."""
    text = text.lower()
    # Remove common stopwords
    stopwords = {"的", "了", "是", "在", "我", "你", "他", "她", "它", "们",
                 "the", "a", "an", "is", "are", "was", "were", "to", "of", "in",
                 "and", "or", "for", "with", "on", "at", "by", "this", "that",
                 "i", "you", "he", "she", "it", "we", "they", "do", "does", "did",
                 "have", "has", "had", "be", "been", "being", "will", "would",
                 "can", "could", "should", "may", "might", "shall", "must",
                 "不", "没", "有", "也", "就", "都", "但", "和", "吗", "呢",
                 "把", "被", "让", "从", "到", "给", "用", "对", "跟"}
    # Split on non-alphanumeric/CJK
    tokens = re.findall(r'[\w\u4e00-\u9fff]+', text)
    return {t for t in tokens if t not in stopwords and len(t) > 1}


class SolutionStore:
    """Stores and retrieves past successful solutions."""

    def __init__(self):
        self._solutions: list[dict] = []
        self._load()

    def _load(self):
        try:
            if os.path.exists(_SOLUTIONS_FILE):
                with open(_SOLUTIONS_FILE, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                self._solutions.append(json.loads(line))
                            except json.JSONDecodeError:
                                pass
                self._solutions = self._solutions[-_MAX_SOLUTIONS:]
        except Exception as e:
            logger.warning(f"RAG: load failed: {e}")

    def _save_solution(self, solution: dict):
        try:
            with open(_SOLUTIONS_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(solution, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"RAG: save failed: {e}")

    def store_solution(
        self,
        task: str,
        solution: str,
        tools_used: list[str] = None,
        category: str = "general",
        score: float = 1.0,
    ):
        """Store a successful solution for future retrieval."""
        entry = {
            "task": task[:500],
            "solution": solution[:2000],
            "tools": tools_used or [],
            "category": category,
            "score": score,
            "keywords": list(_extract_keywords(task))[:30],
            "timestamp": time.time(),
        }
        self._solutions.append(entry)
        self._solutions = self._solutions[-_MAX_SOLUTIONS:]
        self._save_solution(entry)

    def retrieve(self, query: str, top_k: int = 3, category: str = None) -> list[dict]:
        """Retrieve the most relevant past solutions for a query."""
        query_keywords = _extract_keywords(query)
        if not query_keywords:
            return []

        scored = []
        for sol in self._solutions:
            # Category filter
            if category and sol.get("category") != category:
                continue

            sol_keywords = set(sol.get("keywords", []))
            if not sol_keywords:
                continue

            # Jaccard-like similarity
            intersection = len(query_keywords & sol_keywords)
            union = len(query_keywords | sol_keywords)
            similarity = intersection / union if union > 0 else 0

            # Boost recent solutions slightly
            age_days = (time.time() - sol.get("timestamp", 0)) / 86400
            recency_bonus = max(0, 0.1 - age_days * 0.001)

            # Boost high-score solutions
            score_bonus = sol.get("score", 0.5) * 0.1

            total_score = similarity + recency_bonus + score_bonus
            if total_score > 0.05:
                scored.append((total_score, sol))

        scored.sort(key=lambda x: -x[0])
        return [s[1] for s in scored[:top_k]]

    def format_for_prompt(self, solutions: list[dict], max_chars: int = 800) -> str:
        """Format retrieved solutions for injection into a prompt."""
        if not solutions:
            return ""

        lines = ["## Past similar solutions:"]
        chars = 0
        for i, sol in enumerate(solutions, 1):
            entry = f"\n{i}. Task: {sol['task'][:100]}\n   Solution: {sol['solution'][:200]}"
            if sol.get("tools"):
                entry += f"\n   Tools: {', '.join(sol['tools'][:5])}"
            if chars + len(entry) > max_chars:
                break
            lines.append(entry)
            chars += len(entry)

        return "\n".join(lines)

    def get_stats(self) -> dict:
        """Get statistics about the solution store."""
        cats = Counter(s.get("category", "general") for s in self._solutions)
        return {
            "total_solutions": len(self._solutions),
            "categories": dict(cats.most_common(10)),
            "oldest": self._solutions[0].get("timestamp") if self._solutions else None,
            "newest": self._solutions[-1].get("timestamp") if self._solutions else None,
        }


# ── Singleton ──

_store: SolutionStore | None = None

def get_solution_store() -> SolutionStore:
    global _store
    if _store is None:
        _store = SolutionStore()
    return _store
