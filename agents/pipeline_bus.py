"""
Bounded asyncio.Queue stages: RAG → reflection → trader.

Each stage hands off a single PipelineEnvelope so work is not dropped under
backpressure (queues maxsize=1).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


@dataclass
class PipelineEnvelope:
    user_message: str
    chat_id: int
    classification: dict[str, Any]
    project_dir: str | None
    rag_context: str = ""
    reflection_context: str = ""


async def run_rag_reflect_trade_pipeline(
    user_message: str,
    chat_id: int,
    classification: dict[str, Any],
    project_dir: str | None,
    trade_runner: Callable[[PipelineEnvelope], Awaitable[str]],
) -> str:
    q_after_rag: asyncio.Queue[PipelineEnvelope | None] = asyncio.Queue(maxsize=1)
    q_after_reflect: asyncio.Queue[PipelineEnvelope | None] = asyncio.Queue(maxsize=1)
    fatal: list[BaseException | None] = [None]
    trade_results: list[str] = []

    async def stage_rag() -> None:
        try:
            from agents.rag import get_solution_store

            store = get_solution_store()
            hits = store.retrieve(user_message, top_k=3)
            ctx = store.format_for_prompt(hits, max_chars=1200)
            env = PipelineEnvelope(
                user_message=user_message,
                chat_id=chat_id,
                classification=classification,
                project_dir=project_dir,
                rag_context=ctx or "",
            )
            await q_after_rag.put(env)
        except Exception as e:
            logger.exception("pipeline_bus rag: %s", e)
            fatal[0] = e
            await q_after_rag.put(None)

    async def stage_reflect() -> None:
        try:
            env = await q_after_rag.get()
            if env is None:
                await q_after_reflect.put(None)
                return
            try:
                from trading.reflection import reflection_engine

                ref_txt = reflection_engine.format_report()
            except Exception as e:
                logger.debug("pipeline_bus reflection skip: %s", e)
                ref_txt = ""
            if len(ref_txt) > 1600:
                ref_txt = ref_txt[:1600] + "…"
            env.reflection_context = ref_txt or ""
            await q_after_reflect.put(env)
        except Exception as e:
            logger.exception("pipeline_bus reflect: %s", e)
            fatal[0] = e
            await q_after_reflect.put(None)

    async def stage_trade() -> None:
        try:
            env = await q_after_reflect.get()
            if env is None:
                return
            out = await trade_runner(env)
            trade_results.append(out)
        except Exception as e:
            logger.exception("pipeline_bus trade: %s", e)
            fatal[0] = e

    await asyncio.gather(stage_rag(), stage_reflect(), stage_trade())
    if fatal[0] is not None:
        raise fatal[0]
    if not trade_results:
        return "Pipeline failed before trader stage."
    return trade_results[0]


def build_augmented_user_text(env: PipelineEnvelope) -> str:
    parts = [env.user_message]
    if env.rag_context.strip():
        parts.append("## Retrieved solutions (RAG)\n" + env.rag_context.strip())
    if env.reflection_context.strip():
        parts.append("## Trade reflection snapshot\n" + env.reflection_context.strip())
    return "\n\n".join(parts)
