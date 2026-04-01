"""
agents/runner.py — Multi-agent pipeline orchestrator.

Architecture (inspired by Anthropic's patterns + Karpathy's autoresearch):

  FAST PATH (Haiku, ~2s):
    Classify intent → route to single agent if simple

  FULL PATH (Sonnet, ~5s):
    Decompose → multi-agent pipeline → review loop

  EXECUTION:
  Each agent = one HTTP LLM turn (aiohttp via ``llm_http_client``) — no local Claude CLI subprocess.
"""
import asyncio
import json
import logging
import os
import time

import llm_http_client

from agents.prompts import DISPATCHER, COMPUTER, REVIEW, DEBUG, CODE
from agents.pipeline_bus import build_augmented_user_text, run_rag_reflect_trade_pipeline

logger = logging.getLogger(__name__)

# ── Model tiers ──
HAIKU = "claude-haiku-4-5-20251001"
SONNET = "claude-sonnet-4-6"
OPUS = "claude-opus-4-6"

MODEL_MAP = {"haiku": HAIKU, "sonnet": SONNET, "opus": OPUS}

# ── Timeouts ──
CLASSIFY_TIMEOUT = 20   # Haiku classification
PLAN_TIMEOUT = 90       # Sonnet planning (needs time to search for project dirs)
AGENT_TIMEOUTS = {
    "computer": 120,
    "review": 120,
    "debug": 180,
    "code": 180,
}
MAX_REVIEW_ROUNDS = 3


def _agent_state_key(prompt: str, salt: int) -> int:
    return -5_000_000 - (abs(hash(prompt)) % 99_000_000) - (salt % 997)


# ─────────────────────────────────────────────────────────────────────────────
# Core: HTTP LLM agent (aiohttp via llm_http_client)
# ─────────────────────────────────────────────────────────────────────────────

async def _call_agent(
    prompt: str,
    system_prompt: str,
    model: str = SONNET,
    timeout: int = 60,
    cwd: str = None,
    max_turns: int = 0,
) -> tuple[str, int]:
    """Single-shot HTTP LLM turn. Returns (response_text, exit_code 0=ok)."""
    _ = max_turns
    cwd = cwd or os.path.expanduser("~")
    sys_text = system_prompt
    if cwd:
        sys_text = f"Context: primary working directory is `{cwd}`.\n\n{system_prompt}"
    try:
        text, err = await llm_http_client.complete_stateless(
            system_prompt=sys_text[:240_000],
            user_text=(prompt or "")[:200_000],
            model_hint=model,
            timeout_sec=float(timeout),
            state_key=_agent_state_key(prompt + system_prompt[:200], int(time.time()) % 10000),
        )
        if err:
            return f"HTTP LLM error: {err}", 1
        out = (text or "").strip()
        return (out if out else "No output"), 0
    except asyncio.TimeoutError:
        logger.warning("Agent timed out (%s, %ss)", model, timeout)
        return f"Agent timed out after {timeout}s", 1
    except Exception as e:
        logger.error("Agent error: %s", e)
        return f"Agent error: {e}", 1


# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Fast classify with Haiku (~2 seconds)
# ─────────────────────────────────────────────────────────────────────────────

CLASSIFY_PROMPT = """Classify this message into ONE category. Reply with ONLY the JSON, nothing else.

Categories:
- "chat": simple greeting, question, status check, casual conversation
- "single_code": one specific code task (edit a file, fix one bug, add a feature)
- "project_task": multi-step project work (test+fix, build+deploy, explore+modify)
- "computer_control": operate the PC (open app, click, type, browse website)

Output ONLY: {"category": "...", "project": "project name or null", "summary": "what they want in 10 words"}
"""

async def _classify(user_message: str) -> dict:
    """Fast classification via HTTP Haiku-class model (JSON only)."""
    result = ""
    try:
        text, err = await llm_http_client.complete_stateless(
            system_prompt=CLASSIFY_PROMPT,
            user_text=f"Message: {user_message[:500]}",
            model_hint=HAIKU,
            timeout_sec=float(CLASSIFY_TIMEOUT),
            state_key=_agent_state_key(user_message[:200], 1),
        )
        if err:
            logger.warning("Classify HTTP error: %s", err)
        result = (text or "").strip()
    except Exception as e:
        logger.warning("Classify error: %s", e)
        result = ""
    try:
        # Try to find a valid JSON object — handle multiple braces safely
        text = result.strip()
        start = text.find("{")
        if start >= 0:
            # Try progressively shorter substrings from the first { to find valid JSON
            depth = 0
            for i in range(start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            parsed = json.loads(text[start:i+1])
                            logger.info(f"Classified: {parsed}")
                            return parsed
                        except json.JSONDecodeError:
                            break
    except (json.JSONDecodeError, ValueError, IndexError):
        pass
    logger.warning(f"Classification failed, defaulting to project_task: {result[:200]}")
    return {"category": "project_task", "project": None, "summary": user_message[:50]}


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Plan with Sonnet (only for complex tasks, ~5 seconds)
# ─────────────────────────────────────────────────────────────────────────────

async def _plan(user_message: str, classification: dict) -> dict:
    """Decompose a complex task into agent steps. Sonnet, focused prompt."""
    project = classification.get("project", "")
    result, _ = await _call_agent(
        prompt=user_message,
        system_prompt=DISPATCHER,
        model=SONNET,
        timeout=PLAN_TIMEOUT,
    )
    try:
        start = result.find("{")
        end = result.rfind("}") + 1
        if start >= 0 and end > start:
            plan = json.loads(result[start:end])
            logger.info(f"Plan: {json.dumps(plan, ensure_ascii=False)[:500]}")
            return plan
    except (json.JSONDecodeError, ValueError) as e:
        logger.warning(f"Plan parse error: {e}, raw: {result[:300]}")
    return None


async def _full_path_plan_execute(
    user_message: str,
    classification: dict,
    chat_id: int,
    send_status,
    project_dir: str | None,
    start_time: float,
) -> str:
    """Plan → stepped agents → optional final verification (used by queue trader stage)."""
    await send_status("📋 制定计划中...")
    plan = await _plan(user_message, classification)

    if not plan or "steps" not in plan:
        logger.warning("Planning failed, using single agent fallback")
        await send_status("🔧 直接执行...")
        result, _ = await _call_agent(
            prompt=user_message,
            system_prompt=DEBUG,
            model=SONNET,
            timeout=AGENT_TIMEOUTS["debug"],
            cwd=project_dir,
        )
        return result

    steps = plan.get("steps", [])
    proj_dir = project_dir or plan.get("project_dir")
    summary = plan.get("summary", "")
    should_loop = plan.get("loop", False)
    _ = should_loop

    await send_status(f"📋 {summary}\n📊 {len(steps)} 步")

    agent_map = {
        "computer": (COMPUTER, AGENT_TIMEOUTS["computer"]),
        "review": (REVIEW, AGENT_TIMEOUTS["review"]),
        "debug": (DEBUG, AGENT_TIMEOUTS["debug"]),
        "code": (CODE, AGENT_TIMEOUTS["code"]),
    }

    results = []
    review_rounds = 0
    i = 0

    while i < len(steps):
        step = steps[i]
        agent_type = step.get("agent", "debug")
        task = step.get("task", "")
        step_model_key = step.get("model", "sonnet")
        step_model = MODEL_MAP.get(step_model_key, SONNET)

        if agent_type not in agent_map:
            agent_type = "debug"

        sys_prompt, timeout = agent_map[agent_type]
        icon = {"computer": "🖥️", "review": "🔍", "debug": "🔧", "code": "📝"}.get(agent_type, "⚙️")
        model_tag = {"haiku": "⚡", "sonnet": "🎵", "opus": "🧠"}.get(step_model_key, "")

        await send_status(f"{icon}{model_tag} [{i+1}/{len(steps)}] {task[:100]}")

        context = ""
        if results:
            last = results[-2:]
            context = "\n\nPrevious results:\n" + "\n---\n".join(
                f"[{r['agent']}] {r['result'][:500]}" for r in last
            )

        result, exit_code = await _call_agent(
            prompt=f"TASK: {task}{context}",
            system_prompt=sys_prompt,
            model=step_model,
            timeout=timeout,
            cwd=proj_dir,
        )

        results.append({
            "agent": agent_type, "task": task,
            "result": result, "exit_code": exit_code,
        })

        if agent_type in ("debug", "code") and exit_code == 0:
            spec_review_prompt = (
                f"SPEC COMPLIANCE REVIEW:\n"
                f"Task was: {task[:300]}\n"
                f"Agent output: {result[:500]}\n\n"
                f"Check:\n"
                f"1. Did the agent complete ALL requirements of the task?\n"
                f"2. Did it add anything NOT requested? (scope creep)\n"
                f"3. Is there evidence it actually worked? (not just claims)\n\n"
                f"Reply: COMPLIANT or NON_COMPLIANT with one-line reason."
            )
            spec_result, _ = await _call_agent(
                prompt=spec_review_prompt,
                system_prompt="You are a strict spec compliance reviewer. Only say COMPLIANT if ALL requirements are met with evidence.",
                model=HAIKU,
                timeout=30,
            )
            if "NON_COMPLIANT" in spec_result.upper() and review_rounds < MAX_REVIEW_ROUNDS:
                review_rounds += 1
                await send_status(f"🔄 Spec review failed, fixing... (round {review_rounds})")
                extra_steps = [
                    {"agent": agent_type, "task": f"Fix spec compliance issue: {spec_result[:300]}\nOriginal task: {task[:200]}", "model": step_model_key},
                ]
                steps = steps[:i+1] + extra_steps + steps[i+1:]

        if agent_type == "review" and review_rounds < MAX_REVIEW_ROUNDS:
            has_bugs = False
            try:
                rstart = result.find("{")
                rend = result.rfind("}") + 1
                if rstart >= 0 and rend > rstart:
                    review_data = json.loads(result[rstart:rend])
                    has_bugs = review_data.get("status") == "bugs_found"
                else:
                    has_bugs = any(w in result.lower() for w in ["bug", "error", "broken", "问题", "错误", "fail"])
            except (json.JSONDecodeError, ValueError):
                has_bugs = any(w in result.lower() for w in ["bug", "error", "broken", "问题", "错误", "fail"])

            if has_bugs:
                review_rounds += 1
                await send_status(f"🔄 Review round {review_rounds}/{MAX_REVIEW_ROUNDS}: bugs found, fixing...")
                bug_desc = result[:500]
                proj_ctx = f"\nProject dir: {proj_dir}" if proj_dir else ""
                extra_steps = [
                    {"agent": "debug", "task": f"ROOT CAUSE INVESTIGATION (do NOT fix yet):\n{bug_desc}{proj_ctx}\n\nTrace the data flow. Find WHERE it breaks. Report root cause.", "model": "sonnet"},
                    {"agent": "debug", "task": f"IMPLEMENT SINGLE FIX for root cause found above.{proj_ctx}\nOne fix only, no bundling.", "model": "sonnet"},
                    {"agent": "review", "task": "VERIFY with evidence: run tests/check output. Report PASS or FAIL with actual output.", "model": "sonnet"},
                ]
                steps = steps[:i+1] + extra_steps + steps[i+1:]

        i += 1

    if proj_dir and results:
        await send_status("🔍 Final verification...")
        verify_result, _ = await _call_agent(
            prompt=(
                f"FINAL VERIFICATION — evidence before claims:\n"
                f"Original goal: {user_message[:300]}\n"
                f"Steps completed: {len(results)}\n\n"
                f"Run actual tests or checks to verify the work is complete.\n"
                f"DO NOT say 'should work'. Run the verification.\n"
                f"Report: PASS or FAIL with actual evidence."
            ),
            system_prompt=REVIEW,
            model=SONNET,
            timeout=120,
            cwd=proj_dir,
        )
        results.append({
            "agent": "verify", "task": "Final verification",
            "result": verify_result, "exit_code": 0,
        })

    elapsed = time.time() - start_time
    parts = []
    for r in results:
        icon = {"computer": "🖥️", "review": "🔍", "debug": "🔧", "code": "📝", "verify": "✅"}.get(r.get("agent", ""), "⚙️")
        parts.append(f"{icon} {str(r.get('task', ''))[:60]}\n{str(r.get('result', ''))[:300]}")

    final = "\n\n".join(parts)
    final += f"\n\n⏱️ {elapsed:.0f}s | 🔄 {review_rounds} review rounds"
    return final


# ─────────────────────────────────────────────────────────────────────────────
# Main Pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def run_pipeline(
    user_message: str,
    chat_id: int,
    send_status,
    project_dir: str = None,
) -> str:
    start_time = time.time()

    # ── Step 1: Classify (Haiku, ~2s) ──
    await send_status("🧠 分析任务中...")
    classification = await _classify(user_message)
    category = classification.get("category", "project_task")
    project_name = classification.get("project")

    # ── Fast paths ──
    if category == "chat":
        # Simple chat — one Sonnet call, no pipeline needed
        result, _ = await _call_agent(
            prompt=user_message,
            system_prompt="You are a helpful Telegram bot. Be concise (user on phone). Reply in user's language. Never ask questions.",
            model=SONNET,
            timeout=30,
        )
        return result

    if category == "single_code":
        # Single code task — one focused agent
        await send_status("🔧 执行中...")
        result, _ = await _call_agent(
            prompt=user_message,
            system_prompt=DEBUG,
            model=SONNET,
            timeout=AGENT_TIMEOUTS["debug"],
            cwd=project_dir,
        )
        return result

    if category == "computer_control":
        await send_status("🖥️ 操控电脑中...")
        result, _ = await _call_agent(
            prompt=user_message,
            system_prompt=COMPUTER,
            model=SONNET,
            timeout=AGENT_TIMEOUTS["computer"],
        )
        return result

    # ── Full pipeline path (project_task): RAG → reflection → trader (queued stages) ──
    async def _trade_stage(env):
        augmented = build_augmented_user_text(env)
        return await _full_path_plan_execute(
            augmented,
            env.classification,
            env.chat_id,
            send_status,
            env.project_dir,
            start_time,
        )

    try:
        return await run_rag_reflect_trade_pipeline(
            user_message,
            chat_id,
            classification,
            project_dir,
            _trade_stage,
        )
    except Exception as e:
        logger.warning("Queued agent pipeline failed (%s); direct plan path.", e)
        return await _full_path_plan_execute(
            user_message,
            classification,
            chat_id,
            send_status,
            project_dir,
            start_time,
        )
