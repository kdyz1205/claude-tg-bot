"""
agents/runner.py — Multi-agent pipeline orchestrator.

Architecture (inspired by Anthropic's patterns + Karpathy's autoresearch):

  FAST PATH (Haiku, ~2s):
    Classify intent → route to single agent if simple

  FULL PATH (Sonnet, ~5s):
    Decompose → multi-agent pipeline → review loop

  EXECUTION:
    Each agent = one Claude CLI call with focused prompt + temp file
    Git commit each fix (keep/discard like autoresearch)
"""
import asyncio
import json
import logging
import os
import tempfile
import time

from agents.prompts import DISPATCHER, COMPUTER, REVIEW, DEBUG, CODE

logger = logging.getLogger(__name__)

CLAUDE_CMD = os.path.join(
    os.path.expanduser("~"), "AppData", "Roaming", "npm", "claude.cmd"
)
BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

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


# ─────────────────────────────────────────────────────────────────────────────
# Core: Call a Claude CLI agent
# ─────────────────────────────────────────────────────────────────────────────

async def _call_agent(
    prompt: str,
    system_prompt: str,
    model: str = SONNET,
    timeout: int = 60,
    cwd: str = None,
    max_turns: int = 0,
) -> tuple[str, int]:
    """Call Claude CLI as a focused agent. Returns (response_text, exit_code)."""
    cwd = cwd or os.path.expanduser("~")
    prompt_file = None
    proc = None

    try:
        # Write system prompt to temp file (Windows cmd.exe corrupts long strings)
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8", dir=BOT_DIR,
        )
        try:
            prompt_file.write(system_prompt)
        finally:
            prompt_file.close()

        args = [
            CLAUDE_CMD,
            "-p", prompt,
            "--output-format", "json",
            "--dangerously-skip-permissions",
            "--model", model,
            "--append-system-prompt-file", prompt_file.name,
        ]
        if max_turns > 0:
            args.extend(["--max-turns", str(max_turns)])

        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)

    except asyncio.TimeoutError:
        logger.warning(f"Agent timed out ({model}, {timeout}s)")
        if proc:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
        return f"Agent timed out after {timeout}s", 1
    except Exception as e:
        logger.error(f"Agent error: {e}")
        return f"Agent error: {e}", 1
    finally:
        if prompt_file and os.path.exists(prompt_file.name):
            try:
                os.unlink(prompt_file.name)
            except Exception:
                pass

    raw = stdout.decode("utf-8", errors="replace").strip()
    if not raw:
        err = stderr.decode("utf-8", errors="replace").strip()
        logger.warning(f"Agent no output. stderr: {err[:200]}")
        return err[:500] if err else "No output", proc.returncode

    # Parse CLI JSON wrapper
    try:
        data = json.loads(raw)
        result = data.get("result", "").strip()
        if not result:
            result = f"Error: {data.get('error', 'unknown')}" if data.get("is_error") else "Done."
        return result, proc.returncode
    except json.JSONDecodeError:
        idx = raw.find("{")
        if idx > 0:
            try:
                data = json.loads(raw[idx:])
                return data.get("result", raw[:1000]), proc.returncode
            except json.JSONDecodeError:
                pass
        return raw[:1000], proc.returncode


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
    """Fast classification with Haiku (no tools). Returns {category, project, summary}."""
    # Use --tools "" to disable all tools — classifier must ONLY output JSON, not execute
    prompt_file = None
    proc = None
    try:
        prompt_file = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8", dir=BOT_DIR,
        )
        try:
            prompt_file.write(CLASSIFY_PROMPT)
        finally:
            prompt_file.close()

        args = [
            CLAUDE_CMD,
            "-p", f"Message: {user_message[:500]}",
            "--output-format", "json",
            "--dangerously-skip-permissions",
            "--model", HAIKU,
            "--tools", "",  # NO tools — classification only!
            "--append-system-prompt-file", prompt_file.name,
        ]
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=CLASSIFY_TIMEOUT)
        raw = stdout.decode("utf-8", errors="replace").strip()
        result = ""
        if raw:
            try:
                data = json.loads(raw)
                result = data.get("result", "").strip()
            except json.JSONDecodeError:
                result = raw
    except Exception as e:
        logger.warning(f"Classify error: {e}")
        result = ""
    finally:
        if prompt_file and os.path.exists(prompt_file.name):
            try:
                os.unlink(prompt_file.name)
            except Exception:
                pass
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

    # ── Full pipeline path (project_task) ──
    await send_status("📋 制定计划中...")
    plan = await _plan(user_message, classification)

    if not plan or "steps" not in plan:
        # Fallback: single debug agent
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

    await send_status(f"📋 {summary}\n📊 {len(steps)} 步")

    # ── Execute steps (Superpowers subagent-driven-development pattern) ──
    # Each step: implement → spec review → fix if needed → verify
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

        # Context from previous steps
        context = ""
        if results:
            last = results[-2:]
            context = "\n\nPrevious results:\n" + "\n---\n".join(
                f"[{r['agent']}] {r['result'][:500]}" for r in last
            )

        # ── IMPLEMENT: focused subagent per task ──
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

        # ── SPEC REVIEW: verify step actually accomplished its task ──
        # (Superpowers: two-stage review — spec compliance first)
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

        # ── REVIEW LOOP: autoresearch-style bug detection ──
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
                # Superpowers systematic-debugging: investigate root cause first
                extra_steps = [
                    {"agent": "debug", "task": f"ROOT CAUSE INVESTIGATION (do NOT fix yet):\n{bug_desc}{proj_ctx}\n\nTrace the data flow. Find WHERE it breaks. Report root cause.", "model": "sonnet"},
                    {"agent": "debug", "task": f"IMPLEMENT SINGLE FIX for root cause found above.{proj_ctx}\nOne fix only, no bundling.", "model": "sonnet"},
                    {"agent": "review", "task": "VERIFY with evidence: run tests/check output. Report PASS or FAIL with actual output.", "model": "sonnet"},
                ]
                steps = steps[:i+1] + extra_steps + steps[i+1:]

        i += 1

    # ── FINAL VERIFICATION (Superpowers: evidence before claims) ──
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

    # ── Summary ──
    elapsed = time.time() - start_time
    parts = []
    for r in results:
        icon = {"computer": "🖥️", "review": "🔍", "debug": "🔧", "code": "📝", "verify": "✅"}.get(r["agent"], "⚙️")
        parts.append(f"{icon} {r['task'][:60]}\n{r['result'][:300]}")

    final = "\n\n".join(parts)
    final += f"\n\n⏱️ {elapsed:.0f}s | 🔄 {review_rounds} review rounds"
    return final
