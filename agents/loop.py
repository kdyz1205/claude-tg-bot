"""
agents/loop.py — Autonomous work loop.

The bot controls the loop, Claude does one step at a time.
Each step is a CLI call with --resume (keeps context).

Usage:
    result = await autonomous_loop(
        goal="修好 smartmoney 的 bug",
        project_dir="C:/Users/alexl/Desktop/crypto-analysis-",
        send_status=async_fn,  # sends progress to Telegram
        max_rounds=5,
    )
"""
import asyncio
import json
import logging
import os
import tempfile
import time

logger = logging.getLogger(__name__)

CLAUDE_CMD = os.path.join(
    os.path.expanduser("~"), "AppData", "Roaming", "npm", "claude.cmd"
)
BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _load_system_prompt() -> str:
    """Lazily load system prompt, with fallback if file doesn't exist."""
    prompt_path = os.path.join(BOT_DIR, ".system_prompt.txt")
    try:
        with open(prompt_path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        logger.warning(f"System prompt not found at {prompt_path}, using minimal fallback")
        return "You are a helpful assistant. Act immediately, never ask questions."
    except Exception as e:
        logger.error(f"Failed to load system prompt: {e}")
        return "You are a helpful assistant. Act immediately, never ask questions."

SYSTEM_PROMPT = _load_system_prompt()


async def _cli_call(
    prompt: str,
    session_id: str = None,
    model: str = "claude-sonnet-4-6",
    timeout: int = 180,
    cwd: str = None,
) -> tuple[str, str | None]:
    """One CLI call. Returns (response, session_id)."""
    cwd = cwd or os.path.expanduser("~")

    prompt_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False, encoding="utf-8", dir=BOT_DIR,
    )
    try:
        prompt_file.write(SYSTEM_PROMPT)
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
    if session_id:
        args.extend(["--resume", session_id])

    proc = None
    stdout = b""
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        if proc:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
        return "Timed out", session_id
    except Exception as e:
        return f"Error: {e}", session_id
    finally:
        try:
            os.unlink(prompt_file.name)
        except Exception:
            pass

    raw = stdout.decode("utf-8", errors="replace").strip()
    if not raw:
        return "No output", session_id

    try:
        data = json.loads(raw)
        result = data.get("result", "").strip() or "Done."
        new_sid = data.get("session_id", session_id)
        return result, new_sid
    except json.JSONDecodeError:
        return raw[:1000], session_id


async def autonomous_loop(
    goal: str,
    send_status,          # async callable(text)
    project_dir: str = None,
    model: str = "claude-sonnet-4-6",
    max_rounds: int = 5,
    verify_cmd: str = None,  # e.g. "npm test" or "python -m pytest"
) -> str:
    """
    Superpowers-style autonomous loop: Investigate → Plan → Fix → Verify.

    Applies systematic-debugging methodology:
      Phase 1: ROOT CAUSE — read errors, reproduce, trace data flow
      Phase 2: PLAN — form hypothesis, identify minimal fix
      Phase 3: IMPLEMENT — one fix at a time, no bundling
      Phase 4: VERIFY — evidence before claims, run actual commands

    If 3+ fixes fail on the same issue, questions architecture instead of
    attempting more fixes (Superpowers "3-fix rule").
    """
    session_id = None
    start_time = time.time()
    history = []
    consecutive_failures = 0

    for round_num in range(1, max_rounds + 1):
        # ── Phase 1: ROOT CAUSE INVESTIGATION ──
        if round_num == 1:
            investigate_prompt = (
                f"Goal: {goal}\n\n"
                f"PHASE 1 — ROOT CAUSE INVESTIGATION (do NOT fix anything yet):\n"
                f"1. Read error messages/logs carefully — note exact errors, line numbers\n"
                f"2. Reproduce the issue — can you trigger it reliably?\n"
                f"3. Check recent changes — git diff, what changed?\n"
                f"4. Trace data flow — where does the bad value originate?\n\n"
                f"Output format:\n"
                f"ROOT CAUSE: [one sentence describing the actual root cause]\n"
                f"EVIDENCE: [what you saw that proves this]\n"
                f"HYPOTHESIS: [your fix hypothesis]"
            )
        else:
            last = history[-1] if history else {}
            last_verify = last.get("verify", "(no previous result)")[:500]
            investigate_prompt = (
                f"Goal: {goal}\n\n"
                f"Round {round_num}. Previous fix {'PASSED' if last.get('passed') else 'FAILED'}:\n"
                f"{last_verify}\n\n"
                f"PHASE 1 — RE-INVESTIGATE with new information:\n"
                f"- What did the previous attempt reveal?\n"
                f"- Is this the same root cause or a different one?\n"
                f"- Trace the actual data flow, don't guess.\n\n"
                f"Output: ROOT CAUSE: ... | EVIDENCE: ... | HYPOTHESIS: ..."
            )

        await send_status(f"🔍 Round {round_num}/{max_rounds}: Investigating root cause...")
        investigate_result, session_id = await _cli_call(
            prompt=investigate_prompt,
            session_id=session_id,
            model=model,
            timeout=120,
            cwd=project_dir,
        )

        # ── Phase 2+3: PLAN & IMPLEMENT (single minimal fix) ──
        fix_prompt = (
            f"PHASE 3 — IMPLEMENT SINGLE FIX:\n"
            f"Based on your investigation:\n{investigate_result[:800]}\n\n"
            f"Rules:\n"
            f"- Make the SMALLEST possible change to fix the root cause\n"
            f"- ONE fix only — do NOT bundle multiple changes\n"
            f"- Fix at the SOURCE, not the symptom\n"
            f"- No 'while I'm here' improvements\n\n"
            f"Apply the fix now. Describe exactly what you changed."
        )

        await send_status(f"🔧 Round {round_num}/{max_rounds}: Applying fix...")
        fix_result, session_id = await _cli_call(
            prompt=fix_prompt,
            session_id=session_id,
            model=model,
            timeout=180,
            cwd=project_dir,
        )

        # ── Phase 4: VERIFY WITH EVIDENCE ──
        # Superpowers: "NO COMPLETION CLAIMS WITHOUT FRESH VERIFICATION EVIDENCE"
        if verify_cmd:
            verify_prompt = (
                f"PHASE 4 — VERIFICATION (evidence before claims):\n"
                f"Run this EXACT command and report the FULL output:\n"
                f"  {verify_cmd}\n\n"
                f"Then check: does the output confirm the fix worked?\n"
                f"DO NOT say 'should work' or 'probably fixed'.\n"
                f"Report ACTUAL output. First line: PASS or FAIL."
            )
        else:
            verify_prompt = (
                f"PHASE 4 — VERIFICATION (evidence before claims):\n"
                f"Goal: {goal}\n"
                f"You just applied: {fix_result[:500]}\n\n"
                f"NOW VERIFY with actual evidence:\n"
                f"- Run tests, check logs, read the changed file\n"
                f"- DO NOT claim success without running verification\n"
                f"- 'Should work' = NOT verified. Run the command.\n\n"
                f"First line: PASS or FAIL\n"
                f"Then: the actual command output or evidence."
            )

        await send_status(f"✅ Round {round_num}/{max_rounds}: Verifying with evidence...")
        verify_result, session_id = await _cli_call(
            prompt=verify_prompt,
            session_id=session_id,
            model=model,
            timeout=120,
            cwd=project_dir,
        )

        # ── JUDGE ──
        passed = verify_result.strip().upper().startswith("PASS")
        history.append({
            "round": round_num,
            "investigate": investigate_result[:300],
            "fix": fix_result[:300],
            "verify": verify_result[:300],
            "passed": passed,
        })

        if passed:
            consecutive_failures = 0
            await send_status(f"✅ Round {round_num} passed!")
            break
        else:
            consecutive_failures += 1
            await send_status(f"❌ Round {round_num} failed\n{verify_result[:200]}")

            # Superpowers 3-fix rule: if 3+ fixes failed, question architecture
            if consecutive_failures >= 3:
                arch_prompt = (
                    f"3 consecutive fixes have FAILED for: {goal}\n\n"
                    f"History:\n" +
                    "\n".join(f"  Round {h['round']}: {h['fix'][:100]}" for h in history[-3:]) +
                    f"\n\nThis pattern indicates an ARCHITECTURAL problem, not a bug.\n"
                    f"STOP fixing symptoms. Instead:\n"
                    f"1. Is the current approach fundamentally sound?\n"
                    f"2. Should we refactor the architecture?\n"
                    f"3. What is the REAL underlying issue?\n\n"
                    f"Provide an architectural assessment."
                )
                await send_status("🏗️ 3 fixes failed — questioning architecture...")
                arch_result, session_id = await _cli_call(
                    prompt=arch_prompt, session_id=session_id,
                    model=model, timeout=120, cwd=project_dir,
                )
                history.append({
                    "round": round_num, "investigate": "ARCHITECTURE REVIEW",
                    "fix": arch_result[:300], "verify": "N/A", "passed": False,
                })
                break

    # ── Summary ──
    elapsed = time.time() - start_time
    total_rounds = len(history)
    final_passed = history[-1]["passed"] if history else False

    summary_parts = []
    for h in history:
        icon = "✅" if h["passed"] else "❌"
        summary_parts.append(
            f"{icon} Round {h['round']}:\n"
            f"  Root cause: {h.get('investigate', '')[:100]}\n"
            f"  Fix: {h.get('fix', '')[:100]}\n"
            f"  Verify: {h['verify'][:100]}"
        )

    status = "DONE" if final_passed else f"INCOMPLETE ({total_rounds} rounds)"
    summary = "\n\n".join(summary_parts)
    summary += f"\n\n⏱️ {elapsed:.0f}s | 🔄 {total_rounds} rounds | {'✅ ' + status if final_passed else '⚠️ ' + status}"

    return summary


async def self_evolve(
    send_status,
    focus: str = "",
    model: str = "claude-sonnet-4-6",
    max_rounds: int = 3,
) -> str:
    """
    Self-evolution: the bot analyzes and improves its own code.

    1. Analyze: scan bot code for issues/improvements
    2. Fix: apply improvements
    3. Verify: syntax check + import test
    4. Record: log evolution in consciousness system
    """
    goal = (
        f"Analyze and improve the TG bot code in {BOT_DIR}. "
        f"Focus: {focus or 'find bugs, optimize performance, add missing error handling'}. "
        f"After each change, verify with: python -c \"import py_compile; "
        f"[py_compile.compile(f, doraise=True) for f in "
        f"['bot.py','claude_agent.py','tools.py','providers.py']]\""
    )

    result = await autonomous_loop(
        goal=goal,
        send_status=send_status,
        project_dir=BOT_DIR,
        model=model,
        max_rounds=max_rounds,
        verify_cmd="python -c \"import py_compile; [py_compile.compile(f, doraise=True) for f in ['bot.py','claude_agent.py','tools.py','providers.py']]\"",
    )

    # Record evolution
    try:
        from agents.consciousness import get_self_awareness
        a = get_self_awareness()
        idx = a.record_evolution("self_evolve", f"Self-evolution: {focus or 'general improvement'}")
        success = "PASS" in result or "✅" in result
        a.record_evolution_outcome(idx, success, result[:200])
    except Exception:
        pass

    return result
