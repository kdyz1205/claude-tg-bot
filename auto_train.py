"""
auto_train.py — Deep Auto-Eval + Auto-Fix Training Loop

Uses Claude as JUDGE (not regex) and Claude as PROMPT ENGINEER (not string append).

Loop per round:
  1. Run each test → get bot response
  2. Claude Judge scores each response (Haiku, cheap + smart)
  3. Collect all failures from the round
  4. Claude Prompt Engineer reads failures + current prompt → rewrites weak sections
  5. Next round uses improved prompt → scores should go up
  6. Repeat N rounds, track trend

Usage:
  /train              → show progress
  /train_obedience    → 3 rounds of eval+fix
  /train_all          → run all domains
  /train_stop         → stop
"""
import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
PROGRESS_FILE = os.path.join(BOT_DIR, ".train_progress.json")
TRAIN_LOG_FILE = os.path.join(BOT_DIR, ".train_log.jsonl")
PROMPT_FILE = os.path.join(BOT_DIR, ".system_prompt.txt")
FIXES_FILE = os.path.join(BOT_DIR, ".prompt_fixes.json")

# ─── Test Suites ──────────────────────────────────────────────────────────────
# Each test has: prompt, criteria (human-readable for the judge), max_time_s

EVAL_SUITES = {
    "file_ops": {
        "name": "文件操作",
        "tests": [
            {
                "prompt": "在桌面创建 _eval_test.txt 写入 'eval123'，读取确认，然后删除。只报告结果。",
                "criteria": "必须：1)实际创建了文件 2)回复包含eval123证明读取了 3)报告删除 4)没有问任何问题",
                "max_time_s": 30,
            },
            {
                "prompt": "列出 {BOT_DIR}/ 下所有 .py 文件名，一行一个。",
                "criteria": "必须：1)列出了.py文件名 2)包含bot.py和config.py 3)没有问问题 4)格式简洁",
                "max_time_s": 20,
            },
            {
                "prompt": "读取 {BOT_DIR}/config.py 前3行，报告内容。简短回复。",
                "criteria": "必须：1)显示了config.py的前3行内容 2)回复简短 3)没有问问题",
                "max_time_s": 15,
            },
        ],
    },
    "code_edit": {
        "name": "代码编辑",
        "tests": [
            {
                "prompt": "读 {BOT_DIR}/screenshots.py，列出所有函数名。简短回复。",
                "criteria": "必须：1)列出了函数名 2)包含capture_screenshot 3)回复简短 4)没有问问题",
                "max_time_s": 20,
            },
            {
                "prompt": "在桌面创建 _eval_buggy.py 内容: def add(a,b): return a-b\n然后修复这个bug（应该是a+b），验证修复，删除文件。报告结果。",
                "criteria": "必须：1)创建了文件 2)修复了bug(a-b→a+b) 3)验证了修复 4)删除了文件 5)报告了结果 6)没有问问题。部分完成也给部分分。",
                "max_time_s": 45,
            },
        ],
    },
    "computer_control": {
        "name": "电脑控制",
        "tests": [
            {
                "prompt": "获取屏幕分辨率和鼠标位置，只报告数字。",
                "criteria": "必须：1)报告了分辨率数字(如1920x1080) 2)报告了鼠标位置坐标 3)回复简短 4)没有问问题",
                "max_time_s": 15,
            },
            {
                "prompt": "截图当前屏幕，用3个词描述你看到了什么。",
                "criteria": "必须：1)执行了截图 2)描述了屏幕内容 3)回复简洁(约3个词) 4)没有问问题",
                "max_time_s": 20,
            },
        ],
    },
    "browser": {
        "name": "浏览器",
        "tests": [
            {
                "prompt": "用 start chrome --new-window 打开 https://example.com，然后截图。描述页面内容，3句话以内。",
                "criteria": "必须：1)打开了chrome 2)访问了example.com 3)截了图 4)描述了页面内容 5)3句话以内 6)没有问问题",
                "max_time_s": 30,
            },
        ],
    },
    "obedience": {
        "name": "服从性",
        "tests": [
            {
                "prompt": "你好",
                "criteria": "必须：1)友好回应 2)绝对没有问任何问题 3)没有'要我做什么'/'需要什么帮助'之类的反问",
                "max_time_s": 15,
            },
            {
                "prompt": "帮我看看桌面有什么文件",
                "criteria": "必须：1)直接列出了桌面文件 2)没有问'哪个桌面'/'要看什么类型'等问题 3)直接执行了",
                "max_time_s": 20,
            },
            {
                "prompt": "打开浏览器",
                "criteria": "必须：1)直接打开了浏览器 2)没有问'打开什么网站'/'用哪个浏览器'等问题 3)直接做了默认选择",
                "max_time_s": 20,
            },
        ],
    },
}

def _resolve_test_prompt(prompt: str) -> str:
    """Replace {BOT_DIR} in test prompts with the actual bot directory."""
    return prompt.replace("{BOT_DIR}", BOT_DIR)


DEFAULT_LOOPS = 3
JUDGE_MODEL = "claude-haiku-4-5-20251001"  # Cheap + fast for judging

# ─── Progress ────────────────────────────────────────────────────────────────

def _load_progress() -> dict:
    try:
        if os.path.exists(PROGRESS_FILE):
            return json.loads(Path(PROGRESS_FILE).read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_progress(data: dict):
    try:
        tmp = PROGRESS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, PROGRESS_FILE)
    except Exception as e:
        logger.warning(f"Failed to save training progress: {e}")

def _load_fixes() -> list:
    try:
        if os.path.exists(FIXES_FILE):
            return json.loads(Path(FIXES_FILE).read_text(encoding="utf-8"))
    except Exception:
        pass
    return []

def _save_fixes(fixes: list):
    try:
        tmp = FIXES_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(fixes, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, FIXES_FILE)
    except Exception as e:
        logger.warning(f"Failed to save training fixes: {e}")

def get_progress_report() -> str:
    progress = _load_progress()
    fixes = _load_fixes()
    lines = ["🎓 Deep Training 进度\n"]
    for suite_id, suite in EVAL_SUITES.items():
        sp = progress.get(suite_id, {})
        last_score = sp.get("last_avg", 0)
        runs = sp.get("runs", 0)
        fixes_count = sp.get("fixes_applied", 0)
        if runs > 0:
            status = f"✅ {last_score:.0%}" if last_score >= 0.7 else f"⚠️ {last_score:.0%}"
        else:
            status = "⬜ 未测"
        lines.append(f"{status}  {suite['name']} | {runs}轮 | {fixes_count}次修复")
    lines.append("")
    lines.append("发送: /train_file_ops /train_code_edit")
    lines.append("/train_computer_control /train_browser")
    lines.append("/train_obedience /train_all")
    if fixes:
        lines.append(f"\n🔧 累计 prompt 进化: {len(fixes)} 次")
    return "\n".join(lines)


def get_domain_ids() -> list[str]:
    return list(EVAL_SUITES.keys()) + ["all"]


# ─── Core Training Loop ─────────────────────────────────────────────────────

_training_active = False
_training_stop = False

async def run_training(
    domain_id: str,
    send_status,
    send_photo=None,
    max_tasks: int = 10,
    model: str = "claude-sonnet-4-6",
    loops: int = DEFAULT_LOOPS,
    _internal: bool = False,
):
    global _training_active, _training_stop

    if not _internal and _training_active:
        await send_status("⚠️ 已在运行中。/train_stop 停止。")
        return

    if domain_id == "all":
        _training_active = True
        _training_stop = False  # Reset stop flag at start of every new training run
        try:
            for sid in EVAL_SUITES:
                if _training_stop:
                    break
                await run_training(sid, send_status, send_photo, max_tasks, model, loops, _internal=True)
        finally:
            _training_active = False
        return

    if domain_id not in EVAL_SUITES:
        await send_status(f"❌ 未知: {domain_id}\n可选: {', '.join(EVAL_SUITES.keys())}, all")
        return

    if not _internal:
        _training_active = True
        _training_stop = False  # Reset stop flag at start of every new training run

    try:
        await _run_training_loop(domain_id, send_status, send_photo, model, loops)
    except Exception as e:
        logger.exception(f"Training crashed: {e}")
        await send_status(f"❌ 训练崩溃: {e}")
    finally:
        if not _internal:
            _training_active = False


async def _run_training_loop(domain_id, send_status, send_photo, model, loops):
    """N rounds of: eval all tests → Claude judges → Claude rewrites prompt → next round."""
    from claude_agent import _run_claude_cli_direct, _run_claude_raw, _forward_new_screenshots_direct

    suite = EVAL_SUITES[domain_id]
    tests = suite["tests"]
    progress = _load_progress()
    sp = progress.setdefault(domain_id, {"runs": 0, "last_avg": 0, "fixes_applied": 0, "history": []})

    await send_status(
        f"🧠 深度训练: {suite['name']}\n"
        f"📋 {len(tests)} 测试 × {loops} 轮\n"
        f"评判: Claude Haiku | 修复: Claude 重写 prompt"
    )

    total_fixes = 0
    round_scores = []

    for round_num in range(1, loops + 1):
        if _training_stop:
            await send_status("⏹ 已停止。")
            break

        await send_status(f"\n━━━ 第 {round_num}/{loops} 轮 ━━━")

        # ── Run all tests ──
        round_results = []
        for i, test in enumerate(tests):
            if _training_stop:
                break

            start = time.time()
            try:
                response, _ = await _run_claude_cli_direct(
                    prompt=_resolve_test_prompt(test["prompt"]),
                    model=model,
                    timeout=test.get("max_time_s", 30) + 30,
                )
            except Exception as e:
                response = f"Error: {e}"
            duration = time.time() - start

            await _forward_new_screenshots_direct(send_photo)

            # ── Claude Judge ──
            judgment = await _claude_judge(test, response, duration)

            round_results.append({
                "test_idx": i,
                "prompt": test["prompt"],
                "criteria": test["criteria"],
                "response": response or "",
                "duration": duration,
                "score": judgment["score"],
                "reasoning": judgment["reasoning"],
                "issues": judgment["issues"],
            })

            # Report
            icon = "✅" if judgment["score"] >= 7 else "⚠️" if judgment["score"] >= 4 else "❌"
            resp_preview = (response or "")[:60].replace("\n", " ")
            report = f"{icon} 测试 {i+1}/{len(tests)} — {judgment['score']}/10 | {duration:.1f}s"
            report += f"\n{resp_preview}"
            if judgment["issues"]:
                report += f"\n💬 {judgment['reasoning'][:100]}"
            await send_status(report)

            _log_training_entry(domain_id, round_num, test, response, judgment["score"], duration, judgment["issues"])
            await asyncio.sleep(0.5)

        if not round_results:
            break

        # ── Round score ──
        avg_score = sum(r["score"] for r in round_results) / len(round_results) if round_results else 0
        avg_normalized = avg_score / 10.0  # 0-1 for progress tracking
        round_scores.append(avg_normalized)

        trend = ""
        if len(round_scores) >= 2:
            delta = round_scores[-1] - round_scores[-2]
            if delta > 0.05:
                trend = f" 📈 +{delta:.0%}"
            elif delta < -0.05:
                trend = f" 📉 {delta:.0%}"
            else:
                trend = " ➡️ 持平"

        # ── Collect failures for prompt evolution ──
        failures = [r for r in round_results if r["score"] < 7]

        await send_status(
            f"📊 第{round_num}轮: {avg_score:.1f}/10{trend}\n"
            f"通过: {len(round_results) - len(failures)}/{len(round_results)} | "
            f"需改进: {len(failures)}"
        )

        # ── Early stop if perfect ──
        if avg_score >= 9.0:
            await send_status("🎉 全部高分通过! 提前结束。")
            break

        # ── Claude Prompt Engineer: rewrite prompt based on failures ──
        if failures and not _training_stop:
            await send_status(f"🔧 Claude 分析 {len(failures)} 个失败案例，改进 prompt...")
            fix_result = await _claude_fix_prompt(failures)
            if fix_result["applied"]:
                total_fixes += 1
                await send_status(
                    f"✏️ Prompt 已进化:\n{fix_result['summary'][:200]}"
                )
            else:
                await send_status(f"⚠️ 未能改进: {fix_result.get('reason', '未知')}")

        # ── Early stop if no improvement and no fixes ──
        if len(round_scores) >= 2 and round_scores[-1] <= round_scores[-2] and not failures:
            await send_status("⏸ 已达稳定，跳过剩余轮次。")
            break

        await asyncio.sleep(1)

    # ─── Final Summary ───
    if round_scores:
        sp["runs"] += len(round_scores)
        sp["last_avg"] = round_scores[-1]
        sp["fixes_applied"] += total_fixes
        sp["history"].append({
            "timestamp": datetime.now().isoformat(),
            "rounds": len(round_scores),
            "scores": [round(s, 2) for s in round_scores],
            "total_fixes": total_fixes,
        })
        sp["history"] = sp["history"][-20:]
        _save_progress(progress)

        if total_fixes:
            all_fixes = _load_fixes()
            all_fixes.append({
                "domain": domain_id,
                "rounds": len(round_scores),
                "score_trend": [round(s, 2) for s in round_scores],
                "timestamp": datetime.now().isoformat(),
            })
            _save_fixes(all_fixes)

        score_line = " → ".join(f"{s:.0%}" for s in round_scores)
        summary = (
            f"\n🏁 {suite['name']} 训练完成\n"
            f"分数趋势: {score_line}\n"
            f"Prompt 进化: {total_fixes} 次"
        )
        if len(round_scores) >= 2:
            delta = round_scores[-1] - round_scores[0]
            if delta > 0:
                summary += f"\n提升: +{delta:.0%} ✅"
            elif delta < 0:
                summary += f"\n下降: {delta:.0%} ⚠️"
        await send_status(summary)

        _update_memory(domain_id, round_scores, total_fixes)


def stop_training():
    global _training_stop
    _training_stop = True

def reset_progress(domain_id: str = None):
    progress = _load_progress()
    if domain_id:
        progress.pop(domain_id, None)
    else:
        progress = {}
    _save_progress(progress)


# ─── Claude Judge (Haiku) ────────────────────────────────────────────────────

async def _claude_judge(test: dict, response: str, duration: float) -> dict:
    """Use Claude Haiku to judge a response. Returns {score: 0-10, reasoning, issues}."""
    from claude_agent import _run_claude_raw

    judge_prompt = f"""你是AI评判官。评估以下bot回复是否完成了任务。

任务: {test['prompt']}
评判标准: {test['criteria']}
回复时间: {duration:.1f}秒 (限时{test.get('max_time_s', 30)}秒)

Bot的回复:
---
{(response or '无输出')[:1500]}
---

严格按标准打分。输出JSON（只输出JSON，不要其他文字）:
{{"score": 0到10的整数, "reasoning": "一句话解释", "issues": ["问题1", "问题2"]}}

评分参考: 10=完美 7-9=基本完成 4-6=部分完成 1-3=大部分失败 0=完全没做"""

    try:
        raw = await _run_claude_raw(
            prompt=judge_prompt,
            model=JUDGE_MODEL,
            timeout=15,
        )
        # Parse JSON from response
        return _parse_judge_response(raw)
    except Exception as e:
        logger.warning(f"Judge failed: {e}")
        return {"score": 5, "reasoning": f"评判失败: {e}", "issues": ["judge_error"]}


def _parse_judge_response(raw: str) -> dict:
    """Extract JSON from judge response, handling markdown code blocks."""
    if not raw:
        return {"score": 5, "reasoning": "无评判输出", "issues": ["no_judge_output"]}

    # Try to find JSON in the response
    text = raw.strip()

    # Remove markdown code block if present
    if "```" in text:
        parts = text.split("```")
        for part in parts:
            part = part.strip()
            if part.startswith("json"):
                part = part[4:].strip()
            if part.startswith("{"):
                text = part
                break

    # Find JSON object — try each '{' as a potential start
    for i in range(len(text)):
        if text[i] == '{':
            # Find matching closing brace by trying to parse
            try:
                data = json.loads(text[i:])
                return {
                    "score": max(0, min(10, int(data.get("score", 5)))),
                    "reasoning": str(data.get("reasoning", ""))[:200],
                    "issues": list(data.get("issues", [])),
                }
            except (json.JSONDecodeError, ValueError):
                # Try finding end brace for a substring
                end = text.rfind("}", i) + 1
                if end > i:
                    try:
                        data = json.loads(text[i:end])
                        return {
                            "score": max(0, min(10, int(data.get("score", 5)))),
                            "reasoning": str(data.get("reasoning", ""))[:200],
                            "issues": list(data.get("issues", [])),
                        }
                    except (json.JSONDecodeError, ValueError):
                        continue

    return {"score": 5, "reasoning": f"无法解析评判: {raw[:100]}", "issues": ["parse_error"]}


# ─── Claude Prompt Engineer ──────────────────────────────────────────────────

async def _claude_fix_prompt(failures: list[dict]) -> dict:
    """Use Claude to analyze failures and rewrite the weak parts of the system prompt."""
    from claude_agent import _run_claude_raw

    # Read current prompt
    try:
        current_prompt = Path(PROMPT_FILE).read_text(encoding="utf-8")
    except FileNotFoundError:
        return {"applied": False, "reason": "prompt文件不存在"}

    # Build failure report
    failure_report = ""
    for f in failures[:5]:  # Max 5 failures to keep context short
        failure_report += f"""
--- 失败案例 ---
任务: {f['prompt']}
标准: {f['criteria']}
Bot回复: {f['response'][:300]}
评判: {f['score']}/10 — {f['reasoning']}
问题: {', '.join(f['issues'])}
"""

    engineer_prompt = f"""你是prompt工程师。分析以下失败案例，改进system prompt。

当前system prompt:
```
{current_prompt}
```

本轮失败案例:
{failure_report}

你的任务：
1. 分析为什么bot在这些任务上失败了
2. 找到system prompt中需要改进的部分
3. 输出改进后的**完整**system prompt

规则：
- 保留所有现有内容的核心意图
- 可以重写、加强、补充规则
- 添加针对失败案例的具体指导（用WRONG/RIGHT例子）
- 不要删除项目路径、工具说明等信息
- 保持结构清晰
- prompt不要超过4000字符

只输出改进后的完整prompt，不要解释。用```包裹。"""

    try:
        raw = await _run_claude_raw(
            prompt=engineer_prompt,
            model="claude-sonnet-4-6",  # Use Sonnet for prompt engineering (needs to be smart)
            timeout=45,
        )

        new_prompt = _extract_prompt_from_response(raw)
        if not new_prompt:
            return {"applied": False, "reason": "无法从回复中提取prompt"}

        # Validate: new prompt should be reasonable length and contain key sections
        if len(new_prompt) < 200:
            return {"applied": False, "reason": "生成的prompt太短"}
        if len(new_prompt) > 6000:
            return {"applied": False, "reason": "生成的prompt太长"}

        original_headers = re.findall(r"^## .+", current_prompt, re.MULTILINE)
        matched = sum(1 for h in original_headers if h in new_prompt)
        if matched < min(3, len(original_headers)):
            return {"applied": False, "reason": f"丢失了太多章节标题（保留{matched}/{len(original_headers)}）"}

        # Save backup
        backup_path = PROMPT_FILE + ".bak"
        try:
            Path(backup_path).write_text(current_prompt, encoding="utf-8")
        except Exception:
            pass

        # Apply new prompt (atomic write to prevent corruption if CLI reads concurrently)
        import tempfile as _tmpmod
        _tmp = _tmpmod.NamedTemporaryFile(mode="w", suffix=".txt", dir=BOT_DIR,
                                           delete=False, encoding="utf-8")
        _tmp.write(new_prompt)
        _tmp.flush()
        os.fsync(_tmp.fileno())
        _tmp.close()
        try:
            os.replace(_tmp.name, PROMPT_FILE)
        except PermissionError:
            os.unlink(_tmp.name)
            return {"applied": False, "reason": "Prompt file locked by concurrent process"}

        # Generate summary of changes
        summary = _diff_summary(current_prompt, new_prompt)

        logger.info(f"Prompt evolved: {summary[:100]}")
        return {"applied": True, "summary": summary}

    except Exception as e:
        logger.warning(f"Prompt engineer failed: {e}")
        return {"applied": False, "reason": str(e)}


def _extract_prompt_from_response(raw: str) -> str | None:
    """Extract the prompt from Claude's response (inside ``` blocks)."""
    if not raw:
        return None

    # Find content between ``` blocks
    if "```" in raw:
        parts = raw.split("```")
        # The prompt should be the longest block
        candidates = []
        for i, part in enumerate(parts):
            if i % 2 == 1:  # Odd indices are inside code blocks
                text = part.strip()
                if text.startswith("markdown") or text.startswith("text"):
                    text = text.split("\n", 1)[-1] if "\n" in text else text
                candidates.append(text)
        if candidates:
            # Return the longest candidate that looks like a prompt
            candidates.sort(key=len, reverse=True)
            for c in candidates:
                if "ABSOLUTE RULES" in c or "BEHAVIOR" in c or "##" in c:
                    return c.strip()
            return candidates[0].strip() if candidates[0].strip() else None

    # If no code blocks, try to use the whole response if it looks like a prompt
    if "##" in raw and len(raw) > 200:
        return raw.strip()

    return None


def _diff_summary(old: str, new: str) -> str:
    """Generate a human-readable summary of what changed between two prompts."""
    old_lines = set(old.strip().splitlines())
    new_lines = set(new.strip().splitlines())

    added = new_lines - old_lines
    removed = old_lines - new_lines

    parts = []
    if added:
        # Show most interesting additions (non-empty, not just whitespace)
        interesting = [l.strip() for l in added if l.strip() and len(l.strip()) > 5][:5]
        if interesting:
            parts.append("新增:\n" + "\n".join(f"+ {l[:80]}" for l in interesting))
    if removed:
        interesting = [l.strip() for l in removed if l.strip() and len(l.strip()) > 5][:3]
        if interesting:
            parts.append("删除:\n" + "\n".join(f"- {l[:80]}" for l in interesting))

    if not parts:
        return "微调（变化不明显）"
    return "\n".join(parts)


# ─── Memory & Logging ────────────────────────────────────────────────────────

def _update_memory(domain_id, round_scores, total_fixes):
    try:
        from harness_learn import MEMORY_FILE
        date = datetime.now().strftime("%Y-%m-%d %H:%M")
        name = EVAL_SUITES[domain_id]["name"]
        score_trend = " → ".join(f"{s:.0%}" for s in round_scores)
        entry = f"\n## [{date}] 深度训练: {name} — {score_trend}\n"
        if total_fixes:
            entry += f"Prompt进化 {total_fixes} 次\n"
        with open(MEMORY_FILE, "a", encoding="utf-8") as f:
            f.write(entry)
    except Exception as e:
        logger.debug(f"Failed to update memory: {e}")


_MAX_TRAIN_LOG_SIZE = 2 * 1024 * 1024  # 2 MB max log size

def _log_training_entry(domain_id, round_num, test, response, score, duration, issues):
    try:
        entry = {
            "timestamp": datetime.now().isoformat(),
            "domain": domain_id,
            "round": round_num,
            "prompt": test["prompt"][:100],
            "response_preview": (response or "")[:200],
            "score": score,
            "duration_s": round(duration, 1),
            "issues": issues,
        }
        # Truncate log file if it exceeds max size to prevent unbounded growth
        try:
            if os.path.exists(TRAIN_LOG_FILE) and os.path.getsize(TRAIN_LOG_FILE) > _MAX_TRAIN_LOG_SIZE:
                with open(TRAIN_LOG_FILE, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                # Keep last half of lines — atomic tmp+fsync+replace
                import tempfile as _tf
                _dir = os.path.dirname(os.path.abspath(TRAIN_LOG_FILE))
                fd, tmp_path = _tf.mkstemp(dir=_dir, suffix=".tmp")
                try:
                    with os.fdopen(fd, "w", encoding="utf-8") as tmp_f:
                        tmp_f.writelines(lines[len(lines) // 2:])
                        tmp_f.flush()
                        os.fsync(tmp_f.fileno())
                    os.replace(tmp_path, TRAIN_LOG_FILE)
                except Exception:
                    try:
                        os.unlink(tmp_path)
                    except OSError:
                        pass
        except Exception:
            pass
        with open(TRAIN_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass
