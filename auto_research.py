"""
auto_research.py — Karpathy-style Autonomous Research Loop

4-layer self-evolution system:

Layer 1: Skill Library (skill_library.py) — remembers successful scripts
Layer 2: Auto-Experiment Loop — idle time self-improvement (modify → test → measure → keep/discard)
Layer 3: Knowledge Base — accumulates domain knowledge from tasks
Layer 4: Meta-Learning — learns HOW to learn better (optimizes its own learning parameters)

This module implements layers 2-4.

Architecture:
  Bot idle (no user messages for N minutes)
    → check what needs improvement (from scores, failures, skill gaps)
    → design an experiment (try a new approach)
    → run the experiment (Claude CLI)
    → measure results (Claude Judge)
    → keep or discard (git-like checkpoint)
    → repeat

The loop runs as a background asyncio task started from bot.py.
"""
import asyncio
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
KNOWLEDGE_DIR = os.path.join(BOT_DIR, ".knowledge")
EXPERIMENTS_LOG = os.path.join(BOT_DIR, ".experiments.jsonl")
META_FILE = os.path.join(BOT_DIR, ".meta_learning.json")

os.makedirs(KNOWLEDGE_DIR, exist_ok=True)

# Hold references to background tasks to prevent garbage collection
_background_tasks: set = set()

# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 2: Auto-Experiment Loop
# ═══════════════════════════════════════════════════════════════════════════════

_IDLE_THRESHOLD_DEFAULT = 600  # 10 min idle before experimenting
_MAX_EXPERIMENTS_PER_SESSION = 3  # Don't hog resources per idle window
_experiment_count = 0
_last_user_activity: float = time.time()
_last_experiment_reset: float = time.time()


def _get_idle_threshold() -> float:
    """Read optimal idle threshold from meta-learning (Layer 4 feedback)."""
    meta = _load_meta()
    return meta.get("optimal_idle_threshold", _IDLE_THRESHOLD_DEFAULT)


def mark_user_active():
    """Called on every user message to reset idle timer and experiment counter."""
    global _last_user_activity, _experiment_count, _last_experiment_reset
    _last_user_activity = time.time()
    # Reset experiment count when user returns, so next idle window gets fresh budget
    _experiment_count = 0
    _last_experiment_reset = time.time()


def _is_idle() -> bool:
    return time.time() - _last_user_activity > _get_idle_threshold()


async def run_experiment_loop(send_status=None):
    """Background loop: when idle, run self-improvement experiments.

    Karpathy pattern: modify → test → measure → keep/discard → repeat
    """
    global _experiment_count

    while True:
        try:
            # Wait until idle
            await asyncio.sleep(60)  # Check every minute
            if not _is_idle():
                continue
            if _experiment_count >= _MAX_EXPERIMENTS_PER_SESSION:
                continue

            # Respect meta-learning rate: if learning_rate < 1, skip some experiments
            meta = _load_meta()
            lr = meta.get("learning_rate", 1.0)
            if lr < 1.0:
                import random
                if random.random() > lr:
                    await asyncio.sleep(120)
                    continue

            # Find what needs improvement
            experiment = _pick_experiment()
            if not experiment:
                await asyncio.sleep(300)  # Nothing to do, check again in 5 min
                continue

            logger.info(f"Auto-experiment: {experiment['type']} — {experiment['goal']}")
            _experiment_count += 1

            # Run the experiment
            result = await _run_experiment(experiment)

            # Log result
            _log_experiment(experiment, result)

            # Apply if successful
            if result.get("success"):
                logger.info(f"Experiment succeeded: {experiment['goal']}")
                if send_status:
                    try:
                        await send_status(
                            f"🔬 自主实验成功\n"
                            f"目标: {experiment['goal'][:100]}\n"
                            f"结果: {result.get('summary', '')[:100]}"
                        )
                    except Exception:
                        pass
            else:
                logger.info(f"Experiment failed/discarded: {experiment['goal']}")

            # Update meta-learning
            _update_meta(experiment, result)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"Experiment loop error: {e}")
            await asyncio.sleep(300)


def _pick_experiment() -> dict | None:
    """Analyze recent scores/failures to decide what to experiment with."""
    import harness_learn

    scores = harness_learn.get_recent_scores(20)
    if len(scores) < 5:
        return None

    # Find the weakest dimension
    dim_scores = {}
    for s in scores:
        for dim, val in s.get("scores", {}).items():
            dim_scores.setdefault(dim, []).append(val)

    worst_dim = None
    worst_avg = 1.0
    for dim, vals in dim_scores.items():
        avg = sum(vals) / len(vals)
        if avg < worst_avg:
            worst_avg = avg
            worst_dim = dim

    if worst_dim is None or worst_avg > 0.7:
        # Everything is fine, try to learn something new instead
        return _pick_knowledge_experiment()

    # Map dimension to experiment type
    dim_to_experiment = {
        "completion": {
            "type": "prompt_tweak",
            "goal": f"提高任务完成率 (当前{worst_avg:.0%})",
            "target": "completion",
        },
        "obedience": {
            "type": "prompt_tweak",
            "goal": f"减少反问行为 (当前{worst_avg:.0%})",
            "target": "obedience",
        },
        "efficiency": {
            "type": "tool_improvement",
            "goal": f"提高响应速度 (当前{worst_avg:.0%})",
            "target": "efficiency",
        },
        "conciseness": {
            "type": "prompt_tweak",
            "goal": f"回复更简洁 (当前{worst_avg:.0%})",
            "target": "conciseness",
        },
        "model_fit": {
            "type": "routing_tweak",
            "goal": f"优化模型选择 (当前{worst_avg:.0%})",
            "target": "model_fit",
        },
    }

    return dim_to_experiment.get(worst_dim)


def _pick_knowledge_experiment() -> dict | None:
    """When scores are good, pick a knowledge-building experiment."""
    # Check what domains the user has used recently
    import harness_learn
    scores = harness_learn.get_recent_scores(20)
    task_types = [harness_learn._classify_task(s.get("user_message", "")) for s in scores]

    # Find most common task type
    from collections import Counter
    type_counts = Counter(task_types)
    if not type_counts:
        return None

    most_common = type_counts.most_common(1)
    if not most_common:
        return None
    top_type = most_common[0][0]
    if top_type == "general":
        return None

    # Check if we already have knowledge for this
    kb_file = os.path.join(KNOWLEDGE_DIR, f"{top_type}.md")
    if os.path.exists(kb_file):
        size = os.path.getsize(kb_file)
        if size > 5000:  # Already well-documented
            return None

    return {
        "type": "knowledge_build",
        "goal": f"积累 {top_type} 领域知识",
        "target": top_type,
    }


async def _run_experiment(experiment: dict) -> dict:
    """Execute an experiment and return results."""
    exp_type = experiment.get("type", "")

    if exp_type == "prompt_tweak":
        return await _experiment_prompt_tweak(experiment)
    elif exp_type == "knowledge_build":
        return await _experiment_knowledge_build(experiment)
    elif exp_type == "tool_improvement":
        return {"success": False, "summary": "tool improvement not yet implemented"}
    elif exp_type == "routing_tweak":
        return await _experiment_routing_tweak(experiment)
    else:
        return {"success": False, "summary": f"unknown experiment type: {exp_type}"}


async def _experiment_prompt_tweak(experiment: dict) -> dict:
    """Hypothesis-driven prompt modification.

    NOT random tweaking. The flow is:
    1. Analyze failure cases → find common pattern
    2. Generate hypothesis ("if I add X, failure rate drops")
    3. Make targeted prompt change
    4. Run tests to verify
    5. Keep or discard based on evidence
    """
    from claude_agent import _run_claude_raw
    import auto_train

    target = experiment.get("target", "obedience")
    hypothesis = experiment.get("hypothesis")  # May already have one from _pick_experiment

    # Map target to training domain
    target_to_domain = {
        "completion": "file_ops",
        "obedience": "obedience",
        "conciseness": "obedience",
        "efficiency": "computer_control",
    }
    domain = target_to_domain.get(target, "obedience")

    # Backup current prompt
    prompt_path = os.path.join(BOT_DIR, ".system_prompt.txt")
    try:
        original = Path(prompt_path).read_text(encoding="utf-8")
    except Exception:
        return {"success": False, "summary": "could not read prompt"}

    # ── Step 1: Generate hypothesis if we don't have one ──
    if not hypothesis:
        hypothesis = await _generate_hypothesis(target)
        if not hypothesis:
            return {"success": False, "summary": "Could not generate hypothesis"}

    # Check if this hypothesis was already tried and failed
    if _hypothesis_already_failed(hypothesis.get("id", ""), hypothesis.get("prompt_diff", "")):
        return {"success": False, "summary": f"Hypothesis already rejected: {hypothesis.get('id', '')}"}

    logger.info(f"Testing hypothesis: {hypothesis.get('prediction', '')}")

    # ── Step 2: Apply hypothesis to prompt ──
    try:
        prompt_diff = hypothesis.get("prompt_diff", "")
        if not prompt_diff:
            return {"success": False, "summary": "No prompt change in hypothesis"}

        # Apply the targeted change (atomic write: tmp + rename)
        modified = original + f"\n\n## 自动优化 (H-{hypothesis.get('id', '?')})\n{prompt_diff}\n"
        _tmp_fd = tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", dir=BOT_DIR, delete=False, encoding="utf-8",
        )
        _tmp_fd.write(modified)
        _tmp_fd.flush()
        os.fsync(_tmp_fd.fileno())
        _tmp_fd.close()
        try:
            os.replace(_tmp_fd.name, prompt_path)
        except PermissionError:
            # On Windows, file may be locked by a concurrent Claude CLI process
            os.unlink(_tmp_fd.name)
            return {"success": False, "summary": "Prompt file locked by concurrent process"}

        # ── Step 3: Run tests ──
        results = {"scores": []}

        async def _status(text):
            pass  # Silent

        # Check if training is already running (user-triggered)
        if auto_train._training_active:
            # Revert and skip -- don't interfere with user training
            _rv = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", dir=BOT_DIR, delete=False, encoding="utf-8",
            )
            _rv.write(original)
            _rv.flush()
            os.fsync(_rv.fileno())
            _rv.close()
            os.replace(_rv.name, prompt_path)
            return {"success": False, "summary": "Training already active, skipping experiment"}

        await auto_train.run_training(
            domain_id=domain, send_status=_status, loops=1,
        )

        # ── Step 4: Compare with baseline ──
        # NOTE: auto_train.run_training may have further evolved the prompt
        # via its own _claude_fix_prompt. Read what's on disk NOW as the
        # post-experiment state (it includes both our hypothesis AND any
        # training fixes).
        post_experiment_prompt = Path(prompt_path).read_text(encoding="utf-8")
        baseline = hypothesis.get("baseline_score", 0.6)

        # Read latest training score
        import auto_train as at
        progress = at._load_progress()
        domain_progress = progress.get(domain, {})
        new_score = domain_progress.get("last_avg", 0)

        improved = new_score > baseline + 0.05

        # ── Step 5: Keep or discard ──
        hypothesis["experiment_score"] = new_score
        hypothesis["status"] = "confirmed" if improved else "rejected"
        hypothesis["evidence"] = f"baseline={baseline:.2f} experiment={new_score:.2f}"
        _save_hypothesis(hypothesis)

        if not improved:
            # Revert ONLY our hypothesis addition, not training's own fixes.
            # Our hypothesis appended a "## 自动优化 (H-...)" section.
            # Strip it from the current prompt rather than blindly reverting
            # to `original` (which would also revert training improvements).
            h_id = hypothesis.get("id", "?")
            marker = f"## 自动优化 (H-{h_id})"
            if marker in post_experiment_prompt:
                reverted = post_experiment_prompt[:post_experiment_prompt.index(marker)].rstrip() + "\n"
            else:
                # Marker not found (training may have rewritten entire prompt);
                # fall back to original to avoid keeping unverified changes
                reverted = original
            _rv_tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", dir=BOT_DIR, delete=False, encoding="utf-8",
            )
            _rv_tmp.write(reverted)
            _rv_tmp.flush()
            os.fsync(_rv_tmp.fileno())
            _rv_tmp.close()
            os.replace(_rv_tmp.name, prompt_path)
            logger.info(f"Hypothesis rejected: {hypothesis['prediction']}")
            return {"success": False, "summary": f"Hypothesis rejected: {new_score:.0%} vs baseline {baseline:.0%}"}

        logger.info(f"Hypothesis confirmed: {hypothesis['prediction']}")
        return {"success": True, "summary": f"Hypothesis confirmed! {baseline:.0%} → {new_score:.0%}: {hypothesis['prediction']}"}

    except (Exception, asyncio.CancelledError) as e:
        # Restore original on failure or cancellation (atomic write)
        try:
            _err_tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".txt", dir=BOT_DIR, delete=False, encoding="utf-8",
            )
            _err_tmp.write(original)
            _err_tmp.flush()
            os.fsync(_err_tmp.fileno())
            _err_tmp.close()
            os.replace(_err_tmp.name, prompt_path)
        except Exception:
            pass
        if isinstance(e, asyncio.CancelledError):
            raise  # Re-raise cancellation after cleanup
        return {"success": False, "summary": f"Experiment failed: {e}"}


async def _experiment_routing_tweak(experiment: dict) -> dict:
    """Analyze model routing and suggest improvements."""
    import harness_learn

    scores = harness_learn.get_recent_scores(20)
    opus_simple = sum(1 for s in scores if "OPUS_FOR_SIMPLE" in s.get("flags", []))
    haiku_complex = sum(1 for s in scores if "HAIKU_FOR_COMPLEX" in s.get("flags", []))

    insights = []
    if opus_simple >= 3:
        insights.append(f"Opus used for {opus_simple} simple tasks — routing threshold may be too low")
    if haiku_complex >= 2:
        insights.append(f"Haiku used for {haiku_complex} complex tasks — routing may be too aggressive")

    if insights:
        # Write insight to memory
        harness_learn.update_memory_with_insights(insights)
        return {"success": True, "summary": "; ".join(insights)}

    return {"success": False, "summary": "Routing looks fine"}


# ─── Hypothesis Engine (Layer 4 core) ────────────────────────────────────────

HYPOTHESES_FILE = os.path.join(BOT_DIR, ".hypotheses.jsonl")
FAILED_HYPOTHESES_FILE = os.path.join(BOT_DIR, ".failed_hypotheses.json")

# Initialize counter from existing hypotheses to avoid ID collisions after restart.
# Use max ID number found in file rather than line count, because the file is
# periodically truncated (halved when >512KB), so line count would produce
# duplicate IDs that collide with earlier hypotheses.
def _init_hypothesis_counter() -> int:
    max_id = 0
    try:
        if os.path.exists(HYPOTHESES_FILE):
            with open(HYPOTHESES_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        h = json.loads(line)
                        h_id = h.get("id", "")
                        # Parse "H-0042" -> 42
                        if h_id.startswith("H-"):
                            num = int(h_id[2:])
                            max_id = max(max_id, num)
                    except (json.JSONDecodeError, ValueError):
                        pass
    except Exception:
        pass
    return max_id

_hypothesis_counter = _init_hypothesis_counter()


async def _generate_hypothesis(target: str) -> dict | None:
    """Analyze failure cases, find patterns, generate a testable hypothesis.

    This is NOT random tweaking. It:
    1. Reads recent failures
    2. Clusters by failure reason
    3. Generates a prediction + targeted prompt change
    """
    global _hypothesis_counter
    from claude_agent import _run_claude_raw
    import harness_learn

    scores = harness_learn.get_recent_scores(20)
    if not scores:
        return None

    # Collect failure cases
    failures = []
    for s in scores:
        if s.get("overall", 1.0) < 0.7:
            failures.append({
                "task": s.get("user_message", "")[:100],
                "flags": s.get("flags", []),
                "scores": s.get("scores", {}),
                "response": s.get("response_preview", "")[:100],
            })

    if not failures:
        return None

    # Calculate baseline
    avg_overall = sum(s.get("overall", 0) for s in scores) / len(scores)

    # Ask Claude to find pattern and generate hypothesis
    prompt = f"""你是一个科学实验设计师。分析以下失败案例，生成一个可验证的假设。

目标维度: {target}
失败案例 ({len(failures)}个):
{json.dumps(failures[:5], ensure_ascii=False, indent=1)[:1500]}

当前基线分数: {avg_overall:.2f}

你要做:
1. 找出失败的共性模式（不是逐个分析，是找共性）
2. 形成一个可验证的假设
3. 设计一个针对性的prompt修改

输出JSON:
{{"observation": "X个失败中有Y个是因为...",
  "prediction": "如果在prompt中加入'...'，预计...率从X%降到Y%",
  "prompt_diff": "要添加到system prompt的具体文字(1-3句话)",
  "expected_improvement": 0.1}}

只输出JSON。"""

    try:
        raw = await _run_claude_raw(prompt=prompt, model="claude-haiku-4-5-20251001", timeout=15)
        from skill_library import _parse_json_from_response
        data = _parse_json_from_response(raw)
        if not data or not data.get("prompt_diff"):
            return None

        _hypothesis_counter += 1
        hypothesis = {
            "id": f"H-{_hypothesis_counter:04d}",
            "observation": data.get("observation", "")[:200],
            "prediction": data.get("prediction", "")[:200],
            "prompt_diff": data.get("prompt_diff", "")[:300],
            "baseline_score": avg_overall,
            "experiment_score": None,
            "expected_improvement": data.get("expected_improvement", 0.1),
            "status": "pending",
            "evidence": "",
            "created_at": datetime.now().isoformat(),
        }
        return hypothesis

    except Exception as e:
        logger.debug(f"Hypothesis generation failed: {e}")
        return None


def _save_hypothesis(hypothesis: dict):
    """Save hypothesis result (confirmed or rejected)."""
    try:
        # Truncate if too large (prevent unbounded growth)
        try:
            if os.path.exists(HYPOTHESES_FILE) and os.path.getsize(HYPOTHESES_FILE) > 512 * 1024:
                with open(HYPOTHESES_FILE, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                with open(HYPOTHESES_FILE, "w", encoding="utf-8") as f:
                    f.writelines(lines[len(lines) // 2:])
        except Exception:
            pass
        with open(HYPOTHESES_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(hypothesis, ensure_ascii=False) + "\n")

        # If rejected, add to failed list to avoid retrying
        if hypothesis.get("status") == "rejected":
            failed = _load_failed_hypotheses()
            failed.append({
                "id": hypothesis["id"],
                "prediction": hypothesis.get("prediction", ""),
                "prompt_diff_hash": hashlib.sha256(hypothesis.get("prompt_diff", "").encode("utf-8")).hexdigest(),
                "rejected_at": datetime.now().isoformat(),
            })
            failed = failed[-50:]  # Keep last 50
            _tmp = FAILED_HYPOTHESES_FILE + ".tmp"
            with open(_tmp, "w", encoding="utf-8") as f:
                json.dump(failed, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(_tmp, FAILED_HYPOTHESES_FILE)
    except Exception:
        pass


def _load_failed_hypotheses() -> list:
    try:
        if os.path.exists(FAILED_HYPOTHESES_FILE):
            return json.loads(Path(FAILED_HYPOTHESES_FILE).read_text(encoding="utf-8"))
    except Exception:
        pass
    return []


def _hypothesis_already_failed(h_id: str, prompt_diff: str = "") -> bool:
    """Check if a similar hypothesis was already tried and rejected.

    Checks both by ID and by prompt_diff content hash, since IDs reset
    when the bot restarts (_hypothesis_counter is module-level).
    """
    failed = _load_failed_hypotheses()
    if any(f.get("id") == h_id for f in failed):
        return True
    if prompt_diff:
        diff_hash = hashlib.sha256(prompt_diff.encode("utf-8")).hexdigest()
        if any(f.get("prompt_diff_hash") == diff_hash for f in failed):
            return True
    return False


def get_hypothesis_stats() -> str:
    """Human-readable hypothesis experiment stats."""
    try:
        if not os.path.exists(HYPOTHESES_FILE):
            return ""
        with open(HYPOTHESES_FILE, "r", encoding="utf-8") as f:
            hypotheses = []
            for l in f.readlines()[-20:]:
                if l.strip():
                    try:
                        hypotheses.append(json.loads(l))
                    except json.JSONDecodeError:
                        pass
        if not hypotheses:
            return ""
        confirmed = sum(1 for h in hypotheses if h.get("status") == "confirmed")
        rejected = sum(1 for h in hypotheses if h.get("status") == "rejected")
        lines = [f"🔬 假设实验: {confirmed} 证实 / {rejected} 否决"]
        # Show last confirmed hypothesis
        last_confirmed = [h for h in hypotheses if h.get("status") == "confirmed"]
        if last_confirmed:
            h = last_confirmed[-1]
            lines.append(f"最近证实: {h.get('prediction', '')[:60]}")
        return "\n".join(lines)
    except Exception:
        return ""


# ─── Knowledge Gap Detection (Layer 5 core) ──────────────────────────────────

async def detect_and_fill_knowledge_gap(user_message: str) -> str | None:
    """Layer 5: Before executing a task, detect if we're missing knowledge.

    Returns: relevant knowledge string to inject, or None.
    Triggers learning subprocess if knowledge is missing.
    """
    from claude_agent import _run_claude_raw

    # First check existing knowledge
    existing = get_relevant_knowledge(user_message, max_chars=800)
    if existing:
        return existing

    # Ask: what knowledge do we need for this task?
    dep_prompt = f"""分析这个任务需要哪些知识/API/库:
任务: {user_message[:300]}

输出JSON:
{{"needs_knowledge": true/false,
  "domains": ["需要的知识领域"],
  "apis": ["需要的API名称"],
  "libraries": ["需要的库"],
  "doc_urls": ["可能的文档URL(如果知道)"]}}

如果是简单对话/截图等不需要专业知识的任务，输出 {{"needs_knowledge": false}}
只输出JSON。"""

    try:
        raw = await _run_claude_raw(prompt=dep_prompt, model="claude-haiku-4-5-20251001", timeout=10)
        from skill_library import _parse_json_from_response
        data = _parse_json_from_response(raw)

        if not data or not data.get("needs_knowledge"):
            return None

        # Check if we have knowledge for any of the domains
        domains = data.get("domains", []) + data.get("apis", [])
        for domain in domains:
            safe_name = re.sub(r'[^\w]', '_', domain.lower())[:30]
            kb_file = os.path.join(KNOWLEDGE_DIR, f"{safe_name}.md")
            if os.path.exists(kb_file):
                try:
                    content = Path(kb_file).read_text(encoding="utf-8")
                    return content[-800:] if len(content) > 800 else content
                except Exception:
                    pass

        # Knowledge gap detected! Trigger background learning
        # Don't block the user — learn async and have it ready for next time
        task = asyncio.create_task(_learn_domain(domains, data.get("doc_urls", [])))
        _background_tasks.add(task)
        def _on_learn_done(t, _discard=_background_tasks.discard):
            _discard(t)
            try:
                if not t.cancelled():
                    t.result()
            except Exception as e:
                logger.error(f"Background domain learning failed: {e}")
        task.add_done_callback(_on_learn_done)

        return None  # No knowledge available yet, will be ready next time

    except Exception:
        return None


async def _learn_domain(domains: list, doc_urls: list):
    """Background: learn about a domain by synthesizing knowledge.
    Stores compressed operational knowledge, not raw docs."""
    from claude_agent import _run_claude_raw

    for domain in domains[:2]:  # Max 2 domains per learning session
        safe_name = re.sub(r'[^\w]', '_', domain.lower())[:30]
        kb_file = os.path.join(KNOWLEDGE_DIR, f"{safe_name}.md")

        if os.path.exists(kb_file):
            continue  # Already have it

        prompt = f"""你是一个知识压缩系统。为以下领域生成一份"操作手册"——只保留agent执行任务时需要的信息。

领域: {domain}
{'参考URL: ' + ', '.join(doc_urls[:3]) if doc_urls else ''}

输出格式(Markdown):
# {domain} 操作手册

## 认证/配置
- 认证方式、必要配置项

## 核心API/接口
- endpoint / 函数签名 / 必填参数
- 返回格式

## 常见操作模式
- 最常用的3-5种操作，每种给代码片段

## 常见错误和处理
- 典型报错和解决方案

## 注意事项
- 限速、权限、数据格式等

简洁实用，不超过800字。只输出Markdown。"""

        try:
            raw = await _run_claude_raw(prompt=prompt, model="claude-haiku-4-5-20251001", timeout=20)
            if raw and len(raw) > 100:
                Path(kb_file).write_text(raw, encoding="utf-8")
                logger.info(f"Knowledge learned: {domain} ({len(raw)} chars) → {kb_file}")
        except Exception as e:
            logger.debug(f"Failed to learn {domain}: {e}")


_MAX_EXPERIMENTS_LOG_SIZE = 1 * 1024 * 1024  # 1 MB max

def _log_experiment(experiment: dict, result: dict):
    try:
        entry = {
            "timestamp": datetime.now().isoformat(),
            "type": experiment.get("type", ""),
            "goal": experiment.get("goal", ""),
            "target": experiment.get("target", ""),
            "success": result.get("success", False),
            "summary": result.get("summary", "")[:200],
        }
        # Truncate if too large
        try:
            if os.path.exists(EXPERIMENTS_LOG) and os.path.getsize(EXPERIMENTS_LOG) > _MAX_EXPERIMENTS_LOG_SIZE:
                with open(EXPERIMENTS_LOG, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                with open(EXPERIMENTS_LOG, "w", encoding="utf-8") as f:
                    f.writelines(lines[len(lines) // 2:])
        except Exception:
            pass
        with open(EXPERIMENTS_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 3: Knowledge Base
# ═══════════════════════════════════════════════════════════════════════════════

async def _experiment_knowledge_build(experiment: dict) -> dict:
    """Build knowledge about a domain by researching and documenting."""
    from claude_agent import _run_claude_raw

    domain = experiment.get("target", "general")
    kb_file = os.path.join(KNOWLEDGE_DIR, f"{domain}.md")

    # Read existing knowledge if any
    existing = ""
    if os.path.exists(kb_file):
        try:
            existing = Path(kb_file).read_text(encoding="utf-8")
        except Exception:
            pass

    # Ask Claude to build/expand knowledge
    prompt = f"""你是一个知识积累系统。根据以下领域，总结关键知识、常用模式、最佳实践。

领域: {domain}
{'已有知识:\n' + existing[:1000] if existing else '(全新领域，从零开始)'}

输出格式（Markdown）:
# {domain} 领域知识

## 常用工具/API
- ...

## 常见任务模式
- ...

## 注意事项
- ...

## 代码模板
```
...
```

简洁实用，不超过1000字。只输出Markdown。"""

    try:
        raw = await _run_claude_raw(
            prompt=prompt,
            model="claude-haiku-4-5-20251001",
            timeout=20,
        )

        if not raw or len(raw) < 50:
            return {"success": False, "summary": "Knowledge generation too short"}

        # Save knowledge
        Path(kb_file).write_text(raw, encoding="utf-8")
        logger.info(f"Knowledge built: {domain} ({len(raw)} chars)")

        return {"success": True, "summary": f"Built {domain} knowledge ({len(raw)} chars)"}

    except Exception as e:
        return {"success": False, "summary": f"Knowledge build failed: {e}"}


def get_relevant_knowledge(user_message: str, max_chars: int = 800) -> str:
    """Find and return relevant knowledge for a user's task."""
    from harness_learn import _classify_task
    task_type = _classify_task(user_message)

    # Direct match
    kb_file = os.path.join(KNOWLEDGE_DIR, f"{task_type}.md")
    if os.path.exists(kb_file):
        try:
            content = Path(kb_file).read_text(encoding="utf-8")
            return content[-max_chars:] if len(content) > max_chars else content
        except Exception:
            pass

    # Also check keyword matches against knowledge filenames
    msg_lower = user_message.lower()
    try:
        kb_files = os.listdir(KNOWLEDGE_DIR)
    except OSError:
        return ""
    for fname in kb_files:
        if not fname.endswith(".md"):
            continue
        domain = fname[:-3]
        if domain in msg_lower:
            try:
                content = Path(os.path.join(KNOWLEDGE_DIR, fname)).read_text(encoding="utf-8")
                return content[-max_chars:] if len(content) > max_chars else content
            except Exception:
                pass

    return ""


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 4: Meta-Learning — Learning HOW to Learn
# ═══════════════════════════════════════════════════════════════════════════════

def _load_meta() -> dict:
    try:
        if os.path.exists(META_FILE):
            return json.loads(Path(META_FILE).read_text(encoding="utf-8"))
    except Exception:
        pass
    return {
        "total_experiments": 0,
        "successful_experiments": 0,
        "experiment_types_success": {},  # type -> {attempts, successes}
        "best_training_domains": [],     # domains that improved scores most
        "optimal_idle_threshold": _IDLE_THRESHOLD_DEFAULT,
        "learning_rate": 1.0,            # multiplier for experiment frequency
        "last_meta_update": None,
    }


def _save_meta(meta: dict):
    try:
        _tmp = META_FILE + ".tmp"
        with open(_tmp, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(_tmp, META_FILE)
    except Exception:
        pass


def _update_meta(experiment: dict, result: dict):
    """Update meta-learning parameters based on experiment outcome."""
    meta = _load_meta()

    meta["total_experiments"] = meta.get("total_experiments", 0) + 1
    if result.get("success"):
        meta["successful_experiments"] = meta.get("successful_experiments", 0) + 1

    # Track success rate per experiment type
    exp_type = experiment.get("type", "unknown")
    type_stats = meta.get("experiment_types_success", {})
    if exp_type not in type_stats:
        type_stats[exp_type] = {"attempts": 0, "successes": 0}
    type_stats[exp_type]["attempts"] += 1
    if result.get("success"):
        type_stats[exp_type]["successes"] += 1
    meta["experiment_types_success"] = type_stats

    # Adjust learning rate: if experiments keep failing, slow down
    total = meta.get("total_experiments", 1)
    success = meta.get("successful_experiments", 0)
    success_rate = success / total if total > 0 else 0

    if total >= 5:
        if success_rate < 0.2:
            meta["learning_rate"] = max(0.3, meta.get("learning_rate", 1.0) * 0.8)
            logger.info(f"Meta: lowering learning rate to {meta['learning_rate']:.2f} (low success rate)")
        elif success_rate > 0.6:
            meta["learning_rate"] = min(2.0, meta.get("learning_rate", 1.0) * 1.1)
            logger.info(f"Meta: raising learning rate to {meta['learning_rate']:.2f} (high success rate)")

    # Adjust idle threshold based on learning rate
    base_threshold = 600  # 10 min
    meta["optimal_idle_threshold"] = int(base_threshold / meta.get("learning_rate", 1.0))

    meta["last_meta_update"] = datetime.now().isoformat()
    _save_meta(meta)


def get_meta_stats() -> str:
    """Human-readable meta-learning stats."""
    meta = _load_meta()
    total = meta.get("total_experiments", 0)
    success = meta.get("successful_experiments", 0)

    if total == 0:
        return "🧪 自主实验: 尚未开始 (空闲10分钟后自动启动)"

    rate = success / total if total > 0 else 0
    lr = meta.get("learning_rate", 1.0)

    lines = [
        f"🧪 自主实验: {success}/{total} 成功 ({rate:.0%})",
        f"学习速率: {lr:.1f}x",
    ]

    # Show per-type stats
    type_stats = meta.get("experiment_types_success", {})
    for t, stats in type_stats.items():
        s = stats.get("successes", 0)
        a = stats.get("attempts", 0)
        lines.append(f"  {t}: {s}/{a}")

    # Knowledge files
    try:
        kb_count = len([f for f in os.listdir(KNOWLEDGE_DIR) if f.endswith(".md")])
    except OSError:
        kb_count = 0
    if kb_count > 0:
        lines.append(f"📚 知识库: {kb_count} 个领域")

    return "\n".join(lines)
