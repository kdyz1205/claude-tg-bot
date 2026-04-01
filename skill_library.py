"""
skill_library.py — Reusable Skill Memory

When the bot successfully creates a script/solution, it saves it as a "skill".
On future similar tasks, matched skills are injected as context → bot doesn't start from zero.
Skills evolve: each reuse can improve them.

Flow:
  User msg → find_matching_skills() → inject into prompt
  Response → score → maybe_extract_skill() → save new skill
  Reuse + high score → update_skill_from_reuse() → skill evolves

Executable Python skills (``skills/sk_*.py``) use ``skills.base_skill.BaseSkill`` and
``skills.skill_runtime`` for timeouts, hot-reload, and unload. See ``invoke_python_skill_async``.
"""
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
SKILL_DIR = os.path.join(BOT_DIR, ".skill_library")
SKILLS_DIR = os.path.join(SKILL_DIR, "skills")
INDEX_FILE = os.path.join(SKILL_DIR, "index.json")
MD_SKILLS_DIR = os.path.join(BOT_DIR, "skills")  # Markdown skills (superpowers format)

# Ensure directories exist
os.makedirs(SKILLS_DIR, exist_ok=True)

# Max skills to keep / inject
MAX_SKILLS = 100
MAX_INJECT = 2
MAX_SKILL_CHARS = 400  # Per skill in prompt injection


# ─── Storage Layer ────────────────────────────────────────────────────────────

def _load_index() -> dict:
    try:
        if os.path.exists(INDEX_FILE):
            return json.loads(Path(INDEX_FILE).read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"Skill index corrupted, rebuilding: {e}")
        # Try to backup corrupted file
        try:
            if os.path.exists(INDEX_FILE):
                import shutil
                shutil.copy2(INDEX_FILE, INDEX_FILE + ".bak")
        except Exception:
            pass
    return {"entries": [], "last_rebuilt": None}


def _save_index(index: dict):
    try:
        index["last_rebuilt"] = datetime.now().isoformat()
        tmp = INDEX_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, INDEX_FILE)  # Atomic on Windows (unlike shutil.move)
    except Exception as e:
        logger.warning(f"Failed to save skill index: {e}")
        try:
            os.unlink(INDEX_FILE + ".tmp")
        except Exception:
            pass


def _load_skill(skill_id: str) -> dict | None:
    path = os.path.join(SKILLS_DIR, f"{skill_id}.json")
    try:
        if os.path.exists(path):
            return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        pass
    return None


def _save_skill(skill: dict):
    path = os.path.join(SKILLS_DIR, f"{skill['id']}.json")
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(skill, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)  # Atomic on Windows (unlike shutil.move)
    except Exception as e:
        logger.warning(f"Failed to save skill {skill['id']}: {e}")
        try:
            os.unlink(tmp)
        except Exception:
            pass


def _delete_skill(skill_id: str):
    path = os.path.join(SKILLS_DIR, f"{skill_id}.json")
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _rebuild_index():
    """Scan skills/ directory and rebuild index.json."""
    entries = []
    for fname in os.listdir(SKILLS_DIR):
        if not fname.endswith(".json"):
            continue
        skill = _load_skill(fname[:-5])
        if not skill:
            continue
        entries.append({
            "id": skill["id"],
            "title": skill.get("title", ""),
            "keywords": skill.get("keywords", []),
            "task_type": skill.get("task_type", "general"),
            "use_count": skill.get("use_count", 0),
            "avg_score": skill.get("avg_score_when_used"),
        })
    index = {"entries": entries, "last_rebuilt": datetime.now().isoformat()}
    _save_index(index)
    return index


# ─── Skill Matching ──────────────────────────────────────────────────────────

def find_matching_skills(user_message: str, max_results: int = MAX_INJECT) -> list[dict]:
    """Find skills relevant to the user's message. Fast keyword matching, no LLM call.
    Returns list of full skill dicts (max max_results)."""
    index = _load_index()
    if not index.get("entries"):
        return []

    msg_lower = user_message.lower()
    msg_words = set(re.findall(r'[\w\u4e00-\u9fff]+', msg_lower))

    scored = []
    for entry in index["entries"]:
        # Keyword overlap (primary signal)
        skill_keywords = set(w.lower() for w in entry.get("keywords", []))
        overlap = len(msg_words & skill_keywords)

        # Trigger pattern match (Layer 3: crystallized matching)
        trigger = entry.get("trigger_pattern", "").lower()
        if trigger:
            trigger_words = set(re.findall(r'[\w\u4e00-\u9fff]+', trigger))
            trigger_overlap = len(msg_words & trigger_words)
            overlap = max(overlap, trigger_overlap * 1.5)  # Trigger pattern is stronger signal

        # Task type match (bonus)
        try:
            from harness_learn import _classify_task
            task_type = _classify_task(user_message)
        except ImportError:
            task_type = "general"
        type_bonus = 0.5 if task_type == entry.get("task_type") else 0

        # Usage success (bonus)
        usage_bonus = 0.3 if (entry.get("avg_score") or 0) > 0.8 else 0

        total = overlap + type_bonus + usage_bonus
        if total >= 1.0:
            scored.append((total, entry))

    scored.sort(key=lambda x: -x[0])

    results = []
    for _, entry in scored[:max_results]:
        skill = _load_skill(entry["id"])
        if skill:
            results.append(skill)

    return results


def format_skills_for_prompt(skills: list[dict]) -> str:
    """Format matched skills for injection into the system prompt.
    Produces callable function signatures, not just summaries."""
    if not skills:
        return ""

    parts = ["\n## 技能库 (已结晶的可复用技能，直接调用/改参数)\n"]
    for sk in skills:
        text = f"### {sk.get('title', '未命名')}\n"

        # Function signature (Layer 3 crystallization)
        sig = sk.get("function_signature")
        if sig:
            text += f"调用: {sig}\n"

        # Input/output schema
        input_schema = sk.get("input_schema")
        output_schema = sk.get("output_schema")
        if input_schema:
            text += f"输入: {input_schema}\n"
        if output_schema:
            text += f"输出: {output_schema}\n"

        # Template code (the reusable core, not the full script)
        template = sk.get("template_code") or sk.get("code_snippet", "")
        if template:
            text += f"模板:\n```\n{template[:400]}\n```\n"

        # What's generic vs task-specific
        generic = sk.get("generic_steps", [])
        if generic:
            text += f"通用步骤: {'; '.join(generic[:4])}\n"

        # Bound per-skill
        parts.append(text[:MAX_SKILL_CHARS + 200])  # Allow more for structured skills

    return "\n".join(parts)


# ─── Skill Extraction ────────────────────────────────────────────────────────

def _should_extract_skill(user_message: str, response: str, score: dict) -> bool:
    """
    4-Layer skill lifecycle evaluation.
    Uses skill_lifecycle.py for structured Observe → Evaluate → Decide.
    Falls back to heuristics if lifecycle module unavailable.
    """
    # Basic quality gate
    if score.get("overall", 0) < 0.35:
        return False

    # Don't extract if we already have too many skills
    index = _load_index()
    if len(index.get("entries", [])) >= MAX_SKILLS:
        return False

    resp_lower = (response or "").lower()

    # Must show completion signals
    completion_signals = ["✅", "done", "saved", "commit", "created",
                          "wrote", "完成", "成功", "fixed", "修复",
                          "finished", "completed", "generated", "updated",
                          "已保存", "已完成", "已创建", "已更新"]
    has_completion = any(w in resp_lower for w in completion_signals)
    if not has_completion:
        return False

    # Code-like content OR substantial text
    code_signals = ["```", "saved to", "wrote to", "created file", ".py", ".js",
                    ".sh", ".bat", "import ", "def ", "class ", "function ",
                    "npm ", "pip ", "写入", "保存到"]
    has_code = any(s in resp_lower for s in code_signals)
    if not has_code and not (score.get("overall", 0) >= 0.7 and len(response or "") > 300):
        return False

    # Layer 3: Lifecycle evaluation (6-criteria scoring)
    try:
        from skill_lifecycle import classify_task, evaluate_skill_worthiness
        task = classify_task(user_message)
        evaluation = evaluate_skill_worthiness(
            task["type"], user_message, response, score,
        )
        if evaluation["promote"]:
            logger.info("SkillExtract: PROMOTED by lifecycle eval (%.2f) — %s",
                        evaluation["total_score"], evaluation["reason"])
            return True
        else:
            logger.debug("SkillExtract: NOT promoted (%.2f) — %s",
                         evaluation["total_score"], evaluation["reason"])
            # Still allow extraction if score is very high (override)
            if score.get("overall", 0) >= 0.8:
                logger.info("SkillExtract: High-score override (%.2f)", score["overall"])
                return True
            return False
    except Exception as e:
        logger.debug("SkillExtract: lifecycle unavailable (%s), using heuristic", e)

    # Fallback heuristic (if lifecycle module fails)
    try:
        from harness_learn import _classify_task
        task_type = _classify_task(user_message)
    except ImportError:
        task_type = "general"
    substantive = {"code_create", "code_fix", "deploy", "code_review", "config", "general",
                   "trade_analysis", "data_pipeline", "onchain"}
    return task_type in substantive


async def maybe_extract_skill(
    user_message: str, response: str, score: dict
) -> str | None:
    """Check if this interaction produced a reusable skill.
    If yes, extract and save it. Returns skill_id or None."""
    if not _should_extract_skill(user_message, response, score):
        return None

    try:
        from claude_agent import _run_claude_raw

        extract_prompt = f"""你是一个技能结晶器。把这个成功的任务提炼成一个可复用的函数签名。

任务输入: {user_message[:300]}
执行输出: {(response or '')[:2000]}

你要回答3个问题:
1. 这个任务的输入模式是什么？(比如"给定交易策略文档，生成回测代码")
2. 输出模式是什么？(比如"一个可运行的Python模块+参数配置")
3. 中间哪些步骤是通用的(可复用)，哪些是这个任务独有的？

输出JSON（只输出JSON）:
{{"title": "简短标题(动词开头，如'生成趋势策略回测')",
  "function_signature": "run_xxx(param1, param2, ...)",
  "input_schema": "输入是什么、需要哪些参数",
  "output_schema": "输出什么文件/结果",
  "keywords": ["关键词1", "关键词2", "...最多8个"],
  "summary": "一句话：输入X → 经过Y → 输出Z",
  "template_code": "核心可复用代码模式(只保留骨架，参数留空位)，最多300字符",
  "generic_steps": ["通用步骤1", "通用步骤2"],
  "specific_steps": ["这个任务独有的步骤"],
  "trigger_pattern": "什么样的任务描述会匹配到这个技能",
  "files_created": ["文件名"],
  "key_decisions": ["为什么选这种方案而非其他"]}}"""

        raw = await _run_claude_raw(
            prompt=extract_prompt,
            model="claude-haiku-4-5-20251001",
            timeout=15,
        )

        if not raw:
            return None

        # Parse JSON
        data = _parse_json_from_response(raw)
        if not data or not data.get("title"):
            return None

        # Create skill
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_title = re.sub(r'[^\w]', '_', data.get("title", "skill"))[:30]
        skill_id = f"sk_{ts}_{safe_title}"

        try:
            from harness_learn import _classify_task
            task_type_val = _classify_task(user_message)
        except ImportError:
            task_type_val = "general"
        skill = {
            "id": skill_id,
            "title": data.get("title", ""),
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "version": 1,
            "source_task": user_message[:200],
            "task_type": task_type_val,
            # Crystallized function interface
            "function_signature": data.get("function_signature", "")[:200],
            "input_schema": data.get("input_schema", "")[:300],
            "output_schema": data.get("output_schema", "")[:300],
            "trigger_pattern": data.get("trigger_pattern", "")[:200],
            # Reusable vs specific decomposition
            "generic_steps": data.get("generic_steps", [])[:6],
            "specific_steps": data.get("specific_steps", [])[:4],
            # Code and metadata
            "keywords": data.get("keywords", [])[:8],
            "summary": data.get("summary", "")[:300],
            "template_code": data.get("template_code", "")[:500],
            "code_snippet": data.get("code_snippet", "")[:500],  # backward compat
            "files_created": data.get("files_created", [])[:5],
            "key_decisions": data.get("key_decisions", [])[:5],
            # Usage tracking
            "use_count": 0,
            "last_used": None,
            "avg_score_when_used": None,
            "history": [{"version": 1, "date": datetime.now().isoformat(), "change": "crystallized"}],
        }

        _save_skill(skill)
        _rebuild_index()

        # Auto-synthesize to Markdown skill file in skills/ directory
        try:
            synthesize_to_md(skill)
        except Exception as e:
            logger.debug(f"MD skill synthesis failed: {e}")

        logger.info(f"Skill extracted: {skill_id} — {data.get('title', '')}")
        return skill_id

    except Exception as e:
        logger.warning(f"Skill extraction failed: {e}")
        return None


# ─── Markdown Skill Synthesis ────────────────────────────────────────────────

def _skill_name_to_dirname(title: str) -> str:
    """Convert a skill title to a kebab-case directory name."""
    # Transliterate common Chinese patterns to English
    name = title.lower().strip()
    # Replace non-alphanum (including CJK) with hyphens
    name = re.sub(r'[^a-z0-9]+', '-', name)
    name = name.strip('-')[:50]
    return name or "auto-skill"


def synthesize_to_md(skill: dict) -> str | None:
    """Convert a JSON skill to a SKILL.md file in skills/ directory.

    Creates skills/{dirname}/SKILL.md with frontmatter + structured content.
    Returns the directory path or None on failure.
    """
    title = skill.get("title", "")
    if not title:
        return None

    dirname = _skill_name_to_dirname(title)
    skill_dir = os.path.join(MD_SKILLS_DIR, dirname)
    skill_md_path = os.path.join(skill_dir, "SKILL.md")

    # Don't overwrite manually-created skills, but update auto-synthesized ones
    if os.path.exists(skill_md_path):
        try:
            existing = Path(skill_md_path).read_text(encoding="utf-8")
            if "Auto-synthesized" not in existing:
                logger.debug(f"MD skill is manual, skipping: {dirname}")
                return None
            # Auto-synthesized: update allowed (skill evolved)
        except Exception:
            return None

    # Build SKILL.md content
    summary = skill.get("summary", title)
    trigger = skill.get("trigger_pattern", "")
    func_sig = skill.get("function_signature", "")
    input_s = skill.get("input_schema", "")
    output_s = skill.get("output_schema", "")
    generic_steps = skill.get("generic_steps", [])
    specific_steps = skill.get("specific_steps", [])
    template_code = skill.get("template_code", "") or skill.get("code_snippet", "")
    keywords = skill.get("keywords", [])
    key_decisions = skill.get("key_decisions", [])
    files_created = skill.get("files_created", [])

    lines = [
        "---",
        f"name: {dirname}",
        f'description: "{summary}"',
        "---",
        "",
        f"# {title}",
        "",
        f"> {summary}",
        "",
    ]

    # Trigger pattern
    if trigger:
        lines += [f"**Trigger:** {trigger}", ""]

    # Interface
    if func_sig or input_s or output_s:
        lines += ["## Interface", ""]
        if func_sig:
            lines += [f"**Signature:** `{func_sig}`", ""]
        if input_s:
            lines += [f"**Input:** {input_s}", ""]
        if output_s:
            lines += [f"**Output:** {output_s}", ""]

    # Steps
    if generic_steps:
        lines += ["## Steps (reusable)", ""]
        for i, step in enumerate(generic_steps, 1):
            lines.append(f"{i}. {step}")
        lines.append("")

    if specific_steps:
        lines += ["## Task-specific notes", ""]
        for step in specific_steps:
            lines.append(f"- {step}")
        lines.append("")

    # Template code
    if template_code:
        lines += ["## Template", "", "```python", template_code.strip(), "```", ""]

    # Key decisions
    if key_decisions:
        lines += ["## Key Decisions", ""]
        for d in key_decisions:
            lines.append(f"- {d}")
        lines.append("")

    # Files
    if files_created:
        lines += ["## Files Created", ""]
        for f in files_created:
            lines.append(f"- `{f}`")
        lines.append("")

    # Keywords
    if keywords:
        lines += [f"**Keywords:** {', '.join(keywords)}", ""]

    # Metadata
    lines += [
        "---",
        f"*Auto-synthesized from skill `{skill.get('id', '?')}` "
        f"on {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
    ]

    content = "\n".join(lines) + "\n"

    # Write atomically
    os.makedirs(skill_dir, exist_ok=True)
    tmp = skill_md_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, skill_md_path)
        logger.info(f"Skill synthesized to MD: {skill_md_path}")
        return skill_dir
    except Exception as e:
        logger.warning(f"Failed to write MD skill: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return None


def synthesize_all_to_md() -> int:
    """Batch-synthesize all existing JSON skills to Markdown.
    Skips skills that already have a SKILL.md. Returns count of new files."""
    index = _load_index()
    count = 0
    for entry in index.get("entries", []):
        skill = _load_skill(entry["id"])
        if skill and synthesize_to_md(skill):
            count += 1
    return count


def list_synthesized_skills() -> list[str]:
    """List all auto-synthesized skill directories (those with the auto-synthesized marker)."""
    results = []
    if not os.path.isdir(MD_SKILLS_DIR):
        return results
    for name in sorted(os.listdir(MD_SKILLS_DIR)):
        md_path = os.path.join(MD_SKILLS_DIR, name, "SKILL.md")
        if not os.path.isfile(md_path):
            continue
        try:
            with open(md_path, "r", encoding="utf-8") as f:
                content = f.read(2000)
            if "Auto-synthesized" in content:
                results.append(name)
        except Exception:
            pass
    return results


# ─── Skill Evolution ─────────────────────────────────────────────────────────

def update_skill_from_reuse(skill_id: str, score: dict):
    """Update a skill after it was matched and used in an interaction."""
    skill = _load_skill(skill_id)
    if not skill:
        return

    skill["use_count"] = skill.get("use_count", 0) + 1
    skill["last_used"] = datetime.now().isoformat()

    # Update rolling average score
    prev_avg = skill.get("avg_score_when_used") or score.get("overall", 0.5)
    use_count = skill["use_count"]
    skill["avg_score_when_used"] = round(
        (prev_avg * (use_count - 1) + score.get("overall", 0.5)) / use_count, 2
    )

    _save_skill(skill)

    # Update index entry too
    index = _load_index()
    for entry in index.get("entries", []):
        if entry["id"] == skill_id:
            entry["use_count"] = skill["use_count"]
            entry["avg_score"] = skill["avg_score_when_used"]
            break
    _save_index(index)


async def maybe_evolve_skill(skill_id: str, user_message: str, response: str):
    """Check if a reused skill should be updated with new learnings.
    Only called every 3rd reuse to avoid overhead."""
    skill = _load_skill(skill_id)
    if not skill or skill.get("use_count", 0) % 3 != 0:
        return

    try:
        from claude_agent import _run_claude_raw

        evolve_prompt = f"""比较这个技能模板和最新的使用情况。如果有改进，输出更新后的JSON。如果没有改进，输出 {{"no_change": true}}

现有技能:
标题: {skill.get('title', '')}
摘要: {skill.get('summary', '')}
代码模式: {skill.get('code_snippet', '')}
关键词: {', '.join(skill.get('keywords', []))}

最新使用:
任务: {user_message[:200]}
回复: {(response or '')[:1000]}

如果有改进，输出JSON:
{{"title": "...", "summary": "...", "code_snippet": "...", "keywords": [...], "key_decisions": [...]}}
如果没有改进: {{"no_change": true}}"""

        raw = await _run_claude_raw(
            prompt=evolve_prompt,
            model="claude-haiku-4-5-20251001",
            timeout=15,
        )

        data = _parse_json_from_response(raw)
        if not data or data.get("no_change"):
            return

        # Apply updates
        if data.get("title"):
            skill["title"] = data["title"]
        if data.get("summary"):
            skill["summary"] = data["summary"][:300]
        if data.get("code_snippet"):
            skill["code_snippet"] = data["code_snippet"][:500]
        if data.get("keywords"):
            # Merge keywords
            old_kw = set(skill.get("keywords", []))
            new_kw = set(data["keywords"])
            skill["keywords"] = list(old_kw | new_kw)[:10]
        if data.get("key_decisions"):
            skill["key_decisions"] = data["key_decisions"][:5]

        skill["version"] = skill.get("version", 1) + 1
        skill["updated_at"] = datetime.now().isoformat()
        if "history" not in skill:
            skill["history"] = []
        skill["history"].append({
            "version": skill["version"],
            "date": datetime.now().isoformat(),
            "change": f"evolved from reuse #{skill['use_count']}",
        })
        skill["history"] = skill["history"][-10:]

        _save_skill(skill)
        _rebuild_index()

        # Re-synthesize MD (delete old, write new with updated content)
        try:
            dirname = _skill_name_to_dirname(skill.get("title", ""))
            md_path = os.path.join(MD_SKILLS_DIR, dirname, "SKILL.md")
            if os.path.exists(md_path):
                os.remove(md_path)  # Remove so synthesize_to_md can recreate
            synthesize_to_md(skill)
        except Exception:
            pass

        logger.info(f"Skill evolved: {skill['id']} → v{skill['version']}")

    except Exception as e:
        logger.debug(f"Skill evolution check failed: {e}")


# ─── Pruning ─────────────────────────────────────────────────────────────────

def prune_skills():
    """Remove low-value skills if over limit."""
    index = _load_index()
    entries = index.get("entries", [])
    if len(entries) <= MAX_SKILLS:
        return

    # Score each skill by value
    for entry in entries:
        skill = _load_skill(entry["id"])
        if not skill:
            entry["_value"] = -999
            continue
        use_count = skill.get("use_count", 0)
        avg_score = skill.get("avg_score_when_used") or 0
        updated = skill.get("updated_at", "2000-01-01")
        try:
            dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
            age_days = (datetime.now() - dt.replace(tzinfo=None)).days
        except (ValueError, AttributeError):
            try:
                age_days = (datetime.now() - datetime.fromisoformat(updated)).days
            except Exception:
                age_days = 999
        except Exception:
            age_days = 999
        entry["_value"] = use_count * 2 + avg_score * 5 - age_days * 0.1

    entries.sort(key=lambda e: e.get("_value", 0), reverse=True)
    to_remove = entries[MAX_SKILLS:]
    index["entries"] = entries[:MAX_SKILLS]

    # Clean up _value keys
    for e in index["entries"]:
        e.pop("_value", None)

    for entry in to_remove:
        _delete_skill(entry["id"])
        logger.info(f"Pruned skill: {entry['id']}")

    _save_index(index)


# ─── Stats ───────────────────────────────────────────────────────────────────

def get_skill_stats() -> str:
    """Human-readable skill library stats."""
    index = _load_index()
    entries = index.get("entries", [])
    total = len(entries)
    if total == 0:
        return "🧠 技能库: 空 (使用bot完成任务后自动学习)"

    total_uses = sum(e.get("use_count", 0) for e in entries)
    with_scores = [e for e in entries if e.get("avg_score")]
    avg = sum(e["avg_score"] for e in with_scores) / len(with_scores) if with_scores else 0

    synth_count = len(list_synthesized_skills())
    lines = [f"🧠 技能库: {total} 个技能, 被复用 {total_uses} 次, {synth_count} 个已合成MD"]
    if avg > 0:
        lines.append(f"复用时平均分: {avg:.0%}")

    # Top 3 most used
    top = sorted(entries, key=lambda e: e.get("use_count", 0), reverse=True)[:3]
    if top and top[0].get("use_count", 0) > 0:
        lines.append("最常用:")
        for e in top:
            if e.get("use_count", 0) > 0:
                lines.append(f"  {e.get('title', '?')} ({e['use_count']}次)")

    return "\n".join(lines)


# ─── Seed ────────────────────────────────────────────────────────────────────

_SEED_SKILLS = [
    {
        "id": "sk_seed_skill_synthesis",
        "title": "技能自动合成",
        "summary": "成功任务后自动提取可复用技能到JSON+MD格式",
        "task_type": "code_create",
        "function_signature": "maybe_extract_skill(user_message, response, score)",
        "input_schema": "用户消息 + bot回复 + 质量分数dict",
        "output_schema": "skill_id(str) 或 None",
        "trigger_pattern": "自动学习 自动保存技能 skill extraction 技能合成",
        "keywords": ["skill", "技能", "自动学习", "结晶", "提取", "合成", "复用"],
        "generic_steps": ["检测是否值得提取(分数/完成信号/代码内容)", "调用haiku提炼函数签名", "保存JSON+MD", "更新index"],
        "specific_steps": ["heuristic阈值0.45", "MD写入skills/目录"],
        "template_code": "if _should_extract_skill(msg, resp, score):\n    data = await _run_claude_raw(extract_prompt, model='haiku')\n    skill = parse_and_save(data)\n    synthesize_to_md(skill)",
        "key_decisions": ["用haiku节省token", "阈值0.45平衡质量与覆盖率", "MD格式兼容superpowers技能系统"],
        "files_created": [".skill_library/skills/*.json", "skills/*/SKILL.md"],
        "use_count": 0, "last_used": None, "avg_score_when_used": None,
        "version": 1,
        "history": [{"version": 1, "date": "2026-03-29", "change": "seeded from evolution task 1"}],
    },
    {
        "id": "sk_seed_visual_ui",
        "title": "视觉UI定位点击",
        "summary": "SOM截图标注 + fallback链点击UI元素",
        "task_type": "code_create",
        "function_signature": "ui_click_element(name_or_idx, fuzzy=True)",
        "input_schema": "元素名称(模糊匹配) 或 SOM标注序号",
        "output_schema": "click成功/失败",
        "trigger_pattern": "点击元素 UI自动化 截图定位 som click",
        "keywords": ["som", "ui_click", "截图", "点击", "定位", "元素", "fallback"],
        "generic_steps": ["som_screenshot标注元素", "fuzzy名称匹配", "依次尝试browser_click→ui_click→som_click→smartclick"],
        "specific_steps": ["不区分大小写部分匹配", "Windows UI Automation树"],
        "template_code": "# fallback chain\nfor method in [browser_click, ui_click, som_click, smartclick]:\n    try:\n        if method(target): return True\n    except: continue\nreturn False",
        "key_decisions": ["fallback链保证可靠性", "SOM标注用数字序号便于LLM引用"],
        "files_created": [],
        "use_count": 0, "last_used": None, "avg_score_when_used": None,
        "version": 1,
        "history": [{"version": 1, "date": "2026-03-29", "change": "seeded from evolution task 2"}],
    },
    {
        "id": "sk_seed_self_repair",
        "title": "自我修复重启",
        "summary": "崩溃自动重启 + 错误日志 + 热重载",
        "task_type": "code_create",
        "function_signature": "run_with_auto_restart(main_fn, restart_delay=3)",
        "input_schema": "主函数引用",
        "output_schema": "持续运行，崩溃后自动重启",
        "trigger_pattern": "自动重启 崩溃恢复 热重载 watchdog self-repair",
        "keywords": ["重启", "崩溃", "热重载", "watchdog", "自修复", "error_log", "selfcheck"],
        "generic_steps": ["try/except包裹主循环", "except时sleep(3)重启", "写_error_log.txt", "watchdog监控py文件变化"],
        "specific_steps": ["/selfcheck命令检查文件语法", "importlib.reload热重载"],
        "template_code": "while True:\n    try:\n        asyncio.run(main())\n    except Exception as e:\n        log_error(e)\n        time.sleep(3)  # auto-restart",
        "key_decisions": ["3秒延迟避免崩溃循环", "热重载避免重启Telegram session"],
        "files_created": ["_error_log.txt"],
        "use_count": 0, "last_used": None, "avg_score_when_used": None,
        "version": 1,
        "history": [{"version": 1, "date": "2026-03-29", "change": "seeded from evolution task 3"}],
    },
    {
        "id": "sk_seed_parallel_tasks",
        "title": "多任务并发队列",
        "summary": "asyncio并发处理多条消息 + 任务队列管理",
        "task_type": "code_create",
        "function_signature": "enqueue_task(coro) / get_running_tasks()",
        "input_schema": "异步协程",
        "output_schema": "任务ID，后台执行",
        "trigger_pattern": "并发 多任务 队列 asyncio concurrent task queue",
        "keywords": ["asyncio", "并发", "队列", "task", "cancel", "并行", "多任务"],
        "generic_steps": ["asyncio.create_task创建后台任务", "dict跟踪运行中任务", "/tasks显示状态", "/cancel取消"],
        "specific_steps": ["消息handler立即返回不阻塞", "任务完成后从dict移除"],
        "template_code": "_tasks: dict[str, asyncio.Task] = {}\nasync def enqueue(coro, tid):\n    task = asyncio.create_task(coro)\n    _tasks[tid] = task\n    task.add_done_callback(lambda t: _tasks.pop(tid, None))",
        "key_decisions": ["dict存任务引用以便cancel", "done_callback自动清理"],
        "files_created": [],
        "use_count": 0, "last_used": None, "avg_score_when_used": None,
        "version": 1,
        "history": [{"version": 1, "date": "2026-03-29", "change": "seeded from evolution task 4"}],
    },
    {
        "id": "sk_seed_memory_system",
        "title": "结构化JSON记忆系统",
        "summary": "分类记忆存储 + 自动摘要 + 按重要性排序清理",
        "task_type": "code_create",
        "function_signature": "save_memory(content, category, importance) / recall_memory(query)",
        "input_schema": "内容字符串 + 分类 + 重要性0-1",
        "output_schema": "记忆ID 或 匹配记忆列表",
        "trigger_pattern": "记忆 memory 记住 习惯 偏好 学习",
        "keywords": ["记忆", "memory", "存储", "recall", "摘要", "action_memory", "分类"],
        "generic_steps": ["分类存储(JSON)", "关键词/语义检索", "重要性评分", "超限自动清理低分条目"],
        "specific_steps": ["/memory命令查看编辑", "对话结束自动摘要"],
        "template_code": "# action_memory.json structure\n{'entries': [{'id': ..., 'content': ..., 'category': ..., 'importance': 0-1, 'ts': ...}]}",
        "key_decisions": ["JSON比MD更易结构化查询", "重要性分数支持自动清理"],
        "files_created": ["action_memory.json"],
        "use_count": 0, "last_used": None, "avg_score_when_used": None,
        "version": 1,
        "history": [{"version": 1, "date": "2026-03-29", "change": "seeded from evolution task 5"}],
    },
    {
        "id": "sk_seed_dashboard",
        "title": "Flask性能监控仪表盘",
        "summary": "实时web dashboard展示bot运行状态(端口8080)",
        "task_type": "code_create",
        "function_signature": "start_dashboard(port=8080)",
        "input_schema": "端口号",
        "output_schema": "http://localhost:8080 实时监控页面",
        "trigger_pattern": "dashboard 仪表盘 监控 性能 flask web",
        "keywords": ["dashboard", "flask", "监控", "性能", "web", "8080", "实时"],
        "generic_steps": ["Flask路由/", "stats_collector收集指标", "JS定时刷新", "/dashboard命令截图发TG"],
        "specific_steps": ["消息处理速度/成功率/错误率/内存/运行时间", "最近10条执行日志"],
        "template_code": "from flask import Flask\napp = Flask(__name__)\n@app.route('/')\ndef index(): return render_template('dashboard.html', stats=get_stats())\nThread(target=app.run, kwargs={'port':8080}, daemon=True).start()",
        "key_decisions": ["daemon线程不阻塞bot主进程", "截图发TG而非打开浏览器"],
        "files_created": ["dashboard.py"],
        "use_count": 0, "last_used": None, "avg_score_when_used": None,
        "version": 1,
        "history": [{"version": 1, "date": "2026-03-29", "change": "seeded from evolution task 6"}],
    },
    {
        "id": "sk_seed_codex_charger",
        "title": "Codex自充能永续运行",
        "summary": "CLI额度耗尽时自动切换到Codex IDE继续执行任务",
        "task_type": "code_create",
        "function_signature": "run_via_codex(prompt, timeout=300)",
        "input_schema": "任务prompt字符串",
        "output_schema": "执行结果文本",
        "trigger_pattern": "codex 额度 充能 自充值 永续 credits",
        "keywords": ["codex", "充能", "额度", "永续", "browser", "selenium", "fallback"],
        "generic_steps": ["检测CLI额度状态", "浏览器打开claude.ai/code", "粘贴prompt执行", "提取结果"],
        "specific_steps": ["用undetected_chromedriver绕过检测", "等待响应完成再提取"],
        "template_code": "# codex_charger.py\ndef run_via_codex(prompt):\n    driver = uc.Chrome()\n    driver.get('https://claude.ai/code')\n    # paste prompt, wait, extract",
        "key_decisions": ["Codex免费session作为CLI backup", "browser自动化避免手动操作"],
        "files_created": ["codex_charger.py"],
        "use_count": 0, "last_used": None, "avg_score_when_used": None,
        "version": 1,
        "history": [{"version": 1, "date": "2026-03-29", "change": "seeded from evolution task 7"}],
    },
]


def seed_evolution_skills() -> int:
    """Seed skill library with knowledge from the 7 completed evolution tasks.
    Skips skills that already exist. Returns count of newly added skills."""
    index = _load_index()
    existing_ids = {e["id"] for e in index.get("entries", [])}
    added = 0

    for seed in _SEED_SKILLS:
        if seed["id"] in existing_ids:
            continue

        now = datetime.now().isoformat()
        skill = {
            **seed,
            "created_at": now,
            "updated_at": now,
            "code_snippet": seed.get("template_code", "")[:500],
        }
        _save_skill(skill)
        added += 1
        try:
            synthesize_to_md(skill)
        except Exception:
            pass

    if added > 0:
        _rebuild_index()
        logger.info(f"Seeded {added} evolution skills into library")

    return added


# ─── Executable Python skills (BaseSkill / hot-reload) ─────────────────────

async def invoke_python_skill_async(
    module: str | Path,
    payload: dict | None = None,
    *,
    timeout_sec: float = 120.0,
    reload_module: bool = False,
) -> Any:
    """
    Run a ``skills.sk_*`` module or a ``.py`` path via ``skill_runtime``
    (unified timeout; optional ``importlib.reload`` for package modules).
    """
    from skills.skill_runtime import run_skill_module_async

    return await run_skill_module_async(
        module,
        payload,
        timeout_sec=timeout_sec,
        reload_first=reload_module,
    )


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _parse_json_from_response(raw: str) -> dict | None:
    """Extract JSON from LLM response, handling code blocks."""
    if not raw:
        return None
    text = raw.strip()

    # Remove markdown code blocks
    if "```" in text:
        parts = text.split("```")
        for i, part in enumerate(parts):
            if i % 2 == 1:
                part = part.strip()
                if part.startswith("json"):
                    part = part[4:].strip()
                if part.startswith("{"):
                    text = part
                    break

    # Find JSON
    start = text.find("{")
    if start < 0:
        return None

    # Brace-depth matching (skip braces inside strings)
    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        ch = text[i]
        if escape:
            escape = False
            continue
        if ch == '\\' and in_string:
            escape = True
            continue
        if ch == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None
