"""
skill_library.py — Reusable Skill Memory

When the bot successfully creates a script/solution, it saves it as a "skill".
On future similar tasks, matched skills are injected as context → bot doesn't start from zero.
Skills evolve: each reuse can improve them.

Flow:
  User msg → find_matching_skills() → inject into prompt
  Response → score → maybe_extract_skill() → save new skill
  Reuse + high score → update_skill_from_reuse() → skill evolves
"""
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
SKILL_DIR = os.path.join(BOT_DIR, ".skill_library")
SKILLS_DIR = os.path.join(SKILL_DIR, "skills")
INDEX_FILE = os.path.join(SKILL_DIR, "index.json")

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
    """Heuristic: should we extract a skill from this interaction?"""
    # Must have a good score
    if score.get("overall", 0) < 0.55:
        return False

    resp_lower = (response or "").lower()

    # Must show completion signals
    if not any(w in resp_lower for w in ["✅", "done", "saved", "commit", "created",
                                          "wrote", "完成", "成功", "fixed", "修复"]):
        return False

    # Must have code-like content
    code_signals = ["```", "saved to", "wrote to", "created file", ".py", ".js",
                    ".sh", ".bat", "import ", "def ", "class ", "function ",
                    "npm ", "pip ", "写入", "保存到"]
    if not any(s in resp_lower for s in code_signals):
        return False

    # Task must be substantive
    try:
        from harness_learn import _classify_task
        task_type = _classify_task(user_message)
    except ImportError:
        task_type = "general"
    substantive = {"code_create", "code_fix", "deploy", "code_review", "config", "general"}
    if task_type not in substantive:
        return False

    # Don't extract if we already have too many skills
    index = _load_index()
    if len(index.get("entries", [])) >= MAX_SKILLS:
        return False

    return True


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

        logger.info(f"Skill extracted: {skill_id} — {data.get('title', '')}")
        return skill_id

    except Exception as e:
        logger.warning(f"Skill extraction failed: {e}")
        return None


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
            age_days = (datetime.now() - datetime.fromisoformat(updated)).days
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

    lines = [f"🧠 技能库: {total} 个技能, 被复用 {total_uses} 次"]
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
