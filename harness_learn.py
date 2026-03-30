"""
harness_learn.py — Agent Learning Harness

The verifier + scorer + memory updater loop.
Makes the bot smarter over time by:
1. Scoring each interaction (task complexity, success, efficiency)
2. Detecting patterns (repeated failures, successful workflows)
3. Building experience templates from successful runs
4. Pruning stale/wrong memories
5. Adapting model selection based on real performance data

Architecture:
  User msg → CLI call → response
                          ↓
                    [harness_learn]
                    score() → log_interaction() → update_memory()
                          ↓
                    .bot_memory.md (persistent)
                    .bot_scores.jsonl (interaction log)
                    .bot_workflows.json (successful patterns)
"""
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
MEMORY_FILE = os.path.join(BOT_DIR, ".bot_memory.md")
SCORES_FILE = os.path.join(BOT_DIR, ".bot_scores.jsonl")
WORKFLOWS_FILE = os.path.join(BOT_DIR, ".bot_workflows.json")


# ─── Scoring ─────────────────────────────────────────────────────────────────

def score_interaction(
    user_message: str,
    response: str,
    model: str,
    duration_ms: int,
    session_id: str = None,
) -> dict:
    """Score an interaction on multiple dimensions. Returns score dict."""
    score = {
        "timestamp": datetime.now().isoformat(),
        "user_message": user_message[:200],
        "response_preview": response[:200] if response else "",
        "model": model,
        "duration_ms": duration_ms,
        "session_id": session_id,
        "scores": {},
        "flags": [],
    }

    resp_lower = (response or "").lower()

    # ── Completion score (did it actually do something?) ──
    if "无输出" in resp_lower or "无文字输出" in resp_lower:
        # "✅ 任务已执行（无输出）" — potentially incomplete, flag it
        score["scores"]["completion"] = 0.4
        score["flags"].append("NO_OUTPUT")
    elif any(w in resp_lower for w in ["✅", "完成", "done", "commit", "saved", "fixed"]):
        score["scores"]["completion"] = 1.0
    elif any(w in resp_lower for w in ["error", "错误", "失败", "failed", "⚠️"]):
        score["scores"]["completion"] = 0.2
    elif any(w in resp_lower for w in ["超时", "timeout", "⏰"]):
        score["scores"]["completion"] = 0.0
    else:
        score["scores"]["completion"] = 0.6  # Unknown

    # ── Did it ask questions? (BAD — violates rules) ──
    question_patterns = [r"你要.*吗", r"你想.*吗", r"哪种方式", r"要我.*吗", r"which.*prefer", r"do you want"]
    asked_question = any(re.search(p, resp_lower) for p in question_patterns)
    if asked_question:
        score["scores"]["obedience"] = 0.0
        score["flags"].append("ASKED_QUESTION")
    else:
        score["scores"]["obedience"] = 1.0

    # ── Efficiency (response time vs task complexity) ──
    msg_len = len(user_message)
    if msg_len < 20:  # Simple query
        if duration_ms < 10000:
            score["scores"]["efficiency"] = 1.0
        elif duration_ms < 30000:
            score["scores"]["efficiency"] = 0.6
        else:
            score["scores"]["efficiency"] = 0.2
            score["flags"].append("SLOW_FOR_SIMPLE")
    else:  # Complex task
        if duration_ms < 60000:
            score["scores"]["efficiency"] = 1.0
        elif duration_ms < 180000:
            score["scores"]["efficiency"] = 0.7
        else:
            score["scores"]["efficiency"] = 0.4

    # ── Model appropriateness ──
    if msg_len < 20 and model == "claude-opus-4-6":
        score["flags"].append("OPUS_FOR_SIMPLE")
        score["scores"]["model_fit"] = 0.3
    elif msg_len > 100 and model == "claude-haiku-4-5-20251001":
        score["flags"].append("HAIKU_FOR_COMPLEX")
        score["scores"]["model_fit"] = 0.5
    else:
        score["scores"]["model_fit"] = 1.0

    # ── Conciseness (user is on phone, short = good) ──
    resp_len = len(response) if response else 0
    if resp_len < 500:
        score["scores"]["conciseness"] = 1.0
    elif resp_len < 2000:
        score["scores"]["conciseness"] = 0.7
    elif resp_len < 5000:
        score["scores"]["conciseness"] = 0.4
    else:
        score["scores"]["conciseness"] = 0.2
        score["flags"].append("TOO_VERBOSE")

    # ── Overall score ──
    scores = score["scores"]
    score["overall"] = round(sum(scores.values()) / len(scores), 2) if scores else 0.5

    return score


# ─── Interaction Logging ─────────────────────────────────────────────────────

_MAX_SCORES_FILE_SIZE = 2 * 1024 * 1024  # 2 MB max

def log_interaction(score: dict):
    """Append interaction score to JSONL log. Auto-truncates when file exceeds 2 MB."""
    try:
        # Truncate if file is too large (prevent unbounded growth over months)
        try:
            if os.path.exists(SCORES_FILE) and os.path.getsize(SCORES_FILE) > _MAX_SCORES_FILE_SIZE:
                with open(SCORES_FILE, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                # Keep last half
                with open(SCORES_FILE, "w", encoding="utf-8") as f:
                    f.writelines(lines[len(lines) // 2:])
        except Exception:
            pass
        with open(SCORES_FILE, "a", encoding="utf-8", errors="replace") as f:
            f.write(json.dumps(score, ensure_ascii=False, default=str) + "\n")
            f.flush()
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to log interaction: {e}")


def get_recent_scores(n: int = 20) -> list[dict]:
    """Read last N interaction scores without loading the entire file into memory."""
    try:
        if not os.path.exists(SCORES_FILE):
            return []
        file_size = os.path.getsize(SCORES_FILE)
        if file_size == 0:
            return []
        # Estimate ~500 bytes per line, read from end to avoid loading huge files
        read_size = min(file_size, n * 600 + 1024)
        with open(SCORES_FILE, "rb") as f:
            if file_size > read_size:
                f.seek(-read_size, 2)  # Seek from end
                # Skip partial first line
                f.readline()
            tail = f.read().decode("utf-8", errors="replace")
        lines = tail.strip().splitlines()
        scores = []
        for line in lines[-n:]:
            line = line.strip()
            if not line:
                continue
            try:
                scores.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        return scores
    except Exception:
        return []


# ─── Pattern Detection ───────────────────────────────────────────────────────

def detect_patterns(scores: list[dict]) -> list[str]:
    """Detect behavioral patterns from recent scores. Returns list of insights."""
    if len(scores) < 3:
        return []

    insights = []

    # Repeated question-asking
    question_count = sum(1 for s in scores if "ASKED_QUESTION" in s.get("flags", []))
    if question_count >= 2:
        insights.append(f"WARNING: Asked questions {question_count}/{len(scores)} times. System prompt not effective enough.")

    # Slow for simple tasks
    slow_simple = sum(1 for s in scores if "SLOW_FOR_SIMPLE" in s.get("flags", []))
    if slow_simple >= 2:
        insights.append(f"WARNING: Slow for simple tasks {slow_simple} times. Model routing may need adjustment.")

    # Opus overuse
    opus_simple = sum(1 for s in scores if "OPUS_FOR_SIMPLE" in s.get("flags", []))
    if opus_simple >= 2:
        insights.append(f"OPTIMIZATION: Opus used for simple tasks {opus_simple} times. Tighten model routing.")

    # Average completion rate
    completions = [s.get("scores", {}).get("completion", 0) for s in scores]
    avg_completion = sum(completions) / len(completions) if completions else 0
    if avg_completion < 0.5:
        insights.append(f"ALERT: Low completion rate ({avg_completion:.0%}). Agent may be struggling.")
    elif avg_completion > 0.8:
        insights.append(f"GOOD: High completion rate ({avg_completion:.0%}).")

    # Verbosity trend
    verbose_count = sum(1 for s in scores if "TOO_VERBOSE" in s.get("flags", []))
    if verbose_count >= 3:
        insights.append(f"STYLE: Too verbose {verbose_count} times. User is on phone, keep it short.")

    # Average overall score
    overall_scores = [s.get("overall", 0) for s in scores]
    avg_overall = sum(overall_scores) / len(overall_scores) if overall_scores else 0
    insights.append(f"OVERALL: Average score {avg_overall:.2f}/1.00 over last {len(scores)} interactions.")

    return insights


# ─── Workflow Templates ──────────────────────────────────────────────────────

def _load_workflows() -> dict:
    try:
        if os.path.exists(WORKFLOWS_FILE):
            return json.loads(Path(WORKFLOWS_FILE).read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"workflows": [], "failure_patterns": []}


def _save_workflows(data: dict):
    try:
        _tmp = WORKFLOWS_FILE + ".tmp"
        with open(_tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(_tmp, WORKFLOWS_FILE)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to save workflows: {e}")


def record_successful_workflow(task_type: str, steps: list[str], duration_ms: int):
    """Record a successful task workflow as a template for future use."""
    data = _load_workflows()
    workflow = {
        "task_type": task_type,
        "steps": steps,
        "duration_ms": duration_ms,
        "recorded_at": datetime.now().isoformat(),
        "use_count": 0,
    }
    # Deduplicate by task_type
    data["workflows"] = [w for w in data["workflows"] if w["task_type"] != task_type]
    data["workflows"].append(workflow)
    # Keep max 50 workflows
    data["workflows"] = data["workflows"][-50:]
    _save_workflows(data)


def record_failure_pattern(task_type: str, error: str, attempted_fix: str = ""):
    """Record a failure pattern to avoid repeating."""
    data = _load_workflows()
    pattern = {
        "task_type": task_type,
        "error": error[:200],
        "attempted_fix": attempted_fix[:200],
        "recorded_at": datetime.now().isoformat(),
        "occurrences": 1,
    }
    # Increment if same error seen before
    for p in data["failure_patterns"]:
        if p["task_type"] == task_type and p["error"] == pattern["error"]:
            p["occurrences"] += 1
            p["recorded_at"] = pattern["recorded_at"]
            _save_workflows(data)
            return
    data["failure_patterns"].append(pattern)
    data["failure_patterns"] = data["failure_patterns"][-30:]
    _save_workflows(data)


def get_relevant_workflow(task_description: str) -> dict | None:
    """Find the most relevant workflow template for a task."""
    data = _load_workflows()
    task_lower = task_description.lower()
    best = None
    best_score = 0
    for w in data["workflows"]:
        # Simple keyword overlap scoring
        wtype = w["task_type"].lower()
        overlap = sum(1 for word in wtype.split() if word in task_lower)
        if overlap > best_score:
            best_score = overlap
            best = w
    return best


# ─── Memory Management ───────────────────────────────────────────────────────

_last_memory_update: float = 0.0

def update_memory_with_insights(insights: list[str]):
    """Update the performance section in memory file. Rate-limited to once per hour.

    Instead of appending a new section each time (causes bloat),
    replaces the existing '## 性能' section with the latest summary.
    """
    global _last_memory_update
    if not insights:
        return
    if time.time() - _last_memory_update < 3600:
        return
    _last_memory_update = time.time()
    try:
        content = Path(MEMORY_FILE).read_text(encoding="utf-8") if os.path.exists(MEMORY_FILE) else ""
        # Build compact performance summary (replaces old one)
        date = datetime.now().strftime("%Y-%m-%d %H:%M")
        new_perf = f"## 性能\n- 更新: {date}\n"
        for insight in insights:
            new_perf += f"- {insight}\n"

        # Replace existing 性能 section or append
        perf_pattern = re.compile(r"## 性能\n(?:- [^\n]*\n)*(?:- [^\n]*)?", re.MULTILINE)
        if perf_pattern.search(content):
            content = perf_pattern.sub(new_perf, content)
        else:
            content = content.rstrip() + "\n\n" + new_perf

        Path(MEMORY_FILE).write_text(content, encoding="utf-8")
    except Exception:
        pass


def prune_memory(max_lines: int = 100):
    """Keep memory file from growing too large.

    Preserves critical sections (用户画像, 用户项目, 已知失败模式, 性能)
    and only prunes dynamically-generated entries (自愈, 自动分析, etc).
    """
    try:
        if not os.path.exists(MEMORY_FILE):
            return
        content = Path(MEMORY_FILE).read_text(encoding="utf-8")
        lines = content.splitlines(keepends=True)
        if len(lines) <= max_lines:
            return

        # Identify protected sections (user profile, projects, failures, performance)
        _PROTECTED = {"用户画像", "用户项目", "已知失败模式", "性能"}
        protected_lines = []
        dynamic_lines = []
        in_protected = False

        for line in lines:
            if line.startswith("## "):
                section_name = line.strip().lstrip("# ").strip()
                in_protected = any(p in section_name for p in _PROTECTED)
            if line.startswith("# ") and not line.startswith("## "):
                in_protected = True  # Top-level header is always protected

            if in_protected:
                protected_lines.append(line)
            else:
                dynamic_lines.append(line)

        # Keep all protected + last N dynamic lines
        budget = max(max_lines - len(protected_lines), 10)
        kept = protected_lines + dynamic_lines[-budget:]
        Path(MEMORY_FILE).write_text("".join(kept), encoding="utf-8")
    except Exception:
        pass


def get_memory_context(max_chars: int = 2000) -> str:
    """Get relevant memory context for injection into prompts."""
    parts = []

    # 1. Memory file (last N bytes — avoid reading entire large file into RAM)
    try:
        if os.path.exists(MEMORY_FILE):
            read_bytes = max_chars // 2 + 200  # Read a bit extra to avoid mid-line cut
            file_size = os.path.getsize(MEMORY_FILE)
            if file_size <= read_bytes:
                mem = Path(MEMORY_FILE).read_text(encoding="utf-8").strip()
            else:
                with open(MEMORY_FILE, "rb") as f:
                    f.seek(-read_bytes, 2)  # Seek from end
                    raw = f.read()
                # Skip past any partial UTF-8 sequence at the start
                # (seek may land mid-character in multi-byte Chinese text)
                start = 0
                while start < min(4, len(raw)):
                    if (raw[start] & 0xC0) != 0x80:  # Not a continuation byte
                        break
                    start += 1
                tail = raw[start:].decode("utf-8", errors="replace")
                # Drop partial first line
                first_nl = tail.find("\n")
                if first_nl >= 0:
                    tail = tail[first_nl + 1:]
                mem = tail.strip()
            if mem:
                parts.append(mem[-max_chars // 2:])
    except Exception:
        pass

    # 2. Recent failure patterns (avoid repeating mistakes)
    data = _load_workflows()
    frequent_failures = [p for p in data.get("failure_patterns", []) if p.get("occurrences", 0) >= 2]
    if frequent_failures:
        failure_text = "\n### 已知失败模式 (不要重复)\n"
        for p in frequent_failures[-5:]:
            failure_text += f"- {p['task_type']}: {p['error']} (发生{p['occurrences']}次)\n"
        parts.append(failure_text)

    # 3. Recent performance insights
    scores = get_recent_scores(10)
    insights = detect_patterns(scores)
    if insights:
        insight_text = "\n### 近期表现\n"
        for i in insights:
            insight_text += f"- {i}\n"
        parts.append(insight_text)

    combined = "\n".join(parts)
    return combined[-max_chars:] if len(combined) > max_chars else combined


# ─── Harness Loop (called after each interaction) ────────────────────────────

def post_interaction_loop(
    user_message: str,
    response: str,
    model: str,
    duration_ms: int,
    session_id: str = None,
):
    """Run the full harness learning loop after each interaction.

    This is the verifier + scorer + memory updater.
    """
    # 1. Score
    score = score_interaction(user_message, response, model, duration_ms, session_id)

    # 2. Log
    log_interaction(score)

    # 3. Detect patterns (throttled by update_memory_with_insights' 1-hour rate limit)
    scores = get_recent_scores(20)
    if len(scores) >= 10:
        insights = detect_patterns(scores)
        update_memory_with_insights(insights)  # Rate-limited to once per hour internally
        prune_memory()

    # 4. Record failures
    if score["overall"] < 0.4:
        # Classify task type from message
        task_type = _classify_task(user_message)
        error = score["flags"][0] if score["flags"] else "low_score"
        record_failure_pattern(task_type, error)

    # 5. Record successes
    if score["overall"] > 0.8:
        task_type = _classify_task(user_message)
        record_successful_workflow(task_type, ["completed_successfully"], duration_ms)

    # 6. Learn user language patterns (compact, rate-limited)
    _learn_user_language(user_message, response)

    return score


# ─── User Language Learning ──────────────────────────────────────────────────

_USER_LANG_FILE = os.path.join(BOT_DIR, ".user_language.json")
_last_lang_update: float = 0


def _learn_user_language(user_message: str, response: str):
    """Extract and accumulate user language patterns for future prompt injection.

    Tracks: preferred language, common phrases, command shortcuts, topic keywords.
    This lets the bot mirror the user's communication style.
    Rate-limited: updates at most once per 10 interactions.
    """
    global _last_lang_update

    msg = user_message.strip()
    if len(msg) < 2:
        return

    # Load existing profile and increment count (always, regardless of debounce)
    profile = _load_user_language()
    profile["msg_count"] = profile.get("msg_count", 0) + 1

    # Only do full analysis every 10 messages, with 60s debounce
    if profile["msg_count"] % 10 != 0 or time.time() - _last_lang_update < 60:
        _save_user_language(profile)
        return

    _last_lang_update = time.time()

    # --- Language detection ---
    cn_chars = sum(1 for c in msg if '\u4e00' <= c <= '\u9fff')
    en_chars = sum(1 for c in msg if c.isascii() and c.isalpha())
    total = cn_chars + en_chars
    if total > 0:
        cn_ratio = cn_chars / total
        lang_counts = profile.setdefault("lang_counts", {"zh": 0, "en": 0, "mixed": 0})
        if cn_ratio > 0.7:
            lang_counts["zh"] = lang_counts.get("zh", 0) + 1
        elif cn_ratio < 0.2:
            lang_counts["en"] = lang_counts.get("en", 0) + 1
        else:
            lang_counts["mixed"] = lang_counts.get("mixed", 0) + 1

    # --- Message length distribution ---
    lengths = profile.setdefault("msg_lengths", [])
    lengths.append(len(msg))
    if len(lengths) > 100:
        profile["msg_lengths"] = lengths[-100:]

    # --- Common phrases / command patterns ---
    phrases = profile.setdefault("phrases", {})
    # Extract first 2-3 word patterns (command-like)
    words = msg.split()[:3]
    if words:
        key = " ".join(words).lower()[:30]
        phrases[key] = phrases.get(key, 0) + 1
    # Keep only top 30 phrases
    if len(phrases) > 50:
        top = sorted(phrases.items(), key=lambda x: -x[1])[:30]
        profile["phrases"] = dict(top)

    # --- Topic keywords ---
    topics = profile.setdefault("topics", {})
    topic_keywords = {
        "crypto": ["crypto", "token", "btc", "eth", "sol", "okx", "币", "代币", "合约", "交易"],
        "coding": ["code", "bug", "fix", "写", "修", "函数", "class", "import", "error"],
        "browser": ["chrome", "打开", "网页", "open", "browse", "url", "搜索"],
        "system": ["screenshot", "截图", "click", "点", "type", "打字", "窗口"],
        "bot": ["bot", "restart", "重启", "status", "状态"],
    }
    msg_lower = msg.lower()
    for topic, kws in topic_keywords.items():
        if any(kw in msg_lower for kw in kws):
            topics[topic] = topics.get(topic, 0) + 1

    _save_user_language(profile)


def _load_user_language() -> dict:
    try:
        if os.path.exists(_USER_LANG_FILE):
            return json.loads(Path(_USER_LANG_FILE).read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_user_language(profile: dict):
    try:
        Path(_USER_LANG_FILE).write_text(
            json.dumps(profile, ensure_ascii=False, indent=1),
            encoding="utf-8",
        )
    except Exception:
        pass


def get_user_language_summary(max_chars: int = 300) -> str:
    """Get a compact user language summary for prompt injection.

    Returns something like:
    "User: 中英混用(70%中文), avg msg 15chars, topics: crypto>coding>system,
     common: '看看','继续','打开chrome'"
    """
    profile = _load_user_language()
    if not profile or profile.get("msg_count", 0) < 5:
        return ""

    parts = []

    # Language preference
    lang = profile.get("lang_counts", {})
    total_lang = sum(lang.values()) or 1
    dominant = max(lang, key=lambda k: lang.get(k, 0), default="mixed")
    if dominant == "zh":
        parts.append(f"语言:中文为主({lang.get('zh',0)*100//total_lang}%)")
    elif dominant == "en":
        parts.append(f"Lang:English({lang.get('en',0)*100//total_lang}%)")
    else:
        parts.append("语言:中英混用")

    # Avg message length
    lengths = profile.get("msg_lengths", [])
    if lengths:
        avg_len = sum(lengths) // len(lengths)
        parts.append(f"avg {avg_len}字/msg")

    # Top topics
    topics = profile.get("topics", {})
    if topics:
        top_topics = sorted(topics.items(), key=lambda x: -x[1])[:4]
        parts.append("topics:" + ">".join(t[0] for t in top_topics))

    # Common phrases
    phrases = profile.get("phrases", {})
    if phrases:
        top_phrases = sorted(phrases.items(), key=lambda x: -x[1])[:5]
        common = [p[0] for p in top_phrases if p[1] >= 2]
        if common:
            parts.append("常用:" + ",".join(f"'{c}'" for c in common[:4]))

    result = " | ".join(parts)
    return result[:max_chars]


def _classify_task(message: str) -> str:
    """Simple keyword-based task classification."""
    msg = message.lower()
    if any(w in msg for w in ["修复", "fix", "bug", "debug"]):
        return "code_fix"
    if any(w in msg for w in ["截图", "screenshot", "看看"]):
        return "screenshot"
    if any(w in msg for w in ["打开", "open", "浏览器", "chrome"]):
        return "browser"
    if any(w in msg for w in ["点击", "click", "操控", "控制"]):
        return "computer_control"
    if any(w in msg for w in ["写", "创建", "create", "write", "implement"]):
        return "code_create"
    if any(w in msg for w in ["分析", "analyze", "review"]):
        return "code_review"
    if any(w in msg for w in ["部署", "deploy", "上线"]):
        return "deploy"
    return "general"


# ─── Auto-Evolution Engine ────────────────────────────────────────────────────
# Connects scoring → training → prompt evolution automatically

_AUTO_TRAIN_COOLDOWN = 3600  # Don't auto-train more than once per hour
_AUTO_TRAIN_MIN_INTERACTIONS = 10  # Need at least 10 scores before judging
_AUTO_TRAIN_SCORE_THRESHOLD = 0.6  # Auto-train if avg score below this
_INTERACTION_COUNT = 0  # Counts since last auto-train check
# Initialize from disk to prevent auto-train triggering immediately on restart
_last_auto_train_time: float = time.time()  # Pretend we just trained to enforce cooldown after restart

# Which score flags map to which training domain
_FLAG_TO_DOMAIN = {
    "ASKED_QUESTION": "obedience",
    "TOO_VERBOSE": "obedience",
    "SLOW_FOR_SIMPLE": "computer_control",
    "OPUS_FOR_SIMPLE": "obedience",
}


def should_auto_train() -> dict | None:
    """Check if automatic training should be triggered.
    Returns {"domain": str, "reason": str} or None.
    Called after every interaction.
    """
    global _INTERACTION_COUNT, _last_auto_train_time
    _INTERACTION_COUNT += 1

    # Don't check every single interaction — check every 5
    if _INTERACTION_COUNT % 5 != 0:
        return None

    # Cooldown: don't auto-train more than once per hour
    import time
    if time.time() - _last_auto_train_time < _AUTO_TRAIN_COOLDOWN:
        return None

    scores = get_recent_scores(_AUTO_TRAIN_MIN_INTERACTIONS)
    if len(scores) < _AUTO_TRAIN_MIN_INTERACTIONS:
        return None

    # Check overall average
    avg_overall = sum(s.get("overall", 0) for s in scores) / len(scores)
    if avg_overall >= _AUTO_TRAIN_SCORE_THRESHOLD:
        return None  # Doing fine, no training needed

    # Find the worst dimension → map to training domain
    all_flags = []
    for s in scores:
        all_flags.extend(s.get("flags", []))

    # Count flag frequencies
    from collections import Counter
    flag_counts = Counter(all_flags)
    if not flag_counts:
        return {"domain": "obedience", "reason": f"avg score {avg_overall:.2f} < {_AUTO_TRAIN_SCORE_THRESHOLD}"}

    worst_flag = flag_counts.most_common(1)[0][0]
    domain = _FLAG_TO_DOMAIN.get(worst_flag, "obedience")

    return {
        "domain": domain,
        "reason": f"avg={avg_overall:.2f}, worst={worst_flag}({flag_counts[worst_flag]}x)",
    }


def mark_auto_trained():
    """Mark that auto-training just ran (reset cooldown)."""
    global _last_auto_train_time, _INTERACTION_COUNT
    import time
    _last_auto_train_time = time.time()
    _INTERACTION_COUNT = 0


def record_self_heal(error: str, diagnosis: str, fix: str, success: bool):
    """Record self-heal result into the learning system (not just a log file).
    This feeds self-heal outcomes back into the bot's memory so it can avoid
    the same errors in future."""
    if success:
        record_successful_workflow(
            task_type=f"self_heal:{error[:50]}",
            steps=[f"diagnosis:{diagnosis[:100]}", f"fix:{fix[:100]}"],
            duration_ms=0,
        )
    else:
        record_failure_pattern(
            task_type=f"self_heal:{error[:50]}",
            error=f"diagnosis:{diagnosis[:100]} fix_failed:{fix[:100]}",
        )

    # Also write to memory so bot is aware
    try:
        date = datetime.now().strftime("%Y-%m-%d %H:%M")
        result_str = "✅成功" if success else "❌失败"
        entry = f"\n## [{date}] 自愈 {result_str}\n- 错误: {error[:100]}\n- 诊断: {diagnosis[:100]}\n- 修复: {fix[:100]}\n"
        with open(MEMORY_FILE, "a", encoding="utf-8") as f:
            f.write(entry)
        # Prune to prevent unbounded memory file growth from frequent self-heals
        prune_memory(max_lines=100)
    except Exception:
        pass


def get_evolution_stats() -> str:
    """Get a human-readable stats report of the evolution system.
    Shows: interaction count, avg score trend, auto-train history, self-heal stats."""
    lines = ["📊 进化系统状态\n"]

    # Score trend
    scores = get_recent_scores(20)
    if scores:
        avg = sum(s.get("overall", 0) for s in scores) / len(scores)
        # Split into halves to show trend
        half = len(scores) // 2
        if half > 0:
            old_avg = sum(s.get("overall", 0) for s in scores[:half]) / half
            new_avg = sum(s.get("overall", 0) for s in scores[half:]) / (len(scores) - half)
            trend = "📈" if new_avg > old_avg + 0.05 else "📉" if new_avg < old_avg - 0.05 else "➡️"
            lines.append(f"评分: {avg:.0%} (近期{trend} {old_avg:.0%}→{new_avg:.0%})")
        else:
            lines.append(f"评分: {avg:.0%}")

        # Flags summary
        from collections import Counter
        all_flags = Counter()
        for s in scores:
            all_flags.update(s.get("flags", []))
        if all_flags:
            top_flags = ", ".join(f"{f}({c})" for f, c in all_flags.most_common(3))
            lines.append(f"常见问题: {top_flags}")
    else:
        lines.append("评分: 无数据")

    # Self-heal stats
    heal_log = os.path.join(BOT_DIR, ".self_heal.jsonl")
    try:
        if os.path.exists(heal_log):
            with open(heal_log, "r", encoding="utf-8") as f:
                heals = []
                for l in f.readlines()[-20:]:
                    if l.strip():
                        try:
                            heals.append(json.loads(l))
                        except json.JSONDecodeError:
                            pass
            total = len(heals)
            ok = sum(1 for h in heals if h.get("success"))
            lines.append(f"自愈: {ok}/{total} 成功 ({ok/total:.0%})" if total else "自愈: 无记录")
        else:
            lines.append("自愈: 无记录")
    except Exception:
        lines.append("自愈: 读取失败")

    # Auto-train status
    import time
    if _last_auto_train_time > 0:
        ago = int((time.time() - _last_auto_train_time) / 60)
        lines.append(f"上次自动训练: {ago}分钟前")
    else:
        lines.append("自动训练: 尚未触发")

    # Memory size
    try:
        if os.path.exists(MEMORY_FILE):
            mem_size = os.path.getsize(MEMORY_FILE)
            lines.append(f"记忆文件: {mem_size/1024:.1f}KB")
    except Exception:
        pass

    return "\n".join(lines)
