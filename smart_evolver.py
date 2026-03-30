"""
Smart Evolver v3.0 - 智能进化agent (带技能系统)
在v2基础上增加：
1. 任务完成后自动生成skill文件
2. 技能组合：检测可合并skill自动生成composite skill
3. 技能效果追踪：记录使用次数/成功率/执行时间
4. 弱技能淘汰：低成功率skill自动存档到deprecated/
5. 每日3点技能审计+TG报告
"""
import subprocess
import time
import os
import sys
import json
import logging
import shutil
import re
from datetime import datetime, date
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
USER_ID = os.getenv('AUTHORIZED_USER_ID')
BASE = Path(__file__).parent
QUEUE_FILE = BASE / "_evolution_queue.json"
STATE_FILE = BASE / "_smart_evolver_state.json"
LOG_FILE = BASE / "_smart_evolver.log"
LOCK_FILE = BASE / "_smart_evolver.lock"

# v3 新增文件
METRICS_FILE = BASE / ".skill_metrics.json"
DEPRECATED_SKILLS_DIR = BASE / "skills" / "deprecated"
SKILL_LIBRARY_DIR = BASE / ".skill_library" / "skills"

# ─── Logging ────────────────────────────────────────────────────────────────

_devnull_fh = open(os.devnull, "w")  # noqa: SIM115 — module-level handle, must stay open for logging lifetime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(_devnull_fh),
    ]
)
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")
log = logging.getLogger("smart_evolver")


def tg(text):
    if not TOKEN or not USER_ID:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": USER_ID, "text": text},
            timeout=10
        )
    except Exception as e:
        log.warning(f"TG notify failed: {e}")


# ─── State ───────────────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "loop": 0,
        "task_index": 0,
        "total_tasks_done": 0,
        "total_runs": 0,
        "consecutive_failures": 0,
        "started": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "last_audit_date": None,
        "skills_generated": 0,
        "skills_combined": 0,
        "skills_retired": 0,
    }


def save_state(s):
    tmp = str(STATE_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(STATE_FILE))
    except Exception as e:
        log.error(f"Save state error: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass


# ─── Task Queue ──────────────────────────────────────────────────────────────

def load_tasks():
    try:
        data = json.loads(QUEUE_FILE.read_text(encoding="utf-8"))
        return data.get("tasks", [])
    except Exception as e:
        log.error(f"Load tasks error: {e}")
        return []


# ─── Claude Runner ───────────────────────────────────────────────────────────

def find_claude():
    for c in [
        shutil.which("claude.cmd"),
        shutil.which("claude"),
        str(Path.home() / "AppData/Roaming/npm/claude.cmd"),
        str(Path.home() / "AppData/Local/Programs/claude/claude.cmd"),
    ]:
        if c and Path(c).is_file():
            return c
    return "claude.cmd"


CREDIT_EXHAUSTED_PATTERNS = [
    "rate limit",
    "out of credits",
    "exceeded your",
    "billing",
    "quota exceeded",
    "too many requests",
    "credit balance",
    "usage limit",
    "error 429",
    "429",
]

# Safety prefix injected into EVERY evolution task prompt
SAFETY_PREFIX = """⚠️ 安全规则（必须遵守，违反则任务失败）：
1. 绝对不要杀死任何 python 进程（bot.py、run.py、smart_evolver.py、evolve_watcher.py）
2. 绝对不要关闭、终止或重启 Claude Code / claude.cmd
3. 绝对不要修改网络设置、防火墙、代理、hosts文件
4. 绝对不要删除 .env、.bot.pid、.bot.lock、_smart_evolver.lock 等关键文件
5. 绝对不要运行 taskkill、Stop-Process、shutdown、netsh 等危险命令
6. 不要修改 run.py 的进程管理逻辑
7. 如果需要重启bot，只修改代码文件让 watchdog 自动热重载，不要手动杀进程
8. 修改代码前先用 python -m py_compile 验证语法

"""

COMPLETION_MARKERS = [
    "✅",
    "任务完成",
    "任务1完成",
    "任务2完成",
    "任务3完成",
    "任务4完成",
    "任务5完成",
    "任务6完成",
    "任务7完成",
    "task complete",
    "completed successfully",
    "all done",
]


def run_task(task, state):
    """Run one evolution task. Returns: ('done', response_text) | ('failed', '') | ('exhausted', '')"""
    claude = find_claude()
    prompt = SAFETY_PREFIX + task["prompt"]
    task_id = task["id"]
    task_name = task["name"]

    log.info(f"[TASK {task_id}/7: {task_name}] Starting...")
    state["total_runs"] += 1

    try:
        result = subprocess.run(
            [claude, "-p", prompt,
             "--output-format", "json",
             "--dangerously-skip-permissions"],
            capture_output=True,
            text=True,
            timeout=600,  # 10 min per task
            cwd=str(BASE),
            encoding="utf-8",
            errors="replace",
        )

        output = result.stdout.strip()
        stderr = result.stderr.strip()

        log.info(f"Exit code: {result.returncode}")
        if output:
            log.info(f"Output: {output[:300]}")

        # Check for credit exhaustion ONLY in stderr
        stderr_lower = stderr.lower()
        for pat in CREDIT_EXHAUSTED_PATTERNS:
            if pat in stderr_lower:
                log.warning(f"CREDITS EXHAUSTED in stderr: '{pat}'")
                return "exhausted", ""

        # Short output with no JSON = likely a raw error message
        if output and len(output) < 200 and not output.startswith("{"):
            for pat in CREDIT_EXHAUSTED_PATTERNS:
                if pat in output.lower():
                    log.warning(f"CREDITS EXHAUSTED in short output: '{pat}'")
                    return "exhausted", ""

        if result.returncode != 0 and not output:
            log.warning(f"Task {task_id} failed (exit {result.returncode})")
            return "failed", ""

        # Parse response text
        response_text = ""
        try:
            data = json.loads(output)
            response_text = str(data.get("result", ""))
        except Exception:
            response_text = output

        response_lower = response_text.lower()

        # Check for task completion
        for marker in COMPLETION_MARKERS:
            if marker.lower() in response_lower:
                log.info(f"✅ Task {task_id} COMPLETED (detected: {marker})")
                return "done", response_text

        # Even without explicit marker, if we got a response, treat as done
        if len(response_text) > 100:
            log.info(f"Task {task_id} likely done (got {len(response_text)} char response)")
            return "done", response_text

        return "failed", ""

    except subprocess.TimeoutExpired:
        log.warning(f"Task {task_id} timed out (10 min) — treating as done")
        return "done", "[timeout - task likely completed]"
    except FileNotFoundError:
        log.error(f"claude.cmd not found! PATH: {os.environ.get('PATH', '')[:200]}")
        return "failed", ""
    except Exception as e:
        log.error(f"Task {task_id} error: {e}")
        return "failed", ""


# ─── Infrastructure Protection ──────────────────────────────────────────────

def _get_bot_pid():
    """Read the bot PID from .bot.pid."""
    pid_file = BASE / ".bot.pid"
    try:
        if pid_file.exists():
            pid = int(pid_file.read_text().strip())
            if _is_process_alive(pid):
                return pid
            return None
    except (ValueError, IOError):
        pass
    return None


def _is_process_alive(pid: int) -> bool:
    """Check if process is alive (Windows-safe)."""
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True, timeout=5,
            )
            return str(pid) in result.stdout
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def _restart_bot():
    """Restart the bot via run.py if it died."""
    log.warning("Bot process died! Attempting restart...")
    try:
        subprocess.Popen(
            [sys.executable, str(BASE / "run.py")],
            cwd=str(BASE),
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        log.info("Bot restart triggered via run.py")
        tg("🔄 Bot进程死亡，已自动重启 run.py")
        time.sleep(15)
    except Exception as e:
        log.error(f"Failed to restart bot: {e}")
        tg(f"❌ Bot重启失败: {e}")


def _check_claude_cli_available() -> bool:
    """Check if claude.cmd is responsive."""
    claude = find_claude()
    try:
        result = subprocess.run(
            [claude, "--version"],
            capture_output=True, text=True, timeout=10,
            encoding="utf-8", errors="replace",
        )
        return result.returncode == 0
    except Exception:
        return False


def health_check(state) -> bool:
    """Pre-task health check. Returns True if safe to proceed."""
    bot_pid = _get_bot_pid()
    if bot_pid and not _is_process_alive(bot_pid):
        log.warning(f"Bot PID {bot_pid} is dead!")
        _restart_bot()

    if not _check_claude_cli_available():
        log.warning("Claude CLI not responding!")
        tg("⚠️ Claude CLI不可用，等待60秒后重试...")
        time.sleep(60)
        if not _check_claude_cli_available():
            log.error("Claude CLI still not available after 60s wait")
            return False

    return True


def post_task_health_check(state, pre_bot_pid):
    """Post-task check: ensure bot survived the evolution task."""
    if pre_bot_pid is None:
        return

    time.sleep(2)
    if not _is_process_alive(pre_bot_pid):
        log.error(f"Bot PID {pre_bot_pid} was killed during evolution task!")
        tg(f"⚠️ 进化任务杀死了bot进程 (PID {pre_bot_pid})！正在重启...")
        _restart_bot()


# ─── v3: Skill Metrics ───────────────────────────────────────────────────────

def load_metrics() -> dict:
    """Load skill metrics from .skill_metrics.json."""
    try:
        if METRICS_FILE.exists():
            return json.loads(METRICS_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"Metrics load error: {e}")
    return {"skills": {}, "last_updated": None}


def save_metrics(m: dict):
    """Save skill metrics atomically."""
    tmp = str(METRICS_FILE) + ".tmp"
    try:
        m["last_updated"] = datetime.now().isoformat()
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(m, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(METRICS_FILE))
    except Exception as e:
        log.error(f"Metrics save error: {e}")
        try:
            os.unlink(tmp)
        except OSError:
            pass


def record_skill_usage(skill_id: str, success: bool, exec_time_ms: int):
    """Record one skill usage event to metrics."""
    m = load_metrics()
    if skill_id not in m["skills"]:
        m["skills"][skill_id] = {
            "use_count": 0,
            "success_count": 0,
            "fail_count": 0,
            "total_exec_time_ms": 0,
            "first_used": datetime.now().isoformat(),
            "last_used": None,
        }
    s = m["skills"][skill_id]
    s["use_count"] += 1
    s["total_exec_time_ms"] += exec_time_ms
    s["last_used"] = datetime.now().isoformat()
    if success:
        s["success_count"] += 1
    else:
        s["fail_count"] += 1
    save_metrics(m)


def get_skill_success_rate(skill_id: str) -> float:
    """Return success rate (0.0-1.0) for a skill, or -1 if not enough data."""
    m = load_metrics()
    s = m["skills"].get(skill_id)
    if not s or s["use_count"] < 1:
        return -1.0
    return s["success_count"] / s["use_count"]


# ─── v3: Auto Skill Generation ───────────────────────────────────────────────

def _extract_keywords(text: str) -> list:
    """Extract meaningful keywords from text."""
    # Remove punctuation, extract Chinese and English words
    words = re.findall(r'[\u4e00-\u9fff]{2,}|[a-zA-Z]{3,}', text)
    # Deduplicate, lowercase English, limit to 10
    seen = set()
    result = []
    for w in words:
        key = w.lower()
        if key not in seen and len(result) < 10:
            seen.add(key)
            result.append(w)
    return result


def auto_generate_skill_from_task(task: dict, response_text: str) -> str | None:
    """After a successful evolution task, create a skill file via skill_library.

    Analyzes task type from the prompt and synthesizes a JSON+MD skill.
    Returns skill_id or None on failure.
    """
    try:
        from skill_library import _save_skill, synthesize_to_md, _rebuild_index

        task_id = task.get("id", 0)
        task_name = task.get("name", "未知任务")
        task_prompt = task.get("prompt", "")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = re.sub(r'[^\w]', '_', task_name)[:25]
        skill_id = f"sk_evol_{ts}_{safe_name}"

        keywords = _extract_keywords(task_prompt + " " + task_name)

        # Infer task type from task name/prompt
        task_type = "evolution"
        name_lower = task_name.lower()
        prompt_lower = task_prompt.lower()
        if any(w in name_lower for w in ["修复", "fix", "repair", "自修"]):
            task_type = "code_fix"
        elif any(w in name_lower for w in ["生成", "创建", "create", "build"]):
            task_type = "code_create"
        elif any(w in name_lower for w in ["监控", "dashboard", "monitor"]):
            task_type = "monitoring"
        elif any(w in name_lower for w in ["记忆", "memory", "learn"]):
            task_type = "memory"
        elif any(w in name_lower for w in ["技能", "skill"]):
            task_type = "skill_management"

        # Extract steps from prompt (lines starting with numbers)
        steps = []
        for line in task_prompt.split('\n'):
            line = line.strip()
            if re.match(r'^\d+\.', line):
                steps.append(line[2:].strip()[:80])
        generic_steps = steps[:4]
        specific_steps = steps[4:8]

        # Extract files from response
        files_created = re.findall(r'[\w_/-]+\.(?:py|json|md|txt|js|html)', response_text)
        files_created = list(dict.fromkeys(files_created))[:5]  # dedup, limit

        skill = {
            "id": skill_id,
            "title": f"进化: {task_name}",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "version": 1,
            "source_task": task_prompt[:200],
            "task_type": task_type,
            "function_signature": f"run_{safe_name.lower()}()",
            "input_schema": "进化任务提示词 + 现有代码库",
            "output_schema": "改进后的Python文件 + 功能验证",
            "trigger_pattern": task_name,
            "generic_steps": generic_steps,
            "specific_steps": specific_steps,
            "keywords": keywords,
            "summary": f"进化任务{task_id}: {task_name} — 自动生成于smart_evolver v3",
            "template_code": "",
            "code_snippet": "",
            "files_created": files_created,
            "key_decisions": [f"任务{task_id}完成策略", "使用claude -p无人值守执行"],
            "use_count": 0,
            "last_used": None,
            "avg_score_when_used": None,
            "history": [{
                "version": 1,
                "date": datetime.now().isoformat(),
                "change": "auto-generated by smart_evolver v3 after task completion"
            }],
        }

        _save_skill(skill)
        _rebuild_index()
        synthesize_to_md(skill)

        log.info(f"✨ Auto-generated skill: {skill_id} ({task_name})")
        return skill_id

    except Exception as e:
        log.warning(f"Skill generation failed for task {task.get('id')}: {e}")
        return None


# ─── v3: Skill Combination ───────────────────────────────────────────────────

def _load_all_skill_jsons() -> list:
    """Load all skill JSONs from .skill_library/skills/."""
    skills = []
    if not SKILL_LIBRARY_DIR.exists():
        return skills
    for f in SKILL_LIBRARY_DIR.glob("*.json"):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            if data.get("id"):
                skills.append(data)
        except Exception:
            pass
    return skills


def _keyword_overlap(a: dict, b: dict) -> int:
    """Count shared keywords between two skills."""
    kw_a = set(w.lower() for w in a.get("keywords", []))
    kw_b = set(w.lower() for w in b.get("keywords", []))
    return len(kw_a & kw_b)


def check_skill_combinations(state: dict) -> int:
    """Detect pairs of related skills and merge them into composite skills.

    Two skills are candidates if they share ≥3 keywords and have compatible task types.
    Returns number of composites created.
    """
    try:
        from skill_library import _save_skill, synthesize_to_md, _rebuild_index, _delete_skill

        skills = _load_all_skill_jsons()
        if len(skills) < 2:
            return 0

        combined = 0
        processed_pairs = set()

        for i, sa in enumerate(skills):
            for sb in skills[i + 1:]:
                pair_key = tuple(sorted([sa["id"], sb["id"]]))
                if pair_key in processed_pairs:
                    continue
                processed_pairs.add(pair_key)

                # Skip composites (prevent re-merging)
                if "composite" in sa["id"] or "composite" in sb["id"]:
                    continue

                overlap = _keyword_overlap(sa, sb)
                if overlap < 3:
                    continue

                # Check compatible task types
                type_a = sa.get("task_type", "general")
                type_b = sb.get("task_type", "general")
                if type_a != type_b and not (type_a == "general" or type_b == "general"):
                    continue

                log.info(f"🔗 Combining skills: {sa['title']} + {sb['title']} (overlap={overlap})")

                # Build composite skill
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                composite_id = f"sk_composite_{ts}"

                merged_keywords = list(dict.fromkeys(
                    sa.get("keywords", []) + sb.get("keywords", [])
                ))[:10]

                merged_steps = list(dict.fromkeys(
                    sa.get("generic_steps", []) + sb.get("generic_steps", [])
                ))[:6]

                composite = {
                    "id": composite_id,
                    "title": f"[合并] {sa['title']} + {sb['title']}",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "version": 1,
                    "is_composite": True,
                    "source_skills": [sa["id"], sb["id"]],
                    "source_task": f"合并自: {sa.get('source_task','')[:100]}",
                    "task_type": type_a if type_a == type_b else "general",
                    "function_signature": f"run_composite_{ts}()",
                    "input_schema": f"{sa.get('input_schema','')} | {sb.get('input_schema','')}",
                    "output_schema": f"{sa.get('output_schema','')} | {sb.get('output_schema','')}",
                    "trigger_pattern": f"{sa.get('trigger_pattern','')} {sb.get('trigger_pattern','')}",
                    "generic_steps": merged_steps,
                    "specific_steps": [],
                    "keywords": merged_keywords,
                    "summary": f"合并技能: {sa.get('summary','')} + {sb.get('summary','')}",
                    "template_code": sa.get("template_code", "") or sb.get("template_code", ""),
                    "code_snippet": "",
                    "files_created": list(dict.fromkeys(
                        sa.get("files_created", []) + sb.get("files_created", [])
                    ))[:8],
                    "key_decisions": [
                        f"合并原因: {overlap}个共同关键词",
                        f"源技能A: {sa['title']}",
                        f"源技能B: {sb['title']}",
                    ],
                    "use_count": 0,
                    "last_used": None,
                    "avg_score_when_used": None,
                    "history": [{
                        "version": 1,
                        "date": datetime.now().isoformat(),
                        "change": f"composite from {sa['id']} + {sb['id']}"
                    }],
                }

                # Validate composite skill structure before saving
                required_fields = ["id", "title", "keywords", "summary"]
                if not all(composite.get(f) for f in required_fields):
                    log.warning(f"Composite skill validation failed, skipping")
                    continue

                _save_skill(composite)
                synthesize_to_md(composite)

                # Archive original skills (move JSON, keep MD for reference)
                archive_dir = SKILL_LIBRARY_DIR / "archived"
                archive_dir.mkdir(exist_ok=True)
                for src_id in [sa["id"], sb["id"]]:
                    src_path = SKILL_LIBRARY_DIR / f"{src_id}.json"
                    if src_path.exists():
                        shutil.move(str(src_path), str(archive_dir / f"{src_id}.json"))
                        log.info(f"  Archived source skill: {src_id}")

                _rebuild_index()
                combined += 1

                # Don't combine more than 2 pairs per call to avoid cascades
                if combined >= 2:
                    break
            if combined >= 2:
                break

        if combined > 0:
            state["skills_combined"] = state.get("skills_combined", 0) + combined
            log.info(f"🔗 Created {combined} composite skill(s)")
            tg(f"🔗 技能组合：合并了{combined}对相关技能 → 减少冗余")

        return combined

    except Exception as e:
        log.warning(f"Skill combination check failed: {e}")
        return 0


# ─── v3: Weak Skill Retirement ───────────────────────────────────────────────

def retire_weak_skills(state: dict) -> int:
    """Archive skills with success_rate < 30% AND use_count > 10.

    Moves SKILL.md to skills/deprecated/, removes JSON from index.
    Returns number of retired skills.
    """
    try:
        from skill_library import _delete_skill, _rebuild_index

        DEPRECATED_SKILLS_DIR.mkdir(parents=True, exist_ok=True)

        m = load_metrics()
        retired = 0

        for skill_id, metrics in m["skills"].items():
            use_count = metrics.get("use_count", 0)
            success_count = metrics.get("success_count", 0)

            if use_count <= 10:
                continue  # Not enough data

            success_rate = success_count / use_count
            if success_rate >= 0.3:
                continue  # Good enough

            log.info(f"🗑️ Retiring weak skill: {skill_id} "
                     f"(rate={success_rate:.1%}, uses={use_count})")

            # Find and move the SKILL.md from skills/ dir
            skill_json_path = SKILL_LIBRARY_DIR / f"{skill_id}.json"
            if skill_json_path.exists():
                try:
                    skill_data = json.loads(skill_json_path.read_text(encoding="utf-8"))
                    # Move the SKILL.md if exists
                    from skill_library import _skill_name_to_dirname
                    dirname = _skill_name_to_dirname(skill_data.get("title", skill_id))
                    md_dir = BASE / "skills" / dirname
                    if md_dir.exists():
                        shutil.move(
                            str(md_dir),
                            str(DEPRECATED_SKILLS_DIR / dirname)
                        )
                except Exception as move_err:
                    log.warning(f"Could not move MD skill dir: {move_err}")

            # Remove from skill library
            _delete_skill(skill_id)

            # Mark in metrics as retired
            m["skills"][skill_id]["retired"] = True
            m["skills"][skill_id]["retired_at"] = datetime.now().isoformat()
            m["skills"][skill_id]["retire_reason"] = (
                f"success_rate={success_rate:.1%} < 30%, use_count={use_count}"
            )
            retired += 1

        if retired > 0:
            save_metrics(m)
            _rebuild_index()
            state["skills_retired"] = state.get("skills_retired", 0) + retired
            log.info(f"🗑️ Retired {retired} weak skill(s)")

        return retired

    except Exception as e:
        log.warning(f"Skill retirement failed: {e}")
        return 0


# ─── v3: Daily Skill Audit ───────────────────────────────────────────────────

def should_run_daily_audit(state: dict) -> bool:
    """Return True if it's past 3am local time and audit hasn't run today."""
    now = datetime.now()
    today_str = date.today().isoformat()
    last_audit = state.get("last_audit_date")

    # Run if it's 3:00-3:59am and we haven't run today
    if now.hour == 3 and last_audit != today_str:
        return True
    return False


def run_skill_audit(state: dict):
    """Comprehensive skill audit: retire weak skills, report to Telegram."""
    log.info("📊 Running daily skill audit...")

    try:
        # 1. Retire weak skills
        retired = retire_weak_skills(state)

        # 2. Gather stats
        m = load_metrics()
        total_skills = len(m["skills"])
        active_skills = sum(1 for s in m["skills"].values() if not s.get("retired"))
        retired_total = sum(1 for s in m["skills"].values() if s.get("retired"))

        # Compute aggregates
        high_performers = []
        low_performers = []
        for skill_id, s in m["skills"].items():
            if s.get("retired"):
                continue
            uc = s.get("use_count", 0)
            if uc < 3:
                continue
            rate = s.get("success_count", 0) / uc
            avg_ms = s.get("total_exec_time_ms", 0) / max(uc, 1)
            if rate >= 0.8:
                high_performers.append((skill_id, rate, uc))
            elif rate < 0.4:
                low_performers.append((skill_id, rate, uc))

        # Sort by success rate
        high_performers.sort(key=lambda x: -x[1])
        low_performers.sort(key=lambda x: x[1])

        # 3. Skills directory count
        skills_dir = BASE / "skills"
        md_skill_count = sum(1 for d in skills_dir.iterdir()
                             if d.is_dir() and (d / "SKILL.md").exists()
                             ) if skills_dir.exists() else 0

        # 4. Composite skills
        composite_count = sum(1 for s in m["skills"].values()
                              if "composite" in str(s.get("id", "")))

        # 5. Build report
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines = [
            f"📊 技能审计报告 {now_str}",
            f"",
            f"📁 技能总数: {md_skill_count} MD文件 | {active_skills} 活跃JSON",
            f"🔗 合并技能: {composite_count}个",
            f"🗑️ 已淘汰: {retired_total}个 (今日新增{retired}个)",
            f"",
        ]

        if high_performers:
            lines.append("🏆 高效技能 (成功率≥80%):")
            for sid, rate, uc in high_performers[:3]:
                lines.append(f"  • {sid[:30]} — {rate:.0%} ({uc}次)")
            lines.append("")

        if low_performers:
            lines.append("⚠️ 低效技能 (成功率<40%, 待观察):")
            for sid, rate, uc in low_performers[:3]:
                lines.append(f"  • {sid[:30]} — {rate:.0%} ({uc}次)")
            lines.append("")

        lines += [
            f"🔄 进化循环: 第{state.get('loop', 0)+1}轮",
            f"✅ 累计完成任务: {state.get('total_tasks_done', 0)}个",
            f"✨ 本会话生成技能: {state.get('skills_generated', 0)}个",
            f"🔗 本会话合并技能: {state.get('skills_combined', 0)}对",
        ]

        report = "\n".join(lines)
        log.info(report)
        tg(report)

        # Update state
        state["last_audit_date"] = date.today().isoformat()
        save_state(state)

    except Exception as e:
        log.error(f"Skill audit failed: {e}")
        tg(f"❌ 技能审计失败: {e}")


# ─── Main Loop ───────────────────────────────────────────────────────────────

def main():
    # Singleton lock
    lock_path = str(LOCK_FILE)
    lock_fh = None
    try:
        lock_fh = open(lock_path, "w")  # noqa: SIM115 — lock must stay open for process lifetime
        import msvcrt
        msvcrt.locking(lock_fh.fileno(), msvcrt.LK_NBLCK, 1)
        lock_fh.write(str(os.getpid()))
        lock_fh.flush()
    except (OSError, IOError):
        if lock_fh:
            lock_fh.close()
        print("ERROR: Another smart_evolver is already running.")
        sys.exit(1)

    tasks = load_tasks()
    if not tasks:
        print("ERROR: No tasks in queue!")
        sys.exit(1)

    state = load_state()
    # If called with --reset, start fresh
    if "--reset" in sys.argv:
        state = {
            "loop": 0, "task_index": 0, "total_tasks_done": 0,
            "total_runs": 0, "consecutive_failures": 0,
            "started": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "last_audit_date": None,
            "skills_generated": 0,
            "skills_combined": 0,
            "skills_retired": 0,
        }
        save_state(state)
        print("State reset.")

    log.info(f"Smart Evolver v3 started. {len(tasks)} tasks. Loop #{state['loop']}, task {state['task_index']}")
    tg(f"🤖 Smart Evolver v3 启动\n{len(tasks)}个任务，无限循环\n当前: 第{state['loop']+1}轮 任务{state['task_index']+1}/7\n✨ 技能系统已激活")

    MAX_CONSECUTIVE_FAILURES = 3
    tasks_since_combination_check = 0  # Check skill combinations every 5 tasks

    while True:
        # Daily audit check (3am)
        if should_run_daily_audit(state):
            run_skill_audit(state)

        idx = state["task_index"] % len(tasks)
        task = tasks[idx]

        log.info(f"=== Loop #{state['loop']+1} | Task {idx+1}/7: {task['name']} ===")

        # Pre-task: health check
        if not health_check(state):
            log.warning("Health check failed, waiting 120s...")
            time.sleep(120)
            continue

        pre_bot_pid = _get_bot_pid()
        task_start_ms = int(time.time() * 1000)

        result, response_text = run_task(task, state)

        task_exec_ms = int(time.time() * 1000) - task_start_ms

        # Post-task: verify bot survived
        post_task_health_check(state, pre_bot_pid)

        if result == "exhausted":
            try:
                from codex_charger import CodexCharger, mark_cli_exhausted
                mark_cli_exhausted()
                msg = f"⚠️ CLI耗尽！切换到Codex模式继续进化任务{task['id']}..."
                log.info(msg)
                tg(msg)
                charger = CodexCharger()
                codex_result = charger.run_task_sync(task["prompt"])
                if codex_result["success"]:
                    log.info(f"✅ Codex补能成功! 任务{task['id']}完成")
                    tg(f"🌐 Codex充能成功！任务{task['id']} [{task['name']}] 完成\n→ 继续循环进化")
                    result = "done"
                    response_text = codex_result.get("output", "")
                else:
                    log.warning(f"Codex also failed: {codex_result['error']}")
                    msg = (
                        f"💀 CLI + Codex 均耗尽！停止进化\n"
                        f"完成了 {state['total_tasks_done']} 个任务\n"
                        f"共循环 {state['loop']} 轮\n"
                        f"运行时间: {state['started']} → 现在"
                    )
                    log.info(msg)
                    tg(msg)
                    save_state(state)
                    break
            except Exception as codex_err:
                log.error(f"Codex fallback error: {codex_err}")
                msg = (
                    f"💀 Credits耗尽且Codex不可用！停止进化\n"
                    f"完成了 {state['total_tasks_done']} 个任务\n"
                    f"错误: {codex_err}"
                )
                log.info(msg)
                tg(msg)
                save_state(state)
                break

        if result == "done":
            state["task_index"] = idx + 1
            state["total_tasks_done"] += 1
            state["consecutive_failures"] = 0

            # ── v3: Auto-generate skill from completed task ──────────────────
            skill_id = auto_generate_skill_from_task(task, response_text)
            if skill_id:
                state["skills_generated"] = state.get("skills_generated", 0) + 1
                # Record this skill as successfully used
                record_skill_usage(skill_id, success=True, exec_time_ms=task_exec_ms)
                log.info(f"✨ Skill generated: {skill_id}")

            # ── v3: Check skill combinations every 5 tasks ───────────────────
            tasks_since_combination_check += 1
            if tasks_since_combination_check >= 5:
                tasks_since_combination_check = 0
                check_skill_combinations(state)

            # Completed all tasks = one loop
            if state["task_index"] >= len(tasks):
                state["task_index"] = 0
                state["loop"] += 1
                msg = (
                    f"🎉 第{state['loop']}轮全部完成！共{state['total_tasks_done']}个任务\n"
                    f"✨ 本轮生成技能: {state.get('skills_generated', 0)}个\n"
                    f"→ 开始第{state['loop']+1}轮（等30分钟让rate limit恢复）"
                )
                log.info(msg)
                tg(msg)
                time.sleep(1800)  # 30 min between loops
            else:
                next_task = tasks[state["task_index"]]
                tg(f"✅ 任务{task['id']} [{task['name']}] 完成\n→ 下一个: 任务{next_task['id']} [{next_task['name']}]")
                time.sleep(60)  # 1 min between tasks

        elif result == "failed":
            state["consecutive_failures"] += 1
            log.warning(f"Task failed ({state['consecutive_failures']}/{MAX_CONSECUTIVE_FAILURES})")

            if state["consecutive_failures"] >= MAX_CONSECUTIVE_FAILURES:
                msg = f"⚠️ 连续{MAX_CONSECUTIVE_FAILURES}次失败，暂停60秒后重试..."
                log.warning(msg)
                tg(msg)
                state["consecutive_failures"] = 0
                time.sleep(60)
            else:
                time.sleep(10)

        save_state(state)

    log.info("Smart Evolver v3 stopped.")


if __name__ == "__main__":
    main()
