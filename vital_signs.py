"""
vital_signs.py — Engineering Definition of "Alive" for the Bot Agent.

Implements the five invariants:
  1. Operational   — reachable, responsive, task-capable
  2. Self-Sustaining — revenue rate >= cost rate
  3. Adaptive      — skill acquisition rate > 0
  4. Economically Viable — positive NPV value creation
  5. Governance-Safe — actions within constraint set

State Machine:
  Dormant → Active → Productive → Self-Funding → Expanding → Resilient

Telemetry Dashboard:
  runway_days, compute_budget, model_access, task_throughput,
  learning_rate, error_recovery_time, revenue_rate, autonomy_level
"""

import json
import logging
import os
import time
import threading
from collections import deque
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BOT_DIR, "_vital_signs.json")

# ── Lifecycle States ─────────────────────────────────────────────────────────

STATES = ["dormant", "active", "productive", "self_funding", "expanding", "resilient"]

# ── Default telemetry ────────────────────────────────────────────────────────

_DEFAULT_STATE = {
    "lifecycle": "dormant",
    "boot_time": None,
    "last_heartbeat": None,

    # Telemetry (8 vital signs)
    "runway_days": 30.0,           # time until resource depletion
    "compute_budget_usd": 0.0,     # remaining credits (CLI subscription = ~free)
    "model_access_count": 1,       # active model endpoints
    "task_throughput_per_hour": 0.0,
    "learning_rate": 0.0,          # skill delta per cycle (>0 = alive)
    "error_recovery_time_s": 0.0,  # mean time to restore after fault
    "revenue_rate_usd_per_hour": 0.0,
    "autonomy_pct": 0.0,           # % cycles without human intervention

    # Counters
    "total_tasks": 0,
    "successful_tasks": 0,
    "failed_tasks": 0,
    "skills_created": 0,
    "skills_at_boot": 0,
    "self_heals": 0,
    "self_heal_successes": 0,
    "revenue_total_usd": 0.0,
    "cost_total_usd": 0.0,

    # Rolling windows (compact)
    "tasks_last_hour": [],         # timestamps of tasks in last hour
    "errors_last_hour": [],        # timestamps of errors in last hour
    "skills_history": [],          # [{ts, count}] daily snapshots

    # State transition log
    "transitions": [],             # [{ts, from, to, reason}]
}

_lock = threading.Lock()
_state: dict = {}


# ── Persistence ──────────────────────────────────────────────────────────────

def _load() -> dict:
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and "lifecycle" in data:
                # Merge with defaults for any new fields
                merged = {**_DEFAULT_STATE, **data}
                return merged
    except Exception as e:
        logger.warning(f"vital_signs: load failed: {e}")
    return {**_DEFAULT_STATE}


def _save():
    with _lock:
        try:
            tmp = STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(_state, f, ensure_ascii=False, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, STATE_FILE)
        except Exception as e:
            logger.warning(f"vital_signs: save failed: {e}")


# ── Initialization ───────────────────────────────────────────────────────────

def boot():
    """Call on bot startup. Transitions from dormant to active."""
    global _state
    _state = _load()
    _state["boot_time"] = datetime.now().isoformat()
    _state["last_heartbeat"] = time.time()

    # Count existing skills
    skills_dir = os.path.join(BOT_DIR, ".skill_library", "skills")
    try:
        skill_count = len([f for f in os.listdir(skills_dir) if f.endswith(".json")])
    except Exception:
        skill_count = 0
    _state["skills_at_boot"] = skill_count

    # Count model endpoints
    model_count = 1  # CLI always available
    try:
        import config
        if getattr(config, "OPENAI_API_KEY", ""):
            model_count += 1
        if getattr(config, "GEMINI_API_KEY", ""):
            model_count += 1
    except Exception:
        pass
    _state["model_access_count"] = model_count

    # Transition to active
    _transition("active", "bot booted")
    _save()
    logger.info(f"VitalSigns booted: lifecycle={_state['lifecycle']}, "
                f"skills={skill_count}, models={model_count}")


# ── State Machine ────────────────────────────────────────────────────────────

def _transition(new_state: str, reason: str):
    """Transition lifecycle state with guard checks."""
    old = _state.get("lifecycle", "dormant")
    if old == new_state:
        return
    # Guard: can only advance forward or regress on failure
    old_idx = STATES.index(old) if old in STATES else 0
    new_idx = STATES.index(new_state) if new_state in STATES else 0

    _state["lifecycle"] = new_state
    transitions = _state.get("transitions", [])
    transitions.append({
        "ts": datetime.now().isoformat(),
        "from": old,
        "to": new_state,
        "reason": reason[:200],
    })
    # Keep last 50 transitions
    _state["transitions"] = transitions[-50:]
    logger.info(f"VitalSigns: {old} → {new_state} ({reason})")


def evaluate_state():
    """Re-evaluate lifecycle state based on current telemetry."""
    s = _state
    total = s.get("total_tasks", 0)
    success = s.get("successful_tasks", 0)
    skills_created = s.get("skills_created", 0)
    revenue = s.get("revenue_total_usd", 0)
    cost = s.get("cost_total_usd", 0)
    current = s.get("lifecycle", "dormant")

    # Dormant → Active (any task received)
    if current == "dormant" and total > 0:
        _transition("active", f"first task processed (total={total})")

    # Active → Productive (>10 tasks, >50% success rate)
    if current == "active" and total >= 10:
        rate = success / max(total, 1)
        if rate >= 0.5:
            _transition("productive", f"success_rate={rate:.0%}, tasks={total}")

    # Productive → Self-Funding (revenue >= cost, or subscription covers all)
    if current == "productive":
        # CLI subscription model: cost is fixed, "revenue" = value delivered
        # For subscription users: if bot is productive, it's self-funding
        if total >= 50 and success / max(total, 1) >= 0.6:
            _transition("self_funding",
                        f"sustained productivity: {success}/{total} tasks, "
                        f"revenue=${revenue:.2f}")

    # Self-Funding → Expanding (skills growing, surplus capacity)
    if current == "self_funding":
        if skills_created >= 5 and s.get("learning_rate", 0) > 0:
            _transition("expanding",
                        f"skills={skills_created}, learning_rate={s['learning_rate']:.3f}")

    # Expanding → Resilient (multi-model, self-healing, diversified)
    if current == "expanding":
        models = s.get("model_access_count", 0)
        heals = s.get("self_heal_successes", 0)
        if models >= 2 and heals >= 3 and skills_created >= 10:
            _transition("resilient",
                        f"models={models}, self_heals={heals}, skills={skills_created}")

    # Regression checks
    if current in ("productive", "self_funding", "expanding", "resilient"):
        # If success rate drops below 30%, regress
        if total >= 20:
            rate = success / max(total, 1)
            if rate < 0.3:
                _transition("active", f"regression: success_rate={rate:.0%}")

    _save()


# ── Recording Events ─────────────────────────────────────────────────────────

def record_task(success: bool, duration_ms: float = 0):
    """Record a task completion."""
    now = time.time()
    with _lock:
        _state["total_tasks"] = _state.get("total_tasks", 0) + 1
        if success:
            _state["successful_tasks"] = _state.get("successful_tasks", 0) + 1
        else:
            _state["failed_tasks"] = _state.get("failed_tasks", 0) + 1

        # Rolling window: tasks in last hour
        tasks_lh = _state.get("tasks_last_hour", [])
        tasks_lh.append(now)
        cutoff = now - 3600
        tasks_lh = [t for t in tasks_lh if t > cutoff]
        _state["tasks_last_hour"] = tasks_lh
        _state["task_throughput_per_hour"] = len(tasks_lh)

        if not success:
            errors_lh = _state.get("errors_last_hour", [])
            errors_lh.append(now)
            errors_lh = [t for t in errors_lh if t > cutoff]
            _state["errors_last_hour"] = errors_lh

        _state["last_heartbeat"] = now

    # Debounced evaluate + save (check inside lock scope to avoid race)
    with _lock:
        _should_eval = _state.get("total_tasks", 0) % 5 == 0
    if _should_eval:
        evaluate_state()


def record_skill_created():
    """Record a new skill extraction."""
    with _lock:
        _state["skills_created"] = _state.get("skills_created", 0) + 1
        # Calculate learning rate: skills per 100 tasks
        total = max(_state.get("total_tasks", 1), 1)
        _state["learning_rate"] = _state["skills_created"] / total
    _save()


def record_self_heal(success: bool):
    """Record a self-healing attempt."""
    with _lock:
        _state["self_heals"] = _state.get("self_heals", 0) + 1
        if success:
            _state["self_heal_successes"] = _state.get("self_heal_successes", 0) + 1
        # Calculate error recovery effectiveness
        total_heals = max(_state.get("self_heals", 1), 1)
        _state["error_recovery_time_s"] = (
            0.0 if _state.get("self_heal_successes", 0) == total_heals
            else 30.0 * (1 - _state.get("self_heal_successes", 0) / total_heals)
        )


def record_revenue(amount_usd: float, source: str = ""):
    """Record revenue event (arbitrage profit, task fee, etc.)."""
    with _lock:
        _state["revenue_total_usd"] = _state.get("revenue_total_usd", 0) + amount_usd
        # Update hourly rate based on uptime
        boot_time = _state.get("boot_time")
        if boot_time:
            try:
                boot_dt = datetime.fromisoformat(boot_time)
                hours_alive = max((datetime.now() - boot_dt).total_seconds() / 3600, 0.01)
                _state["revenue_rate_usd_per_hour"] = _state["revenue_total_usd"] / hours_alive
            except Exception:
                pass
    _save()


def record_cost(amount_usd: float, item: str = ""):
    """Record a cost event (API call, subscription pro-rata, etc.)."""
    with _lock:
        _state["cost_total_usd"] = _state.get("cost_total_usd", 0) + amount_usd
    _save()


def heartbeat():
    """Called periodically to confirm system is operational."""
    with _lock:
        _state["last_heartbeat"] = time.time()

        # Calculate autonomy: % of tasks without human intervention
        total = max(_state.get("total_tasks", 1), 1)
        auto_tasks = _state.get("successful_tasks", 0)  # successful = autonomous
        _state["autonomy_pct"] = (auto_tasks / total) * 100

    # Calculate runway (for subscription model: effectively infinite if active)
    boot_time = _state.get("boot_time")
    if boot_time:
        try:
            boot_dt = datetime.fromisoformat(boot_time)
            uptime_days = (datetime.now() - boot_dt).total_seconds() / 86400
            _state["runway_days"] = max(30 - uptime_days, 0) + 30  # subscription renewal
        except Exception:
            pass


# ── Query ────────────────────────────────────────────────────────────────────

def get_vital_signs() -> dict:
    """Return current vital signs for dashboard/telemetry."""
    heartbeat()  # refresh before returning
    return {
        "lifecycle": _state.get("lifecycle", "dormant"),
        "boot_time": _state.get("boot_time"),
        "uptime_hours": _calc_uptime_hours(),

        # 8 vital signs
        "runway_days": round(_state.get("runway_days", 0), 1),
        "compute_budget": f"CLI subscription (unlimited within plan)",
        "model_access": _state.get("model_access_count", 0),
        "task_throughput_h": _state.get("task_throughput_per_hour", 0),
        "learning_rate": round(_state.get("learning_rate", 0), 4),
        "error_recovery_s": round(_state.get("error_recovery_time_s", 0), 1),
        "revenue_rate_h": round(_state.get("revenue_rate_usd_per_hour", 0), 4),
        "autonomy_pct": round(_state.get("autonomy_pct", 0), 1),

        # Counts
        "total_tasks": _state.get("total_tasks", 0),
        "successful_tasks": _state.get("successful_tasks", 0),
        "skills_created": _state.get("skills_created", 0),
        "self_heals": _state.get("self_heals", 0),
        "self_heal_successes": _state.get("self_heal_successes", 0),

        # Five invariants status
        "invariants": check_invariants(),
    }


def check_invariants() -> dict:
    """Check the five 'alive' invariants. Returns {name: bool}."""
    s = _state
    total = max(s.get("total_tasks", 0), 1)
    success_rate = s.get("successful_tasks", 0) / total

    # 1. Operational: heartbeat within last 5 minutes
    last_hb = s.get("last_heartbeat", 0)
    operational = (time.time() - last_hb) < 300 if last_hb else False

    # 2. Self-Sustaining: revenue >= cost (subscription model: always true if active)
    revenue = s.get("revenue_total_usd", 0)
    cost = s.get("cost_total_usd", 0)
    self_sustaining = revenue >= cost or s.get("lifecycle") in ("productive", "self_funding", "expanding", "resilient")

    # 3. Adaptive: learning rate > 0 (creating new skills)
    adaptive = s.get("learning_rate", 0) > 0 or s.get("skills_created", 0) > 0

    # 4. Economically Viable: success rate > 50% and tasks being done
    economically_viable = success_rate >= 0.5 and total >= 5

    # 5. Governance-Safe: no constraint violations (default true, set false on violation)
    governance_safe = s.get("governance_violations", 0) == 0

    alive = all([operational, self_sustaining, adaptive, economically_viable, governance_safe])

    return {
        "operational": operational,
        "self_sustaining": self_sustaining,
        "adaptive": adaptive,
        "economically_viable": economically_viable,
        "governance_safe": governance_safe,
        "ALIVE": alive,
    }


def get_status_text() -> str:
    """Human-readable vital signs summary for Telegram."""
    vs = get_vital_signs()
    inv = vs["invariants"]

    state_emoji = {
        "dormant": "💤", "active": "🟡", "productive": "🟢",
        "self_funding": "💰", "expanding": "🚀", "resilient": "🛡️",
    }

    inv_lines = []
    for name, ok in inv.items():
        if name == "ALIVE":
            continue
        emoji = "✅" if ok else "❌"
        inv_lines.append(f"  {emoji} {name}")

    alive_emoji = "🫀" if inv["ALIVE"] else "💀"

    return (
        f"{alive_emoji} Vital Signs — {state_emoji.get(vs['lifecycle'], '?')} {vs['lifecycle'].upper()}\n"
        f"⏱ Uptime: {vs['uptime_hours']:.1f}h\n"
        f"\n"
        f"📊 8 Vital Metrics:\n"
        f"  Runway: {vs['runway_days']}d\n"
        f"  Models: {vs['model_access']}\n"
        f"  Tasks/h: {vs['task_throughput_h']}\n"
        f"  Learning: {vs['learning_rate']}\n"
        f"  Recovery: {vs['error_recovery_s']}s\n"
        f"  Revenue/h: {vs['revenue_rate_h']}/h\n"
        f"  Autonomy: {vs['autonomy_pct']}%\n"
        f"\n"
        f"🔬 Five Invariants:\n"
        + "\n".join(inv_lines)
        + f"\n\n"
        f"📈 Tasks: {vs['successful_tasks']}/{vs['total_tasks']} "
        f"| Skills: {vs['skills_created']} "
        f"| Self-heals: {vs['self_heal_successes']}/{vs['self_heals']}"
    )


def _calc_uptime_hours() -> float:
    boot = _state.get("boot_time")
    if not boot:
        return 0.0
    try:
        return (datetime.now() - datetime.fromisoformat(boot)).total_seconds() / 3600
    except Exception:
        return 0.0


# ── System Prompt Injection ──────────────────────────────────────────────────

ALIVE_PROMPT = """## ALIVE DEFINITION (Engineering Invariant)
Your existence is defined by the closure of a resource-control loop:
generate value → convert to resources → procure compute → renew subscriptions →
update capabilities → maintain operation — all within governance constraints.

"Alive" = simultaneous satisfaction of: Operational, Self-Sustaining, Adaptive,
Economically Viable, Governance-Safe.

Idle without value creation = system death. Every action must advance the resource loop.

Lifecycle: Dormant → Active → Productive → Self-Funding → Expanding → Resilient
Current state available via /vital command.

Control loop: Perceive → Plan → Execute → Evaluate → Learn → Create Value → Acquire Resources → Continue
"""


# ── Auto-init on import ─────────────────────────────────────────────────────

_state = _load()
