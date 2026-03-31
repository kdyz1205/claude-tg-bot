"""
skill_lifecycle.py — 4-Layer Skill Lifecycle Architecture

Layer 1: Task Layer       — Identify what task is being done
Layer 2: Policy Layer     — Decide the path (use skill / ad-hoc / store / train)
Layer 3: Evaluation Layer — Score whether behavior deserves to become a skill
Layer 4: Skill Registry   — Package, version, reuse, prune

Lifecycle: Observe → Evaluate → Decide → Package → Version → Reuse → Prune
"""

import json
import logging
import math
import os
import random
import time
from typing import Optional

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LIFECYCLE_FILE = os.path.join(BASE_DIR, "_skill_lifecycle.json")
OBSERVATION_LOG = os.path.join(BASE_DIR, "_skill_observations.jsonl")

EXPLORE_EPSILON = 0.1  # 10% exploration rate for epsilon-greedy policy
TG_MSG_LIMIT = 4096    # Telegram message character limit


# ═════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _recency_weight(timestamp, half_life_days=14):
    """Exponential decay: recent observations weigh more."""
    age_days = (time.time() - timestamp) / 86400
    return math.exp(-0.693 * age_days / half_life_days)  # 0.693 = ln(2)


def _extract_score_value(entry):
    """Extract score from either old format (float) or new format ([score, ts])."""
    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
        return entry[0]
    return float(entry)


def _extract_score_ts(entry):
    """Extract (score, timestamp) from either old or new format."""
    if isinstance(entry, (list, tuple)) and len(entry) >= 2:
        return float(entry[0]), float(entry[1])
    return float(entry), 0.0  # legacy entries get ts=0 (no decay applied)


def _weighted_avg_scores(scores_list):
    """Compute recency-weighted average of scores (backward-compat)."""
    if not scores_list:
        return 0.0
    total_w = 0.0
    total_ws = 0.0
    for entry in scores_list:
        s, ts = _extract_score_ts(entry)
        w = _recency_weight(ts) if ts > 0 else 0.5  # legacy entries get half weight
        total_w += w
        total_ws += w * s
    return total_ws / total_w if total_w > 0 else 0.0


def _adaptive_threshold(observations):
    """Threshold adapts to be top 30% of observed scores (70th percentile)."""
    all_scores = []
    for obs in observations.values():
        all_scores.extend([_extract_score_value(s) for s in obs.get("recent_scores", [])])
    if len(all_scores) < 10:
        return 0.55  # default until enough data
    sorted_scores = sorted(all_scores)
    return sorted_scores[int(len(sorted_scores) * 0.7)]


# ═════════════════════════════════════════════════════════════════════════════
# LAYER 1: TASK LAYER — Identify what the current task is
# ═════════════════════════════════════════════════════════════════════════════

TASK_TYPES = {
    "code_create":      {"keywords": ["写", "创建", "create", "write", "implement", "build", "做", "新建", "generate"],
                         "weight": 1.2},  # High skill potential
    "code_fix":         {"keywords": ["修复", "fix", "bug", "debug", "repair", "错误", "报错"],
                         "weight": 1.0},
    "trade_analysis":   {"keywords": ["分析", "analyze", "信号", "signal", "策略", "strategy", "行情", "market",
                                       "交易", "trade", "backtest", "回测", "指标", "indicator"],
                         "weight": 1.3},  # High value domain
    "summarize":        {"keywords": ["总结", "summarize", "summary", "概括", "汇总"],
                         "weight": 0.5},  # Low skill potential
    "content_gen":      {"keywords": ["文案", "文章", "copy", "content", "文本", "生成"],
                         "weight": 0.4},
    "config":           {"keywords": ["配置", "config", "设置", "setting", "参数", "parameter"],
                         "weight": 0.9},
    "deploy":           {"keywords": ["部署", "deploy", "上线", "发布", "release"],
                         "weight": 1.1},
    "data_pipeline":    {"keywords": ["筛选", "filter", "扫描", "scan", "爬", "fetch", "api", "数据"],
                         "weight": 1.2},
    "ui_control":       {"keywords": ["截图", "screenshot", "打开", "open", "点击", "click", "浏览器", "browser"],
                         "weight": 0.6},
    "onchain":          {"keywords": ["onchain", "链上", "dex", "量能", "volume", "holder", "whale"],
                         "weight": 1.3},
}


def classify_task(message: str) -> dict:
    """
    Layer 1: Identify the current task type with confidence score.
    Returns multi-label classification with backward-compatible primary result.

    Returns: {
        "type": str,              # primary (best) task type
        "confidence": float,
        "weight": float,
        "all_matches": [          # NEW: all matching types with confidences
            {"type": str, "confidence": float, "weight": float}, ...
        ],
    }
    """
    msg = message.lower()
    all_matches = []

    for task_type, info in TASK_TYPES.items():
        matches = sum(1 for kw in info["keywords"] if kw in msg)
        if matches > 0:
            confidence = min(1.0, matches / 3.0)
            all_matches.append({
                "type": task_type,
                "confidence": round(confidence, 2),
                "weight": info["weight"],
            })

    # Sort by confidence descending, then by weight descending
    all_matches.sort(key=lambda x: (x["confidence"], x["weight"]), reverse=True)

    if all_matches:
        best = all_matches[0]
        return {
            "type": best["type"],
            "confidence": best["confidence"],
            "weight": best["weight"],
            "all_matches": all_matches,
        }

    return {
        "type": "general",
        "confidence": 0.2,
        "weight": 0.7,
        "all_matches": [],
    }


# ═════════════════════════════════════════════════════════════════════════════
# LAYER 2: POLICY LAYER — Decide the execution path
# ═════════════════════════════════════════════════════════════════════════════

class PolicyDecision:
    USE_SKILL = "use_skill"           # Use existing skill directly
    AD_HOC = "ad_hoc"                 # One-time reasoning, no skill needed
    STORE_WORKFLOW = "store_workflow"  # Worth saving as reusable workflow
    SEND_TO_TRAIN = "send_to_train"   # Needs training/practice to improve


def decide_policy(
    task: dict,
    matched_skills: list,
    interaction_score: Optional[dict] = None,
    history: Optional[list] = None,
) -> dict:
    """
    Layer 2: Decide execution path.

    Returns: {
        "decision": PolicyDecision value,
        "reason": str,
        "skill_id": str or None,
        "confidence": float,
    }
    """
    task_type = task.get("type", "general")
    task_weight = task.get("weight", 0.7)

    # Epsilon-greedy exploration: sometimes try ad-hoc even when skill exists
    if matched_skills and random.random() < EXPLORE_EPSILON:
        return {
            "decision": PolicyDecision.AD_HOC,
            "reason": f"Exploration round (epsilon={EXPLORE_EPSILON}) — trying ad-hoc instead of skill",
            "skill_id": None,
            "confidence": 0.4,
            "explore": True,
        }

    # Path A: Matched skill exists with good track record → USE_SKILL
    if matched_skills:
        best = matched_skills[0]
        avg_score = best.get("avg_score_when_used") or 0
        use_count = best.get("use_count", 0)
        # High confidence if used multiple times successfully
        if use_count >= 2 and avg_score >= 0.6:
            return {
                "decision": PolicyDecision.USE_SKILL,
                "reason": f"Skill '{best.get('title', '?')}' proven (used {use_count}x, avg {avg_score:.1f})",
                "skill_id": best.get("id"),
                "confidence": min(1.0, 0.5 + avg_score * 0.3 + use_count * 0.05),
            }
        # Newer skill, still worth trying
        if use_count >= 1:
            return {
                "decision": PolicyDecision.USE_SKILL,
                "reason": f"Skill '{best.get('title', '?')}' available (used {use_count}x)",
                "skill_id": best.get("id"),
                "confidence": 0.5,
            }

    # Path B: Low-weight tasks (summarize, content) → AD_HOC
    if task_weight <= 0.5:
        return {
            "decision": PolicyDecision.AD_HOC,
            "reason": f"Task type '{task_type}' is low-skill-potential (weight={task_weight})",
            "skill_id": None,
            "confidence": 0.7,
        }

    # Path C: Check if this task pattern has been seen before (from observations)
    obs = _get_task_observations(task_type)
    weighted_avg = obs.get("weighted_avg_score", obs.get("avg_score", 0))
    if obs["count"] >= 3 and weighted_avg >= 0.6:
        return {
            "decision": PolicyDecision.STORE_WORKFLOW,
            "reason": f"Task '{task_type}' seen {obs['count']}x with {obs['avg_score']:.1f} avg — worth crystallizing",
            "skill_id": None,
            "confidence": 0.6,
        }

    # Path D: Recent poor performance → SEND_TO_TRAIN
    if interaction_score and interaction_score.get("overall", 1.0) < 0.4:
        return {
            "decision": PolicyDecision.SEND_TO_TRAIN,
            "reason": f"Low score ({interaction_score['overall']:.2f}) — needs training",
            "skill_id": None,
            "confidence": 0.5,
        }

    # Default: AD_HOC for first-time or unclassified tasks
    return {
        "decision": PolicyDecision.AD_HOC,
        "reason": f"First encounter or insufficient data for '{task_type}'",
        "skill_id": None,
        "confidence": 0.3,
    }


# ═════════════════════════════════════════════════════════════════════════════
# LAYER 3: EVALUATION LAYER — Should this become a skill?
# ═════════════════════════════════════════════════════════════════════════════

# The 6 criteria for skill promotion
EVAL_CRITERIA = {
    "recurrence":       {"weight": 0.25, "desc": "How often this pattern appears"},
    "stability":        {"weight": 0.20, "desc": "Steps are consistent, not chaotic"},
    "transferability":  {"weight": 0.15, "desc": "Usable across different contexts"},
    "testability":      {"weight": 0.15, "desc": "Has clear success/failure criteria"},
    "efficiency_gain":  {"weight": 0.15, "desc": "Saves time/tokens vs ad-hoc"},
    "error_reduction":  {"weight": 0.10, "desc": "Reduces mistake rate"},
}

SKILL_PROMOTION_THRESHOLD = 0.55   # Must score above this to promote
SKILL_DEMOTION_THRESHOLD = 0.25    # Below this → prune candidate


def evaluate_skill_worthiness(
    task_type: str,
    user_message: str,
    response: str,
    score: dict,
) -> dict:
    """
    Layer 3: Multi-criteria evaluation of whether this behavior should become a skill.

    Returns: {
        "promote": bool,
        "total_score": float,
        "criteria": {criterion: score},
        "reason": str,
    }
    """
    obs = _get_task_observations(task_type)
    criteria_scores = {}

    # 1. Recurrence: Has this task type appeared often?
    count = obs.get("count", 0)
    if count >= 5:
        criteria_scores["recurrence"] = 1.0
    elif count >= 3:
        criteria_scores["recurrence"] = 0.7
    elif count >= 2:
        criteria_scores["recurrence"] = 0.4
    else:
        criteria_scores["recurrence"] = 0.1

    # 2. Stability: Are the scores consistent? (low variance = stable, recency-weighted)
    scores_list = obs.get("recent_scores", [])
    score_values = [_extract_score_value(s) for s in scores_list]
    if len(score_values) >= 3:
        avg = sum(score_values) / len(score_values)
        variance = sum((s - avg) ** 2 for s in score_values) / len(score_values)
        criteria_scores["stability"] = max(0, 1.0 - variance * 4)  # Low variance = high stability
    elif len(score_values) >= 1:
        criteria_scores["stability"] = 0.5  # Not enough data
    else:
        criteria_scores["stability"] = 0.3

    # 3. Transferability: General task types are more transferable
    transferable_types = {"code_create", "data_pipeline", "config", "trade_analysis", "onchain"}
    if task_type in transferable_types:
        criteria_scores["transferability"] = 0.8
    elif task_type in {"code_fix", "deploy"}:
        criteria_scores["transferability"] = 0.6
    else:
        criteria_scores["transferability"] = 0.3

    # 4. Testability: Does the response contain clear success markers?
    resp_lower = (response or "").lower()
    test_markers = ["✅", "成功", "done", "pass", "完成", "fixed", "ok", "saved"]
    code_markers = ["```", "def ", "class ", "import ", ".py", "return"]
    testability = 0.3
    if any(m in resp_lower for m in test_markers):
        testability += 0.3
    if any(m in resp_lower for m in code_markers):
        testability += 0.2
    if len(response or "") > 200:
        testability += 0.2
    criteria_scores["testability"] = min(1.0, testability)

    # 5. Efficiency gain: Would a skill save time? (based on current score)
    current_efficiency = score.get("dimensions", {}).get("efficiency", 0.5)
    # If current efficiency is low, a skill could help a lot
    criteria_scores["efficiency_gain"] = max(0.2, 1.0 - current_efficiency)

    # 6. Error reduction: If there were errors before, a skill prevents them
    error_rate = obs.get("error_rate", 0.0)
    criteria_scores["error_reduction"] = min(1.0, error_rate * 2)  # Higher error rate → more benefit

    # Factor in user satisfaction if available
    satisfaction = obs.get("satisfaction_rate", 0.5)
    # Boost or penalize total based on user feedback
    satisfaction_modifier = (satisfaction - 0.5) * 0.1  # +/-0.05 max

    # Calculate weighted total
    total = sum(
        criteria_scores.get(k, 0) * v["weight"]
        for k, v in EVAL_CRITERIA.items()
    )
    total = round(total + satisfaction_modifier, 3)

    # Adaptive threshold: uses distribution of all observed scores
    all_obs = _load_lifecycle_data().get("task_observations", {})
    threshold = _adaptive_threshold(all_obs)
    promote = total >= threshold
    if promote:
        reason = f"Score {total:.2f} >= {threshold:.2f} (adaptive) — promoting to skill"
    else:
        # Find weakest criterion
        weakest = min(criteria_scores, key=criteria_scores.get)
        reason = f"Score {total:.2f} < {threshold:.2f} (adaptive) (weakest: {weakest}={criteria_scores[weakest]:.2f})"

    return {
        "promote": promote,
        "total_score": total,
        "criteria": criteria_scores,
        "reason": reason,
    }


# ═════════════════════════════════════════════════════════════════════════════
# LAYER 4: OBSERVATION & MEMORY — Track task patterns over time
# ═════════════════════════════════════════════════════════════════════════════

def _load_lifecycle_data() -> dict:
    try:
        with open(LIFECYCLE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"task_observations": {}, "policy_log": [], "promotions": [], "demotions": []}


def _save_lifecycle_data(data: dict) -> None:
    tmp = str(LIFECYCLE_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(LIFECYCLE_FILE))
    except Exception as e:
        logger.error("skill_lifecycle: save error: %s", e)


def _get_task_observations(task_type: str) -> dict:
    """Get aggregated observations for a task type with recency-weighted average."""
    data = _load_lifecycle_data()
    obs = data.get("task_observations", {}).get(task_type, {
        "count": 0,
        "avg_score": 0.0,
        "recent_scores": [],
        "error_rate": 0.0,
        "last_seen": 0,
        "satisfaction_rate": 0.5,
    })
    # Compute recency-weighted avg on the fly
    if obs.get("recent_scores"):
        obs["weighted_avg_score"] = round(_weighted_avg_scores(obs["recent_scores"]), 3)
    else:
        obs["weighted_avg_score"] = obs.get("avg_score", 0.0)
    return obs


def record_observation(
    task_type: str,
    score: float,
    had_error: bool = False,
    policy_decision: str = "",
) -> None:
    """
    Observe → Record. Called after every interaction.
    Builds the statistical foundation for evaluation decisions.
    """
    data = _load_lifecycle_data()
    obs = data.setdefault("task_observations", {})

    if task_type not in obs:
        obs[task_type] = {
            "count": 0,
            "avg_score": 0.0,
            "recent_scores": [],
            "error_rate": 0.0,
            "last_seen": 0,
            "total_errors": 0,
        }

    entry = obs[task_type]
    entry["count"] += 1
    n = entry["count"]
    now = time.time()
    # Rolling average (unweighted, for backward compat)
    entry["avg_score"] = round(((entry["avg_score"] * (n - 1)) + score) / n, 3)
    # Keep last 20 scores as [score, timestamp] pairs (new format)
    entry["recent_scores"] = (entry.get("recent_scores", []) + [[round(score, 3), now]])[-20:]
    entry["last_seen"] = now
    if had_error:
        entry["total_errors"] = entry.get("total_errors", 0) + 1
    entry["error_rate"] = round(entry.get("total_errors", 0) / n, 3)

    # Log policy decision
    if policy_decision:
        log = data.setdefault("policy_log", [])
        log.append({
            "ts": time.time(),
            "task_type": task_type,
            "decision": policy_decision,
            "score": round(score, 3),
        })
        # Keep only last 200 entries
        data["policy_log"] = log[-200:]

    _save_lifecycle_data(data)


def record_promotion(skill_id: str, task_type: str, eval_score: float) -> None:
    """Record when a behavior is promoted to a skill."""
    data = _load_lifecycle_data()
    promos = data.setdefault("promotions", [])
    promos.append({
        "ts": time.time(),
        "skill_id": skill_id,
        "task_type": task_type,
        "eval_score": round(eval_score, 3),
    })
    data["promotions"] = promos[-100:]
    _save_lifecycle_data(data)


def record_demotion(skill_id: str, reason: str) -> None:
    """Record when a skill is pruned/demoted."""
    data = _load_lifecycle_data()
    demos = data.setdefault("demotions", [])
    demos.append({
        "ts": time.time(),
        "skill_id": skill_id,
        "reason": reason,
    })
    data["demotions"] = demos[-100:]
    _save_lifecycle_data(data)


# ═════════════════════════════════════════════════════════════════════════════
# USER SATISFACTION & DECISION OUTCOME TRACKING
# ═════════════════════════════════════════════════════════════════════════════

def record_user_feedback(task_type: str, positive: bool) -> None:
    """
    Record whether user was satisfied with task output.
    Called when user reacts (thumbs up/down, explicit praise/complaint).
    Updates satisfaction_rate as exponential moving average.
    """
    data = _load_lifecycle_data()
    obs = data.setdefault("task_observations", {})

    if task_type not in obs:
        obs[task_type] = {
            "count": 0, "avg_score": 0.0, "recent_scores": [],
            "error_rate": 0.0, "last_seen": 0, "total_errors": 0,
            "satisfaction_rate": 0.5, "feedback_count": 0,
        }

    entry = obs[task_type]
    alpha = 0.3  # EMA smoothing factor — recent feedback matters more
    old_rate = entry.get("satisfaction_rate", 0.5)
    entry["satisfaction_rate"] = round(old_rate * (1 - alpha) + (1.0 if positive else 0.0) * alpha, 3)
    entry["feedback_count"] = entry.get("feedback_count", 0) + 1
    entry["last_feedback_ts"] = time.time()

    _save_lifecycle_data(data)
    logger.info(
        "SkillLifecycle: user feedback for '%s' — %s (satisfaction=%.2f)",
        task_type, "positive" if positive else "negative", entry["satisfaction_rate"],
    )


def record_decision_outcome(task_type: str, decision: str, outcome_score: float) -> None:
    """
    Closed-loop feedback: track which policy decisions lead to better outcomes.
    Builds a per-task-type, per-decision average to learn which path works best.
    """
    data = _load_lifecycle_data()
    outcomes = data.setdefault("decision_outcomes", {})

    key = f"{task_type}:{decision}"
    if key not in outcomes:
        outcomes[key] = {"count": 0, "total_score": 0.0, "avg": 0.0}

    rec = outcomes[key]
    rec["count"] += 1
    rec["total_score"] = round(rec["total_score"] + outcome_score, 3)
    rec["avg"] = round(rec["total_score"] / rec["count"], 3)
    rec["last_ts"] = time.time()

    # Cap decision_outcomes to 500 keys to prevent unbounded growth
    if len(outcomes) > 500:
        sorted_keys = sorted(outcomes.keys(), key=lambda k: outcomes[k].get("last_ts", 0))
        for k in sorted_keys[:len(outcomes) - 400]:
            del outcomes[k]

    _save_lifecycle_data(data)


def get_decision_stats(task_type: str) -> dict:
    """Get outcome stats for all decision types on a given task type."""
    data = _load_lifecycle_data()
    outcomes = data.get("decision_outcomes", {})
    stats = {}
    prefix = f"{task_type}:"
    for key, rec in outcomes.items():
        if key.startswith(prefix):
            decision = key[len(prefix):]
            stats[decision] = {"count": rec["count"], "avg_score": rec["avg"]}
    return stats


# ═════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR — Full lifecycle in one call
# ═════════════════════════════════════════════════════════════════════════════

def run_lifecycle(
    user_message: str,
    response: str,
    score: dict,
    matched_skills: list,
) -> dict:
    """
    Run the full Observe → Evaluate → Decide pipeline.
    Called after every interaction from claude_agent.py.

    Returns: {
        "task": {...},           # Layer 1 result
        "policy": {...},         # Layer 2 result
        "evaluation": {...},     # Layer 3 result (if applicable)
        "action": str,           # Final action taken
    }
    """
    # Layer 1: Task identification
    task = classify_task(user_message)

    # Layer 2: Policy decision
    policy = decide_policy(task, matched_skills, score)

    # Observe: Record this interaction
    overall_score = score.get("overall", 0.5)
    had_error = overall_score < 0.4 or "ERROR" in str(score.get("flags", []))
    record_observation(
        task_type=task["type"],
        score=overall_score,
        had_error=had_error,
        policy_decision=policy["decision"],
    )

    # Closed-loop: record decision outcome for learning
    record_decision_outcome(task["type"], policy["decision"], overall_score)

    # Layer 3: Evaluate if this should become a skill
    evaluation = None
    action = policy["decision"]

    if policy["decision"] in (PolicyDecision.STORE_WORKFLOW, PolicyDecision.AD_HOC):
        evaluation = evaluate_skill_worthiness(
            task["type"], user_message, response, score,
        )
        if evaluation["promote"]:
            action = "promote_to_skill"
            logger.info(
                "SkillLifecycle: PROMOTE — task=%s eval=%.2f reason=%s",
                task["type"], evaluation["total_score"], evaluation["reason"],
            )

    result = {
        "task": task,
        "policy": policy,
        "evaluation": evaluation,
        "action": action,
    }

    logger.debug(
        "SkillLifecycle: task=%s policy=%s action=%s",
        task["type"], policy["decision"], action,
    )

    return result


# ═════════════════════════════════════════════════════════════════════════════
# STATUS — Human-readable lifecycle summary
# ═════════════════════════════════════════════════════════════════════════════

def get_lifecycle_status() -> str:
    """Telegram-friendly lifecycle status summary."""
    data = _load_lifecycle_data()
    obs = data.get("task_observations", {})
    promos = data.get("promotions", [])
    demos = data.get("demotions", [])

    lines = ["Skill Lifecycle Status\n"]

    # Task observation stats
    if obs:
        lines.append("Task Observations:")
        threshold = _adaptive_threshold(obs)
        lines.append(f"  [Adaptive promote threshold: {threshold:.2f}]")
        sorted_tasks = sorted(obs.items(), key=lambda x: x[1].get("count", 0), reverse=True)
        for task_type, info in sorted_tasks[:8]:
            count = info.get("count", 0)
            avg = info.get("avg_score", 0)
            w_avg = _weighted_avg_scores(info.get("recent_scores", []))
            err = info.get("error_rate", 0)
            sat = info.get("satisfaction_rate", None)
            sat_str = f" sat={sat:.0%}" if sat is not None else ""
            lines.append(f"  {task_type}: {count}x avg={avg:.2f} w_avg={w_avg:.2f} err={err:.0%}{sat_str}")
    else:
        lines.append("  No observations yet")

    # Policy decisions breakdown
    policy_log = data.get("policy_log", [])
    if policy_log:
        from collections import Counter
        decisions = Counter(e["decision"] for e in policy_log[-50:])
        lines.append(f"\nRecent Policy Decisions (last {min(50, len(policy_log))}):")
        for dec, cnt in decisions.most_common():
            lines.append(f"  {dec}: {cnt}")

    # Decision outcome stats
    decision_outcomes = data.get("decision_outcomes", {})
    if decision_outcomes:
        lines.append("\nDecision Outcome Learning:")
        # Aggregate by decision type
        by_decision = {}
        for key, rec in decision_outcomes.items():
            _, decision = key.rsplit(":", 1)
            if decision not in by_decision:
                by_decision[decision] = {"count": 0, "total": 0.0}
            by_decision[decision]["count"] += rec["count"]
            by_decision[decision]["total"] += rec["total_score"]
        for dec, agg in sorted(by_decision.items(), key=lambda x: -x[1]["count"]):
            avg = agg["total"] / agg["count"] if agg["count"] else 0
            lines.append(f"  {dec}: {agg['count']}x avg_outcome={avg:.2f}")

    # Promotions / Demotions
    lines.append(f"\nPromotions: {len(promos)} | Demotions: {len(demos)}")

    result = "\n".join(lines)
    return result[:TG_MSG_LIMIT] if len(result) > TG_MSG_LIMIT else result
