"""
meta_learner.py — 自主学习进化循环 (Phase 3 Task 15)

每天自动分析过去7天信号+结果，提取成功/失败模式，进化策略。

功能:
  1. 每日分析: 过去7天信号 → 提取成功/失败模式
  2. 模式写入 skill_library (成功模式→新技能, 失败模式→黑名单)
  3. 策略A/B测试: 维护A/B两个策略版本, 每周淘汰差的
  4. 进化日志: 每次改进记录到 .evolution_changelog.jsonl
  5. 市场状态→策略映射表
  6. 每周日自动发Telegram进化周报

数据文件:
  _signal_history.json       — 来自 profit_tracker
  _ab_test_state.json        — A/B测试状态
  _market_strategy_map.json  — 市场状态→策略映射
  .evolution_changelog.jsonl — 进化变更日志
"""

import asyncio
import json
import logging
import math
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Callable, Coroutine, Optional

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── File paths ───────────────────────────────────────────────────────────────
SIGNAL_HISTORY_FILE    = os.path.join(BASE_DIR, "_signal_history.json")
AB_STATE_FILE          = os.path.join(BASE_DIR, "_ab_test_state.json")
MARKET_MAP_FILE        = os.path.join(BASE_DIR, "_market_strategy_map.json")
EVOLUTION_CHANGELOG    = os.path.join(BASE_DIR, ".evolution_changelog.jsonl")
META_STATS_FILE        = os.path.join(BASE_DIR, "_meta_stats.json")
SKILL_BLACKLIST_FILE   = os.path.join(BASE_DIR, "_skill_blacklist.json")

# ─── Schedules ────────────────────────────────────────────────────────────────
DAILY_ANALYSIS_HOUR  = 2    # 凌晨2点做日分析
WEEKLY_REPORT_HOUR   = 9    # 周日9点发周报
WEEKLY_REPORT_DOW    = 6    # 0=Monday … 6=Sunday
AB_EVAL_INTERVAL_S   = 7 * 24 * 3600  # 7天评估一次A/B


# ─── File helpers ─────────────────────────────────────────────────────────────

def _load_json(path: str, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, data) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception as e:
        logger.warning(f"Failed to save {path}: {e}")
        try:
            os.unlink(tmp)
        except Exception:
            pass


_MAX_CHANGELOG_LINES = 5000


def _append_changelog(entry: dict) -> None:
    """Append one entry to .evolution_changelog.jsonl (truncated to last _MAX_CHANGELOG_LINES)."""
    entry["timestamp"] = datetime.now().isoformat()
    try:
        with open(EVOLUTION_CHANGELOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        # Truncate if too many lines (atomic: tmp+fsync+replace)
        try:
            with open(EVOLUTION_CHANGELOG, "r", encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > _MAX_CHANGELOG_LINES:
                tmp_trunc = EVOLUTION_CHANGELOG + ".trunc.tmp"
                with open(tmp_trunc, "w", encoding="utf-8") as f:
                    f.writelines(lines[-_MAX_CHANGELOG_LINES:])
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp_trunc, EVOLUTION_CHANGELOG)
        except Exception:
            pass
    except Exception as e:
        logger.warning(f"Failed to append changelog: {e}")


# ─── Pattern Extraction ───────────────────────────────────────────────────────

def _load_recent_signals(days: int = 7) -> list:
    """Load resolved signals from the past N days."""
    signals = _load_json(SIGNAL_HISTORY_FILE, [])
    cutoff = time.time() - days * 86400
    return [
        s for s in signals
        if s.get("timestamp", 0) >= cutoff
        and s.get("status") in ("win", "loss")
    ]


def _classify_market_state(signals: list) -> str:
    """Infer market state from signal history: trending / ranging / volatile."""
    if len(signals) < 5:
        return "unknown"

    pnls = [s.get("final_pnl_pct", 0) for s in signals if s.get("final_pnl_pct") is not None]
    if not pnls:
        return "unknown"

    avg = sum(pnls) / len(pnls)
    stddev = math.sqrt(sum((p - avg) ** 2 for p in pnls) / len(pnls)) if len(pnls) > 1 else 0

    # High stddev = volatile; high positive avg + low stddev = trending; otherwise ranging
    if stddev > 5.0:
        return "volatile"
    elif avg > 1.0 and stddev < 3.0:
        return "trending"
    else:
        return "ranging"


def analyze_patterns(signals: list) -> dict:
    """
    从信号列表提取成功/失败模式。
    返回:
      {
        "success_patterns": [{"key": ..., "win_rate": ..., "sample": ...}, ...],
        "failure_patterns": [{"key": ..., "win_rate": ..., "sample": ...}, ...],
        "by_type": {signal_type: {"wins": n, "losses": n, "win_rate": f}},
        "by_symbol": {symbol: {"wins": n, "losses": n, "win_rate": f}},
        "by_hour": {hour: {"wins": n, "losses": n, "win_rate": f}},
        "market_state": str,
        "total": int,
        "win_rate_overall": float,
      }
    """
    if not signals:
        return {
            "success_patterns": [],
            "failure_patterns": [],
            "by_type": {},
            "by_symbol": {},
            "by_hour": {},
            "market_state": "unknown",
            "total": 0,
            "win_rate_overall": 0.0,
        }

    # Aggregate by signal_type, symbol, hour
    by_type: dict   = defaultdict(lambda: {"wins": 0, "losses": 0})
    by_symbol: dict = defaultdict(lambda: {"wins": 0, "losses": 0})
    by_hour: dict   = defaultdict(lambda: {"wins": 0, "losses": 0})

    for s in signals:
        outcome = "wins" if s.get("status") == "win" else "losses"
        by_type[s.get("signal_type", "unknown")][outcome] += 1
        by_symbol[s.get("symbol", "unknown")][outcome] += 1
        dt = datetime.fromtimestamp(s.get("timestamp", 0))
        by_hour[dt.hour][outcome] += 1

    def _rate(d: dict) -> float:
        total = d["wins"] + d["losses"]
        return round(d["wins"] / total, 3) if total else 0.0

    def _enrich(mapping: dict, prefix: str) -> list:
        result = []
        for k, v in mapping.items():
            total = v["wins"] + v["losses"]
            if total < 3:
                continue
            result.append({
                "key": f"{prefix}:{k}",
                "win_rate": _rate(v),
                "wins": v["wins"],
                "losses": v["losses"],
                "sample": total,
            })
        return sorted(result, key=lambda x: -x["win_rate"])

    all_patterns = (
        _enrich(by_type, "type")
        + _enrich(by_symbol, "symbol")
        + _enrich(by_hour, "hour")
    )

    success_patterns = [p for p in all_patterns if p["win_rate"] >= 0.65]
    failure_patterns = [p for p in all_patterns if p["win_rate"] <= 0.35]

    wins_total = sum(1 for s in signals if s.get("status") == "win")
    win_rate = round(wins_total / len(signals), 3) if signals else 0.0

    return {
        "success_patterns": success_patterns[:10],
        "failure_patterns": failure_patterns[:10],
        "by_type":   {k: {**v, "win_rate": _rate(v)} for k, v in by_type.items()},
        "by_symbol": {k: {**v, "win_rate": _rate(v)} for k, v in by_symbol.items()},
        "by_hour":   {k: {**v, "win_rate": _rate(v)} for k, v in by_hour.items()},
        "market_state": _classify_market_state(signals),
        "total": len(signals),
        "win_rate_overall": win_rate,
    }


# ─── Skill Library Integration ────────────────────────────────────────────────

def inject_pattern_skills(analysis: dict) -> list:
    """
    将成功模式写入 skill_library 作为新技能。
    将失败模式加入黑名单。
    返回新增的 skill_id 列表。
    """
    try:
        import skill_library as sl
    except ImportError:
        logger.warning("skill_library not available")
        return []

    added = []
    now = datetime.now()

    for pat in analysis.get("success_patterns", []):
        skill_id = f"meta_pattern_{pat['key'].replace(':', '_')}_{now.strftime('%Y%m%d')}"
        title    = f"高胜率信号模式: {pat['key']} (胜率{pat['win_rate']*100:.0f}%)"
        skill = {
            "id": skill_id,
            "title": title,
            "keywords": ["信号", "胜率", pat["key"].split(":")[0], pat["key"].split(":")[-1]],
            "task_type": "trading",
            "summary": f"信号模式 {pat['key']} 在过去7天胜率 {pat['win_rate']*100:.0f}% (样本{pat['sample']})",
            "template_code": (
                f"# 高胜率模式: {pat['key']}\n"
                f"# 胜率: {pat['win_rate']*100:.0f}%  样本: {pat['sample']}\n"
                f"# 建议: 优先使用此类信号"
            ),
            "generic_steps": [
                f"筛选 {pat['key']} 类信号",
                f"确认样本量 >= 3",
                f"在胜率 >= 65% 时提高仓位权重",
            ],
            "source": "meta_learner",
            "created_at": now.isoformat(),
            "use_count": 0,
            "avg_score_when_used": pat["win_rate"],
            "win_rate": pat["win_rate"],
            "sample": pat["sample"],
        }

        # Only add if not already exists (check by title)
        try:
            index = sl._load_index()
        except Exception as exc:
            logger.warning("MetaLearner: failed to load skill_library index (private API _load_index may have changed): %s", exc)
            continue
        existing_titles = {e.get("title", "") for e in index.get("entries", [])}
        if title not in existing_titles:
            sl._save_skill(skill)
            # Rebuild index to include new skill
            index["entries"].append({
                "id": skill_id,
                "title": title,
                "keywords": skill["keywords"],
                "task_type": "trading",
                "use_count": 0,
                "avg_score": pat["win_rate"],
            })
            try:
                sl._save_index(index)
            except Exception as exc:
                logger.warning("MetaLearner: failed to save skill_library index (private API _save_index may have changed): %s", exc)
                continue
            added.append(skill_id)
            logger.info(f"MetaLearner: added pattern skill {skill_id}")

            _append_changelog({
                "type": "skill_added",
                "skill_id": skill_id,
                "title": title,
                "reason": f"Pattern {pat['key']} win_rate={pat['win_rate']} sample={pat['sample']}",
                "before": None,
                "after": {"win_rate": pat["win_rate"], "sample": pat["sample"]},
            })

    # Blacklist failure patterns
    blacklist = _load_json(SKILL_BLACKLIST_FILE, {"patterns": [], "updated_at": None})
    existing_keys = {p["key"] for p in blacklist["patterns"]}
    new_blacklisted = []
    for pat in analysis.get("failure_patterns", []):
        if pat["key"] not in existing_keys:
            blacklist["patterns"].append({
                "key": pat["key"],
                "win_rate": pat["win_rate"],
                "sample": pat["sample"],
                "blacklisted_at": now.isoformat(),
            })
            new_blacklisted.append(pat["key"])

    if new_blacklisted:
        blacklist["updated_at"] = now.isoformat()
        _save_json(SKILL_BLACKLIST_FILE, blacklist)
        _append_changelog({
            "type": "blacklist_updated",
            "patterns_added": new_blacklisted,
            "reason": "Persistent failure patterns (win_rate <= 35%)",
            "before": None,
            "after": {"blacklisted": new_blacklisted},
        })
        logger.info(f"MetaLearner: blacklisted patterns: {new_blacklisted}")

    return added


# ─── Market State → Strategy Mapping ─────────────────────────────────────────

def update_market_strategy_map(analysis: dict) -> None:
    """
    根据当前市场状态+信号表现，更新市场状态→策略映射表。
    """
    market_state = analysis.get("market_state", "unknown")
    if market_state == "unknown":
        return

    mapping = _load_json(MARKET_MAP_FILE, {})

    # Find best performing signal type for this market state
    by_type = analysis.get("by_type", {})
    best_type = None
    best_wr = 0.0
    for sig_type, stats in by_type.items():
        if stats.get("sample", stats.get("wins", 0) + stats.get("losses", 0)) < 3:
            continue
        wr = stats.get("win_rate", 0)
        if wr > best_wr:
            best_wr = wr
            best_type = sig_type

    if best_type is None:
        return

    prev = mapping.get(market_state, {})
    entry = {
        "best_signal_type": best_type,
        "win_rate": best_wr,
        "updated_at": datetime.now().isoformat(),
        "sample_week": analysis.get("total", 0),
    }
    mapping[market_state] = entry
    _save_json(MARKET_MAP_FILE, mapping)

    if prev.get("best_signal_type") != best_type:
        _append_changelog({
            "type": "market_strategy_updated",
            "market_state": market_state,
            "before": prev,
            "after": entry,
            "reason": f"Best signal type changed for {market_state} market",
        })

    logger.info(f"MetaLearner: market_state={market_state} → best_type={best_type} wr={best_wr:.2%}")


def get_strategy_for_market(market_state: str) -> Optional[dict]:
    """查询当前市场状态应使用的最佳策略。"""
    mapping = _load_json(MARKET_MAP_FILE, {})
    return mapping.get(market_state)


# ─── A/B Testing ─────────────────────────────────────────────────────────────

def _default_ab_state() -> dict:
    return {
        "version": 1,
        "variants": {
            "A": {
                "name": "A",
                "description": "保守策略: 高胜率优先 (score_threshold=70, 低仓位)",
                "config": {"score_threshold": 70, "position_size": 0.5, "bias": "conservative"},
                "wins": 0,
                "losses": 0,
                "total_pnl": 0.0,
                "created_at": datetime.now().isoformat(),
                "active": True,
            },
            "B": {
                "name": "B",
                "description": "激进策略: 高收益优先 (score_threshold=60, 正常仓位)",
                "config": {"score_threshold": 60, "position_size": 1.0, "bias": "aggressive"},
                "wins": 0,
                "losses": 0,
                "total_pnl": 0.0,
                "created_at": datetime.now().isoformat(),
                "active": True,
            },
        },
        "current_variant": "A",
        "last_evaluated_at": None,
        "evaluations": [],
    }


def get_ab_state() -> dict:
    state = _load_json(AB_STATE_FILE, None)
    if state is None:
        state = _default_ab_state()
        _save_json(AB_STATE_FILE, state)
    return state


def get_active_variant() -> str:
    """返回当前A/B测试的活跃策略变体名。"""
    return get_ab_state().get("current_variant", "A")


def record_ab_result(variant: str, pnl_pct: float) -> None:
    """记录某变体的单次信号结果。"""
    state = get_ab_state()
    if variant not in state.get("variants", {}):
        return
    v = state["variants"][variant]
    if pnl_pct > 0:
        v["wins"] += 1
    else:
        v["losses"] += 1
    v["total_pnl"] = round(v.get("total_pnl", 0.0) + pnl_pct, 4)
    _save_json(AB_STATE_FILE, state)


def evaluate_ab_test() -> dict:
    """
    每周评估A/B测试：淘汰表现差的变体，生成新挑战者。
    返回评估摘要。
    """
    state = get_ab_state()
    variants = state.get("variants", {})

    results = {}
    for name, v in variants.items():
        total = v.get("wins", 0) + v.get("losses", 0)
        win_rate = v["wins"] / total if total > 0 else 0.0
        avg_pnl  = v.get("total_pnl", 0.0) / total if total > 0 else 0.0
        # Composite score: 60% win_rate + 40% avg_pnl_normalized
        score = win_rate * 0.6 + max(min(avg_pnl / 5.0, 1.0), -1.0) * 0.4
        results[name] = {
            "total": total,
            "win_rate": round(win_rate, 3),
            "avg_pnl": round(avg_pnl, 3),
            "score": round(score, 4),
        }

    if len(results) < 2:
        return {"action": "insufficient_data", "results": results}

    # Sort: best first
    ranked = sorted(results.items(), key=lambda x: -x[1]["score"])
    winner_name, winner_stats = ranked[0]
    loser_name,  loser_stats  = ranked[-1]

    # If winner has >10 signals and score gap > 0.05, eliminate loser
    eliminated = False
    new_challenger = None
    if winner_stats["total"] >= 10 and (winner_stats["score"] - loser_stats["score"]) > 0.05:
        # Build new challenger from winner config with mutation
        winner_config = dict(variants[winner_name].get("config", {}))
        challenger_config = _mutate_config(winner_config)
        new_name = loser_name  # reuse slot
        old_variant = dict(variants[loser_name])
        variants[new_name] = {
            "name": new_name,
            "description": f"进化挑战者 (来自{winner_name}变异) {datetime.now().strftime('%Y-%m-%d')}",
            "config": challenger_config,
            "wins": 0,
            "losses": 0,
            "total_pnl": 0.0,
            "created_at": datetime.now().isoformat(),
            "active": True,
            "parent": winner_name,
        }
        state["current_variant"] = winner_name
        eliminated = True
        new_challenger = challenger_config

        _append_changelog({
            "type": "ab_test_evaluation",
            "winner": winner_name,
            "loser": loser_name,
            "winner_stats": winner_stats,
            "loser_stats": loser_stats,
            "action": f"Eliminated {loser_name}, spawned new challenger",
            "before": {"loser_config": old_variant.get("config")},
            "after": {"challenger_config": challenger_config},
        })
    else:
        state["current_variant"] = winner_name
        _append_changelog({
            "type": "ab_test_evaluation",
            "winner": winner_name,
            "winner_stats": winner_stats,
            "action": "No elimination (insufficient gap or sample)",
        })

    state["last_evaluated_at"] = datetime.now().isoformat()
    state["evaluations"].append({
        "at": datetime.now().isoformat(),
        "results": results,
        "winner": winner_name,
        "eliminated": loser_name if eliminated else None,
    })
    state["evaluations"] = state["evaluations"][-52:]  # keep 1 year
    _save_json(AB_STATE_FILE, state)

    return {
        "action": "eliminated" if eliminated else "no_change",
        "winner": winner_name,
        "loser": loser_name,
        "results": results,
        "new_challenger": new_challenger,
        "eliminated": eliminated,
    }


def _mutate_config(config: dict) -> dict:
    """Small random mutation of a strategy config."""
    import random
    new = dict(config)
    # Mutate score_threshold ± 5
    if "score_threshold" in new:
        new["score_threshold"] = max(50, min(90, new["score_threshold"] + random.randint(-5, 5)))
    # Mutate position_size ± 0.1
    if "position_size" in new:
        new["position_size"] = round(max(0.3, min(1.5, new["position_size"] + random.uniform(-0.1, 0.1))), 2)
    return new


# ─── Weekly Report ────────────────────────────────────────────────────────────

def _load_changelog_last_n_days(days: int = 7) -> list:
    if not os.path.exists(EVOLUTION_CHANGELOG):
        return []
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    entries = []
    try:
        with open(EVOLUTION_CHANGELOG, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                    if e.get("timestamp", "") >= cutoff:
                        entries.append(e)
                except Exception:
                    pass
    except Exception:
        pass
    return entries


def generate_weekly_report(analysis: dict, ab_eval: dict) -> str:
    """生成Markdown格式进化周报。"""
    now = datetime.now()
    week_str = now.strftime("%Y-%m-%d")

    # Changelog summary
    changelog = _load_changelog_last_n_days(7)
    skills_added  = [e for e in changelog if e.get("type") == "skill_added"]
    bl_updates    = [e for e in changelog if e.get("type") == "blacklist_updated"]
    map_updates   = [e for e in changelog if e.get("type") == "market_strategy_updated"]
    ab_evals      = [e for e in changelog if e.get("type") == "ab_test_evaluation"]

    # Signal performance
    total        = analysis.get("total", 0)
    win_rate     = analysis.get("win_rate_overall", 0.0)
    market_state = analysis.get("market_state", "unknown")
    top_success  = analysis.get("success_patterns", [])[:3]
    top_failure  = analysis.get("failure_patterns", [])[:3]

    # Best signal type
    by_type = analysis.get("by_type", {})
    best_type = max(by_type.items(), key=lambda x: x[1].get("win_rate", 0), default=(None, {}))[0]

    # Market map
    market_map = _load_json(MARKET_MAP_FILE, {})

    lines = [
        f"🧬 *进化周报 {week_str}*",
        "",
        f"📊 *本周信号表现*",
        f"  总信号数: {total}",
        f"  整体胜率: {win_rate*100:.1f}%",
        f"  市场状态: {market_state}",
        f"  最佳信号类型: {best_type or '暂无'}",
        "",
        f"✅ *本周进化成果*",
        f"  新增技能: {len(skills_added)} 个",
        f"  新增黑名单: {sum(len(e.get('patterns_added',[])) for e in bl_updates)} 个模式",
        f"  市场策略更新: {len(map_updates)} 次",
        f"  A/B测试评估: {len(ab_evals)} 次",
    ]

    # A/B test result
    if ab_eval.get("action") == "eliminated":
        lines += [
            "",
            f"🔬 *A/B测试结果*",
            f"  胜者: 变体{ab_eval.get('winner', '?')} (score {ab_eval.get('results', {}).get(ab_eval.get('winner', ''), {}).get('score', 0):.3f})",
            f"  淘汰: 变体{ab_eval.get('loser', '?')} → 生成新挑战者",
        ]
    else:
        lines += [
            "",
            f"🔬 *A/B测试*: 暂无显著差异，继续观察",
        ]

    # Top patterns
    if top_success:
        lines += ["", f"🌟 *高胜率模式 (Top3)*"]
        for p in top_success:
            lines.append(f"  {p['key']}: {p['win_rate']*100:.0f}% (n={p['sample']})")

    if top_failure:
        lines += ["", f"⛔ *低胜率模式 (黑名单)*"]
        for p in top_failure:
            lines.append(f"  {p['key']}: {p['win_rate']*100:.0f}% (n={p['sample']})")

    # Market strategy map
    if market_map:
        lines += ["", f"🗺️ *市场状态→策略映射*"]
        for state, info in market_map.items():
            lines.append(f"  {state}: {info.get('best_signal_type','?')} (胜率{info.get('win_rate',0)*100:.0f}%)")

    lines += [
        "",
        f"📅 *下周计划*",
        f"  继续A/B测试: 变体A vs 变体B",
        f"  重点监控: {market_state}市场信号",
        f"  目标胜率: >{max(60.0, win_rate*100 + 2.0):.0f}%",
    ]

    return "\n".join(lines)


# ─── Daily Analysis Job ───────────────────────────────────────────────────────

async def run_daily_analysis() -> dict:
    """
    日分析任务:
      1. 加载过去7天信号
      2. 提取模式
      3. 更新 skill_library + 黑名单
      4. 更新市场状态→策略映射
      5. 更新元学习统计
    返回分析结果摘要。
    """
    logger.info("MetaLearner: starting daily analysis")
    signals = _load_recent_signals(days=7)

    if not signals:
        logger.info("MetaLearner: no resolved signals in last 7 days, skipping")
        return {"skipped": True, "reason": "no resolved signals"}

    analysis = analyze_patterns(signals)

    # Update skill library
    added_skills = inject_pattern_skills(analysis)

    # Update market strategy map
    update_market_strategy_map(analysis)

    # Save meta stats
    stats = _load_json(META_STATS_FILE, {"runs": []})
    stats["runs"].append({
        "at": datetime.now().isoformat(),
        "total_signals": analysis["total"],
        "win_rate": analysis["win_rate_overall"],
        "market_state": analysis["market_state"],
        "skills_added": len(added_skills),
        "success_patterns": len(analysis["success_patterns"]),
        "failure_patterns": len(analysis["failure_patterns"]),
    })
    stats["runs"] = stats["runs"][-365:]
    _save_json(META_STATS_FILE, stats)

    _append_changelog({
        "type": "daily_analysis",
        "total_signals": analysis["total"],
        "win_rate": analysis["win_rate_overall"],
        "market_state": analysis["market_state"],
        "skills_added": added_skills,
        "patterns_found": len(analysis["success_patterns"]) + len(analysis["failure_patterns"]),
    })

    logger.info(
        f"MetaLearner: daily analysis done — signals={analysis['total']} "
        f"win_rate={analysis['win_rate_overall']:.2%} "
        f"skills_added={len(added_skills)}"
    )
    return analysis


async def run_weekly_report(send_func=None) -> str:
    """
    周报任务: 分析 + A/B评估 + 生成周报 → 发Telegram。
    """
    logger.info("MetaLearner: generating weekly report")

    signals = _load_recent_signals(days=7)
    analysis = analyze_patterns(signals)
    ab_eval  = evaluate_ab_test()

    report = generate_weekly_report(analysis, ab_eval)

    _append_changelog({
        "type": "weekly_report_sent",
        "win_rate": analysis.get("win_rate_overall"),
        "market_state": analysis.get("market_state"),
        "ab_action": ab_eval.get("action"),
    })

    if send_func:
        try:
            await send_func(report)
        except Exception as e:
            logger.warning(f"MetaLearner: failed to send weekly report: {e}")

    return report


# ─── MetaLearner Background Service ──────────────────────────────────────────

class MetaLearner:
    """
    后台异步服务:
      - 每天凌晨2点: run_daily_analysis()
      - 每周日9点:   run_weekly_report() + AB评估
    """

    def __init__(self, send_func: Optional[Callable[..., Coroutine]] = None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="meta_learner")
        self._task.add_done_callback(self._on_done)
        logger.info("MetaLearner started (daily analysis + weekly report)")

    async def stop(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    def _on_done(self, task: asyncio.Task):
        try:
            exc = task.exception()
            if exc:
                logger.error(f"MetaLearner task crashed: {exc}", exc_info=exc)
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass

    async def _loop(self) -> None:
        """Main scheduler loop. Checks every minute if a job should run."""
        last_daily_date   = None
        last_weekly_date  = None

        while self._running:
            try:
                now = datetime.now()

                # Daily analysis at DAILY_ANALYSIS_HOUR
                today = now.date()
                if (
                    now.hour == DAILY_ANALYSIS_HOUR
                    and now.minute < 5
                    and last_daily_date != today
                ):
                    last_daily_date = today
                    try:
                        await run_daily_analysis()
                    except Exception as e:
                        logger.error(f"MetaLearner daily analysis error: {e}", exc_info=True)

                # Weekly report on Sunday at WEEKLY_REPORT_HOUR
                if (
                    now.weekday() == WEEKLY_REPORT_DOW
                    and now.hour == WEEKLY_REPORT_HOUR
                    and now.minute < 5
                    and last_weekly_date != today
                ):
                    last_weekly_date = today
                    try:
                        await run_weekly_report(send_func=self._send)
                    except Exception as e:
                        logger.error(f"MetaLearner weekly report error: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"MetaLearner loop error: {e}", exc_info=True)

            await asyncio.sleep(60)  # check every minute

    # ── Public API ──────────────────────────────────────────────────────────

    async def trigger_daily_analysis(self) -> dict:
        """手动触发日分析（用于测试或命令行触发）。"""
        return await run_daily_analysis()

    async def trigger_weekly_report(self) -> str:
        """手动触发周报（用于测试或命令行触发）。"""
        return await run_weekly_report(send_func=self._send)

    def get_market_strategy(self, market_state: str) -> Optional[dict]:
        return get_strategy_for_market(market_state)

    def get_ab_variant(self) -> str:
        return get_active_variant()

    def format_status(self) -> str:
        """格式化当前元学习状态摘要。"""
        stats = _load_json(META_STATS_FILE, {"runs": []})
        runs = stats.get("runs", [])
        last_run = runs[-1] if runs else None

        ab_state = get_ab_state()
        variants = ab_state.get("variants", {})

        blacklist = _load_json(SKILL_BLACKLIST_FILE, {"patterns": []})

        lines = ["🧬 *MetaLearner 状态*", ""]
        if last_run:
            lines += [
                f"最近分析: {last_run.get('at', '未知')[:16]}",
                f"信号总量: {last_run.get('total_signals', 0)}",
                f"整体胜率: {last_run.get('win_rate', 0)*100:.1f}%",
                f"市场状态: {last_run.get('market_state', '?')}",
                f"本次新增技能: {last_run.get('skills_added', 0)}",
                "",
            ]
        else:
            lines.append("尚未运行日分析\n")

        lines.append(f"⛔ 黑名单模式数: {len(blacklist.get('patterns', []))}")
        lines.append("")
        lines.append("🔬 *A/B测试变体*")
        for name, v in variants.items():
            total = v.get("wins", 0) + v.get("losses", 0)
            wr = v["wins"] / total if total else 0
            marker = "← 当前" if name == ab_state.get("current_variant") else ""
            lines.append(f"  变体{name}: 胜率{wr*100:.0f}% (n={total}) {marker}")

        return "\n".join(lines)


# ─── Module-level singleton ───────────────────────────────────────────────────

meta_learner = MetaLearner()
