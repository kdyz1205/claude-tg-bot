"""
Intelligence Skill — Self-learning, pattern recognition, and adaptive behavior.
This skill makes the bot genuinely smarter over time by:
1. Analyzing what works and what doesn't
2. Extracting reusable patterns from successful interactions
3. Adapting strategies based on failure analysis
4. Building a knowledge graph of capabilities and their effectiveness
"""

import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Failure categories
# ---------------------------------------------------------------------------

_FAILURE_CATEGORIES = {
    "tool_error": [
        "toolerror", "tool_use", "tool not found", "invalid tool",
        "bash error", "command not found", "no such file",
    ],
    "permission": [
        "permission denied", "access denied", "unauthorized", "forbidden",
        "not allowed", "eacces", "eperm",
    ],
    "timeout": [
        "timeout", "timed out", "deadline exceeded", "took too long",
        "asyncio.timeouterror", "read timed out",
    ],
    "logic": [
        "keyerror", "typeerror", "valueerror", "attributeerror",
        "indexerror", "nameerror", "zerodivision", "assertion",
    ],
    "resource": [
        "out of memory", "disk full", "no space", "quota exceeded",
        "rate limit", "too many requests", "429",
    ],
    "network": [
        "connection refused", "connection reset", "dns", "unreachable",
        "ssl", "certificate", "econnrefused",
    ],
    "user_misunderstanding": [
        "unclear", "ambiguous", "what do you mean", "please clarify",
        "not sure what", "which one",
    ],
}


# ---------------------------------------------------------------------------
# IntelligenceSkill
# ---------------------------------------------------------------------------

class IntelligenceSkill:
    """Self-learning intelligence engine that integrates with the bot's
    existing harness_learn, self_monitor, and skill_library systems."""

    def __init__(self, data_dir: str = "intelligence_data"):
        self._data_dir = os.path.join(BOT_DIR, data_dir)
        os.makedirs(self._data_dir, exist_ok=True)

        # File paths
        self._failures_file = os.path.join(self._data_dir, "failures.json")
        self._patterns_file = os.path.join(self._data_dir, "patterns.json")
        self._evaluations_file = os.path.join(self._data_dir, "evaluations.json")
        self._capabilities_file = os.path.join(self._data_dir, "capabilities.json")

        # Load existing data
        self._failures: list[dict] = self._load_json(self._failures_file, [])
        self._patterns: list[dict] = self._load_json(self._patterns_file, [])
        self._evaluations: list[dict] = self._load_json(self._evaluations_file, [])
        self._capabilities: dict[str, dict] = self._load_json(self._capabilities_file, {})

    # ── persistence helpers ───────────────────────────────────────────────

    @staticmethod
    def _load_json(path: str, default: Any) -> Any:
        """Load JSON file, return *default* on any error."""
        try:
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as exc:
            logger.warning("intelligence: failed to load %s: %s", path, exc)
        # Return a fresh copy so callers don't share the same mutable default
        return type(default)() if isinstance(default, (list, dict)) else default

    def _save_json(self, path: str, data: Any) -> None:
        """Atomic write: write to tmp then move into place."""
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        except Exception as exc:
            logger.warning("intelligence: failed to save %s: %s", path, exc)
            try:
                os.unlink(tmp)
            except OSError:
                pass

    def _persist_all(self) -> None:
        """Save all data stores to disk."""
        self._save_json(self._failures_file, self._failures)
        self._save_json(self._patterns_file, self._patterns)
        self._save_json(self._evaluations_file, self._evaluations)
        self._save_json(self._capabilities_file, self._capabilities)

    # ── 1. analyze_failure ────────────────────────────────────────────────

    def analyze_failure(self, action: str, error: str, context: dict) -> dict:
        """Categorize a failure, check history, and generate fix suggestions.

        Returns:
            {category, frequency, suggestions: list, similar_failures: list}
        """
        category = self._categorize_failure(error)
        error_norm = self._normalize_error(error)

        # Record this failure
        entry = {
            "ts": datetime.now().isoformat(),
            "action": action[:200],
            "error": error[:500],
            "error_norm": error_norm,
            "category": category,
            "context": _truncate_dict(context),
        }
        self._failures.append(entry)
        # Cap at 2000 entries
        if len(self._failures) > 2000:
            self._failures = self._failures[-2000:]
        self._save_json(self._failures_file, self._failures)

        # Find similar past failures
        similar = [
            f for f in self._failures[:-1]
            if f["error_norm"] == error_norm or f["action"] == action[:200]
        ]
        frequency = len(similar) + 1  # including current

        # Generate suggestions based on category
        suggestions = self._generate_fix_suggestions(category, action, error, similar)

        # Update capability tracking
        self._record_capability_outcome(action, success=False)

        # Also feed into harness_learn failure patterns if available
        try:
            from harness_learn import record_failure_pattern
            record_failure_pattern(task_type=action[:50], error=error[:200])
        except ImportError:
            pass

        return {
            "category": category,
            "frequency": frequency,
            "suggestions": suggestions,
            "similar_failures": [
                {
                    "action": f["action"],
                    "error": f["error"][:200],
                    "ts": f["ts"],
                    "category": f["category"],
                }
                for f in similar[-5:]
            ],
        }

    def _categorize_failure(self, error: str) -> str:
        """Map an error string to a failure category."""
        err_lower = error.lower()
        best_category = "unknown"
        best_count = 0
        for category, keywords in _FAILURE_CATEGORIES.items():
            hits = sum(1 for kw in keywords if kw in err_lower)
            if hits > best_count:
                best_count = hits
                best_category = category
        return best_category

    @staticmethod
    def _normalize_error(error: str) -> str:
        """Collapse variable parts of an error into a stable signature."""
        s = error.strip()[:300]
        s = re.sub(r"0x[0-9a-fA-F]+", "0x...", s)
        s = re.sub(r"line \d+", "line N", s)
        s = re.sub(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}[:\d.]*", "TS", s)
        s = re.sub(r"\d{5,}", "NUM", s)
        s = re.sub(r"'[^']{50,}'", "'...'", s)
        s = re.sub(r'"[^"]{50,}"', '"..."', s)
        return s.strip()

    @staticmethod
    def _generate_fix_suggestions(
        category: str, action: str, error: str, similar: list[dict]
    ) -> list[str]:
        """Generate actionable suggestions based on failure category and history."""
        suggestions: list[str] = []

        category_hints = {
            "tool_error": [
                "Verify the tool/command exists and is installed",
                "Check that all required arguments are provided",
                "Try an alternative tool that accomplishes the same goal",
            ],
            "permission": [
                "Run with elevated privileges if appropriate",
                "Check file/directory ownership and permissions",
                "Verify API keys and authentication tokens are valid",
            ],
            "timeout": [
                "Increase the timeout value for this operation",
                "Break the task into smaller chunks",
                "Check network connectivity and server availability",
            ],
            "logic": [
                "Validate input data types and formats before processing",
                "Add null/empty checks for the failing variable",
                "Review the data flow to find where the wrong type is introduced",
            ],
            "resource": [
                "Free up system resources (memory, disk space)",
                "Implement exponential backoff for rate limits",
                "Process data in smaller batches to reduce memory usage",
            ],
            "network": [
                "Check internet connectivity",
                "Verify the target host/URL is correct and reachable",
                "Retry with exponential backoff for transient failures",
            ],
            "user_misunderstanding": [
                "Make a reasonable assumption and proceed (do not ask questions)",
                "Use context clues to determine the most likely intent",
                "Choose the most common/default option when ambiguous",
            ],
        }
        suggestions.extend(category_hints.get(category, [
            "Review the error message for specific clues",
            "Try a different approach to accomplish the same goal",
        ]))

        # If this failure recurs, add escalation suggestions
        if len(similar) >= 3:
            suggestions.insert(0, f"RECURRING ({len(similar)+1}x): This exact failure keeps happening. "
                               "The previous approach is not working -- try a fundamentally different strategy.")
        elif len(similar) >= 1:
            suggestions.insert(0, f"Seen {len(similar)+1}x: Consider a different approach than last time.")

        return suggestions

    # ── 2. extract_pattern ────────────────────────────────────────────────

    def extract_pattern(self, interaction: dict) -> dict | None:
        """From a successful interaction, extract a reusable pattern.

        Args:
            interaction: {
                "user_request": str,
                "response": str,
                "steps": list[str],  # steps taken
                "tools_used": list[str],  # tools/commands used
                "duration_ms": int,
                "score": float,  # 0.0-1.0
                "context": dict,  # any relevant context
            }

        Returns:
            Extracted pattern dict, or None if interaction is not pattern-worthy.
        """
        score = interaction.get("score", 0)
        if score < 0.7:
            return None

        request = interaction.get("user_request", "")
        steps = interaction.get("steps", [])
        if not request or not steps:
            return None

        # Build the trigger: extract key action words from the request
        trigger = self._extract_trigger(request)
        if not trigger:
            return None

        # Determine context requirements
        context = interaction.get("context", {})
        context_requirements = [k for k, v in context.items() if v] if context else []

        # Check for existing similar pattern
        existing = self._find_similar_pattern(trigger)

        if existing is not None:
            # Update existing pattern with new data
            idx, pattern = existing
            use_count = pattern.get("use_count", 1)
            old_rate = pattern.get("success_rate", 0.5)
            pattern["success_rate"] = round(
                (old_rate * use_count + score) / (use_count + 1), 3
            )
            pattern["use_count"] = use_count + 1
            pattern["last_used"] = datetime.now().isoformat()
            # Merge steps if the new interaction has more detail
            if len(steps) > len(pattern.get("steps", [])):
                pattern["steps"] = steps[:20]
            # Merge context requirements
            old_reqs = set(pattern.get("context_requirements", []))
            pattern["context_requirements"] = list(old_reqs | set(context_requirements))[:10]
            self._patterns[idx] = pattern
        else:
            # Create new pattern
            pattern = {
                "id": f"pat_{int(time.time())}_{hash(trigger) % 10000:04d}",
                "trigger": trigger,
                "steps": steps[:20],
                "tools_used": interaction.get("tools_used", [])[:10],
                "success_rate": score,
                "use_count": 1,
                "context_requirements": context_requirements[:10],
                "created_at": datetime.now().isoformat(),
                "last_used": datetime.now().isoformat(),
                "source_request": request[:300],
            }
            self._patterns.append(pattern)

        # Cap at 500 patterns
        if len(self._patterns) > 500:
            # Remove least-used, oldest patterns
            self._patterns.sort(
                key=lambda p: (p.get("use_count", 0), p.get("last_used", "")),
                reverse=True,
            )
            self._patterns = self._patterns[:500]

        self._save_json(self._patterns_file, self._patterns)

        # Also try to feed into skill_library
        try:
            from harness_learn import record_successful_workflow, _classify_task
            task_type = _classify_task(request)
            record_successful_workflow(task_type, steps[:10], interaction.get("duration_ms", 0))
        except ImportError:
            pass

        return pattern

    def _extract_trigger(self, request: str) -> str:
        """Extract a generalizable trigger phrase from a user request."""
        # Remove specific names, paths, URLs -- keep the action pattern
        trigger = request[:200].lower()
        # Remove file paths
        trigger = re.sub(r'[a-zA-Z]:[/\\][^\s]+', 'FILE_PATH', trigger)
        trigger = re.sub(r'/[\w/.-]+', 'FILE_PATH', trigger)
        # Remove URLs
        trigger = re.sub(r'https?://\S+', 'URL', trigger)
        # Remove quoted strings (specific values)
        trigger = re.sub(r'"[^"]*"', 'VALUE', trigger)
        trigger = re.sub(r"'[^']*'", 'VALUE', trigger)
        # Remove numbers (specific counts/ids)
        trigger = re.sub(r'\b\d+\b', 'N', trigger)
        # Collapse whitespace
        trigger = re.sub(r'\s+', ' ', trigger).strip()
        return trigger if len(trigger) > 5 else ""

    def _find_similar_pattern(self, trigger: str) -> tuple[int, dict] | None:
        """Find a pattern with a similar trigger. Returns (index, pattern) or None."""
        trigger_words = set(trigger.split())
        best_idx = -1
        best_overlap = 0
        for i, pattern in enumerate(self._patterns):
            pat_words = set(pattern.get("trigger", "").split())
            overlap = len(trigger_words & pat_words)
            # Require at least 60% word overlap
            max_words = max(len(trigger_words), len(pat_words), 1)
            if overlap / max_words >= 0.6 and overlap > best_overlap:
                best_overlap = overlap
                best_idx = i
        if best_idx >= 0:
            return best_idx, self._patterns[best_idx]
        return None

    # ── 3. suggest_approach ───────────────────────────────────────────────

    def suggest_approach(self, user_request: str, context: dict) -> dict:
        """Given a user request, find the best approach from learned patterns.

        Returns:
            {approach: str, confidence: float, steps: list, alternatives: list}
        """
        trigger = self._extract_trigger(user_request)
        req_lower = user_request.lower()
        req_words = set(re.findall(r'[\w\u4e00-\u9fff]+', req_lower))

        scored_patterns: list[tuple[float, dict]] = []

        for pattern in self._patterns:
            score = 0.0

            # Trigger similarity
            pat_trigger = pattern.get("trigger", "")
            pat_words = set(pat_trigger.split())
            if pat_words:
                trigger_words = set(trigger.split()) if trigger else set()
                word_overlap = len(trigger_words & pat_words) / max(len(pat_words), 1)
                score += word_overlap * 3.0

            # Keyword overlap with source request
            source = pattern.get("source_request", "").lower()
            source_words = set(re.findall(r'[\w\u4e00-\u9fff]+', source))
            if source_words:
                kw_overlap = len(req_words & source_words) / max(len(source_words), 1)
                score += kw_overlap * 2.0

            # Success rate bonus
            success_rate = pattern.get("success_rate", 0.5)
            score += success_rate * 1.0

            # Usage count bonus (well-tested patterns are more reliable)
            use_count = pattern.get("use_count", 0)
            score += min(use_count / 10.0, 1.0) * 0.5

            # Context match bonus
            ctx_reqs = set(pattern.get("context_requirements", []))
            ctx_keys = set(context.keys()) if context else set()
            if ctx_reqs:
                ctx_match = len(ctx_reqs & ctx_keys) / len(ctx_reqs)
                score += ctx_match * 1.0

            # Recency bonus
            last_used = pattern.get("last_used", "")
            if last_used:
                try:
                    age_days = (datetime.now() - datetime.fromisoformat(last_used)).days
                    score += max(0, (30 - age_days) / 30) * 0.5
                except (ValueError, TypeError):
                    pass

            if score > 1.0:
                scored_patterns.append((score, pattern))

        scored_patterns.sort(key=lambda x: -x[0])

        # Also check action_memory for best approach
        action_memory_suggestion = None
        try:
            from self_monitor import action_memory
            task_type = self._classify_request(user_request)
            action_memory_suggestion = action_memory.get_best_approach(task_type, context or {})
        except (ImportError, Exception):
            pass

        # Also check harness_learn workflows
        workflow_suggestion = None
        try:
            from harness_learn import get_relevant_workflow
            workflow_suggestion = get_relevant_workflow(user_request)
        except ImportError:
            pass

        if not scored_patterns:
            # Fall back to workflow suggestion or action memory
            approach = "No learned pattern found."
            steps: list[str] = []
            confidence = 0.0

            if workflow_suggestion:
                approach = f"Workflow template: {workflow_suggestion.get('task_type', 'unknown')}"
                steps = workflow_suggestion.get("steps", [])
                confidence = 0.4

            if action_memory_suggestion:
                approach += f" | Past success: {action_memory_suggestion.get('action_type', '')}"
                confidence = max(confidence, 0.3)

            return {
                "approach": approach,
                "confidence": confidence,
                "steps": steps,
                "alternatives": [],
            }

        best_score, best_pattern = scored_patterns[0]
        max_possible = 8.0  # rough max score from all factors
        confidence = min(best_score / max_possible, 1.0)

        # Build alternatives from the next-best patterns
        alternatives = []
        for alt_score, alt_pattern in scored_patterns[1:4]:
            alternatives.append({
                "approach": alt_pattern.get("trigger", alt_pattern.get("source_request", ""))[:150],
                "confidence": round(min(alt_score / max_possible, 1.0), 2),
                "steps": alt_pattern.get("steps", [])[:5],
            })

        # Include workflow as alternative if available
        if workflow_suggestion and confidence < 0.8:
            alternatives.append({
                "approach": f"Workflow: {workflow_suggestion.get('task_type', '')}",
                "confidence": 0.4,
                "steps": workflow_suggestion.get("steps", [])[:5],
            })

        return {
            "approach": best_pattern.get("source_request", best_pattern.get("trigger", ""))[:300],
            "confidence": round(confidence, 2),
            "steps": best_pattern.get("steps", []),
            "alternatives": alternatives,
        }

    # ── 4. evaluate_response ──────────────────────────────────────────────

    def evaluate_response(
        self, request: str, response: str, user_feedback: str = None
    ) -> dict:
        """Score how good a response was.

        Returns:
            {score: float, strengths: list, weaknesses: list, improvement_notes: list}
        """
        strengths: list[str] = []
        weaknesses: list[str] = []
        improvement_notes: list[str] = []
        scores: dict[str, float] = {}

        resp_lower = (response or "").lower()
        req_lower = (request or "").lower()

        # ── Relevance: does the response address the request? ──
        req_words = set(re.findall(r'[\w\u4e00-\u9fff]{2,}', req_lower))
        resp_words = set(re.findall(r'[\w\u4e00-\u9fff]{2,}', resp_lower))
        if req_words:
            overlap_ratio = len(req_words & resp_words) / len(req_words)
            scores["relevance"] = min(overlap_ratio * 1.5, 1.0)
            if scores["relevance"] > 0.7:
                strengths.append("Response directly addresses the request")
            elif scores["relevance"] < 0.3:
                weaknesses.append("Response may not address the actual request")
                improvement_notes.append("Focus more on what the user specifically asked for")
        else:
            scores["relevance"] = 0.5

        # ── Completeness: did it finish the task? ──
        completion_signals = ["done", "complete", "saved", "created", "fixed",
                              "commit", "wrote", "deployed", "installed",
                              "finished", "success"]
        completion_zh = ["完成", "成功", "已", "保存", "创建", "修复", "写入"]
        has_completion = (
            any(s in resp_lower for s in completion_signals)
            or any(s in resp_lower for s in completion_zh)
            or "✅" in response
        )
        error_signals = ["error", "failed", "exception", "traceback",
                         "could not", "unable to"]
        has_error = any(s in resp_lower for s in error_signals)

        if has_completion and not has_error:
            scores["completeness"] = 1.0
            strengths.append("Task completed successfully")
        elif has_completion and has_error:
            scores["completeness"] = 0.6
            weaknesses.append("Task completed but with errors")
            improvement_notes.append("Resolve errors before reporting completion")
        elif has_error:
            scores["completeness"] = 0.2
            weaknesses.append("Task failed with errors")
        else:
            scores["completeness"] = 0.5

        # ── Accuracy: no question-asking (violates bot rules) ──
        question_patterns = [
            r"你要.*吗", r"你想.*吗", r"哪种方式", r"要我.*吗",
            r"which.*prefer", r"do you want", r"should i",
            r"would you like", r"shall i",
        ]
        asked_question = any(re.search(p, resp_lower) for p in question_patterns)
        if asked_question:
            scores["accuracy"] = 0.2
            weaknesses.append("Asked the user a question instead of acting")
            improvement_notes.append("Never ask questions -- make a decision and execute")
        else:
            scores["accuracy"] = 1.0
            strengths.append("Took action without asking unnecessary questions")

        # ── Helpfulness / conciseness ──
        resp_len = len(response) if response else 0
        if resp_len == 0:
            scores["helpfulness"] = 0.0
            weaknesses.append("Empty response")
        elif resp_len < 500:
            scores["helpfulness"] = 1.0
            strengths.append("Concise and to-the-point")
        elif resp_len < 2000:
            scores["helpfulness"] = 0.8
        elif resp_len < 5000:
            scores["helpfulness"] = 0.5
            weaknesses.append("Response is verbose")
            improvement_notes.append("Keep responses shorter -- user is on mobile")
        else:
            scores["helpfulness"] = 0.3
            weaknesses.append("Response is excessively long")
            improvement_notes.append("Drastically reduce output length")

        # ── User feedback override ──
        if user_feedback:
            fb_lower = user_feedback.lower()
            positive = ["good", "great", "thanks", "perfect", "nice", "awesome",
                        "correct", "right", "yes", "ok", "love",
                        "好", "棒", "对", "谢", "完美", "可以", "行"]
            negative = ["bad", "wrong", "no", "fail", "broken", "not what",
                        "terrible", "awful", "incorrect", "fix",
                        "不对", "错", "不行", "重来", "不是", "差"]
            if any(w in fb_lower for w in positive):
                scores["user_satisfaction"] = 1.0
                strengths.append("User expressed positive feedback")
            elif any(w in fb_lower for w in negative):
                scores["user_satisfaction"] = 0.0
                weaknesses.append("User expressed negative feedback")
                improvement_notes.append(f"User feedback: {user_feedback[:200]}")
            else:
                scores["user_satisfaction"] = 0.5

        # ── Overall score ──
        overall = round(sum(scores.values()) / len(scores), 3) if scores else 0.5

        # Record evaluation
        evaluation = {
            "ts": datetime.now().isoformat(),
            "request": request[:200],
            "response_len": resp_len,
            "scores": scores,
            "overall": overall,
            "user_feedback": (user_feedback or "")[:200],
        }
        self._evaluations.append(evaluation)
        if len(self._evaluations) > 2000:
            self._evaluations = self._evaluations[-2000:]
        self._save_json(self._evaluations_file, self._evaluations)

        # Update capability tracking
        task_type = self._classify_request(request)
        self._record_capability_outcome(task_type, success=(overall >= 0.6), score=overall)

        return {
            "score": overall,
            "strengths": strengths,
            "weaknesses": weaknesses,
            "improvement_notes": improvement_notes,
        }

    # ── 5. get_capability_map ─────────────────────────────────────────────

    def get_capability_map(self) -> dict:
        """Map of all known capabilities with their success rates.

        Returns dict organized by category:
            {category: [{capability_name, success_rate, avg_response_time,
                         last_used, usage_count}, ...]}
        """
        # Refresh from action_memory stats if available
        try:
            from self_monitor import action_memory
            for action_type, stats in action_memory._data.get("stats", {}).items():
                total = stats.get("ok", 0) + stats.get("fail", 0)
                if total == 0:
                    continue
                if action_type not in self._capabilities:
                    self._capabilities[action_type] = {
                        "success_count": 0,
                        "failure_count": 0,
                        "total_score": 0.0,
                        "score_count": 0,
                        "total_duration_ms": 0,
                        "last_used": "",
                        "category": self._infer_category(action_type),
                    }
                cap = self._capabilities[action_type]
                cap["success_count"] = stats.get("ok", 0)
                cap["failure_count"] = stats.get("fail", 0)
                cap["total_duration_ms"] = stats.get("total_ms", 0)
        except (ImportError, Exception):
            pass

        # Organize by category
        capability_map: dict[str, list[dict]] = {}
        for name, cap in self._capabilities.items():
            total = cap.get("success_count", 0) + cap.get("failure_count", 0)
            if total == 0:
                continue
            category = cap.get("category", "general")
            success_rate = cap["success_count"] / total if total else 0.0
            avg_time = cap.get("total_duration_ms", 0) / total if total else 0
            score_count = cap.get("score_count", 0)
            avg_score = (
                cap.get("total_score", 0) / score_count if score_count else None
            )

            entry = {
                "capability_name": name,
                "success_rate": round(success_rate, 3),
                "avg_response_time_ms": round(avg_time, 1),
                "avg_quality_score": round(avg_score, 3) if avg_score is not None else None,
                "last_used": cap.get("last_used", ""),
                "usage_count": total,
            }

            capability_map.setdefault(category, []).append(entry)

        # Sort each category by usage count descending
        for entries in capability_map.values():
            entries.sort(key=lambda e: e["usage_count"], reverse=True)

        return capability_map

    def _record_capability_outcome(
        self, action: str, success: bool, score: float = None
    ) -> None:
        """Update capability tracking for an action."""
        if not action:
            return
        if action not in self._capabilities:
            self._capabilities[action] = {
                "success_count": 0,
                "failure_count": 0,
                "total_score": 0.0,
                "score_count": 0,
                "total_duration_ms": 0,
                "last_used": "",
                "category": self._infer_category(action),
            }
        cap = self._capabilities[action]
        if success:
            cap["success_count"] += 1
        else:
            cap["failure_count"] += 1
        if score is not None:
            cap["total_score"] += score
            cap["score_count"] += 1
        cap["last_used"] = datetime.now().isoformat()
        self._save_json(self._capabilities_file, self._capabilities)

    @staticmethod
    def _infer_category(action_type: str) -> str:
        """Infer a capability category from the action type name."""
        a = action_type.lower()
        categories = {
            "pc_control": ["click", "type", "key", "mouse", "screenshot", "screen",
                           "window", "desktop", "computer"],
            "web": ["browser", "chrome", "navigate", "url", "http", "web",
                     "download", "fetch", "scrape"],
            "trading": ["trade", "order", "buy", "sell", "portfolio", "stock",
                        "crypto", "exchange", "binance", "策略"],
            "code": ["code", "git", "commit", "file", "write", "create",
                     "fix", "debug", "deploy", "script", "python"],
            "content": ["write", "draft", "email", "message", "translate",
                        "summarize", "content", "text"],
            "system": ["install", "service", "process", "restart", "config",
                       "setup", "self_heal", "monitor"],
        }
        for cat, keywords in categories.items():
            if any(kw in a for kw in keywords):
                return cat
        return "general"

    # ── 6. generate_training_data ─────────────────────────────────────────

    def generate_training_data(self, n: int = 10) -> list[dict]:
        """Generate synthetic training scenarios focused on weak areas.

        Returns list of {scenario, expected_behavior, difficulty}.
        """
        cap_map = self.get_capability_map()
        training_data: list[dict] = []

        # Find weak capabilities (low success rate or low quality score)
        weak_caps: list[tuple[str, dict]] = []
        for category, entries in cap_map.items():
            for entry in entries:
                success_rate = entry.get("success_rate", 1.0)
                quality = entry.get("avg_quality_score")
                usage = entry.get("usage_count", 0)
                # Weak = low success rate, or low quality, and used enough to judge
                if usage >= 3 and (success_rate < 0.7 or (quality and quality < 0.6)):
                    weak_caps.append((category, entry))

        # Also pull from failure patterns
        failure_types: Counter = Counter()
        for failure in self._failures[-200:]:
            failure_types[failure.get("category", "unknown")] += 1

        # Generate scenarios for weak areas
        scenario_templates = {
            "tool_error": [
                "Execute a command that requires a tool that may not be installed",
                "Use a shell command with edge-case arguments",
                "Handle a tool that produces unexpected output format",
            ],
            "permission": [
                "Modify a file in a restricted directory",
                "Access a resource requiring authentication",
                "Perform an operation requiring elevated privileges",
            ],
            "timeout": [
                "Process a very large file efficiently",
                "Make an API call to a slow endpoint",
                "Handle a long-running operation with proper status updates",
            ],
            "logic": [
                "Process input with unexpected data types",
                "Handle edge cases in string parsing",
                "Work with nested data structures with missing keys",
            ],
            "network": [
                "Fetch data from an unreliable API endpoint",
                "Handle connection drops mid-transfer",
                "Work with DNS resolution failures",
            ],
            "resource": [
                "Process data that could exceed memory limits",
                "Handle disk space limitations gracefully",
                "Work within API rate limits",
            ],
        }

        # Prioritize scenarios for the most common failure categories
        for category, count in failure_types.most_common():
            if len(training_data) >= n:
                break
            templates = scenario_templates.get(category, [])
            for template in templates:
                if len(training_data) >= n:
                    break
                difficulty = "hard" if count >= 5 else "medium" if count >= 2 else "easy"
                training_data.append({
                    "scenario": template,
                    "expected_behavior": (
                        f"Handle {category} failures gracefully: detect the issue, "
                        f"try alternative approaches, and report clearly."
                    ),
                    "difficulty": difficulty,
                    "target_category": category,
                    "failure_frequency": count,
                })

        # Fill remaining with weak-capability scenarios
        for category, entry in weak_caps:
            if len(training_data) >= n:
                break
            cap_name = entry["capability_name"]
            success_rate = entry["success_rate"]
            training_data.append({
                "scenario": (
                    f"Perform a {cap_name} task with success rate currently at "
                    f"{success_rate:.0%}. Focus on reliability and error handling."
                ),
                "expected_behavior": (
                    f"Complete the {cap_name} task successfully. "
                    f"Current weak point in category '{category}'."
                ),
                "difficulty": "hard" if success_rate < 0.4 else "medium",
                "target_category": category,
                "current_success_rate": success_rate,
            })

        # If still not enough, add general improvement scenarios
        general_scenarios = [
            {
                "scenario": "User sends a vague one-word message. Interpret and act.",
                "expected_behavior": "Make a reasonable interpretation and execute without asking questions.",
                "difficulty": "medium",
            },
            {
                "scenario": "Multiple tasks requested in a single message.",
                "expected_behavior": "Execute all tasks sequentially, report results concisely.",
                "difficulty": "hard",
            },
            {
                "scenario": "Task requires a tool that is currently unavailable.",
                "expected_behavior": "Find an alternative approach or install the required tool.",
                "difficulty": "medium",
            },
        ]
        while len(training_data) < n and general_scenarios:
            training_data.append(general_scenarios.pop(0))

        return training_data[:n]

    # ── 7. get_intelligence_report ────────────────────────────────────────

    def get_intelligence_report(self) -> str:
        """Human-readable report of the bot's intelligence level."""
        lines: list[str] = []
        lines.append("=" * 50)
        lines.append("  INTELLIGENCE REPORT")
        lines.append("=" * 50)
        lines.append("")

        # ── Overall score ──
        recent_evals = self._evaluations[-50:]
        if recent_evals:
            scores = [e.get("overall", 0) for e in recent_evals]
            avg_score = sum(scores) / len(scores) if scores else 0
            lines.append(f"Overall Intelligence Score: {avg_score:.1%}")

            # Trend: compare first half vs second half
            half = len(scores) // 2
            if half > 0:
                old_avg = sum(scores[:half]) / half
                new_avg = sum(scores[half:]) / (len(scores) - half)
                if new_avg > old_avg + 0.03:
                    trend = "IMPROVING"
                elif new_avg < old_avg - 0.03:
                    trend = "DECLINING"
                else:
                    trend = "STABLE"
                lines.append(f"Trend: {trend} ({old_avg:.1%} -> {new_avg:.1%})")
            lines.append(f"Evaluated interactions: {len(recent_evals)}")
        else:
            lines.append("Overall Intelligence Score: N/A (no evaluations yet)")

        lines.append("")

        # ── Strongest areas ──
        cap_map = self.get_capability_map()
        all_caps: list[tuple[str, str, float, int]] = []
        for category, entries in cap_map.items():
            for entry in entries:
                if entry["usage_count"] >= 2:
                    all_caps.append((
                        category,
                        entry["capability_name"],
                        entry["success_rate"],
                        entry["usage_count"],
                    ))

        if all_caps:
            all_caps.sort(key=lambda x: x[2], reverse=True)
            lines.append("STRONGEST AREAS:")
            for cat, name, rate, count in all_caps[:5]:
                lines.append(f"  [{cat}] {name}: {rate:.0%} success ({count} uses)")

            lines.append("")
            lines.append("WEAKEST AREAS:")
            all_caps.sort(key=lambda x: x[2])
            for cat, name, rate, count in all_caps[:5]:
                lines.append(f"  [{cat}] {name}: {rate:.0%} success ({count} uses)")
        else:
            lines.append("CAPABILITY DATA: Insufficient data for analysis")

        lines.append("")

        # ── Failure analysis ──
        recent_failures = self._failures[-100:]
        if recent_failures:
            category_counts = Counter(f["category"] for f in recent_failures)
            lines.append("FAILURE BREAKDOWN (last 100):")
            for cat, count in category_counts.most_common():
                bar = "#" * min(count, 30)
                lines.append(f"  {cat:25s} {count:3d} {bar}")
        else:
            lines.append("FAILURE BREAKDOWN: No failures recorded")

        lines.append("")

        # ── Pattern library ──
        lines.append(f"LEARNED PATTERNS: {len(self._patterns)}")
        if self._patterns:
            high_quality = [p for p in self._patterns if p.get("success_rate", 0) >= 0.8]
            lines.append(f"  High quality (>80%): {len(high_quality)}")
            most_used = sorted(self._patterns, key=lambda p: p.get("use_count", 0), reverse=True)
            if most_used[:3]:
                lines.append("  Most used patterns:")
                for p in most_used[:3]:
                    lines.append(
                        f"    - {p.get('trigger', '?')[:60]} "
                        f"(used {p.get('use_count', 0)}x, {p.get('success_rate', 0):.0%})"
                    )

        lines.append("")

        # ── Integration status ──
        lines.append("SYSTEM INTEGRATION:")
        integrations = {
            "harness_learn": False,
            "self_monitor": False,
            "skill_library": False,
        }
        try:
            import harness_learn  # noqa: F401
            integrations["harness_learn"] = True
        except ImportError:
            pass
        try:
            import self_monitor  # noqa: F401
            integrations["self_monitor"] = True
        except ImportError:
            pass
        try:
            import skill_library  # noqa: F401
            integrations["skill_library"] = True
        except ImportError:
            pass

        for module, available in integrations.items():
            status = "CONNECTED" if available else "NOT AVAILABLE"
            lines.append(f"  {module}: {status}")

        # ── Harness evolution stats ──
        try:
            from harness_learn import get_evolution_stats
            lines.append("")
            lines.append("EVOLUTION ENGINE:")
            lines.append(get_evolution_stats())
        except ImportError:
            pass

        # ── Skill library stats ──
        try:
            from skill_library import get_skill_stats
            lines.append("")
            lines.append("SKILL LIBRARY:")
            lines.append(get_skill_stats())
        except ImportError:
            pass

        lines.append("")
        lines.append("=" * 50)
        lines.append(f"Report generated: {datetime.now().isoformat()}")

        return "\n".join(lines)

    # ── Utility ───────────────────────────────────────────────────────────

    @staticmethod
    def _classify_request(request: str) -> str:
        """Classify request into a task type. Uses harness_learn if available."""
        try:
            from harness_learn import _classify_task
            return _classify_task(request)
        except ImportError:
            pass
        msg = request.lower()
        if any(w in msg for w in ["click", "type", "screenshot", "screen"]):
            return "computer_control"
        if any(w in msg for w in ["open", "browse", "chrome", "url"]):
            return "browser"
        if any(w in msg for w in ["write", "create", "fix", "code"]):
            return "code"
        return "general"


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _truncate_dict(d: Any, max_val_len: int = 200) -> dict:
    """Return a copy of *d* with string values truncated."""
    if not isinstance(d, dict):
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(v, str) and len(v) > max_val_len:
            out[k] = v[:max_val_len] + "..."
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

intelligence = IntelligenceSkill()
