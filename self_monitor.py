"""
Self-Monitoring and Action Memory System.
Tracks what actions succeed/fail, learns patterns, enables self-healing.
"""

import asyncio
import json
import logging
import os
import re
import threading
import time
from collections import Counter, deque
from datetime import datetime, timedelta
from typing import Any, Callable

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------------
# ActionMemory — persistent record of every action and its outcome
# ---------------------------------------------------------------------------

class ActionMemory:
    """Tracks action outcomes, learns success patterns, and suggests alternatives."""

    def __init__(self, memory_file: str = "action_memory.json", max_entries: int = 5000):
        self._path = os.path.join(BOT_DIR, memory_file)
        self._max_entries = max_entries
        self._data: dict[str, Any] = self._load()
        self._dirty_count = 0  # Track unsaved changes for debounced saving
        self._last_save_time: float = 0.0
        self._save_lock = threading.Lock()
        import atexit
        atexit.register(self._flush_on_exit)

    def _flush_on_exit(self) -> None:
        if self._dirty_count > 0:
            self._save()

    # ── persistence ──────────────────────────────────────────────────────

    def _load(self) -> dict:
        try:
            if os.path.exists(self._path):
                with open(self._path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and "actions" in data:
                    return data
        except Exception as exc:
            logger.warning("ActionMemory: failed to load %s: %s", self._path, exc)
        return {"actions": [], "stats": {}}

    def _save(self) -> None:
        with self._save_lock:
            try:
                tmp = self._path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(self._data, f, ensure_ascii=False, default=str)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self._path)
            except Exception as exc:
                logger.warning("ActionMemory: failed to save: %s", exc)

    # ── public API ───────────────────────────────────────────────────────

    def record_action(
        self,
        action_type: str,
        params: dict,
        success: bool,
        error: str | None = None,
        duration_ms: float = 0,
    ) -> None:
        """Log every action with its parameters, outcome, and timing."""
        entry = {
            "ts": datetime.now().isoformat(),
            "action_type": action_type,
            "params": _safe_truncate_dict(params),
            "success": success,
            "error": (error or "")[:500],
            "duration_ms": round(duration_ms, 1),
        }
        self._data["actions"].append(entry)

        # Update per-type running stats
        stats = self._data["stats"].setdefault(action_type, {"ok": 0, "fail": 0, "total_ms": 0})
        if success:
            stats["ok"] += 1
        else:
            stats["fail"] += 1
        stats["total_ms"] += duration_ms

        # Auto-prune when over limit
        if len(self._data["actions"]) > self._max_entries:
            self._data["actions"] = self._data["actions"][-self._max_entries:]

        # Debounced save: write at most every 30 seconds or every 20 actions
        self._dirty_count += 1
        now = time.time()
        if self._dirty_count >= 20 or (now - self._last_save_time) >= 30:
            self._save()
            self._dirty_count = 0
            self._last_save_time = now

    def get_success_rate(self, action_type: str) -> float:
        """Historical success rate for *action_type* (0.0 .. 1.0)."""
        stats = self._data["stats"].get(action_type)
        if not stats:
            return 0.0
        total = stats["ok"] + stats["fail"]
        return stats["ok"] / total if total else 0.0

    def get_best_approach(self, action_type: str, context: dict) -> dict | None:
        """Find the params of the most recent *successful* action of this type
        whose context keys overlap with *context*."""
        candidates = [
            a for a in reversed(self._data["actions"])
            if a["action_type"] == action_type and a["success"]
        ]
        if not candidates:
            return None

        # Score candidates by key-overlap with context
        ctx_keys = set(str(v).lower() for v in context.values() if v)
        best, best_score = None, -1
        for c in candidates[:200]:  # scan last 200 matches max
            c_vals = set(str(v).lower() for v in c["params"].values() if v)
            overlap = len(ctx_keys & c_vals)
            if overlap > best_score:
                best_score = overlap
                best = c
        return best

    def get_failure_patterns(self) -> list[dict]:
        """Identify recurring failures (same action_type + similar error seen >= 3 times)."""
        error_counter: dict[tuple[str, str], int] = {}
        first_seen: dict[tuple[str, str], str] = {}
        last_seen: dict[tuple[str, str], str] = {}

        for a in self._data["actions"]:
            if a["success"]:
                continue
            key = (a["action_type"], _normalise_error(a.get("error", "")))
            error_counter[key] = error_counter.get(key, 0) + 1
            if key not in first_seen:
                first_seen[key] = a["ts"]
            last_seen[key] = a["ts"]

        patterns = []
        for (atype, err_norm), count in error_counter.items():
            if count >= 3:
                patterns.append({
                    "action_type": atype,
                    "error_signature": err_norm,
                    "count": count,
                    "first_seen": first_seen[(atype, err_norm)],
                    "last_seen": last_seen[(atype, err_norm)],
                })
        patterns.sort(key=lambda p: p["count"], reverse=True)
        return patterns

    def suggest_alternative(self, failed_action: str, params: dict) -> dict | None:
        """Suggest alternative approach: find a *different* action type that
        succeeded in a similar context, or the same action type with different params."""
        # Strategy 1: same type, different params that succeeded
        same_type_ok = [
            a for a in reversed(self._data["actions"])
            if a["action_type"] == failed_action and a["success"]
        ]
        if same_type_ok:
            # Pick one whose params differ from the failing ones
            for candidate in same_type_ok[:50]:
                if candidate["params"] != _safe_truncate_dict(params):
                    return {
                        "strategy": "different_params",
                        "action_type": failed_action,
                        "suggested_params": candidate["params"],
                        "source_ts": candidate["ts"],
                    }

        # Strategy 2: different action type that succeeded in similar context
        param_vals = set(str(v).lower() for v in params.values() if v)
        for a in reversed(self._data["actions"]):
            if a["action_type"] == failed_action or not a["success"]:
                continue
            a_vals = set(str(v).lower() for v in a["params"].values() if v)
            if param_vals & a_vals:
                return {
                    "strategy": "different_action",
                    "action_type": a["action_type"],
                    "suggested_params": a["params"],
                    "source_ts": a["ts"],
                }
        return None

    def prune(self, max_age_days: int = 30) -> int:
        """Remove entries older than *max_age_days*. Returns number removed.
        Also rebuilds stats to remove stale action types."""
        cutoff = (datetime.now() - timedelta(days=max_age_days)).isoformat()
        before = len(self._data["actions"])
        self._data["actions"] = [
            a for a in self._data["actions"] if a.get("ts", "") >= cutoff
        ]
        removed = before - len(self._data["actions"])
        if removed:
            # Rebuild stats from remaining data (drops stale action_type keys)
            self._rebuild_stats()
            self._save()
            logger.info("ActionMemory: pruned %d entries older than %d days", removed, max_age_days)
        # Also cap total stats keys to prevent unbounded growth from unique action types
        max_stat_keys = 500
        if len(self._data["stats"]) > max_stat_keys:
            # Keep only action types still present in recent actions
            active_types = {a["action_type"] for a in self._data["actions"]}
            self._data["stats"] = {
                k: v for k, v in self._data["stats"].items() if k in active_types
            }
            self._save()
        return removed

    # ── internal helpers ─────────────────────────────────────────────────

    def _rebuild_stats(self) -> None:
        stats: dict[str, dict] = {}
        for a in self._data["actions"]:
            s = stats.setdefault(a["action_type"], {"ok": 0, "fail": 0, "total_ms": 0})
            if a["success"]:
                s["ok"] += 1
            else:
                s["fail"] += 1
            s["total_ms"] += a.get("duration_ms", 0)
        self._data["stats"] = stats


# ---------------------------------------------------------------------------
# SelfMonitor — continuous async health / anomaly monitor
# ---------------------------------------------------------------------------

class SelfMonitor:
    """Async monitoring loop that checks system health, bot status, and detects anomalies."""

    # Service health states
    STATE_HEALTHY = "healthy"       # Last N messages succeeded
    STATE_DEGRADED = "degraded"     # Some failures, still working
    STATE_BROKEN = "broken"         # Many consecutive failures
    STATE_CRITICAL = "critical"     # All services down

    def __init__(self, check_interval: int = 60):
        self._interval = check_interval
        self._running = False
        self._task: asyncio.Task | None = None
        self._alert_handlers: list[Callable] = []

        # Rolling windows for anomaly detection
        self._error_window: deque[float] = deque(maxlen=3600)  # timestamps of errors in last hour
        self._memory_samples: deque[tuple[float, float]] = deque(maxlen=120)  # (ts, mem_pct)
        self._response_times: deque[tuple[float, float]] = deque(maxlen=500)  # (ts, ms)
        self._recent_errors: deque[tuple[float, str]] = deque(maxlen=200)

        self._last_successful_msg_time: float = 0.0
        self._bot_start_time: float = time.time()
        self._last_health: dict = {}
        self._last_alert_times: dict[str, float] = {}

        # Service health tracking — tracks each backend independently
        self._service_health: dict[str, dict] = {}  # service -> {failures, last_success, last_failure, state}
        self._overall_state: str = self.STATE_HEALTHY
        self._consecutive_msg_failures: int = 0
        self._silence_alerted: bool = False  # Only alert once per silence period

    # ── lifecycle ────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the background monitoring loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("SelfMonitor started (interval=%ds)", self._interval)

    async def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("SelfMonitor stopped")

    async def _loop(self) -> None:
        _prune_counter = 0
        while self._running:
            try:
                health = await self.check_health()
                self._last_health = health
                anomalies = await self.detect_anomalies()
                if anomalies:
                    await self._fire_alerts(anomalies)
                # Periodic action memory pruning (every ~60 iterations = ~1 hour at 60s interval)
                _prune_counter += 1
                if _prune_counter >= 60:
                    _prune_counter = 0
                    try:
                        action_memory.prune(max_age_days=30)
                    except Exception as _pe:
                        logger.warning("Action memory prune failed: %s", _pe)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("SelfMonitor loop error: %s", exc, exc_info=True)
            await asyncio.sleep(self._interval)

    # ── external event hooks (call from bot code) ────────────────────────

    def record_message_success(self) -> None:
        """Call when a message is successfully processed."""
        self._last_successful_msg_time = time.time()
        self._consecutive_msg_failures = 0
        self._silence_alerted = False  # Reset so next silence period gets one alert
        # Update overall state
        if self._overall_state != self.STATE_HEALTHY:
            logger.info("Bot recovered to HEALTHY state")
            self._overall_state = self.STATE_HEALTHY

    def record_service_success(self, service: str) -> None:
        """Call when a specific service (cli/api/webai) succeeds."""
        info = self._service_health.setdefault(service, {
            "failures": 0, "last_success": 0, "last_failure": 0, "state": self.STATE_HEALTHY,
        })
        info["failures"] = 0
        info["last_success"] = time.time()
        info["state"] = self.STATE_HEALTHY

    def record_service_failure(self, service: str, error: str = "") -> None:
        """Call when a specific service fails."""
        info = self._service_health.setdefault(service, {
            "failures": 0, "last_success": 0, "last_failure": 0, "state": self.STATE_HEALTHY,
        })
        info["failures"] += 1
        info["last_failure"] = time.time()
        if error:
            info["last_error"] = error[:300]
        # State transitions
        if info["failures"] >= 10:
            info["state"] = self.STATE_BROKEN
        elif info["failures"] >= 3:
            info["state"] = self.STATE_DEGRADED

    def record_message_failure(self) -> None:
        """Call when message processing fails (after all fallbacks)."""
        self._consecutive_msg_failures += 1
        if self._consecutive_msg_failures >= 10:
            self._overall_state = self.STATE_CRITICAL
        elif self._consecutive_msg_failures >= 3:
            self._overall_state = self.STATE_BROKEN

    def get_service_state(self, service: str) -> str:
        """Get the health state of a specific service."""
        info = self._service_health.get(service, {})
        return info.get("state", self.STATE_HEALTHY)

    def get_overall_state(self) -> str:
        """Get overall bot health state."""
        return self._overall_state

    def get_health_summary(self) -> dict:
        """Get a summary of all service health states."""
        return {
            "overall": self._overall_state,
            "consecutive_failures": self._consecutive_msg_failures,
            "services": {
                svc: {"state": info.get("state"), "failures": info.get("failures", 0)}
                for svc, info in self._service_health.items()
            },
            "last_success_ago": round(time.time() - self._last_successful_msg_time)
            if self._last_successful_msg_time > 0 else None,
        }

    def record_error(self, error_msg: str) -> None:
        """Call when an error occurs anywhere in the system."""
        now = time.time()
        self._error_window.append(now)
        self._recent_errors.append((now, error_msg[:300]))

    def record_response_time(self, duration_ms: float) -> None:
        """Call after each request completes."""
        self._response_times.append((time.time(), duration_ms))

    # ── health checks ───────────────────────────────────────────────────

    async def check_health(self) -> dict:
        """Check system health: CPU, memory, disk, network."""
        health: dict[str, Any] = {"ts": datetime.now().isoformat(), "ok": True, "checks": {}}

        # CPU
        cpu = await self._check_cpu()
        health["checks"]["cpu"] = cpu
        if cpu.get("usage_pct", 0) > 90:
            health["ok"] = False

        # Memory
        mem = await self._check_memory()
        health["checks"]["memory"] = mem
        if mem.get("usage_pct", 0) > 90:
            health["ok"] = False
        # Record sample for leak detection
        self._memory_samples.append((time.time(), mem.get("usage_pct", 0)))

        # Disk
        disk = await self._check_disk()
        health["checks"]["disk"] = disk
        if disk.get("usage_pct", 0) > 95:
            health["ok"] = False

        # Network (Telegram API reachable)
        net = await self.check_telegram_connection()
        health["checks"]["network"] = net
        if not net.get("reachable"):
            health["ok"] = False

        # Bot
        bot = await self.check_bot_status()
        health["checks"]["bot"] = bot

        # Error rate
        error_rate = self._error_rate_last_hour()
        health["checks"]["error_rate_1h"] = error_rate

        return health

    async def _check_cpu(self) -> dict:
        if HAS_PSUTIL:
            usage = await asyncio.to_thread(psutil.cpu_percent, interval=0.5)
            return {"usage_pct": usage, "status": "ok" if usage < 90 else "high"}
        # Fallback: use wmic on Windows
        try:
            proc = await asyncio.create_subprocess_exec(
                "wmic", "cpu", "get", "loadpercentage",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
            lines = stdout.decode(errors="replace").strip().splitlines()
            for line in lines:
                line = line.strip()
                if line.isdigit():
                    pct = float(line)
                    return {"usage_pct": pct, "status": "ok" if pct < 90 else "high"}
        except Exception:
            pass
        return {"usage_pct": -1, "status": "unknown"}

    async def _check_memory(self) -> dict:
        if HAS_PSUTIL:
            vm = await asyncio.to_thread(psutil.virtual_memory)
            return {
                "usage_pct": vm.percent,
                "used_gb": round(vm.used / (1024 ** 3), 2),
                "total_gb": round(vm.total / (1024 ** 3), 2),
                "status": "ok" if vm.percent < 85 else "high" if vm.percent < 95 else "critical",
            }
        # Fallback
        try:
            proc = await asyncio.create_subprocess_exec(
                "wmic", "OS", "get", "FreePhysicalMemory,TotalVisibleMemorySize", "/format:csv",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
            for line in stdout.decode(errors="replace").splitlines():
                parts = line.strip().split(",")
                if len(parts) >= 3:
                    try:
                        free_kb = int(parts[1])
                        total_kb = int(parts[2])
                        used_pct = round((1 - free_kb / total_kb) * 100, 1)
                        return {"usage_pct": used_pct, "status": "ok" if used_pct < 85 else "high"}
                    except (ValueError, ZeroDivisionError):
                        continue
        except Exception:
            pass
        return {"usage_pct": -1, "status": "unknown"}

    async def _check_disk(self) -> dict:
        if HAS_PSUTIL:
            usage = await asyncio.to_thread(psutil.disk_usage, BOT_DIR)
            return {
                "usage_pct": usage.percent,
                "free_gb": round(usage.free / (1024 ** 3), 2),
                "status": "ok" if usage.percent < 90 else "low" if usage.percent < 95 else "critical",
            }
        # Fallback for Windows
        try:
            drive = os.path.splitdrive(BOT_DIR)[0] or "C:"
            proc = await asyncio.create_subprocess_exec(
                "powershell", "-NoProfile", "-Command",
                f"(Get-PSDrive {drive[0]}).Free / 1GB",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
            free_gb = float(stdout.decode(errors="replace").strip())
            return {"free_gb": round(free_gb, 2), "usage_pct": -1, "status": "ok" if free_gb > 5 else "low"}
        except Exception:
            pass
        return {"usage_pct": -1, "status": "unknown"}

    async def check_bot_status(self) -> dict:
        """Check if the bot process is running and responsive."""
        result: dict[str, Any] = {"alive": True}

        # Uptime
        uptime_s = time.time() - self._bot_start_time
        result["uptime_s"] = round(uptime_s)
        result["uptime_human"] = _format_duration(uptime_s)

        # Last successful message
        if self._last_successful_msg_time > 0:
            ago = time.time() - self._last_successful_msg_time
            result["last_msg_ago_s"] = round(ago)
            result["last_msg_status"] = "ok" if ago < 600 else "stale" if ago < 3600 else "dead"
        else:
            result["last_msg_status"] = "no_messages_yet"

        # Error rate
        result["error_rate_1h"] = self._error_rate_last_hour()

        # Python process memory (if psutil available)
        if HAS_PSUTIL:
            try:
                proc = psutil.Process(os.getpid())
                mem = proc.memory_info()
                result["process_rss_mb"] = round(mem.rss / (1024 ** 2), 1)
            except Exception:
                pass

        return result

    async def check_telegram_connection(self) -> dict:
        """Verify Telegram API is reachable."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "powershell", "-NoProfile", "-Command",
                "(Invoke-WebRequest -Uri 'https://api.telegram.org' -Method HEAD -TimeoutSec 5 -UseBasicParsing).StatusCode",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
            code = stdout.decode(errors="replace").strip()
            reachable = code in ("200", "302", "301", "404")  # any HTTP response = reachable
            return {"reachable": reachable, "status_code": code}
        except asyncio.TimeoutError:
            return {"reachable": False, "error": "timeout"}
        except Exception as exc:
            return {"reachable": False, "error": str(exc)[:200]}

    # ── anomaly detection ────────────────────────────────────────────────

    async def detect_anomalies(self) -> list[dict]:
        """Detect unusual patterns that may indicate problems."""
        anomalies: list[dict] = []
        now = time.time()

        # 1. Sudden spike in error rate
        error_rate = self._error_rate_last_hour()
        if error_rate > 20:
            anomalies.append({
                "type": "error_spike",
                "severity": "critical" if error_rate > 50 else "warning",
                "message": f"Error rate: {error_rate} errors/hour",
                "value": error_rate,
            })

        # 2. Memory leak detection (steadily increasing usage)
        if len(self._memory_samples) >= 10:
            samples = list(self._memory_samples)
            half = len(samples) // 2
            if half > 0 and len(samples) - half > 0:
                first_half_avg = sum(s[1] for s in samples[:half]) / half
                second_half_avg = sum(s[1] for s in samples[half:]) / (len(samples) - half)
            else:
                first_half_avg = second_half_avg = 0
            increase = second_half_avg - first_half_avg
            if increase > 5:  # >5% steady increase
                anomalies.append({
                    "type": "memory_leak",
                    "severity": "warning",
                    "message": f"Memory usage increasing: {first_half_avg:.1f}% -> {second_half_avg:.1f}% (+{increase:.1f}%)",
                    "value": increase,
                })

        # 3. Response time degradation
        if len(self._response_times) >= 20:
            times = list(self._response_times)
            half = len(times) // 2
            first_avg = sum(t[1] for t in times[:half]) / half
            second_avg = sum(t[1] for t in times[half:]) / (len(times) - half)
            if first_avg > 0 and second_avg > first_avg * 2:
                anomalies.append({
                    "type": "response_degradation",
                    "severity": "warning",
                    "message": f"Response time doubled: {first_avg:.0f}ms -> {second_avg:.0f}ms",
                    "value": second_avg,
                })

        # 4. Repeated same error
        if len(self._recent_errors) >= 5:
            cutoff = now - 600  # last 10 minutes
            recent = [e[1] for e in self._recent_errors if e[0] >= cutoff]
            if len(recent) >= 3:
                counter = Counter(recent)
                top_err, top_count = counter.most_common(1)[0]
                if top_count >= 3:
                    anomalies.append({
                        "type": "repeated_error",
                        "severity": "warning",
                        "message": f"Same error {top_count}x in 10min: {top_err[:150]}",
                        "value": top_count,
                    })

        # 5. No messages for a long time (if we ever received any)
        # Alert ONCE when silence exceeds 1 hour, then stop.
        # _silence_alerted is reset when a successful message is processed.
        if self._last_successful_msg_time > 0 and not self._silence_alerted:
            silence = now - self._last_successful_msg_time
            if silence > 3600:
                self._silence_alerted = True  # Only alert once per silence period
                anomalies.append({
                    "type": "message_silence",
                    "severity": "warning",
                    "message": f"No successful messages for {_format_duration(silence)}. Bot state: {self._overall_state}",
                    "value": silence,
                })

        return anomalies

    # ── alerts ───────────────────────────────────────────────────────────

    def register_alert_handler(self, handler: Callable) -> None:
        """Register a callback ``async def handler(anomalies: list[dict])``.
        Prevents duplicate registrations to avoid handler accumulation on restarts."""
        if handler not in self._alert_handlers:
            self._alert_handlers.append(handler)

    _ALERT_DEDUP_SECONDS = 1800  # Don't resend same alert type within 30 min

    async def _fire_alerts(self, anomalies: list[dict]) -> None:
        import inspect
        now = time.time()

        # Deduplicate: skip anomalies of the same type sent recently
        # NOTE: key uses ONLY the type, not message content — messages like
        # "No successful messages for 5h 12m" change every check, which would
        # bypass dedup if included in the key.
        filtered = []
        for a in anomalies:
            key = a.get("type", "unknown")
            last = self._last_alert_times.get(key, 0)
            if now - last >= self._ALERT_DEDUP_SECONDS:
                filtered.append(a)
                self._last_alert_times[key] = now

        if not filtered:
            return

        # Clean old entries + cap at 100
        cutoff = now - self._ALERT_DEDUP_SECONDS * 2
        self._last_alert_times = {k: v for k, v in self._last_alert_times.items() if v > cutoff}
        if len(self._last_alert_times) > 100:
            items = sorted(self._last_alert_times.items(), key=lambda x: x[1], reverse=True)
            self._last_alert_times = dict(items[:100])

        for handler in self._alert_handlers:
            try:
                result = handler(filtered)
                if inspect.isawaitable(result):
                    await result
            except Exception as exc:
                logger.warning("Alert handler error: %s", exc)

    # ── reporting ────────────────────────────────────────────────────────

    def get_status_report(self) -> str:
        """Human-readable status summary."""
        lines = ["=== Self-Monitor Status ==="]

        h = self._last_health
        if not h:
            lines.append("No health data yet (monitor may not be running).")
            return "\n".join(lines)

        # Overall
        lines.append(f"Overall: {'OK' if h.get('ok') else 'DEGRADED'}")
        lines.append(f"Checked: {h.get('ts', '?')}")

        # CPU
        cpu = h.get("checks", {}).get("cpu", {})
        lines.append(f"CPU: {cpu.get('usage_pct', '?')}% [{cpu.get('status', '?')}]")

        # Memory
        mem = h.get("checks", {}).get("memory", {})
        mem_str = f"{mem.get('usage_pct', '?')}%"
        if "used_gb" in mem:
            mem_str += f" ({mem['used_gb']}/{mem['total_gb']} GB)"
        lines.append(f"Memory: {mem_str} [{mem.get('status', '?')}]")

        # Disk
        disk = h.get("checks", {}).get("disk", {})
        disk_str = f"{disk.get('usage_pct', '?')}%"
        if "free_gb" in disk:
            disk_str += f" ({disk['free_gb']} GB free)"
        lines.append(f"Disk: {disk_str} [{disk.get('status', '?')}]")

        # Network
        net = h.get("checks", {}).get("network", {})
        lines.append(f"Telegram API: {'reachable' if net.get('reachable') else 'UNREACHABLE'}")

        # Bot
        bot = h.get("checks", {}).get("bot", {})
        if bot:
            lines.append(f"Uptime: {bot.get('uptime_human', '?')}")
            lines.append(f"Last msg: {bot.get('last_msg_status', '?')}")
            if "process_rss_mb" in bot:
                lines.append(f"Process memory: {bot['process_rss_mb']} MB")

        # Error rate
        er = h.get("checks", {}).get("error_rate_1h", 0)
        lines.append(f"Errors (1h): {er}")

        return "\n".join(lines)

    # ── internal helpers ─────────────────────────────────────────────────

    def _error_rate_last_hour(self) -> int:
        """Count errors in the last 3600 seconds."""
        cutoff = time.time() - 3600
        return sum(1 for ts in self._error_window if ts >= cutoff)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _safe_truncate_dict(d: Any, max_val_len: int = 200) -> dict:
    """Return a copy of *d* with string values truncated. Returns empty dict if *d* is not a dict."""
    if not isinstance(d, dict):
        return {}
    out = {}
    for k, v in d.items():
        if isinstance(v, str) and len(v) > max_val_len:
            out[k] = v[:max_val_len] + "..."
        else:
            out[k] = v
    return out


def _normalise_error(error: str) -> str:
    """Collapse variable parts of an error message into a stable signature."""
    s = error.strip()[:200]
    # Remove hex addresses, line numbers, timestamps
    s = re.sub(r"0x[0-9a-fA-F]+", "0x...", s)
    s = re.sub(r"line \d+", "line N", s)
    s = re.sub(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}[:\d.]*", "TIMESTAMP", s)
    s = re.sub(r"\d{5,}", "NUM", s)
    return s


def _format_duration(seconds: float) -> str:
    """Human-friendly duration string."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h}h {m}m"


# ---------------------------------------------------------------------------
# Convenience: module-level singletons (import and use directly)
# ---------------------------------------------------------------------------

action_memory = ActionMemory()
self_monitor = SelfMonitor()
