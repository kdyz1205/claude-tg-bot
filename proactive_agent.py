"""
Proactive Agent -- Acts autonomously without user prompting.
Monitors, alerts, optimizes, and reports on its own schedule.
"""

import asyncio
import glob
import json
import logging
import os
import tempfile
import time
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta
from typing import Any, Callable, Coroutine

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

logger = logging.getLogger(__name__)

BOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BOT_DIR, "bot.log")

# ---------------------------------------------------------------------------
# Configuration -- toggle each proactive behavior on/off
# ---------------------------------------------------------------------------

PROACTIVE_CONFIG = {
    "morning_briefing": True,
    "health_watchdog": True,
    "error_digest": True,
    "trading_alerts": False,   # opt-in
    "self_improvement": True,
    "stale_cleanup": True,
}

# Rate-limit: max 1 proactive message per category per this many seconds
_RATE_LIMIT_SECONDS = 600  # 10 minutes


# ---------------------------------------------------------------------------
# ProactiveAgent
# ---------------------------------------------------------------------------

class ProactiveAgent:
    """Runs autonomous background loops that monitor, alert, and maintain
    the system without waiting for user input."""

    def __init__(self, send_func: Callable[..., Coroutine] | None = None):
        """
        Args:
            send_func: An async callable ``send_func(text)`` that delivers a
                       message to the user (e.g. via Telegram).
        """
        self._send = send_func
        self._running = False
        self._tasks: list[asyncio.Task] = []

        # Per-category last-send timestamps for rate limiting
        self._last_sent: dict[str, float] = {}

        # Shared error buffer -- external code can push errors here
        self._error_buffer: list[dict] = []
        self._error_buffer_lock: asyncio.Lock | None = None

    def _get_error_lock(self) -> asyncio.Lock:
        if self._error_buffer_lock is None:
            self._error_buffer_lock = asyncio.Lock()
        return self._error_buffer_lock

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start all proactive loops as background tasks."""
        if self._running:
            logger.warning("ProactiveAgent already running")
            return

        self._running = True
        logger.info("ProactiveAgent starting all loops")

        loop_methods = [
            ("morning_briefing", self._morning_briefing_loop),
            ("health_watchdog", self._health_watchdog_loop),
            ("error_digest", self._error_digest_loop),
            ("trading_alerts", self._trading_alert_loop),
            ("self_improvement", self._self_improvement_loop),
            ("stale_cleanup", self._stale_process_cleanup_loop),
        ]

        for name, coro_func in loop_methods:
            if PROACTIVE_CONFIG.get(name, False):
                task = asyncio.create_task(coro_func(), name=f"proactive_{name}")
                self._tasks.append(task)
                def _on_done(t):
                    try:
                        if not t.cancelled():
                            t.result()
                    except Exception as e:
                        logger.error(f"Proactive loop {t.get_name()} failed: {e}", exc_info=True)
                    try:
                        self._tasks.remove(t)
                    except (ValueError, KeyError):
                        pass
                task.add_done_callback(_on_done)
                logger.info("  Started loop: %s", name)
            else:
                logger.info("  Skipped loop (disabled): %s", name)

    async def stop(self) -> None:
        """Gracefully stop all loops."""
        self._running = False
        for task in self._tasks:
            if not task.done():
                task.cancel()
        # Wait for all tasks to finish cancellation
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("ProactiveAgent stopped")

    # ------------------------------------------------------------------
    # External API
    # ------------------------------------------------------------------

    _MAX_ERROR_BUFFER = 500  # cap to prevent unbounded growth between digests

    async def push_error(self, error_type: str, message: str, source: str = "") -> None:
        """Push an error into the buffer for the hourly digest."""
        async with self._get_error_lock():
            self._error_buffer.append({
                "ts": datetime.now().isoformat(),
                "type": error_type,
                "message": message[:500],
                "source": source,
            })
            # Prevent unbounded growth if digest loop is slow or disabled
            if len(self._error_buffer) > self._MAX_ERROR_BUFFER:
                self._error_buffer = self._error_buffer[-self._MAX_ERROR_BUFFER:]

    # ------------------------------------------------------------------
    # Message delivery with rate limiting
    # ------------------------------------------------------------------

    async def _send_message(self, category: str, text: str) -> bool:
        """Send a message to the user, respecting per-category rate limits.
        Returns True if the message was actually sent."""
        if not self._send:
            logger.debug("ProactiveAgent: no send_func, logging instead:\n%s", text)
            return False

        now = time.time()
        last = self._last_sent.get(category, 0)
        if now - last < _RATE_LIMIT_SECONDS:
            logger.debug("ProactiveAgent: rate-limited category=%s (%.0fs left)",
                         category, _RATE_LIMIT_SECONDS - (now - last))
            return False

        try:
            await self._send(text)
            self._last_sent[category] = now
            logger.info("ProactiveAgent sent [%s]: %s", category, text[:120])
            return True
        except Exception as exc:
            logger.error("ProactiveAgent send error [%s]: %s", category, exc)
            return False

    # ------------------------------------------------------------------
    # Loop 1: Morning Briefing (daily at 08:00 local)
    # ------------------------------------------------------------------

    async def _morning_briefing_loop(self) -> None:
        """Every day at 08:00 local time, send a comprehensive briefing."""
        while self._running:
            try:
                now = datetime.now()
                # Calculate seconds until next 08:00
                target = now.replace(hour=8, minute=0, second=0, microsecond=0)
                if now >= target:
                    target += timedelta(days=1)
                wait_seconds = (target - now).total_seconds()

                logger.info("Morning briefing: next fire in %.0f seconds (%s)",
                            wait_seconds, target.isoformat())
                await asyncio.sleep(wait_seconds)

                if not self._running:
                    break

                briefing = await self._build_morning_briefing()
                await self._send_message("morning_briefing", briefing)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Morning briefing loop error: %s", exc, exc_info=True)
                await asyncio.sleep(60)  # retry after 1 min on error

    async def _build_morning_briefing(self) -> str:
        """Assemble the morning briefing text."""
        lines = ["--- Morning Briefing ---", f"Date: {datetime.now().strftime('%A, %B %d, %Y')}", ""]

        # System health
        lines.append("[System Health]")
        health = await self._quick_health_check()
        lines.append(f"  CPU:    {health['cpu']}%")
        lines.append(f"  Memory: {health['mem']}%")
        lines.append(f"  Disk:   {health['disk']}%  ({health['disk_free_gb']} GB free)")
        lines.append(f"  Status: {'All OK' if health['ok'] else 'Issues detected'}")
        lines.append("")

        # Overnight errors
        error_count = await self._count_recent_log_errors(hours=12)
        lines.append("[Overnight Errors]")
        if error_count > 0:
            lines.append(f"  {error_count} errors in the last 12 hours")
        else:
            lines.append("  No errors -- clean night")
        lines.append("")

        # Bot uptime
        bot_pid_info = await self._get_bot_process_info()
        lines.append("[Bot Status]")
        lines.append(f"  PID: {os.getpid()}")
        lines.append(f"  Process memory: {bot_pid_info.get('rss_mb', '?')} MB")
        lines.append("")

        # Trading status placeholder
        trading_status = await asyncio.to_thread(self._check_trading_status)
        if trading_status:
            lines.append("[Trading]")
            lines.append(f"  {trading_status}")
            lines.append("")

        lines.append("Have a productive day.")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Loop 2: Health Watchdog (every 5 minutes)
    # ------------------------------------------------------------------

    async def _health_watchdog_loop(self) -> None:
        """Every 15 minutes, check system vitals and alert if critical."""
        # Keep a short history for trend detection (capped with maxlen)
        mem_history: deque[float] = deque(maxlen=60)

        while self._running:
            try:
                await asyncio.sleep(900)  # 15 minutes
                if not self._running:
                    break

                health = await self._quick_health_check()
                alerts: list[str] = []

                # CPU critical
                if health["cpu"] > 95:
                    alerts.append(f"CPU critically high: {health['cpu']}%")

                # Memory critical (95% — 85% is normal on desktop Windows)
                if health["mem"] > 95:
                    alerts.append(f"Memory critically high: {health['mem']}%")

                # Disk critical
                if health["disk"] > 95:
                    alerts.append(f"Disk nearly full: {health['disk']}% ({health['disk_free_gb']} GB free)")

                # Memory trend: track slow climb
                mem_history.append(health["mem"])
                if len(mem_history) >= 12:
                    # Compare first quarter vs last quarter
                    q = max(1, len(mem_history) // 4)
                    first_avg = sum(mem_history[:q]) / q
                    last_avg = sum(mem_history[-q:]) / q
                    climb = last_avg - first_avg
                    if climb > 10:
                        alerts.append(
                            f"Memory trending up: {first_avg:.1f}% -> {last_avg:.1f}% "
                            f"(+{climb:.1f}% over {len(mem_history) * 15}min)"
                        )

                if alerts:
                    text = "HEALTH ALERT\n\n" + "\n".join(f"  - {a}" for a in alerts)
                    await self._send_message("health_watchdog", text)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Health watchdog error: %s", exc, exc_info=True)
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Loop 3: Error Digest (every hour)
    # ------------------------------------------------------------------

    async def _error_digest_loop(self) -> None:
        """Every hour, collect errors and send a digest if >5 occurred."""
        while self._running:
            try:
                await asyncio.sleep(3600)  # 1 hour
                if not self._running:
                    break

                # Drain the error buffer
                async with self._get_error_lock():
                    errors = list(self._error_buffer)
                    self._error_buffer.clear()

                # Also scan the log file for recent ERROR lines
                log_errors = await self._collect_log_errors(hours=1)
                errors.extend(log_errors)

                if len(errors) < 5:
                    logger.debug("Error digest: only %d errors, skipping", len(errors))
                    continue

                # Group by type
                by_type: dict[str, list[dict]] = defaultdict(list)
                for e in errors:
                    by_type[e.get("type", "unknown")].append(e)

                lines = [f"Error Digest -- {len(errors)} errors in the last hour", ""]
                for etype, elist in sorted(by_type.items(), key=lambda x: -len(x[1])):
                    lines.append(f"  [{etype}] x{len(elist)}")
                    # Show up to 3 sample messages
                    for sample in elist[:3]:
                        msg = sample.get("message", "")[:150]
                        lines.append(f"    - {msg}")
                    if len(elist) > 3:
                        lines.append(f"    ... and {len(elist) - 3} more")
                    lines.append("")

                # Suggest fixes for common patterns
                suggestions = self._suggest_fixes(by_type)
                if suggestions:
                    lines.append("[Suggestions]")
                    for s in suggestions:
                        lines.append(f"  - {s}")

                await self._send_message("error_digest", "\n".join(lines))

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error digest loop error: %s", exc, exc_info=True)
                await asyncio.sleep(60)

    def _suggest_fixes(self, by_type: dict[str, list]) -> list[str]:
        """Generate fix suggestions based on error patterns."""
        suggestions: list[str] = []
        for etype, elist in by_type.items():
            # Cap to avoid building a huge string from thousands of error messages
            sample = elist[:50]
            combined = " ".join(e.get("message", "")[:200] for e in sample).lower()
            if "timeout" in combined:
                suggestions.append(f"[{etype}] Timeouts detected -- consider increasing timeout or checking network")
            if "memory" in combined or "memoryerror" in combined:
                suggestions.append(f"[{etype}] Memory issues -- check for leaks, consider restarting")
            if "permission" in combined or "access denied" in combined:
                suggestions.append(f"[{etype}] Permission errors -- check file/process permissions")
            if "connection" in combined or "connectionerror" in combined:
                suggestions.append(f"[{etype}] Connection failures -- check network/API availability")
            if "rate" in combined and "limit" in combined:
                suggestions.append(f"[{etype}] Rate limiting -- add backoff or reduce request frequency")
            if "disk" in combined or "no space" in combined:
                suggestions.append(f"[{etype}] Disk space issues -- clean temp files and logs")
        return suggestions

    # ------------------------------------------------------------------
    # Loop 4: Trading Alerts (every 15 minutes, opt-in)
    # ------------------------------------------------------------------

    async def _trading_alert_loop(self) -> None:
        """Every 15 minutes, check for significant trading events."""
        # Previous prices for comparison
        prev_prices: dict[str, float] = {}

        while self._running:
            try:
                await asyncio.sleep(900)  # 15 minutes
                if not self._running:
                    break

                alerts: list[str] = []

                # Try to read trading state if it exists
                trading_state_path = os.path.join(BOT_DIR, "trading_state.json")
                if not os.path.exists(trading_state_path):
                    logger.debug("Trading alert: no trading_state.json, skipping")
                    continue

                try:
                    with open(trading_state_path, "r", encoding="utf-8") as f:
                        state = json.load(f)
                except (json.JSONDecodeError, OSError):
                    continue

                # Check price moves
                positions = state.get("positions", {})
                prices = state.get("prices", {})
                for symbol, price in prices.items():
                    if not isinstance(price, (int, float)) or price <= 0:
                        continue
                    if symbol in prev_prices and prev_prices[symbol] > 0:
                        change_pct = ((price - prev_prices[symbol]) / prev_prices[symbol]) * 100
                        if abs(change_pct) > 3:
                            direction = "UP" if change_pct > 0 else "DOWN"
                            alerts.append(
                                f"{symbol}: {direction} {abs(change_pct):.1f}% "
                                f"({prev_prices[symbol]:.2f} -> {price:.2f})"
                            )
                    prev_prices[symbol] = price

                # Drawdown warnings
                for pos_name, pos in positions.items():
                    if not isinstance(pos, dict):
                        continue
                    pnl_pct = pos.get("pnl_pct", 0)
                    if pnl_pct < -5:
                        alerts.append(
                            f"Drawdown warning: {pos_name} at {pnl_pct:.1f}% loss"
                        )

                # Funding rate opportunities
                funding = state.get("funding_rates", {})
                for symbol, rate in funding.items():
                    if isinstance(rate, (int, float)) and abs(rate) > 0.05:
                        alerts.append(
                            f"Funding rate opportunity: {symbol} at {rate:.4f} "
                            f"({'pay shorts' if rate > 0 else 'pay longs'})"
                        )

                if alerts:
                    text = "Trading Alert\n\n" + "\n".join(f"  - {a}" for a in alerts)
                    await self._send_message("trading_alerts", text)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Trading alert loop error: %s", exc, exc_info=True)
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Loop 5: Self-Improvement (every 6 hours)
    # ------------------------------------------------------------------

    async def _self_improvement_loop(self) -> None:
        """Every 6 hours, analyze action memory and identify improvements."""
        while self._running:
            try:
                await asyncio.sleep(21600)  # 6 hours
                if not self._running:
                    break

                report = await self._analyze_for_improvement()
                if report:
                    await self._send_message("self_improvement", report)

                    # Actually try to fix the most common failure
                    try:
                        fix_result = await self._auto_fix_top_failure()
                        if fix_result:
                            await self._send_message("self_improvement", f"Auto-fix applied:\n{fix_result}")
                    except Exception as fix_err:
                        logger.debug(f"Auto-fix attempt failed: {fix_err}")

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Self-improvement loop error: %s", exc, exc_info=True)
                await asyncio.sleep(60)

    async def _auto_fix_top_failure(self) -> str | None:
        """Identify the most common failure and use Claude CLI to fix it.

        Returns a summary of what was fixed, or None if nothing to fix.
        """
        memory_path = os.path.join(BOT_DIR, "action_memory.json")
        if not os.path.exists(memory_path):
            return None

        try:
            with open(memory_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

        # Find the most common recent failure
        cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
        recent_failures: list[str] = []
        for a in data.get("actions", []):
            if not a.get("success") and a.get("ts", "") >= cutoff:
                err = a.get("error", "")[:200]
                if err:
                    recent_failures.append(err)

        if not recent_failures:
            return None

        counter = Counter(recent_failures)
        most_common = counter.most_common(1)
        if not most_common:
            return None
        top_error, count = most_common[0]
        if count < 2:
            return None  # Only fix recurring issues

        logger.info(f"Auto-fix: targeting top failure (x{count}): {top_error[:100]}")

        # Use the autonomous loop to fix it
        try:
            from agents.loop import autonomous_loop

            async def _log_status(text: str):
                logger.info(f"Auto-fix status: {text}")

            result = await autonomous_loop(
                goal=(
                    f"Fix this recurring error in the TG bot code:\n{top_error}\n\n"
                    f"This error occurred {count} times in the last 24 hours. "
                    f"Find the root cause in the bot code and fix it. "
                    f"Verify the fix with a syntax check."
                ),
                send_status=_log_status,
                project_dir=BOT_DIR,
                model="claude-sonnet-4-6",
                max_rounds=3,
                verify_cmd=(
                    'python -c "import py_compile; '
                    "[py_compile.compile(f, doraise=True) for f in "
                    "['bot.py','claude_agent.py','tools.py','providers.py','proactive_agent.py']]\""
                ),
            )

            # Record in consciousness if available
            try:
                from agents.consciousness import get_self_awareness
                sa = get_self_awareness()
                idx = sa.record_evolution("auto_fix", f"Auto-fixed: {top_error[:100]}")
                success = "PASS" in result or "✅" in result
                sa.record_evolution_outcome(idx, success, result[:200])
            except Exception:
                pass

            return f"Top failure (x{count}): {top_error[:80]}\n\nResult:\n{result[:500]}"

        except Exception as e:
            logger.warning(f"Auto-fix failed: {e}")
            return None

    async def _analyze_for_improvement(self) -> str | None:
        """Analyze action_memory.json for patterns and suggest improvements."""
        memory_path = os.path.join(BOT_DIR, "action_memory.json")
        if not os.path.exists(memory_path):
            return None

        try:
            with open(memory_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

        actions = data.get("actions", [])
        stats = data.get("stats", {})

        if not actions:
            return None

        lines = ["Self-Improvement Analysis", ""]

        # Most-failed action types
        failures: list[tuple[str, int, int]] = []
        for atype, s in stats.items():
            total = s.get("ok", 0) + s.get("fail", 0)
            if total >= 3 and s.get("fail", 0) > 0:
                fail_rate = s.get("fail", 0) / total
                if fail_rate > 0.2:
                    failures.append((atype, s.get("fail", 0), total))
        failures.sort(key=lambda x: x[1], reverse=True)

        if failures:
            lines.append("[Most-Failed Actions]")
            for atype, fail_count, total in failures[:10]:
                rate = (fail_count / total) * 100
                lines.append(f"  {atype}: {fail_count}/{total} failed ({rate:.0f}%)")
            lines.append("")

        # Slowest action types (avg duration)
        slow: list[tuple[str, float]] = []
        for atype, s in stats.items():
            total = s.get("ok", 0) + s.get("fail", 0)
            if total >= 3:
                avg_ms = s.get("total_ms", 0) / total
                if avg_ms > 5000:
                    slow.append((atype, avg_ms))
        slow.sort(key=lambda x: x[1], reverse=True)

        if slow:
            lines.append("[Slowest Actions (avg > 5s)]")
            for atype, avg_ms in slow[:10]:
                lines.append(f"  {atype}: {avg_ms / 1000:.1f}s avg")
            lines.append("")

        # Recurring error patterns (from action memory)
        cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
        recent_errors: list[str] = []
        for a in actions:
            if not a.get("success") and a.get("ts", "") >= cutoff:
                err = a.get("error", "")[:100]
                if err:
                    recent_errors.append(err)

        if recent_errors:
            counter = Counter(recent_errors)
            repeated = [(err, cnt) for err, cnt in counter.most_common(5) if cnt >= 2]
            if repeated:
                lines.append("[Recurring Errors (24h)]")
                for err, cnt in repeated:
                    lines.append(f"  x{cnt}: {err}")
                lines.append("")

        # Improvement suggestions
        suggestions: list[str] = []
        if failures:
            top_fail = failures[0][0]
            suggestions.append(f"Investigate why '{top_fail}' fails so often")
        if slow:
            top_slow = slow[0][0]
            suggestions.append(f"Optimize '{top_slow}' -- currently averaging {slow[0][1]/1000:.1f}s")
        if len(actions) > 4000:
            suggestions.append("Action memory is large -- consider pruning old entries")

        if suggestions:
            lines.append("[Suggestions]")
            for s in suggestions:
                lines.append(f"  - {s}")

        # Only send if there is something actionable
        if not failures and not slow and not recent_errors:
            logger.info("Self-improvement: nothing actionable found")
            return None

        # Log to evolution file for long-term tracking
        self._log_evolution(lines)

        return "\n".join(lines)

    _MAX_EVOLUTION_LOG_MB = 10  # rotate when evolution log exceeds this size

    def _log_evolution(self, report_lines: list[str]) -> None:
        """Append improvement report to an evolution log, rotating if too large."""
        evo_path = os.path.join(BOT_DIR, "evolution_log.jsonl")
        try:
            # Rotate if the file is too large
            try:
                if os.path.exists(evo_path):
                    size = os.path.getsize(evo_path)
                    if size > self._MAX_EVOLUTION_LOG_MB * 1024 * 1024:
                        # Keep last ~2MB
                        keep_bytes = 2 * 1024 * 1024
                        with open(evo_path, "rb") as rf:
                            rf.seek(max(0, size - keep_bytes))
                            rf.readline()  # skip partial line
                            tail = rf.read()
                        tmp_path = evo_path + ".tmp"
                        with open(tmp_path, "wb") as wf:
                            wf.write(tail)
                        os.replace(tmp_path, evo_path)
                        logger.info("Rotated evolution_log.jsonl from %dMB", size // (1024 * 1024))
            except OSError:
                pass

            entry = {
                "ts": datetime.now().isoformat(),
                "report": "\n".join(report_lines),
            }
            with open(evo_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.warning("Failed to write evolution log: %s", exc)

    # ------------------------------------------------------------------
    # Loop 6: Stale Process Cleanup (every 30 minutes)
    # ------------------------------------------------------------------

    async def _stale_process_cleanup_loop(self) -> None:
        """Every 30 minutes, clean zombie processes and old temp files."""
        while self._running:
            try:
                await asyncio.sleep(1800)  # 30 minutes
                if not self._running:
                    break

                cleaned: list[str] = []

                # 1. Kill stale Claude CLI sessions
                killed = await self._kill_stale_claude_sessions()
                if killed:
                    cleaned.append(f"Killed {killed} stale Claude CLI session(s)")

                # 2. Clean temp files older than 24h in the bot directory
                removed_temps = self._clean_old_temp_files()
                if removed_temps:
                    cleaned.append(f"Removed {removed_temps} temp file(s) older than 24h")

                # 2b. Clean stale screenshots (older than 1 hour) that failed to send
                removed_screenshots = self._clean_stale_screenshots()
                if removed_screenshots:
                    cleaned.append(f"Removed {removed_screenshots} stale screenshot(s)")

                # 3. Prune old log file if too large (> 50MB)
                pruned = self._prune_large_logs()
                if pruned:
                    cleaned.append(pruned)

                if cleaned:
                    logger.info("Stale cleanup: %s", "; ".join(cleaned))
                    # Only notify user if something significant was cleaned
                    if any("Kill" in c for c in cleaned):
                        text = "Maintenance Cleanup\n\n" + "\n".join(f"  - {c}" for c in cleaned)
                        await self._send_message("stale_cleanup", text)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Stale cleanup loop error: %s", exc, exc_info=True)
                await asyncio.sleep(60)

    async def _kill_stale_claude_sessions(self) -> int:
        """Find and kill Claude CLI processes running longer than 45 minutes."""
        if not HAS_PSUTIL:
            return 0

        killed = 0
        try:
            now = time.time()
            for proc in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
                try:
                    info = proc.info
                    name = (info.get("name") or "").lower()
                    cmdline = " ".join(info.get("cmdline") or []).lower()

                    # Look for claude CLI processes
                    is_claude = ("claude" in name and "cli" in cmdline) or \
                                ("node" in name and "claude" in cmdline)
                    if not is_claude:
                        continue

                    # Check age
                    create_time = info.get("create_time", 0)
                    age_min = (now - create_time) / 60 if create_time else 0
                    if age_min > 45:
                        logger.info("Killing stale Claude CLI (PID=%s, age=%.0fmin)",
                                    info["pid"], age_min)
                        proc.terminate()
                        killed += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        except Exception as exc:
            logger.warning("Error scanning for stale Claude sessions: %s", exc)

        return killed

    def _clean_stale_screenshots(self) -> int:
        """Remove screenshots older than 1 hour from _tg_screenshots/ that failed to send."""
        removed = 0
        screenshots_dir = os.path.join(BOT_DIR, "_tg_screenshots")
        if not os.path.isdir(screenshots_dir):
            return 0
        cutoff = time.time() - 3600  # 1 hour
        try:
            for fname in os.listdir(screenshots_dir):
                fpath = os.path.join(screenshots_dir, fname)
                if not os.path.isfile(fpath):
                    continue
                try:
                    if os.path.getmtime(fpath) < cutoff:
                        os.remove(fpath)
                        removed += 1
                except OSError:
                    continue
        except OSError:
            pass
        return removed

    def _clean_old_temp_files(self) -> int:
        """Remove .tmp files in bot directory older than 24 hours."""
        removed = 0
        cutoff = time.time() - 86400  # 24 hours

        patterns = [
            os.path.join(BOT_DIR, "*.tmp"),
            os.path.join(BOT_DIR, "tmp*.txt"),
            os.path.join(BOT_DIR, "tmp*.json"),
        ]
        # Also check system temp dir for bot-related files
        tmp_dir = tempfile.gettempdir()
        patterns.append(os.path.join(tmp_dir, "claude_bot_*.tmp"))

        for pattern in patterns:
            for filepath in glob.glob(pattern):
                try:
                    mtime = os.path.getmtime(filepath)
                    if mtime < cutoff:
                        os.remove(filepath)
                        removed += 1
                        logger.debug("Cleaned old temp file: %s", filepath)
                except OSError:
                    continue

        return removed

    def _prune_large_logs(self) -> str | None:
        """If bot.log exceeds 50MB, truncate to the last 10MB."""
        try:
            if not os.path.exists(LOG_FILE):
                return None
            size = os.path.getsize(LOG_FILE)
            if size < 50 * 1024 * 1024:
                return None

            size_mb = size / (1024 * 1024)
            # Read last 10MB and rewrite
            keep_bytes = 10 * 1024 * 1024
            with open(LOG_FILE, "rb") as f:
                f.seek(size - keep_bytes)
                # Skip to next newline to avoid partial lines
                f.readline()
                tail = f.read()

            tmp_path = LOG_FILE + ".tmp"
            with open(tmp_path, "wb") as f:
                header = f"[Log pruned by ProactiveAgent at {datetime.now().isoformat()}]\n"
                f.write(header.encode("utf-8"))
                f.write(tail)
            os.replace(tmp_path, LOG_FILE)

            return f"Pruned bot.log from {size_mb:.0f}MB to ~10MB"

        except OSError as exc:
            logger.warning("Log pruning failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    async def _quick_health_check(self) -> dict:
        """Fast health snapshot: CPU, memory, disk."""
        result = {"cpu": 0.0, "mem": 0.0, "disk": 0.0, "disk_free_gb": 0.0, "ok": True}

        if HAS_PSUTIL:
            try:
                result["cpu"] = await asyncio.to_thread(psutil.cpu_percent, interval=0.5)
            except Exception:
                result["cpu"] = -1

            try:
                vm = await asyncio.to_thread(psutil.virtual_memory)
                result["mem"] = vm.percent
            except Exception:
                result["mem"] = -1

            try:
                disk = await asyncio.to_thread(psutil.disk_usage, BOT_DIR)
                result["disk"] = disk.percent
                result["disk_free_gb"] = round(disk.free / (1024 ** 3), 2)
            except Exception:
                result["disk"] = -1
                result["disk_free_gb"] = -1
        else:
            # Fallback: try psutil-free methods via subprocess
            try:
                proc = await asyncio.create_subprocess_exec(
                    "powershell", "-NoProfile", "-Command",
                    "[math]::Round((Get-CimInstance Win32_Processor).LoadPercentage,1)",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5)
                result["cpu"] = float(stdout.decode(errors="replace").strip() or 0)
            except Exception:
                result["cpu"] = -1

        # Determine overall health
        result["ok"] = (
            result["cpu"] < 90 and
            result["mem"] < 85 and
            result["disk"] < 95
        )
        return result

    async def _count_recent_log_errors(self, hours: int = 1) -> int:
        """Count ERROR lines in bot.log from the last N hours."""
        if not os.path.exists(LOG_FILE):
            return 0

        cutoff = datetime.now() - timedelta(hours=hours)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M")

        def _count_sync() -> int:
            count = 0
            try:
                with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                    # Read from the end for efficiency (seek last 2MB)
                    try:
                        f.seek(max(0, os.path.getsize(LOG_FILE) - 2 * 1024 * 1024))
                        f.readline()  # skip partial line
                    except OSError:
                        pass

                    for line in f:
                        if " - ERROR - " in line or " ERROR " in line:
                            # Try to parse timestamp
                            ts_part = line[:19]  # "2026-03-28 08:00:00"
                            if ts_part >= cutoff_str:
                                count += 1
            except OSError:
                pass
            return count

        return await asyncio.to_thread(_count_sync)

    _MAX_LOG_ERRORS_COLLECTED = 200  # cap per collection to bound memory

    async def _collect_log_errors(self, hours: int = 1) -> list[dict]:
        """Collect structured error entries from bot.log."""
        if not os.path.exists(LOG_FILE):
            return []

        cutoff = datetime.now() - timedelta(hours=hours)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M")
        max_errors = self._MAX_LOG_ERRORS_COLLECTED

        def _collect_sync() -> list[dict]:
            errors: list[dict] = []
            try:
                with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                    try:
                        f.seek(max(0, os.path.getsize(LOG_FILE) - 2 * 1024 * 1024))
                        f.readline()
                    except OSError:
                        pass

                    for line in f:
                        if " - ERROR - " not in line and " ERROR " not in line:
                            continue
                        ts_part = line[:19]
                        if ts_part < cutoff_str:
                            continue

                        # Categorize
                        error_type = "general"
                        lower = line.lower()
                        if "timeout" in lower:
                            error_type = "timeout"
                        elif "connection" in lower:
                            error_type = "connection"
                        elif "permission" in lower or "access" in lower:
                            error_type = "permission"
                        elif "memory" in lower:
                            error_type = "memory"
                        elif "telegram" in lower:
                            error_type = "telegram_api"

                        errors.append({
                            "ts": ts_part,
                            "type": error_type,
                            "message": line.strip()[:300],
                            "source": "bot.log",
                        })
                        if len(errors) >= max_errors:
                            break
            except OSError:
                pass
            return errors

        return await asyncio.to_thread(_collect_sync)

    async def _get_bot_process_info(self) -> dict:
        """Get current process info."""
        def _sync() -> dict:
            info: dict[str, Any] = {}
            if HAS_PSUTIL:
                try:
                    proc = psutil.Process(os.getpid())
                    mem = proc.memory_info()
                    info["rss_mb"] = round(mem.rss / (1024 ** 2), 1)
                    info["threads"] = proc.num_threads()
                    info["cpu_pct"] = proc.cpu_percent(interval=0.1)
                except Exception:
                    pass
            return info
        return await asyncio.to_thread(_sync)

    def _check_trading_status(self) -> str | None:
        """Check if trading is active and return a brief status."""
        state_path = os.path.join(BOT_DIR, "trading_state.json")
        if not os.path.exists(state_path):
            return None
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                state = json.load(f)
            positions = state.get("positions", {})
            if not positions:
                return "Trading active, no open positions"
            total_pnl = sum(p.get("pnl_pct", 0) for p in positions.values() if isinstance(p, dict))
            return (
                f"{len(positions)} open position(s), "
                f"total PnL: {total_pnl:+.2f}%"
            )
        except (json.JSONDecodeError, OSError):
            return "Trading state file unreadable"


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

proactive_agent = ProactiveAgent()
