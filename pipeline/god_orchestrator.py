"""
God Loop — dual-rail scheduler: InfiniteEvolver (forge) + live delta-neutral radar.

Bridges promoted backtest genes into ``GLOBAL_BEST_STRATEGY`` and drives
``live_trader.execute_delta_neutral_buy`` with a fixed SOL notional while a
session loss fuse is armed.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)


class GodOrchestrator:
    """技能控制总线：运行时热切换雷达使用的 skill_id（Jarvis / 外部可调用）。"""

    __slots__ = ("active_skill",)

    def __init__(self) -> None:
        self.active_skill: str | None = None

    def hot_swap_skill(self, skill_name: str) -> None:
        name = (skill_name or "").strip()
        self.active_skill = name or None
        label = self.active_skill or "(全局择优/遗传同步)"
        logger.info("⚡ 雷达已实时切换至: %s", label)

    def reload_skills(self) -> None:
        """
        读取 ``session_commander_config.json`` 的 ``active_skills``（及兼容 ``god_active_skill``），
        热切换雷达并尝试 ``load_skill_from_file`` 对应 ``skills/{id}.py``。
        """
        global _session_cfg_mtime, _watchdog_last_reload_mono
        now = time.monotonic()
        if now - _watchdog_last_reload_mono < _WATCHDOG_DEBOUNCE_SEC:
            return
        _watchdog_last_reload_mono = now
        p = _SESSION_COMMANDER_CFG
        if not p.is_file():
            logger.debug("reload_skills: missing %s", p)
            return
        try:
            _session_cfg_mtime = p.stat().st_mtime
            data = json.loads(p.read_text(encoding="utf-8"))
            raw = data.get("active_skills")
            primary = ""
            if isinstance(raw, list) and raw:
                primary = str(raw[0] or "").strip()
            elif isinstance(raw, str):
                primary = raw.strip()
            if not primary:
                primary = str(data.get("god_active_skill") or "").strip()
            self.hot_swap_skill(primary)
            if primary:
                py_path = p.parent / "skills" / f"{primary}.py"
                if py_path.is_file():
                    try:
                        from skills.skill_runtime import load_skill_from_file

                        load_skill_from_file(py_path)
                    except Exception:
                        logger.debug(
                            "reload_skills load_skill_from_file failed",
                            exc_info=True,
                        )
            _refresh_global_best_sync()
        except Exception:
            logger.warning("reload_skills failed", exc_info=True)


GOD_ORCHESTRATOR = GodOrchestrator()

EVOLVE_INTERVAL_SEC = float(os.environ.get("GOD_EVOLVE_INTERVAL_SEC", "7200"))
TRADE_NOTIONAL_SOL = float(os.environ.get("GOD_TRADE_SOL", "0.5"))
MAX_SESSION_LOSS_SOL = float(os.environ.get("GOD_MAX_LOSS_SOL", "0.1"))
RADAR_POLL_SEC = float(os.environ.get("GOD_RADAR_POLL_SEC", "45"))
DEFAULT_CONFIDENCE_FLOOR = float(os.environ.get("GOD_CONFIDENCE_FLOOR", "0.85"))

_GLOBAL_LOCK = threading.RLock()

GLOBAL_BEST_STRATEGY: dict[str, Any] = {
    "strategy_params": {},
    "skill_id": None,
    "title": "",
    "sharpe": -999.0,
    "win_rate": None,
    "confidence_floor": DEFAULT_CONFIDENCE_FLOOR,
    "source": "bootstrap",
    "updated_at": None,
}

_session_net_pnl_sol: float = 0.0
_circuit_tripped: bool = False
_running: bool = False
_stop_event: Optional[asyncio.Event] = None
_bg_tasks: list[asyncio.Task[Any]] = []
_alert_sender: Optional[Callable[[str], Awaitable[None]]] = None
_trip_once = threading.Event()
# When set, forge/radar tasks use PTB ``Application.create_task`` (same loop as polling).
_telegram_create_task: Optional[Callable[..., Any]] = None

# session_commander_config.json — watchdog 热更 + mtime 去抖
_SESSION_COMMANDER_CFG: Path = Path(__file__).resolve().parents[1] / "session_commander_config.json"
_session_cfg_mtime: float = 0.0
_watchdog_observer: Any = None
_watchdog_lock = threading.Lock()
_watchdog_last_reload_mono: float = 0.0
_WATCHDOG_DEBOUNCE_SEC = 0.35


def is_god_hard_stop() -> bool:
    with _GLOBAL_LOCK:
        return _circuit_tripped


def get_global_best_snapshot() -> dict[str, Any]:
    with _GLOBAL_LOCK:
        return copy.deepcopy(GLOBAL_BEST_STRATEGY)


def reset_god_session_guard() -> None:
    global _session_net_pnl_sol, _circuit_tripped
    with _GLOBAL_LOCK:
        _session_net_pnl_sol = 0.0
        _circuit_tripped = False
    _trip_once.clear()


def _skill_library_index_path() -> Path:
    from infinite_evolver import BASE

    return Path(BASE) / ".skill_library" / "index.json"


def _refresh_global_best_unlocked() -> None:
    """Call with ``_GLOBAL_LOCK`` held."""
    from infinite_evolver import _load_genetics

    g = _load_genetics()
    params = g.get("last_good_params")
    hist = g.get("sharpe_history") or []
    sharpe = float(hist[-1]) if hist else float(GLOBAL_BEST_STRATEGY.get("sharpe") or -999.0)

    if isinstance(params, dict) and params:
        GLOBAL_BEST_STRATEGY["strategy_params"] = copy.deepcopy(params)
    GLOBAL_BEST_STRATEGY["sharpe"] = sharpe
    GLOBAL_BEST_STRATEGY["source"] = "genetics"
    GLOBAL_BEST_STRATEGY["updated_at"] = time.time()

    idx_path = _skill_library_index_path()
    if idx_path.exists():
        try:
            idx = json.loads(idx_path.read_text(encoding="utf-8"))
            entries = idx.get("entries") if isinstance(idx, dict) else None
            if isinstance(entries, list) and entries:
                best = max(
                    entries,
                    key=lambda e: float((e or {}).get("sharpe") or (e or {}).get("avg_score") or -999),
                )
                sk = float(best.get("sharpe") or best.get("avg_score") or -999)
                if sk > float(GLOBAL_BEST_STRATEGY.get("sharpe") or -999):
                    GLOBAL_BEST_STRATEGY["sharpe"] = sk
                    GLOBAL_BEST_STRATEGY["skill_id"] = best.get("id")
                    GLOBAL_BEST_STRATEGY["title"] = str(best.get("title") or "")
                    GLOBAL_BEST_STRATEGY["source"] = "skill_library"
        except Exception as e:
            logger.debug("skill index merge skipped: %s", e)

    if not GLOBAL_BEST_STRATEGY.get("strategy_params"):
        try:
            from trading.okx_executor import AgentState

            GLOBAL_BEST_STRATEGY["strategy_params"] = copy.deepcopy(AgentState().strategy_params)
            GLOBAL_BEST_STRATEGY["source"] = "agent_state_default"
        except Exception as e:
            logger.debug("bootstrap strategy_params: %s", e)


async def refresh_global_best_from_evolver_state() -> None:
    await asyncio.to_thread(_refresh_global_best_sync)


def _refresh_global_best_sync() -> None:
    with _GLOBAL_LOCK:
        _refresh_global_best_unlocked()


async def on_god_trade_closed(pos: dict) -> None:
    """Hook from ``live_trader.sell_token`` — session PnL fuse for god-tagged trades."""
    global _session_net_pnl_sol, _circuit_tripped
    sig = pos.get("signal") or {}
    if not sig.get("god_engine"):
        return
    pnl = float(pos.get("pnl_sol") or 0)
    should_trip = False
    cum = 0.0
    with _GLOBAL_LOCK:
        _session_net_pnl_sol += pnl
        cum = _session_net_pnl_sol
        if _session_net_pnl_sol <= -MAX_SESSION_LOSS_SOL:
            _circuit_tripped = True
            should_trip = True
    logger.warning(
        "God session PnL cum=%.4f SOL (Δ%+.4f) trip=%s",
        cum,
        pnl,
        should_trip,
    )
    if should_trip and not _trip_once.is_set():
        _trip_once.set()
        await _trip_kill_switch_async(
            f"🚨 奇点熔断 — 全自动会话累计净亏损已达 {abs(cum):.4f} SOL "
            f"（阈值 {MAX_SESSION_LOSS_SOL} SOL）。进程将硬停止。"
        )


async def _trip_kill_switch_async(message: str) -> None:
    global _running
    sender = _alert_sender
    if sender:
        try:
            await sender(message[:4096])
        except Exception as e:
            logger.error("God fuse TG alert failed: %s", e)

    _running = False
    if _stop_event is not None:
        _stop_event.set()

    for t in list(_bg_tasks):
        if not t.done():
            t.cancel()

    logger.critical("GOD_CIRCUIT_BREAKER — os._exit(1)")
    os._exit(1)


def _spawn_loop_task(coro: Any, *, name: str) -> asyncio.Task[Any]:
    fn = _telegram_create_task
    if fn is not None:
        return fn(coro, name=name)
    return asyncio.create_task(coro, name=name)


async def _forge_loop(stop: asyncio.Event) -> None:
    from infinite_evolver import infinite_evolver

    logger.info("God forge loop started (interval=%ss)", EVOLVE_INTERVAL_SEC)
    while not stop.is_set():
        if is_god_hard_stop():
            break
        prev = os.environ.get("EVOLVER_BACKTEST_OFFLINE_ONLY")
        os.environ["EVOLVER_BACKTEST_OFFLINE_ONLY"] = "1"
        try:
            try:
                await infinite_evolver._sweep()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception("God forge sweep error: %s", e)
        finally:
            if prev is None:
                os.environ.pop("EVOLVER_BACKTEST_OFFLINE_ONLY", None)
            else:
                os.environ["EVOLVER_BACKTEST_OFFLINE_ONLY"] = prev
        try:
            await refresh_global_best_from_evolver_state()
        except Exception as e:
            logger.warning("refresh_global_best: %s", e)
        try:
            await asyncio.wait_for(stop.wait(), timeout=EVOLVE_INTERVAL_SEC)
        except asyncio.TimeoutError:
            pass


async def _god_radar_once() -> None:
    if is_god_hard_stop():
        return

    import live_trader as lt

    cfg = lt._load_config()
    if not cfg.get("enabled"):
        return

    snap = get_global_best_snapshot()
    if GOD_ORCHESTRATOR.active_skill:
        snap = dict(snap)
        snap["skill_id"] = GOD_ORCHESTRATOR.active_skill
    params = snap.get("strategy_params") or {}
    thr = float(snap.get("confidence_floor") or DEFAULT_CONFIDENCE_FLOOR)
    mint = (cfg.get("neural_dex_mint") or "").strip()
    if not mint or len(mint) < 32:
        logger.debug("God radar: neural_dex_mint unset — skip tick")
        return

    try:
        from trading.live_tensor_stream import ensure_stream_started
        from trading.strategy_brain import get_default_strategy_brain
    except ImportError as e:
        logger.debug("God radar imports: %s", e)
        return

    brain = get_default_strategy_brain()
    brain.executor.load_state()
    if isinstance(params, dict) and params:
        brain.executor.state.strategy_params.update(copy.deepcopy(params))

    await brain.reload_singularity_weights(force=False)
    seq_len = 64
    if brain._singularity_bundle:
        seq_len = int(brain._singularity_bundle.get("seq_len", 64))

    inst = cfg.get("neural_okx_inst") or "BTC-USDT-SWAP"
    stream = await ensure_stream_started(inst, window=max(512, seq_len * 4))
    tens = await stream.build_model_tensor(seq_len=seq_len)
    try:
        from trading.local_ohlcv_cache import write_live_1m_snapshot

        snap = await stream.snapshot_ohlcv()
        if len(snap) >= 64:
            await asyncio.to_thread(write_live_1m_snapshot, stream.inst_id, snap)
    except Exception as e:
        logger.debug("tensor cache snapshot: %s", e)
    if tens is None:
        return

    pred = await brain.live_predict(tens)
    if not pred:
        return
    if float(pred.get("confidence", 0)) < thr or pred.get("action") != "long":
        return

    import secure_wallet

    balance = await secure_wallet.get_sol_balance()
    if not balance:
        return
    gas_floor = max(float(cfg.get("min_sol_reserve", 0.05)), 0.015)
    trade_sol = min(TRADE_NOTIONAL_SOL, max(0.01, balance - gas_floor))

    hedge_sym = cfg.get("neural_hedge_symbol") or "SOLUSDT"
    sigd = {
        "source": "god_orchestrator",
        "god_engine": True,
        "confidence": pred.get("confidence"),
        "prob_up": pred.get("prob_up"),
        "neural": pred,
        "global_best_sharpe": snap.get("sharpe"),
        "skill_id": snap.get("skill_id"),
    }
    gw = lt.get_live_execution_gateway()
    out = await gw.execute_atomic_hedge(
        mint,
        trade_sol,
        mint[:8],
        hedge_sym,
        signal_data=sigd,
    )
    logger.info("God radar exec: %s", out)
    send = _alert_sender
    if send and out.get("ok"):
        try:
            await send(
                f"⚡ 奇点雷达 Δ-对冲已触发\n"
                f"SOL≈{trade_sol:.4f} | ok={out.get('ok')}\n"
                f"USD~{out.get('notional_usd', 0):.0f}"
            )
        except Exception:
            pass


def _start_session_commander_watchdog() -> None:
    """在独立线程中监控 ``session_commander_config.json``，变更即 ``reload_skills``。"""
    global _watchdog_observer

    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        logger.warning(
            "watchdog 未安装，跳过 session_commander_config 文件监控；"
            "请 pip install watchdog 或依赖 Jarvis 写配置后的即时 reload_skills() 调用。"
        )
        return

    cfg_path = _SESSION_COMMANDER_CFG.resolve()

    class _Handler(FileSystemEventHandler):
        def on_modified(self, event):  # type: ignore[override]
            if getattr(event, "is_directory", False):
                return
            try:
                if Path(event.src_path).resolve() != cfg_path:
                    return
            except OSError:
                return
            with _watchdog_lock:
                try:
                    GOD_ORCHESTRATOR.reload_skills()
                except Exception:
                    logger.debug("watchdog reload_skills", exc_info=True)

    parent = str(cfg_path.parent)
    obs = Observer()
    obs.schedule(_Handler(), parent, recursive=False)
    obs.start()
    _watchdog_observer = obs
    logger.info("watchdog observing %s for God reload_skills", cfg_path)


def _stop_session_commander_watchdog() -> None:
    global _watchdog_observer
    obs = _watchdog_observer
    _watchdog_observer = None
    if obs is not None:
        try:
            obs.stop()
            obs.join(timeout=5.0)
        except Exception:
            logger.debug("watchdog stop failed", exc_info=True)


async def _radar_loop(stop: asyncio.Event, *, paper_mode: bool) -> None:
    import live_trader as lt

    logger.info("God radar loop started paper=%s poll=%ss", paper_mode, RADAR_POLL_SEC)
    last_pos_check = 0.0
    while not stop.is_set():
        if is_god_hard_stop():
            break
        if paper_mode:
            await asyncio.sleep(min(RADAR_POLL_SEC, 60.0))
            continue
        now = time.time()
        cfg = lt._load_config()
        ci = max(10.0, float(cfg.get("check_interval", 30)))
        if now - last_pos_check >= ci:
            last_pos_check = now
            try:
                await lt.check_positions(_alert_sender)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("God radar check_positions failed")
        try:
            await _god_radar_once()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("God radar tick failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=RADAR_POLL_SEC)
        except asyncio.TimeoutError:
            pass


def _apply_live_config_for_god() -> None:
    import live_trader as lt

    cfg = lt._load_config()
    cfg["enabled"] = True
    # God radar runs its own tensor → hedge path; disable built-in neural listener to avoid double fires.
    cfg["neural_execution_enabled"] = False
    cfg["max_trade_sol"] = TRADE_NOTIONAL_SOL
    lt._save_config(cfg)


async def start_autonomous_engine(
    alert_sender: Optional[Callable[[str], Awaitable[None]]] = None,
    *,
    paper_mode: bool = False,
    application: Any = None,
) -> bool:
    """
    Spawn two daemon tasks: forge (evolver sweeps) + radar (live tensor → Δ-neutral).

    Returns False if already running. ``paper_mode`` runs forge only (radar idles).
    """
    global _running, _stop_event, _bg_tasks, _alert_sender, _telegram_create_task, _session_cfg_mtime

    if _running:
        logger.info("God engine already running")
        return False

    reset_god_session_guard()
    _alert_sender = alert_sender
    if application is not None and hasattr(application, "create_task"):
        _telegram_create_task = lambda coro, name=None: application.create_task(  # type: ignore[misc]
            coro,
            name=name,
        )
    else:
        _telegram_create_task = None
    _stop_event = asyncio.Event()
    _stop_event.clear()
    _bg_tasks = []

    _refresh_global_best_sync()

    if not paper_mode:
        _apply_live_config_for_god()

    _running = True

    try:
        GOD_ORCHESTRATOR.reload_skills()
    except Exception:
        logger.debug("god initial reload_skills skipped", exc_info=True)

    _start_session_commander_watchdog()

    t_forge = _spawn_loop_task(_forge_loop(_stop_event), name="god_forge")
    t_radar = _spawn_loop_task(
        _radar_loop(_stop_event, paper_mode=paper_mode),
        name="god_radar",
    )
    _bg_tasks = [t_forge, t_radar]

    logger.info(
        "God autonomous engine started (paper_mode=%s trade_sol=%s loss_fuse=%s)",
        paper_mode,
        TRADE_NOTIONAL_SOL,
        MAX_SESSION_LOSS_SOL,
    )
    return True


async def stop_autonomous_engine() -> None:
    """Cancel forge/radar tasks (no process exit)."""
    global _running, _stop_event, _bg_tasks, _telegram_create_task

    if not _running:
        return
    _running = False
    if _stop_event is not None:
        _stop_event.set()
    for t in list(_bg_tasks):
        if not t.done():
            t.cancel()
    for t in list(_bg_tasks):
        try:
            await t
        except asyncio.CancelledError:
            pass
    _bg_tasks = []
    _telegram_create_task = None
    _stop_session_commander_watchdog()
