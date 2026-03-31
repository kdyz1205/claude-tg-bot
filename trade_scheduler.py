"""
Trade Scheduler — Auto-scheduling, wake-on-reset, session orchestration.

Manages the lifecycle of trading sessions:
- Starts/stops LiveTrader and ProStrategyEngine on schedule
- Tracks session P&L and performance
- Auto-resumes after interruptions
"""

import os, json, logging, time, asyncio
from pathlib import Path
from typing import Optional, Callable

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
SCHEDULER_STATE_FILE = BASE_DIR / "_scheduler_state.json"


def _load_state() -> dict:
    try:
        if SCHEDULER_STATE_FILE.exists():
            with open(SCHEDULER_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {
        "active": False,
        "start_time": 0,
        "last_scan_time": 0,
        "total_scans": 0,
        "total_signals": 0,
        "total_trades": 0,
        "session_pnl_sol": 0,
        "errors": 0,
    }


def _save_state(state: dict):
    try:
        tmp = str(SCHEDULER_STATE_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, str(SCHEDULER_STATE_FILE))
    except Exception as e:
        logger.error(f"Scheduler state save failed: {e}")


class TradeScheduler:
    """Orchestrates trading sessions with auto-resume capability."""

    def __init__(self, send_func: Optional[Callable] = None):
        self._send = send_func
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._live_trader = None
        self._strategy_engine = None

    async def start(self, mode: str = "live") -> str:
        """Start a trading session.

        Args:
            mode: "live" for real trading, "paper" for paper trading
        """
        if self._running:
            return "Scheduler already running"

        state = _load_state()
        state["active"] = True
        state["start_time"] = time.time()
        state["mode"] = mode
        _save_state(state)

        self._running = True
        self._task = asyncio.create_task(self._main_loop(mode))

        return f"Trading scheduler started ({mode} mode)"

    async def stop(self) -> str:
        """Stop the trading session."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None

        # Stop sub-engines
        if self._live_trader:
            await self._live_trader.stop()
        if self._strategy_engine:
            await self._strategy_engine.stop()

        state = _load_state()
        state["active"] = False
        _save_state(state)

        return "Trading scheduler stopped"

    async def _main_loop(self, mode: str):
        """Main orchestration loop."""
        try:
            if mode == "live":
                await self._run_live_mode()
            else:
                await self._run_paper_mode()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Scheduler main loop error: {e}")
            state = _load_state()
            state["errors"] = state.get("errors", 0) + 1
            _save_state(state)

    async def _run_live_mode(self):
        """Run live trading mode."""
        import live_trader
        import secure_wallet

        # Verify wallet exists
        if not secure_wallet.wallet_exists():
            if self._send:
                await self._send("\u274c Wallet not configured. Use /wallet_setup first.")
            self._running = False
            return

        # Start live trader
        self._live_trader = live_trader.LiveTrader(send_func=self._send)
        await self._live_trader.start()

        if self._send:
            balance = await secure_wallet.get_sol_balance()
            pubkey = secure_wallet.get_public_key()
            await self._send(
                f"\U0001f680 Live Trading Started\n"
                f"\u94b1\u5305: {pubkey[:8]}...{pubkey[-6:]}\n"
                f"\u4f59\u989d: {balance:.4f} SOL\n"
                f"\u6a21\u5f0f: \u5b9e\u76d8\u4ea4\u6613"
            )

        # Monitor loop — periodic status updates
        scan_count = 0
        while self._running:
            await asyncio.sleep(300)  # Status update every 5 minutes
            scan_count += 1

            state = _load_state()
            state["last_scan_time"] = time.time()
            state["total_scans"] = state.get("total_scans", 0) + 1
            _save_state(state)

            # Periodic status (every 30 min = 6 checks)
            if scan_count % 6 == 0 and self._send:
                stats = live_trader.get_live_stats()
                balance = await secure_wallet.get_sol_balance()
                await self._send(
                    f"\U0001f4ca \u5b9a\u671f\u62a5\u544a\n"
                    f"\u4f59\u989d: {balance:.4f} SOL\n"
                    f"\u4eca\u65e5PnL: {stats['daily_pnl_sol']:+.4f} SOL\n"
                    f"\u603bPnL: {stats['total_pnl_sol']:+.4f} SOL\n"
                    f"\u6301\u4ed3: {stats['open_positions']} | \u5df2\u5e73: {stats['closed_trades']}"
                )

    async def _run_paper_mode(self):
        """Run paper trading mode."""
        import pro_strategy
        import paper_trader

        self._strategy_engine = pro_strategy.ProStrategyEngine(send_func=self._send)
        await self._strategy_engine.start()

        if self._send:
            await self._send("\U0001f4dd Paper Trading Scheduler Started")

        while self._running:
            await asyncio.sleep(300)
            state = _load_state()
            state["last_scan_time"] = time.time()
            state["total_scans"] = state.get("total_scans", 0) + 1
            _save_state(state)

    def status(self) -> str:
        """Get scheduler status."""
        state = _load_state()
        if not state.get("active"):
            return "Scheduler: INACTIVE"

        uptime = time.time() - state.get("start_time", 0)
        hours = uptime / 3600
        mode = state.get("mode", "unknown")

        return (
            f"Scheduler: ACTIVE ({mode})\n"
            f"Uptime: {hours:.1f}h\n"
            f"Scans: {state.get('total_scans', 0)}\n"
            f"Last scan: {time.strftime('%H:%M', time.localtime(state.get('last_scan_time', 0)))}\n"
            f"Errors: {state.get('errors', 0)}"
        )

    @property
    def running(self):
        return self._running
