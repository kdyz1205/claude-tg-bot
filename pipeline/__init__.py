"""Pipeline package — the full Telegram → Dispatch → Browser → Git pipeline."""

from pipeline.auto_dev_orchestrator import AutoDevOrchestrator, AutoDevResult
from pipeline.net_gate import AsyncRateLimiter, alpha_http_limiter, TradeMemoryGate, trade_memory_gate, FailureRecord
from pipeline.orchestrator import Orchestrator
from pipeline.paper_alpha_feed import run_academic_to_alpha_pipeline

__all__ = [
    "Orchestrator",
    "run_academic_to_alpha_pipeline",
    "AutoDevOrchestrator",
    "AutoDevResult",
    "AsyncRateLimiter",
    "alpha_http_limiter",
    "TradeMemoryGate",
    "trade_memory_gate",
    "FailureRecord",
]
