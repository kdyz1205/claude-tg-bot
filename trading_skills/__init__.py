"""
trading_skills/ — 生产级量化交易技能模块

8 个独立可用的交易 Skill，每个都是对冲基金级别的实现。
直接接入 crypto-analysis- 的 agent_brain.py 和 okx_trader.py。

Skills:
1. Market Regime Detector — 市场状态分类 (趋势/震荡/极端)
2. Drawdown Guardian — 实时回撤守护 (自适应动态阈值)
3. Signal Confidence Scorer — 信号置信度连续打分 (0-100)
4. Multi-Timeframe Confluence — 多周期共振过滤
5. Correlation Hedge Monitor — 相关性对冲监控
6. Entry Timing Optimizer — 入场微调 (限价单优化)
7. Post-Trade Analyzer — 交易复盘引擎
8. Funding Rate Scanner — 资金费率套利扫描
"""

from .regime_detector import MarketRegimeDetector, MarketRegime
from .drawdown_guardian import DrawdownGuardian, status_triggers_hard_kill
from .confidence_scorer import SignalConfidenceScorer
from .mtf_confluence import MultiTimeframeConfluence
from .correlation_monitor import CorrelationHedgeMonitor
from .entry_optimizer import EntryTimingOptimizer
from .post_trade_analyzer import PostTradeAnalyzer
from .funding_scanner import FundingRateScanner

__all__ = [
    "MarketRegimeDetector", "MarketRegime",
    "DrawdownGuardian",
    "status_triggers_hard_kill",
    "SignalConfidenceScorer",
    "MultiTimeframeConfluence",
    "CorrelationHedgeMonitor",
    "EntryTimingOptimizer",
    "PostTradeAnalyzer",
    "FundingRateScanner",
]
