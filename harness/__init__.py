"""
Multi-Agent Coordinate Harness Framework

A framework for agent self-loop iteration and cross-session collaboration.
Supports file-based, git-based, and API-based communication channels.
"""

from harness.protocol import Handoff, Message, AgentRole
from harness.agent import Agent, AgentConfig
from harness.loop import LoopController
from harness.core import Harness

__version__ = "0.1.0"
__all__ = ["Harness", "Agent", "AgentConfig", "Handoff", "Message", "AgentRole", "LoopController"]
