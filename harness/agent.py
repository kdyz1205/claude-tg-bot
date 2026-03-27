"""
Agent Layer — defines what an agent is and how it behaves.

An Agent has a role, capabilities, and a processing function.
It can receive handoffs, process them, and produce new handoffs.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable
from pathlib import Path

from harness.protocol import AgentRole, Handoff, Message, MessageType


@dataclass
class AgentConfig:
    """Configuration for an agent instance."""
    name: str
    role: AgentRole
    description: str = ""
    capabilities: list[str] = field(default_factory=list)
    working_dir: str = "."
    max_iterations: int = 10
    convergence_threshold: float = 0.9
    auto_loop: bool = True
    channel_type: str = "file"  # "file", "git", "api"
    channel_config: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_natural_language(cls, description: str) -> AgentConfig:
        """
        Parse a natural language description into an AgentConfig.

        Example:
            "A coder agent that writes Python code in /project/src,
             loops up to 5 times, uses git to communicate"
        """
        name = "agent"
        role = AgentRole.CUSTOM
        capabilities = []
        max_iter = 10
        channel = "file"
        working_dir = "."

        desc_lower = description.lower()

        # Detect role
        role_map = {
            "coder": AgentRole.CODER,
            "code": AgentRole.CODER,
            "write": AgentRole.CODER,
            "review": AgentRole.REVIEWER,
            "critic": AgentRole.REVIEWER,
            "integrat": AgentRole.INTEGRATOR,
            "merg": AgentRole.INTEGRATOR,
            "plan": AgentRole.PLANNER,
            "architect": AgentRole.PLANNER,
            "test": AgentRole.TESTER,
        }
        for keyword, r in role_map.items():
            if keyword in desc_lower:
                role = r
                name = r.value
                break

        # Detect channel
        if "git" in desc_lower:
            channel = "git"
        elif "api" in desc_lower:
            channel = "api"

        # Detect iteration limit
        import re
        iter_match = re.search(r"(\d+)\s*(?:time|iter|loop|round)", desc_lower)
        if iter_match:
            max_iter = int(iter_match.group(1))

        # Detect capabilities
        cap_keywords = {
            "python": "python", "javascript": "javascript", "typescript": "typescript",
            "react": "react", "vue": "vue", "css": "css", "html": "html",
            "api": "api_design", "database": "database", "sql": "sql",
            "test": "testing", "deploy": "deployment", "docker": "docker",
        }
        for kw, cap in cap_keywords.items():
            if kw in desc_lower:
                capabilities.append(cap)

        # Detect working directory
        path_match = re.search(r"(?:in|at|dir(?:ectory)?)\s+([/\\][\w/\\.-]+)", desc_lower)
        if path_match:
            working_dir = path_match.group(1)

        return cls(
            name=name,
            role=role,
            description=description,
            capabilities=capabilities,
            working_dir=working_dir,
            max_iterations=max_iter,
            convergence_threshold=0.9,
            auto_loop=True,
            channel_type=channel,
        )


class Agent:
    """
    An agent that can process handoffs and participate in loops.

    Usage:
        agent = Agent(config)
        agent.on_receive(my_handler)
        result = agent.process(handoff)
    """

    def __init__(self, config: AgentConfig):
        self.config = config
        self.name = config.name
        self.role = config.role
        self._handler: Callable[[Handoff], Handoff] | None = None
        self._history: list[Handoff] = []
        self.is_running = False

    def on_receive(self, handler: Callable[[Handoff], Handoff]):
        """Register a handler function for incoming handoffs."""
        self._handler = handler
        return handler

    def process(self, handoff: Handoff) -> Handoff:
        """Process an incoming handoff and return the result."""
        if not self._handler:
            raise RuntimeError(f"Agent '{self.name}' has no handler registered. Use @agent.on_receive")

        handoff.iteration += 1
        handoff.updated_at = time.time()

        # Add a status message
        handoff.add_message(Message(
            msg_type=MessageType.STATUS,
            sender=self.name,
            receiver=handoff.source_agent,
            content=f"Processing iteration {handoff.iteration}",
        ))

        result = self._handler(handoff)
        self._history.append(result)
        return result

    def create_handoff(self, target: str, instructions: str = "") -> Handoff:
        """Create a new handoff to send to another agent."""
        return Handoff(
            source_agent=self.name,
            target_agent=target,
            instructions=instructions,
            max_iterations=self.config.max_iterations,
            convergence_threshold=self.config.convergence_threshold,
        )

    def send(self, handoff: Handoff, channel=None):
        """Send a handoff through the configured channel."""
        if channel is None:
            from harness.channels import get_channel
            channel = get_channel(self.config.channel_type, self.config.channel_config)
        channel.send(handoff)

    def receive(self, channel=None) -> Handoff | None:
        """Receive a handoff from the configured channel."""
        if channel is None:
            from harness.channels import get_channel
            channel = get_channel(self.config.channel_type, self.config.channel_config)
        return channel.receive(self.name)

    @property
    def history(self) -> list[Handoff]:
        return list(self._history)
