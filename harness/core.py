"""
Harness Core — the main orchestrator that ties everything together.

A Harness:
1. Creates agents from config (or natural language)
2. Sets up communication channels
3. Runs loops (self, ping-pong, pipeline)
4. Tracks convergence across all agents
5. Produces a final integrated result

Usage:
    # From YAML config
    harness = Harness.from_config("config/my_harness.yaml")
    result = harness.run()

    # From natural language
    harness = Harness.from_natural_language('''
        Create a coder agent and a reviewer agent.
        The coder writes Python code, the reviewer checks quality.
        They loop until the code scores above 0.9.
        Use git to communicate.
    ''')
    result = harness.run()

    # Programmatic
    harness = Harness()
    harness.add_agent(coder)
    harness.add_agent(reviewer)
    harness.set_loop(LoopMode.PING_PONG)
    result = harness.run()
"""

from __future__ import annotations

import re
import yaml
import logging
from pathlib import Path
from typing import Any, Callable

from harness.agent import Agent, AgentConfig
from harness.protocol import AgentRole, Handoff
from harness.loop import LoopController, LoopMode, LoopResult
from harness.channels import get_channel

logger = logging.getLogger(__name__)


class Harness:
    """
    The main orchestrator for multi-agent coordination.
    """

    def __init__(self, name: str = "harness"):
        self.name = name
        self.agents: dict[str, Agent] = {}
        self.loop_mode: LoopMode = LoopMode.SELF
        self.max_iterations: int = 10
        self.convergence_threshold: float = 0.9
        self.channel_type: str = "file"
        self.channel_config: dict = {}
        self._on_iteration: Callable | None = None
        self._on_complete: Callable | None = None

    def add_agent(self, agent: Agent) -> Harness:
        """Add an agent to the harness."""
        self.agents[agent.name] = agent
        return self

    def create_agent(self, config: AgentConfig) -> Agent:
        """Create and register an agent from config."""
        agent = Agent(config)
        self.add_agent(agent)
        return agent

    def set_loop(self, mode: LoopMode, max_iterations: int = 10, convergence_threshold: float = 0.9):
        """Configure the loop mode."""
        self.loop_mode = mode
        self.max_iterations = max_iterations
        self.convergence_threshold = convergence_threshold

    def on_iteration(self, callback: Callable):
        """Register a callback for each iteration."""
        self._on_iteration = callback

    def on_complete(self, callback: Callable):
        """Register a callback for loop completion."""
        self._on_complete = callback

    def run(self, initial_handoff: Handoff | None = None) -> LoopResult:
        """Run the harness loop."""
        if not self.agents:
            raise RuntimeError("No agents registered. Use add_agent() or create_agent().")

        controller = LoopController(
            mode=self.loop_mode,
            max_iterations=self.max_iterations,
            convergence_threshold=self.convergence_threshold,
            on_iteration=self._on_iteration,
            on_convergence=self._on_complete,
        )

        handoff = initial_handoff or Handoff(
            max_iterations=self.max_iterations,
            convergence_threshold=self.convergence_threshold,
        )

        agent_list = list(self.agents.values())

        if self.loop_mode == LoopMode.SELF:
            return controller.run(agent=agent_list[0], initial_handoff=handoff)
        else:
            return controller.run(agents=agent_list, initial_handoff=handoff)

    @classmethod
    def from_config(cls, config_path: str | Path) -> Harness:
        """Create a Harness from a YAML config file."""
        path = Path(config_path)
        with open(path) as f:
            config = yaml.safe_load(f)

        harness = cls(name=config.get("name", "harness"))
        harness.channel_type = config.get("channel", {}).get("type", "file")
        harness.channel_config = config.get("channel", {}).get("config", {})

        # Set loop mode
        loop_config = config.get("loop", {})
        mode_str = loop_config.get("mode", "self")
        harness.loop_mode = LoopMode(mode_str)
        harness.max_iterations = loop_config.get("max_iterations", 10)
        harness.convergence_threshold = loop_config.get("convergence_threshold", 0.9)

        # Create agents
        for agent_def in config.get("agents", []):
            agent_config = AgentConfig(
                name=agent_def["name"],
                role=AgentRole(agent_def.get("role", "custom")),
                description=agent_def.get("description", ""),
                capabilities=agent_def.get("capabilities", []),
                working_dir=agent_def.get("working_dir", "."),
                max_iterations=harness.max_iterations,
                channel_type=harness.channel_type,
                channel_config=harness.channel_config,
            )
            harness.create_agent(agent_config)

        return harness

    @classmethod
    def from_natural_language(cls, description: str) -> Harness:
        """
        Create a Harness from a natural language description.

        Parses the description to extract:
        - Agent definitions (roles, capabilities)
        - Loop mode and parameters
        - Channel type
        - Convergence criteria
        """
        harness = cls()
        desc_lower = description.lower()

        # Detect loop mode
        if any(kw in desc_lower for kw in ["alternate", "ping-pong", "back and forth", "review each other"]):
            harness.loop_mode = LoopMode.PING_PONG
        elif any(kw in desc_lower for kw in ["pipeline", "chain", "pass through", "sequence"]):
            harness.loop_mode = LoopMode.PIPELINE
        else:
            harness.loop_mode = LoopMode.SELF

        # Detect channel
        if "git" in desc_lower:
            harness.channel_type = "git"
        elif "api" in desc_lower or "http" in desc_lower:
            harness.channel_type = "api"

        # Detect iterations
        iter_match = re.search(r"(\d+)\s*(?:time|iter|loop|round|cycle)", desc_lower)
        if iter_match:
            harness.max_iterations = int(iter_match.group(1))

        # Detect convergence threshold
        score_match = re.search(r"(?:score|threshold|above|over)\s*(0\.\d+)", desc_lower)
        if score_match:
            harness.convergence_threshold = float(score_match.group(1))

        # Detect agents from role keywords
        role_patterns = {
            AgentRole.CODER: r"coder|code\s*writer|developer|programmer",
            AgentRole.REVIEWER: r"reviewer|critic|checker|validator",
            AgentRole.INTEGRATOR: r"integrator|merger|combiner",
            AgentRole.PLANNER: r"planner|architect|designer",
            AgentRole.TESTER: r"tester|qa|test\s*runner",
        }

        found_roles = []
        for role, pattern in role_patterns.items():
            if re.search(pattern, desc_lower):
                found_roles.append(role)

        if not found_roles:
            found_roles = [AgentRole.CODER]

        for role in found_roles:
            config = AgentConfig(
                name=role.value,
                role=role,
                description=f"Auto-generated {role.value} agent",
                channel_type=harness.channel_type,
                channel_config=harness.channel_config,
                max_iterations=harness.max_iterations,
            )
            harness.create_agent(config)

        return harness

    def to_yaml(self) -> str:
        """Export the current harness configuration as YAML."""
        config = {
            "name": self.name,
            "channel": {
                "type": self.channel_type,
                "config": self.channel_config,
            },
            "loop": {
                "mode": self.loop_mode.value,
                "max_iterations": self.max_iterations,
                "convergence_threshold": self.convergence_threshold,
            },
            "agents": [
                {
                    "name": agent.name,
                    "role": agent.role.value,
                    "description": agent.config.description,
                    "capabilities": agent.config.capabilities,
                    "working_dir": agent.config.working_dir,
                }
                for agent in self.agents.values()
            ],
        }
        return yaml.dump(config, default_flow_style=False, allow_unicode=True)

    def status(self) -> dict:
        """Get current harness status."""
        return {
            "name": self.name,
            "agents": list(self.agents.keys()),
            "loop_mode": self.loop_mode.value,
            "max_iterations": self.max_iterations,
            "convergence_threshold": self.convergence_threshold,
            "channel": self.channel_type,
        }
