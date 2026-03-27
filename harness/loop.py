"""
Loop Controller — manages agent self-iteration and multi-agent loops.

Supports three loop modes:
1. Self-loop: Single agent iterates on its own output until convergence
2. Ping-pong: Two agents alternate (e.g., coder + reviewer)
3. Pipeline: Chain of agents, each processes output of previous
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable

from harness.agent import Agent
from harness.protocol import Handoff, Message, MessageType

logger = logging.getLogger(__name__)


class LoopMode(Enum):
    SELF = "self"           # Agent iterates alone
    PING_PONG = "ping_pong" # Two agents alternate
    PIPELINE = "pipeline"    # Chain of agents


@dataclass
class LoopResult:
    """Result of a completed loop."""
    final_handoff: Handoff
    iterations: int
    converged: bool
    duration_seconds: float
    history: list[Handoff] = field(default_factory=list)


class LoopController:
    """
    Controls iteration loops between agents.

    Usage:
        # Self-loop
        controller = LoopController(mode=LoopMode.SELF)
        result = controller.run(agent=coder, initial_handoff=handoff)

        # Ping-pong
        controller = LoopController(mode=LoopMode.PING_PONG)
        result = controller.run(agents=[coder, reviewer], initial_handoff=handoff)

        # Pipeline
        controller = LoopController(mode=LoopMode.PIPELINE)
        result = controller.run(agents=[planner, coder, reviewer, tester], initial_handoff=handoff)
    """

    def __init__(
        self,
        mode: LoopMode = LoopMode.SELF,
        max_iterations: int = 10,
        convergence_threshold: float = 0.9,
        on_iteration: Callable[[int, Handoff], None] | None = None,
        on_convergence: Callable[[LoopResult], None] | None = None,
    ):
        self.mode = mode
        self.max_iterations = max_iterations
        self.convergence_threshold = convergence_threshold
        self.on_iteration = on_iteration
        self.on_convergence = on_convergence

    def run(
        self,
        agent: Agent | None = None,
        agents: list[Agent] | None = None,
        initial_handoff: Handoff | None = None,
    ) -> LoopResult:
        """Run the loop and return the result."""
        if self.mode == LoopMode.SELF:
            if agent is None:
                raise ValueError("Self-loop requires a single agent")
            return self._run_self_loop(agent, initial_handoff or Handoff())

        if agents is None or len(agents) < 2:
            raise ValueError(f"{self.mode.value} mode requires at least 2 agents")

        if self.mode == LoopMode.PING_PONG:
            return self._run_ping_pong(agents[0], agents[1], initial_handoff or Handoff())

        if self.mode == LoopMode.PIPELINE:
            return self._run_pipeline(agents, initial_handoff or Handoff())

        raise ValueError(f"Unknown loop mode: {self.mode}")

    def _run_self_loop(self, agent: Agent, handoff: Handoff) -> LoopResult:
        """Agent iterates on its own output."""
        start = time.time()
        history = []

        for i in range(self.max_iterations):
            logger.info(f"Self-loop iteration {i + 1}/{self.max_iterations}")
            handoff = agent.process(handoff)
            history.append(handoff)

            if self.on_iteration:
                self.on_iteration(i + 1, handoff)

            if handoff.check_convergence():
                logger.info(f"Converged at iteration {i + 1}")
                break

        result = LoopResult(
            final_handoff=handoff,
            iterations=handoff.iteration,
            converged=handoff.is_converged,
            duration_seconds=time.time() - start,
            history=history,
        )

        if self.on_convergence:
            self.on_convergence(result)

        return result

    def _run_ping_pong(self, agent_a: Agent, agent_b: Agent, handoff: Handoff) -> LoopResult:
        """Two agents alternate processing."""
        start = time.time()
        history = []
        current_agent = agent_a

        for i in range(self.max_iterations):
            logger.info(f"Ping-pong iteration {i + 1}: {current_agent.name}")
            handoff = current_agent.process(handoff)
            history.append(handoff)

            if self.on_iteration:
                self.on_iteration(i + 1, handoff)

            if handoff.check_convergence():
                logger.info(f"Converged at iteration {i + 1}")
                break

            # Swap agents
            current_agent = agent_b if current_agent is agent_a else agent_a

        result = LoopResult(
            final_handoff=handoff,
            iterations=handoff.iteration,
            converged=handoff.is_converged,
            duration_seconds=time.time() - start,
            history=history,
        )

        if self.on_convergence:
            self.on_convergence(result)

        return result

    def _run_pipeline(self, agents: list[Agent], handoff: Handoff) -> LoopResult:
        """Chain of agents, each processes output of previous. Repeats until convergence."""
        start = time.time()
        history = []

        for cycle in range(self.max_iterations):
            logger.info(f"Pipeline cycle {cycle + 1}/{self.max_iterations}")

            for agent in agents:
                handoff = agent.process(handoff)
                history.append(handoff)

            if self.on_iteration:
                self.on_iteration(cycle + 1, handoff)

            if handoff.check_convergence():
                logger.info(f"Pipeline converged at cycle {cycle + 1}")
                break

        result = LoopResult(
            final_handoff=handoff,
            iterations=handoff.iteration,
            converged=handoff.is_converged,
            duration_seconds=time.time() - start,
            history=history,
        )

        if self.on_convergence:
            self.on_convergence(result)

        return result
