"""
Browser Agents — Playwright-based automation for AI web interfaces.

Each platform controller knows how to:
1. Open the AI's web page
2. Find the input box
3. Paste the prompt
4. Wait for the response to complete
5. Extract the output (code, text)

Supported platforms:
- ChatGPT (chatgpt.com)
- Grok (grok.com)
- Claude Web (claude.ai)
- Claude Code Web (claude.ai/code)
- Codex (chatgpt.com/codex or similar)
"""

from browser_agents.base import BrowserAgent, BrowserConfig, AgentResult
from browser_agents.chatgpt import ChatGPTAgent
from browser_agents.grok import GrokAgent
from browser_agents.claude_web import ClaudeWebAgent
from browser_agents.claude_code_web import ClaudeCodeWebAgent

PLATFORM_AGENTS = {
    "gpt": ChatGPTAgent,
    "chatgpt": ChatGPTAgent,
    "grok": GrokAgent,
    "claude_web": ClaudeWebAgent,
    "claude_code": ClaudeCodeWebAgent,
    "codex": ChatGPTAgent,  # Codex runs within ChatGPT interface
}


def get_browser_agent(platform: str, config: BrowserConfig | None = None) -> BrowserAgent:
    """Factory: get the right browser agent for a platform."""
    agent_cls = PLATFORM_AGENTS.get(platform)
    if agent_cls is None:
        raise ValueError(f"Unknown platform: {platform}. Available: {list(PLATFORM_AGENTS.keys())}")
    return agent_cls(config or BrowserConfig())


__all__ = [
    "BrowserAgent", "BrowserConfig", "AgentResult",
    "ChatGPTAgent", "GrokAgent", "ClaudeWebAgent", "ClaudeCodeWebAgent",
    "get_browser_agent", "PLATFORM_AGENTS",
]
