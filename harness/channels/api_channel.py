"""
API Channel — agents communicate via HTTP API.

Level: Advanced (real-time, remote, scalable)
Use when: Agents need real-time communication across networks.

This provides the interface. You can plug in any backend:
- A simple Flask/FastAPI server
- Redis pub/sub
- WebSocket server
- Cloud message queues (SQS, Pub/Sub, etc.)
"""

from __future__ import annotations

import json
import time
import urllib.request
import urllib.error
from typing import Any

from harness.protocol import Handoff


class APIChannel:
    """HTTP API-based communication channel."""

    def __init__(self, base_url: str = "http://localhost:8080", api_key: str = "", **kwargs):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _request(self, method: str, path: str, data: dict | None = None) -> dict:
        """Make an HTTP request to the API."""
        url = f"{self.base_url}{path}"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        body = json.dumps(data).encode() if data else None
        req = urllib.request.Request(url, data=body, headers=headers, method=method)

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.URLError as e:
            raise ConnectionError(f"API request failed: {e}")

    def send(self, handoff: Handoff):
        """Send a handoff via the API."""
        self._request("POST", "/handoffs", json.loads(handoff.to_json()))

    def receive(self, agent_name: str) -> Handoff | None:
        """Receive the next handoff for this agent."""
        try:
            data = self._request("GET", f"/handoffs/{agent_name}/next")
            if data:
                return Handoff.from_json(json.dumps(data))
        except ConnectionError:
            return None
        return None

    def receive_all(self, agent_name: str) -> list[Handoff]:
        """Receive all pending handoffs."""
        try:
            data = self._request("GET", f"/handoffs/{agent_name}")
            return [Handoff.from_json(json.dumps(h)) for h in data.get("handoffs", [])]
        except ConnectionError:
            return []

    def broadcast(self, handoff: Handoff):
        """Broadcast a handoff to all agents."""
        self._request("POST", "/handoffs/broadcast", json.loads(handoff.to_json()))

    def status(self) -> dict:
        """Check API server status."""
        try:
            return self._request("GET", "/status")
        except ConnectionError:
            return {"status": "offline"}
