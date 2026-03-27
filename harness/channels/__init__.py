"""
Communication Channels — how agents exchange handoffs.

Three channel types:
- FileChannel: Local filesystem (fastest, same machine)
- GitChannel: Git branches (cross-machine, versioned)
- APIChannel: HTTP API (real-time, remote)
"""

from harness.channels.file_channel import FileChannel
from harness.channels.git_channel import GitChannel
from harness.channels.api_channel import APIChannel


def get_channel(channel_type: str, config: dict = None):
    """Factory function to create a channel by type."""
    config = config or {}
    channels = {
        "file": FileChannel,
        "git": GitChannel,
        "api": APIChannel,
    }
    if channel_type not in channels:
        raise ValueError(f"Unknown channel type: {channel_type}. Choose from: {list(channels.keys())}")
    return channels[channel_type](**config)


__all__ = ["FileChannel", "GitChannel", "APIChannel", "get_channel"]
