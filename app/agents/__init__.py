"""Pluggable agents registry for crawl workers.

Define lightweight agent configs (name, fetcher, headers, proxy_pref).
Workers can rotate or select agents from this module to vary fetching
strategies (e.g., different User-Agents, proxy pools, or fetch backends).
"""

from .agents import (
    AGENTS,
    Agent,
    fetch_with_agent_rotation,
    fetch_with_retry,
    get_agent_for_job,
    rotate_agent_for_job,
)

__all__ = [
    "AGENTS",
    "Agent",
    "fetch_with_agent_rotation",
    "fetch_with_retry",
    "get_agent_for_job",
    "rotate_agent_for_job",
]
