"""Shared Tor-routed ``httpx.AsyncClient`` builder.

Extracted from ``workers/dark-worker/main.py`` (``_build_tor_http_client``) so
that the dark crawl worker and dark-network topic discovery build the *same*
client instead of maintaining two copies. The dark worker keeps its local
``_build_tor_http_client()`` wrapper, which now delegates here.

Why a persistent client: a new client per request through Tor is extremely
slow. Reusing one client keeps SOCKS5 connections and Tor circuits alive, so a
new client must be created if the Tor proxy restarts.
"""

from __future__ import annotations

import httpx

try:
    from httpx_socks import AsyncProxyTransport
except ImportError:  # pragma: no cover - dark-worker installs httpx-socks explicitly
    AsyncProxyTransport = None


def build_tor_http_client(
    *,
    proxy_url: str,
    max_connections: int = 10,
    connect_timeout: float = 60.0,
    read_timeout: float = 60.0,
    write_timeout: float = 60.0,
    pool_timeout: float = 60.0,
    headers: dict[str, str] | None = None,
    verify: bool = False,
    keepalive_expiry: float = 30.0,
) -> httpx.AsyncClient:
    """Create a long-lived ``httpx.AsyncClient`` routed through Tor.

    Args:
        proxy_url: SOCKS5 proxy, e.g. ``socks5h://tor:9050``. The ``socks5h``
            scheme is rewritten to ``socks5`` with ``rdns=True`` so DNS (and
            ``.onion`` hostnames) resolve through the proxy.
        max_connections: Pool size (also used as the keepalive limit).
        connect_timeout/read_timeout/write_timeout/pool_timeout: httpx timeouts.
        headers: Default request headers; ``None`` sends httpx's defaults.
        verify: TLS verification. Defaults to ``False`` because Tor exit nodes
            may present untrusted certificates.
        keepalive_expiry: Seconds to keep idle pooled connections.

    Raises:
        RuntimeError: if ``httpx-socks`` is not installed in this environment.
    """
    if AsyncProxyTransport is None:
        raise RuntimeError(
            "httpx-socks is required for Tor-routed clients but is not installed"
        )

    timeout = httpx.Timeout(
        connect=connect_timeout,
        read=read_timeout,
        write=write_timeout,
        pool=pool_timeout,
    )
    limits = httpx.Limits(
        max_connections=max_connections,
        max_keepalive_connections=max_connections,
        keepalive_expiry=keepalive_expiry,
    )
    # python-socks doesn't support the "socks5h" scheme directly;
    # use "socks5" + rdns=True to resolve DNS through Tor.
    socks_url = proxy_url.replace("socks5h://", "socks5://")
    transport = AsyncProxyTransport.from_url(
        socks_url,
        verify=verify,
        rdns=True,
    )
    return httpx.AsyncClient(
        transport=transport,
        headers=headers,
        follow_redirects=True,
        timeout=timeout,
        trust_env=False,  # Do not pick up system proxy settings
        limits=limits,
    )
