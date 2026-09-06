import logging
import random
import time
from urllib.parse import urlparse

logger = logging.getLogger("DukaScraper.ProxyManager")


def parse_proxy_url(proxy_url: str) -> dict[str, str] | None:
    """Parse a proxy URL into a Playwright/Patchright-compatible dict.

    Accepts:
      http://host:port
      http://user:pass@host:port
      socks5://host:port
      socks5h://host:port
      host:port  (assumed HTTP)

    Returns dict suitable for ``browser.new_context(proxy=...)`` or ``None``.
    """
    url = proxy_url.strip()
    if not url:
        return None

    # Bare host:port → treat as HTTP
    if ":" in url and not url.startswith(("http://", "https://", "socks5://", "socks5h://")):
        url = f"http://{url}"

    # Playwright/Patchright only supports socks5://, not socks5h://
    # socks5h means "DNS through proxy" — replace with socks5://
    if url.startswith("socks5h://"):
        url = url.replace("socks5h://", "socks5://", 1)

    try:
        parsed = urlparse(url)
        server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
        result: dict[str, str] = {"server": server}
        if parsed.username:
            result["username"] = parsed.username
        if parsed.password:
            result["password"] = parsed.password
        return result
    except Exception as exc:
        logger.warning("Failed to parse proxy URL '%s': %s", proxy_url, exc)
        return None


class ProxyManager:
    """Shared proxy manager for all crawl workers.

    Handles rotation, failure tracking, cooldown, and provides both
    raw URL strings (for httpx) and Playwright/Patchright-compatible
    dicts (for browser contexts).
    """

    def __init__(
        self,
        proxy_list: list[str],
        failure_threshold: int = 3,
        cooldown_seconds: int = 300,
    ):
        self.proxies = [p.strip() for p in proxy_list if p.strip()]
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self.proxy_failures: dict[str, int] = {p: 0 for p in self.proxies}
        self.proxy_cooldowns: dict[str, float] = {}
        logger.info("ProxyManager initialized with %d proxies.", len(self.proxies))

    # ---- Core API --------------------------------------------------------

    def get_proxy(self) -> str | None:
        """Return a random healthy proxy URL string, or None if pool is empty."""
        if not self.proxies:
            return None
        current_time = time.time()
        available = [
            p for p in self.proxies
            if self.proxy_failures.get(p, 0) < self.failure_threshold
            and current_time > self.proxy_cooldowns.get(p, 0)
        ]
        if not available:
            logger.warning("All proxies in cooldown — resetting health.")
            self.reset_health()
            available = self.proxies
        return random.choice(available) if available else None

    def get_browser_proxy(self) -> dict[str, str] | None:
        """Return a Playwright/Patchright-compatible proxy dict for browser contexts."""
        url = self.get_proxy()
        return parse_proxy_url(url) if url else None

    def report_failure(self, proxy: str) -> None:
        """Record a failed request for the given proxy."""
        if not proxy:
            return
        self.proxy_failures[proxy] = self.proxy_failures.get(proxy, 0) + 1
        logger.warning(
            "Proxy failure: %s (count=%d)", proxy, self.proxy_failures[proxy]
        )
        if self.proxy_failures[proxy] >= self.failure_threshold:
            self.proxy_cooldowns[proxy] = time.time() + self.cooldown_seconds
            logger.error(
                "Proxy %s in cooldown for %ds.", proxy, self.cooldown_seconds
            )

    def report_success(self, proxy: str) -> None:
        """Decrement failure count on success to restore proxy health."""
        if not proxy:
            return
        if proxy in self.proxy_failures and self.proxy_failures[proxy] > 0:
            self.proxy_failures[proxy] = max(0, self.proxy_failures[proxy] - 1)

    def reset_health(self) -> None:
        """Reset all failure counts and active cooldowns."""
        self.proxy_failures = {p: 0 for p in self.proxies}
        self.proxy_cooldowns.clear()

    @property
    def is_active(self) -> bool:
        """True if at least one proxy is configured."""
        return len(self.proxies) > 0