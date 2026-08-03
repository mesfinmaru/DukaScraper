import logging
import random
import time

logger = logging.getLogger("DukaScraper.ProxyManager")


class ProxyManager:
    """
    Advanced Proxy Manager designed for large-scale, multi-country,
    and multi-website web scraping. Handles automatic rotation,
    failure tracking, and temporary cooldown periods to prevent IP blocks.
    """

    def __init__(self, proxy_list: list[str], failure_threshold: int = 3, cooldown_seconds: int = 300):
        self.proxies = proxy_list
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds

        # Tracking proxy health and cooldown status
        self.proxy_failures: dict[str, int] = {p: 0 for p in proxy_list}
        self.proxy_cooldowns: dict[str, float] = {}

    def get_proxy(self) -> str | None:
        """
        Retrieves a healthy, randomly selected proxy from the pool,
        excluding proxies currently in cooldown due to failures.
        """
        if not self.proxies:
            return None

        current_time = time.time()

        # Filter out proxies that have exceeded failure threshold and are in cooldown
        available_proxies = [p for p in self.proxies if self.proxy_failures.get(p, 0) < self.failure_threshold and current_time > self.proxy_cooldowns.get(p, 0)]

        # If all proxies are temporarily blocked, reset health or fallback
        if not available_proxies:
            logger.warning("All proxies are currently in cooldown. Resetting failure counts temporarily.")
            self.reset_health()
            available_proxies = self.proxies

        if not available_proxies:
            return None

        # Random selection across global pool to ensure broad geographic distribution
        selected_proxy = random.choice(available_proxies)
        return selected_proxy

    def report_failure(self, proxy: str):
        """
        Reports a failed request (e.g., 403 Forbidden, 429 Too Many Requests, or Timeout)
        associated with a specific proxy, incrementing its failure count.
        """
        if not proxy:
            return

        self.proxy_failures[proxy] = self.proxy_failures.get(proxy, 0) + 1
        logger.warning(f"Proxy failure reported for: {proxy}. Failure count: {self.proxy_failures[proxy]}")

        # Put proxy into cooldown if it breaches the failure threshold
        if self.proxy_failures[proxy] >= self.failure_threshold:
            self.proxy_cooldowns[proxy] = time.time() + self.cooldown_seconds
            logger.error(f"Proxy {proxy} placed in cooldown for {self.cooldown_seconds} seconds due to repeated blocks.")

    def report_success(self, proxy: str):
        """
        Decrements failure count on a successful request to restore proxy health status.
        """
        if not proxy:
            return
        if proxy in self.proxy_failures and self.proxy_failures[proxy] > 0:
            self.proxy_failures[proxy] = max(0, self.proxy_failures[proxy] - 1)

    def reset_health(self):
        """Resets all failure counts and active cooldowns."""
        self.proxy_failures = {p: 0 for p in self.proxies}
        self.proxy_cooldowns.clear()
