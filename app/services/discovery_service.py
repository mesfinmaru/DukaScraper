"""Topic-based seed discovery, shared by the surface, deep and dark networks.

Turns a ``SearchRequest`` (topic/query + ``network``) into ordinary
``CrawlRequest`` messages on ``crawl.requests``. The ``network`` field only
selects which engine list from ``configs/search_engines.json`` is queried;
dark engines are fetched through the *same* Tor client the dark crawl worker
uses (``app.services.tor_http_client``), and surface/deep engines over plain
HTTP. Nothing here is dark-specific - the code path is identical for all three
networks, only the engine block differs.

Design notes:
  - Engine parsing is generic: ``searxng_json`` reads the SearXNG JSON API,
    ``html_links`` extracts anchors from any results page and can unwrap
    redirect parameters (DuckDuckGo's ``uddg``) and require ``.onion`` results.
  - Emitted seeds are deduplicated (via ``LinkExtractionService.normalize_url``)
    and SSRF-guarded (``is_safe_fetch_url``, onion allowed only for the dark
    network) so a malicious search index cannot point a crawl worker at
    localhost or cloud metadata.
  - Seed routing uses the existing ``WorkerAssignmentEngine``: a topic search
    routes to surface/deep/dark exactly like a hand-entered URL.
  - Fan-out mirrors ``recursive_crawl_service``: child task slots are
    registered BEFORE publishing, so the job cannot complete before the seeds
    land.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse

import httpx
from aiokafka import AIOKafkaProducer

from app.common.config.settings import settings
from app.common.constants.worker_assignment import assign_worker
from app.common.logger.logger import logger
from app.pipeline.schemas import CrawlRequest, SearchRequest
from app.services.link_extraction_service import LinkExtractionService
from app.storage.postgres.client import pg_client

try:
    from lxml import html as lxml_html
except ImportError:  # pragma: no cover - lxml is a project dependency
    lxml_html = None

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,am;q=0.8",
}

#: Networks a discovery request may target. "all" queries every real network
#: and merges the results - it is not itself a network with its own engines.
SUPPORTED_NETWORKS = ("surface", "deep", "dark")

#: Concrete networks searched when a request asks for "all".
ALL_NETWORKS = SUPPORTED_NETWORKS


@dataclass(frozen=True)
class EngineSpec:
    """One configured search engine for a network."""

    name: str
    type: str
    network: str
    enabled: bool = True
    url_template: str = ""
    base_url: str = ""
    link_pattern: str = ""
    unwrap_query_param: str = ""
    require_onion: bool = False
    result_domains_exclude: tuple[str, ...] = field(default_factory=tuple)


class TopicDiscoveryService:
    """Resolve a topic/query into seed URLs and fan them out as CrawlRequests."""

    def __init__(
        self,
        *,
        engines_path: str | None = None,
        http_client: httpx.AsyncClient | None = None,
        tor_client: httpx.AsyncClient | None = None,
        timeout: float | None = None,
        max_concurrent_engines: int | None = None,
    ):
        self.engines_path = Path(engines_path or settings.SEARCH_ENGINES_CONFIG_PATH)
        self.timeout = timeout if timeout is not None else settings.SEARCH_ENGINE_TIMEOUT_SECONDS
        self.max_concurrent_engines = (
            max_concurrent_engines
            if max_concurrent_engines is not None
            else settings.SEARCH_MAX_CONCURRENT_ENGINES
        )
        self._http_client = http_client
        self._tor_client = tor_client
        self._owns_clients = http_client is None and tor_client is None

    # ------------------------------------------------------------------
    # Config
    # ------------------------------------------------------------------

    def load_engines(self, network: str) -> list[EngineSpec]:
        """Return the enabled engine specs for *network*.

        Unknown network or unreadable/invalid config yields an empty list
        rather than raising, so one bad config cannot crash the worker.
        """
        network = (network or "").lower()
        if network not in SUPPORTED_NETWORKS:
            logger.warning("Unknown discovery network '%s' — no engines loaded", network)
            return []
        try:
            with self.engines_path.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            logger.error("Could not read search engine config %s: %s", self.engines_path, exc)
            return []

        block = data.get(network) or {}
        specs: list[EngineSpec] = []
        for raw in block.get("engines", []):
            if not raw.get("enabled", True):
                continue
            base_url = raw.get("base_url", "") or ""
            if raw.get("type") == "searxng_json" and settings.SEARXNG_BASE_URL:
                base_url = settings.SEARXNG_BASE_URL
            specs.append(
                EngineSpec(
                    name=raw.get("name", "engine"),
                    type=raw.get("type", "html_links"),
                    network=network,
                    enabled=True,
                    url_template=raw.get("url_template", ""),
                    base_url=base_url,
                    link_pattern=raw.get("link_pattern", ""),
                    unwrap_query_param=raw.get("unwrap_query_param", ""),
                    require_onion=bool(raw.get("require_onion", False)),
                    result_domains_exclude=tuple(raw.get("result_domains_exclude", [])),
                )
            )
        return specs

    # ------------------------------------------------------------------
    # HTTP clients (one per network family)
    # ------------------------------------------------------------------

    async def _client_for(self, network: str) -> httpx.AsyncClient:
        # Defensive: "all" is expanded before any client is chosen, because the
        # dark network must use the Tor client and the others must not.
        if network == "all":
            network = "surface"
        if network == "dark":
            if self._tor_client is None:
                from app.services.tor_http_client import build_tor_http_client

                self._tor_client = build_tor_http_client(
                    proxy_url=settings.tor_proxy_url,
                    max_connections=self.max_concurrent_engines,
                    connect_timeout=self.timeout,
                    read_timeout=self.timeout,
                    write_timeout=self.timeout,
                    pool_timeout=self.timeout,
                    headers=DEFAULT_HEADERS,
                )
            return self._tor_client
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                headers=DEFAULT_HEADERS,
                follow_redirects=True,
                timeout=httpx.Timeout(self.timeout),
                trust_env=False,
            )
        return self._http_client

    async def aclose(self) -> None:
        """Close any clients this service created itself."""
        if not self._owns_clients:
            return
        for client in (self._http_client, self._tor_client):
            if client is not None:
                try:
                    await client.aclose()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    async def discover(self, request: SearchRequest) -> list[str]:
        """Query every enabled engine for *request* and return deduped seeds.

        ``request.network == "all"`` searches surface, deep and dark together and
        returns the union. Each network keeps its own HTTP client (dark must go
        through Tor, the others must not), and ``max_results`` caps the merged
        result rather than each network separately.
        """
        network = (request.network or "surface").lower()
        if network == "all":
            return await self._discover_all_networks(request)
        return await self._discover_one_network(request, network)

    async def _discover_all_networks(self, request: SearchRequest) -> list[str]:
        """Union of every network's seeds for one query.

        Networks run concurrently, but the per-network result cap is divided up
        front rather than each network being allowed the full ``max_results`` -
        otherwise "all" would return up to three times what was asked for. Any
        single network failing (typically dark with no Tor available) is logged
        and skipped rather than failing the whole query.
        """
        networks = list(ALL_NETWORKS)
        per_network = max(1, request.max_results // len(networks) + 1)

        async def run(net: str) -> list[str]:
            scoped = request.model_copy(
                update={"network": net, "max_results": per_network}
            )
            try:
                return await self._discover_one_network(scoped, net)
            except Exception as exc:
                logger.warning(
                    "[%s] Discovery network=%s failed during 'all' search: %s",
                    request.job_id, net, exc,
                )
                return []

        per_network_seeds = await asyncio.gather(*(run(net) for net in networks))

        # Merge in network order, keeping the first occurrence of each
        # normalized URL, and stop at the caller's limit.
        seeds: list[str] = []
        seen: set[str] = set()
        for group in per_network_seeds:
            for url in group:
                key = LinkExtractionService.normalize_url(url)
                if key in seen:
                    continue
                seen.add(key)
                seeds.append(url)
                if len(seeds) >= request.max_results:
                    break
            if len(seeds) >= request.max_results:
                break

        logger.info(
            "[%s] Discovery network=all query=%r \u2192 %d seed(s) across %s",
            request.job_id, request.query, len(seeds), "/".join(networks),
        )
        return seeds

    async def _discover_one_network(self, request: SearchRequest, network: str) -> list[str]:
        """Query one network's enabled engines and return deduped seeds."""
        specs = self.load_engines(network)
        if request.engines:
            allowed = {name.lower() for name in request.engines}
            specs = [s for s in specs if s.name.lower() in allowed]
        if not specs:
            logger.warning(
                "[%s] No discovery engines available for network=%s", request.job_id, network
            )
            return []

        client = await self._client_for(network)
        semaphore = asyncio.Semaphore(max(1, self.max_concurrent_engines))
        per_engine_limit = max(1, request.max_results)

        async def run(spec: EngineSpec) -> list[str]:
            async with semaphore:
                try:
                    return await self._query_engine(client, spec, request, per_engine_limit)
                except Exception as exc:
                    logger.warning(
                        "[%s] Discovery engine '%s' failed: %s: %s",
                        request.job_id, spec.name, type(exc).__name__, exc or "(no message)",
                    )
                    return []

        results = await asyncio.gather(*(run(spec) for spec in specs))

        seeds: list[str] = []
        seen: set[str] = set()
        for engine_urls in results:
            for url in engine_urls:
                key = LinkExtractionService.normalize_url(url)
                if key in seen:
                    continue
                seen.add(key)
                seeds.append(url)
                if len(seeds) >= request.max_results:
                    break
            if len(seeds) >= request.max_results:
                break

        logger.info(
            "[%s] Discovery network=%s query=%r → %d seed(s) from %d engine(s)",
            request.job_id, network, request.query, len(seeds), len(specs),
        )
        return seeds

    async def _query_engine(
        self, client: httpx.AsyncClient, spec: EngineSpec, request: SearchRequest, limit: int
    ) -> list[str]:
        if spec.type == "searxng_json":
            return await self._query_searxng(client, spec, request, limit)
        return await self._query_html_links(client, spec, request, limit)

    async def _query_searxng(
        self, client: httpx.AsyncClient, spec: EngineSpec, request: SearchRequest, limit: int
    ) -> list[str]:
        base = (spec.base_url or "").rstrip("/")
        if not base:
            raise ValueError(f"engine '{spec.name}' has no base_url")
        resp = await client.get(
            f"{base}/search",
            params={"q": request.query, "format": "json"},
        )
        resp.raise_for_status()
        data = resp.json()
        urls: list[str] = []
        for item in data.get("results", []):
            url = item.get("url")
            if url and self._accept(url, spec):
                urls.append(url)
            if len(urls) >= limit:
                break
        return urls

    async def _query_html_links(
        self, client: httpx.AsyncClient, spec: EngineSpec, request: SearchRequest, limit: int
    ) -> list[str]:
        if not spec.url_template:
            raise ValueError(f"engine '{spec.name}' has no url_template")
        url = spec.url_template.replace("{query}", quote_plus(request.query))
        resp = await client.get(url)
        resp.raise_for_status()

        results: list[str] = []
        seen: set[str] = set()
        for candidate in self._extract_anchor_urls(resp.text, str(resp.url), spec):
            if not self._accept(candidate, spec):
                continue
            key = LinkExtractionService.normalize_url(candidate)
            if key in seen:
                continue
            seen.add(key)
            results.append(candidate)
            if len(results) >= limit:
                break
        return results

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _extract_anchor_urls(self, html: str, base_url: str, spec: EngineSpec) -> list[str]:
        """Yield candidate result URLs from a results page, unwrapping redirects."""
        if not html:
            return []
        if lxml_html is None:
            return [
                unwrapped
                for link in LinkExtractionService.extract_links(html, base_url)
                if (unwrapped := self._unwrap_redirect(link, spec))
            ]
        try:
            tree = lxml_html.fromstring(html)
        except Exception:
            return []

        urls: list[str] = []
        for el in tree.xpath("//a[@href]"):
            href = (el.get("href") or "").strip()
            if not href or href.startswith(("#", "javascript:", "mailto:")):
                continue
            try:
                absolute = urljoin(base_url, href)
            except Exception:
                continue
            unwrapped = self._unwrap_redirect(absolute, spec)
            if unwrapped:
                urls.append(unwrapped)
        return urls

    def _unwrap_redirect(self, url: str, spec: EngineSpec) -> str:
        """Resolve a search-engine redirect wrapper (e.g. DuckDuckGo's uddg=)."""
        if not spec.unwrap_query_param:
            return url
        try:
            params = parse_qs(urlparse(url).query)
            values = params.get(spec.unwrap_query_param)
            if values:
                candidate = unquote(values[0])
                if candidate.startswith(("http://", "https://")):
                    return candidate
        except Exception:
            pass
        return url

    def _accept(self, url: str, spec: EngineSpec) -> bool:
        """Apply per-engine and safety filters to a candidate seed."""
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            return False
        host = parsed.hostname.lower()
        if spec.require_onion and not host.endswith(".onion"):
            return False
        if spec.link_pattern and not re.search(spec.link_pattern, url, re.IGNORECASE):
            return False
        if any(blocked and blocked in host for blocked in spec.result_domains_exclude):
            return False
        return LinkExtractionService.is_safe_fetch_url(url, allow_onion=spec.network == "dark")

    # ------------------------------------------------------------------
    # Fan-out
    # ------------------------------------------------------------------

    async def fan_out(
        self, producer: AIOKafkaProducer, request: SearchRequest, topic: str
    ) -> dict[str, int]:
        """Discover seeds and publish them as CrawlRequests on *topic*.

        Registers child task slots before publishing so the job cannot settle
        before the seeds are in flight. Returns ``{"seeds": n, "queued": n}``.
        """
        seeds = await self.discover(request)
        if not seeds:
            return {"seeds": 0, "queued": 0}

        await pg_client.register_job_tasks(request.job_id, len(seeds))

        queued = 0
        for url in seeds:
            child = build_seed_crawl_request(request, url)
            try:
                await producer.send_and_wait(
                    topic,
                    value=child.model_dump_json().encode("utf-8"),
                    key=request.job_id.encode("utf-8"),
                )
                queued += 1
            except Exception as exc:
                await pg_client.complete_job_task(request.job_id)
                logger.warning("[%s] Failed to queue seed %s: %s", request.job_id, url, exc)

        logger.info(
            "[%s] Discovery fan-out: %d seed(s), %d queued onto %s",
            request.job_id, len(seeds), queued, topic,
        )
        return {"seeds": len(seeds), "queued": queued}


def build_seed_crawl_request(request: SearchRequest, url: str) -> CrawlRequest:
    """Build a plain CrawlRequest for one discovered seed URL.

    Kept module-level and pure so it is trivially unit-testable. Worker routing
    is left to the existing rules engine so a topic-derived seed is routed
    exactly like a hand-entered URL.
    """
    recursive_config = dict(request.recursive_config or {})
    recursive_config.setdefault("seed_url", url)
    recursive_config.setdefault("same_domain_only", True)
    return CrawlRequest(
        job_id=request.job_id,
        url=url,
        language=request.language,
        worker_type=assign_worker(url),
        depth=0,
        max_depth=request.max_depth,
        parent_url=None,
        recursive_config=recursive_config,
        job_params=request.job_params,
    )
