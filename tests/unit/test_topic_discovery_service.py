"""Unit tests: topic-based discovery (TopicDiscoveryService).

Covers engine-config loading per network, redirect unwrapping, result
filtering (SSRF guard, .onion requirement, patterns), seed CrawlRequest
construction, and fan-out publishing — without any Kafka/network dependency.
"""

import json
import os
from unittest.mock import AsyncMock

from app.pipeline.schemas import CrawlRequest, SearchRequest
from app.services.discovery_service import (
    EngineSpec,
    TopicDiscoveryService,
    build_seed_crawl_request,
)

# Public IP literals are used as "safe" hosts so the SSRF check never triggers
# a real DNS lookup in tests.
PUBLIC_A = "http://93.184.216.34/page-a"
PUBLIC_B = "http://93.184.216.35/page-b"


def _write_config(tmp_path, config) -> str:
    path = tmp_path / "search_engines.json"
    path.write_text(json.dumps(config), encoding="utf-8")
    return str(path)


class _FakeResponse:
    def __init__(self, *, text="", url="", json_data=None, status=200):
        self.text = text
        self.url = url
        self._json = json_data
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._json


class _FakeClient:
    """Minimal httpx.AsyncClient stand-in keyed by URL substring."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    async def get(self, url, params=None):
        self.calls.append((url, params))
        for needle, resp in self.responses.items():
            if needle in url:
                return resp
        return _FakeResponse(text="", url=url)


class _FakeProducer:
    def __init__(self):
        self.sent = []

    async def send_and_wait(self, topic, value, key):
        self.sent.append((topic, value, key))


class TestEngineLoading:
    def test_loads_only_enabled_engines_per_network(self, tmp_path):
        svc = TopicDiscoveryService(
            engines_path=_write_config(
                tmp_path,
                {
                    "surface": {
                        "engines": [
                            {"name": "on", "type": "html_links", "enabled": True,
                             "url_template": "https://search.test/?q={query}"},
                            {"name": "off", "type": "html_links", "enabled": False,
                             "url_template": "https://search.test/?q={query}"},
                        ]
                    },
                    "dark": {
                        "engines": [
                            {"name": "ahmia", "type": "html_links", "enabled": True,
                             "url_template": "https://ahmia.fi/search/?q={query}",
                             "require_onion": True},
                        ]
                    },
                },
            )
        )
        assert [e.name for e in svc.load_engines("surface")] == ["on"]
        dark = svc.load_engines("dark")
        assert len(dark) == 1 and dark[0].network == "dark" and dark[0].require_onion

    def test_unknown_network_returns_empty(self, tmp_path):
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, {"surface": {"engines": []}}))
        assert svc.load_engines("nope") == []

    def test_missing_config_returns_empty(self, tmp_path):
        svc = TopicDiscoveryService(engines_path=str(tmp_path / "missing.json"))
        assert svc.load_engines("surface") == []

    def test_invalid_json_returns_empty(self, tmp_path):
        bad = tmp_path / "search_engines.json"
        bad.write_text("{not json", encoding="utf-8")
        svc = TopicDiscoveryService(engines_path=str(bad))
        assert svc.load_engines("surface") == []

    def test_searxng_base_url_setting_overrides_config(self, tmp_path, monkeypatch):
        from app.common.config.settings import settings

        monkeypatch.setattr(settings, "SEARXNG_BASE_URL", "http://my-searxng:8080")
        svc = TopicDiscoveryService(
            engines_path=_write_config(
                tmp_path,
                {"surface": {"engines": [
                    {"name": "searxng", "type": "searxng_json", "enabled": True,
                     "base_url": "http://config-searxng:8080"}
                ]}},
            )
        )
        assert svc.load_engines("surface")[0].base_url == "http://my-searxng:8080"


class TestAcceptFiltering:
    def _spec(self, **overrides):
        base = {"name": "e", "type": "html_links", "network": "surface"}
        base.update(overrides)
        return EngineSpec(**base)

    def test_rejects_localhost_and_private(self):
        svc = TopicDiscoveryService()
        spec = self._spec()
        assert svc._accept("http://localhost:8000/x", spec) is False
        assert svc._accept("http://127.0.0.1/x", spec) is False
        assert svc._accept("http://10.0.0.5/x", spec) is False
        assert svc._accept("http://169.254.169.254/latest/meta-data", spec) is False

    def test_accepts_public_host(self):
        svc = TopicDiscoveryService()
        assert svc._accept(PUBLIC_A, self._spec()) is True

    def test_rejects_non_http_scheme(self):
        svc = TopicDiscoveryService()
        assert svc._accept("ftp://example.com/f", self._spec()) is False

    def test_require_onion_rejects_clearnet(self):
        svc = TopicDiscoveryService()
        spec = self._spec(network="dark", require_onion=True)
        assert svc._accept("http://93.184.216.34/x", spec) is False
        assert svc._accept("http://abc123.onion/x", spec) is True

    def test_onion_rejected_on_surface_network(self):
        svc = TopicDiscoveryService()
        assert svc._accept("http://abc123.onion/x", self._spec()) is False

    def test_link_pattern_filter(self):
        svc = TopicDiscoveryService()
        spec = self._spec(link_pattern=r"/article/")
        assert svc._accept("http://93.184.216.34/article/1", spec) is True
        assert svc._accept("http://93.184.216.34/other", spec) is False

    def test_result_domains_exclude(self):
        svc = TopicDiscoveryService()
        spec = self._spec(result_domains_exclude=("duckduckgo.com",))
        assert svc._accept("http://duckduckgo.com/l/", spec) is False


class TestRedirectUnwrap:
    def test_unwraps_ddg_uddg_param(self):
        svc = TopicDiscoveryService()
        spec = EngineSpec(name="ddg", type="html_links", network="surface",
                          unwrap_query_param="uddg")
        wrapped = "http://duckduckgo.com/l/?uddg=https%3A%2F%2F93.184.216.34%2Fa&rut=x"
        assert svc._unwrap_redirect(wrapped, spec) == "https://93.184.216.34/a"

    def test_leaves_urls_without_param_unchanged(self):
        svc = TopicDiscoveryService()
        spec = EngineSpec(name="e", type="html_links", network="surface",
                          unwrap_query_param="uddg")
        assert svc._unwrap_redirect(PUBLIC_A, spec) == PUBLIC_A

    def test_no_unwrap_configured_is_noop(self):
        svc = TopicDiscoveryService()
        spec = EngineSpec(name="e", type="html_links", network="surface")
        assert svc._unwrap_redirect(PUBLIC_A, spec) == PUBLIC_A


class TestAnchorExtraction:
    def test_extracts_and_unwraps_result_links(self):
        svc = TopicDiscoveryService()
        spec = EngineSpec(name="ddg", type="html_links", network="surface",
                          unwrap_query_param="uddg")
        html = """
        <html><body>
          <a href="//duckduckgo.com/l/?uddg=https%3A%2F%2F93.184.216.34%2Fa">A</a>
          <a href="https://93.184.216.35/b">B</a>
          <a href="#frag">frag</a>
          <a href="javascript:void(0)">js</a>
        </body></html>
        """
        urls = svc._extract_anchor_urls(html, "https://duckduckgo.com/html/?q=x", spec)
        assert "https://93.184.216.34/a" in urls
        assert "https://93.184.216.35/b" in urls
        assert all(not u.startswith("#") and not u.startswith("javascript:") for u in urls)


class TestDiscover:
    async def test_discovers_and_dedups_across_engines(self, tmp_path):
        config = {
            "surface": {"engines": [
                {"name": "e1", "type": "html_links", "enabled": True,
                 "url_template": "https://s1.test/?q={query}"},
                {"name": "e2", "type": "searxng_json", "enabled": True,
                 "base_url": "https://searx.test"},
            ]}
        }
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, config))
        e1_html = f'<a href="{PUBLIC_A}">a</a><a href="{PUBLIC_B}">b</a>'
        client = _FakeClient({
            "s1.test": _FakeResponse(text=e1_html, url="https://s1.test/?q=x"),
            "searx.test": _FakeResponse(
                url="https://searx.test/search",
                json_data={"results": [{"url": PUBLIC_A}, {"url": "http://93.184.216.36/c"}]},
            ),
        })
        svc._http_client = client  # injected; service does not own it

        seeds = await svc.discover(
            SearchRequest(job_id="JOB1", query="ethiopia", network="surface", max_results=10)
        )
        assert PUBLIC_A in seeds
        assert PUBLIC_B in seeds
        assert "http://93.184.216.36/c" in seeds
        assert len(seeds) == 3  # PUBLIC_A deduped across both engines

    async def test_respects_max_results(self, tmp_path):
        config = {"surface": {"engines": [
            {"name": "e1", "type": "searxng_json", "enabled": True, "base_url": "https://searx.test"}
        ]}}
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, config))
        svc._http_client = _FakeClient({
            "searx.test": _FakeResponse(
                json_data={"results": [{"url": f"http://93.184.216.{i}/p"} for i in range(1, 9)]}
            ),
        })
        seeds = await svc.discover(
            SearchRequest(job_id="JOB1", query="q", network="surface", max_results=3)
        )
        assert len(seeds) == 3

    async def test_no_engines_returns_empty(self, tmp_path):
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, {"surface": {"engines": []}}))
        assert await svc.discover(SearchRequest(job_id="JOB1", query="q")) == []

    async def test_engine_allowlist_filters_engines(self, tmp_path):
        config = {"surface": {"engines": [
            {"name": "e1", "type": "searxng_json", "enabled": True, "base_url": "https://s1.test"},
            {"name": "e2", "type": "searxng_json", "enabled": True, "base_url": "https://s2.test"},
        ]}}
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, config))
        svc._http_client = _FakeClient({
            "s1.test": _FakeResponse(json_data={"results": [{"url": PUBLIC_A}]}),
            "s2.test": _FakeResponse(json_data={"results": [{"url": PUBLIC_B}]}),
        })
        seeds = await svc.discover(
            SearchRequest(job_id="JOB1", query="q", engines=["e2"])
        )
        assert seeds == [PUBLIC_B]
        assert [c[0] for c in svc._http_client.calls] == ["https://s2.test/search"]

    async def test_failing_engine_does_not_abort_others(self, tmp_path):
        config = {"surface": {"engines": [
            {"name": "bad", "type": "searxng_json", "enabled": True, "base_url": "https://bad.test"},
            {"name": "good", "type": "searxng_json", "enabled": True, "base_url": "https://good.test"},
        ]}}
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, config))
        svc._http_client = _FakeClient({
            "bad.test": _FakeResponse(status=500),
            "good.test": _FakeResponse(json_data={"results": [{"url": PUBLIC_A}]}),
        })
        seeds = await svc.discover(SearchRequest(job_id="JOB1", query="q"))
        assert seeds == [PUBLIC_A]


class TestDiscoverAllNetworks:
    """``network="all"`` fans out across surface + deep + dark concurrently."""

    @staticmethod
    def _three_network_config() -> dict:
        return {
            "surface": {"engines": [
                {"name": "s1", "type": "searxng_json", "enabled": True, "base_url": "https://s1.test"}
            ]},
            "deep": {"engines": [
                {"name": "d1", "type": "searxng_json", "enabled": True, "base_url": "https://d1.test"}
            ]},
            "dark": {"engines": [
                {"name": "k1", "type": "html_links", "enabled": True,
                 "url_template": "https://ahmia.test/search/?q={query}", "require_onion": True}
            ]},
        }

    @staticmethod
    def _patch_client_for(monkeypatch) -> None:
        async def _client_for(self, network):  # noqa: ANN001
            if network == "dark":
                return _FakeClient({
                    "ahmia.test": _FakeResponse(
                        text='<a href="http://abcdefghij234567.onion/">x</a>',
                        url="https://ahmia.test/search/?q=x",
                    )
                })
            return _FakeClient({
                "s1.test": _FakeResponse(json_data={"results": [{"url": PUBLIC_A}]}),
                "d1.test": _FakeResponse(json_data={"results": [{"url": PUBLIC_B}]}),
            })

        monkeypatch.setattr(TopicDiscoveryService, "_client_for", _client_for)

    async def test_all_merges_every_network(self, tmp_path, monkeypatch):
        self._patch_client_for(monkeypatch)
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, self._three_network_config()))

        seeds = await svc.discover(
            SearchRequest(job_id="JOB1", query="ethiopia", network="all", max_results=10)
        )

        assert PUBLIC_A in seeds, "surface seeds missing"
        assert PUBLIC_B in seeds, "deep seeds missing"
        assert "http://abcdefghij234567.onion/" in seeds, "dark (.onion) seed missing"

    async def test_all_dedups_across_networks(self, tmp_path, monkeypatch):
        self._patch_client_for(monkeypatch)
        # Both surface and deep now return the same seed.
        config = self._three_network_config()
        config["deep"]["engines"][0]["base_url"] = "https://s1.test"
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, config))

        seeds = await svc.discover(
            SearchRequest(job_id="JOB1", query="ethiopia", network="all", max_results=10)
        )
        assert seeds.count(PUBLIC_A) == 1

    async def test_all_caps_merged_result_at_max_results(self, tmp_path, monkeypatch):
        self._patch_client_for(monkeypatch)
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, self._three_network_config()))

        # The per-network quota is split before merging, so asking for 2 cannot
        # yield 6 seeds.
        seeds = await svc.discover(
            SearchRequest(job_id="JOB1", query="ethiopia", network="all", max_results=2)
        )
        assert len(seeds) <= 2

    async def test_one_dead_network_does_not_sink_the_query(self, tmp_path, monkeypatch):
        async def _client_for(self, network):  # noqa: ANN001
            if network == "dark":
                raise RuntimeError("no Tor available")
            return _FakeClient({
                "s1.test": _FakeResponse(json_data={"results": [{"url": PUBLIC_A}]}),
                "d1.test": _FakeResponse(json_data={"results": [{"url": PUBLIC_B}]}),
            })

        monkeypatch.setattr(TopicDiscoveryService, "_client_for", _client_for)
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, self._three_network_config()))

        seeds = await svc.discover(
            SearchRequest(job_id="JOB1", query="ethiopia", network="all", max_results=10)
        )
        assert PUBLIC_A in seeds
        assert PUBLIC_B in seeds


class TestSubmitDiscoveryJobNetworks:
    """The API layer must accept 'all' - it is rejected as an unknown network otherwise."""

    def test_all_is_a_valid_discovery_network(self):
        from app.pipeline.main import DISCOVERY_NETWORKS

        assert "all" in DISCOVERY_NETWORKS
        assert {"surface", "deep", "dark"} <= set(DISCOVERY_NETWORKS)

    def test_discover_request_accepts_all(self):
        from app.api.routes.jobs import DiscoverRequest

        req = DiscoverRequest(query="ethiopian news", network="all")
        assert req.network == "all"


class TestBuildSeedCrawlRequest:
    def test_maps_search_request_to_crawl_request(self):
        search = SearchRequest(
            job_id="JOB00000001",
            query="ethiopian telecom",
            network="surface",
            language="am",
            max_depth=3,
            recursive_config={"enable_extraction": True, "skip_domains": ["ads.com"]},
        )
        crawl = build_seed_crawl_request(search, "https://93.184.216.34/a")
        assert isinstance(crawl, CrawlRequest)
        assert crawl.job_id == "JOB00000001"
        assert crawl.url == "https://93.184.216.34/a"
        assert crawl.language == "am"
        assert crawl.depth == 0
        assert crawl.max_depth == 3
        # Each seed is its own crawl root, so it must be scoped to its own domain.
        assert crawl.recursive_config["seed_url"] == "https://93.184.216.34/a"
        assert crawl.recursive_config["same_domain_only"] is True
        assert crawl.recursive_config["skip_domains"] == ["ads.com"]

    def test_routes_onion_seed_to_dark_worker(self):
        search = SearchRequest(job_id="JOB1", query="market", network="dark")
        crawl = build_seed_crawl_request(search, "http://abc123.onion/x")
        assert crawl.worker_type == "dark"

    def test_does_not_mutate_parent_recursive_config(self):
        search = SearchRequest(job_id="JOB1", query="q", recursive_config={})
        build_seed_crawl_request(search, PUBLIC_A)
        assert search.recursive_config == {}


class TestFanOut:
    async def test_registers_tasks_and_publishes_crawl_requests(self, tmp_path, monkeypatch):
        from app.storage.postgres.client import pg_client

        config = {"surface": {"engines": [
            {"name": "e1", "type": "searxng_json", "enabled": True, "base_url": "https://searx.test"}
        ]}}
        svc = TopicDiscoveryService(engines_path=_write_config(tmp_path, config))
        svc._http_client = _FakeClient({
            "searx.test": _FakeResponse(json_data={"results": [
                {"url": PUBLIC_A}, {"url": PUBLIC_B}
            ]}),
        })
        register = AsyncMock()
        complete = AsyncMock()
        monkeypatch.setattr(pg_client, "register_job_tasks", register)
        monkeypatch.setattr(pg_client, "complete_job_task", complete)

        producer = _FakeProducer()
        result = await svc.fan_out(
            producer,
            SearchRequest(job_id="JOB1", query="q", network="surface"),
            "crawl.requests",
        )

        assert result == {"seeds": 2, "queued": 2}
        register.assert_awaited_once_with("JOB1", 2)
        assert len(producer.sent) == 2
        for topic, value, key in producer.sent:
            assert topic == "crawl.requests"
            assert key == b"JOB1"
            parsed = json.loads(value)
            assert parsed["job_id"] == "JOB1"
            assert parsed["url"] in (PUBLIC_A, PUBLIC_B)

    async def test_no_seeds_publishes_nothing(self, tmp_path, monkeypatch):
        from app.storage.postgres.client import pg_client

        svc = TopicDiscoveryService(
            engines_path=_write_config(tmp_path, {"surface": {"engines": []}})
        )
        monkeypatch.setattr(pg_client, "register_job_tasks", AsyncMock())
        monkeypatch.setattr(pg_client, "complete_job_task", AsyncMock())
        producer = _FakeProducer()
        result = await svc.fan_out(
            producer, SearchRequest(job_id="JOB1", query="q"), "crawl.requests"
        )
        assert result == {"seeds": 0, "queued": 0}
        assert producer.sent == []


class TestConfigFileSchema:
    """The shipped config must be valid and cover all three networks."""

    def test_repo_config_loads_for_every_network(self):
        from app.common.config.settings import settings

        assert os.path.exists(settings.SEARCH_ENGINES_CONFIG_PATH)
        svc = TopicDiscoveryService()
        for network in ("surface", "deep", "dark"):
            specs = svc.load_engines(network)
            assert specs, f"no enabled engines for network={network}"
            for spec in specs:
                assert spec.type in ("searxng_json", "html_links")
