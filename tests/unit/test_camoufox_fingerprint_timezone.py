"""Unit tests for the Camoufox launch kwargs that keep a fingerprint coherent.

Regression guard for the challenge-interstitial hang: Camoufox's BrowserForge
fingerprint carries no timezone, so an unset one silently became the container's
UTC while the profile claimed Windows + en-US. Cloudflare answers that pairing
with an interstitial that never resolves - the engine starts, Turnstile loads, no
widget is ever rendered, and no cf_clearance is issued. Measured against
www.scrapingcourse.com/cloudflare-challenge: 0/3 passes with the implicit UTC
default, 3/3 once every profile carried a timezone.

These tests load the worker with its heavy browser dependencies stubbed, the same
way tests/unit/test_cloudflare_challenge_detection.py does.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "../..")
sys.path.insert(0, _PROJECT_ROOT)

_MOCK_MODULES = [
    "aiokafka", "aiokafka.consumer", "aiokafka.consumer.group_coordinator",
    "aiokafka.consumer.subscription_state", "aiokafka.producer", "aiokafka.producer.producer",
    "minio",
    "patchright", "patchright.async_api",
    "camoufox", "camoufox.async_api",
    "app.common.config.settings",
    "app.common.config.wsl_settings",
    "app.common.logger.logger",
    "app.language.cleaning.cleaner",
    "app.language.language_detection.detector",
    "app.language.quality.scorer",
    "app.pipeline.schemas",
    "app.services.link_extraction_service",
    "app.common.utils.minio_naming",
    "app.services.portal_handler",
    "app.services.auto_signup_handler",
    "app.services.credential_service",
    "app.storage.postgres.client",
    "workers.health",
    "workers.metrics",
    "browserforge", "browserforge.fingerprints",
]

_MODULES_BEFORE_MOCKS = dict(sys.modules)
_ALWAYS_SHADOW = ("app.common.config.settings", "app.common.config.wsl_settings")
for _mod_name in _MOCK_MODULES:
    if _mod_name in _ALWAYS_SHADOW or _mod_name not in sys.modules:
        sys.modules[_mod_name] = MagicMock()


class _FakeScreen:
    def __init__(self, max_width: int = 1920, max_height: int = 1080):
        self.max_width = max_width
        self.max_height = max_height


sys.modules["browserforge.fingerprints"].Screen = _FakeScreen

_settings_mock = sys.modules["app.common.config.settings"]
_settings_mock.settings = MagicMock()
_settings_mock.settings.DEEP_BROWSER_GEOIP = False
_settings_mock.settings.DEEP_BROWSER_TIMEZONE = ""
_settings_mock.settings.browser_timezone.return_value = "America/New_York"
# Values the worker reads at import time; MagicMock would break the arithmetic
# and list/dict iterations that run while the module is being executed.
_settings_mock.settings.DEEP_MAX_CONCURRENT_JOBS = 5
_settings_mock.settings.DEEP_CHROMIUM_ARGS = []
_settings_mock.settings.DEEP_DEFAULT_USER_AGENT = "Mozilla/5.0"
_settings_mock.settings.DEEP_BROWSER_CONTEXT_KWARGS = {"viewport": {"width": 1920, "height": 1080}}
_settings_mock.settings.DEEP_STEALTH_INIT_SCRIPT = ""
_settings_mock.settings.deep_timeout_seconds = 45
_settings_mock.settings.DEEP_PAGE_VALIDATORS = []
_settings_mock.settings.DEEP_RESPONSE_VALIDATORS = []
_settings_mock.settings.KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
_settings_mock.settings.crawl_request_topic = "crawl.requests"
_settings_mock.settings.crawl_parsed_topic = "crawl.parsed"
_settings_mock.settings.MINIO_ENDPOINT = "localhost:9000"
_settings_mock.settings.MINIO_RAW_BUCKET = "raw"
_settings_mock.settings.MINIO_PARSED_BUCKET = "parsed"
_settings_mock.settings.MINIO_ROOT_USER = "minioadmin"
_settings_mock.settings.MINIO_ROOT_PASSWORD = "minioadmin"
_settings_mock.settings.MINIO_SECURE = False

_ph_before = sys.modules.get("app.services.portal_handler")
ph = sys.modules["app.services.portal_handler"] = MagicMock()
ph.PortalHandler = MagicMock
ph.PortalConfig = MagicMock
ph.find_config_for_url = MagicMock(return_value=None)
ph.handle_http_interstitial = MagicMock(return_value=None)
ph.generic_portal_login = MagicMock(return_value=None)

_spec = importlib.util.spec_from_file_location(
    "workers.deep_worker_camoufox_tz",
    os.path.join(_PROJECT_ROOT, "workers", "deep-worker", "main.py"),
    submodule_search_locations=[],
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["workers.deep_worker_camoufox_tz"] = _mod
_spec.loader.exec_module(_mod)

for _mod_name in list(sys.modules):
    if _mod_name in _MODULES_BEFORE_MOCKS:
        if sys.modules[_mod_name] is not _MODULES_BEFORE_MOCKS[_mod_name]:
            sys.modules[_mod_name] = _MODULES_BEFORE_MOCKS[_mod_name]
    else:
        sys.modules.pop(_mod_name, None)

if "app.services.portal_handler" in sys.modules and sys.modules[
    "app.services.portal_handler"
] is not _ph_before:
    if _ph_before is None:
        sys.modules.pop("app.services.portal_handler", None)
    else:
        sys.modules["app.services.portal_handler"] = _ph_before


@pytest.fixture
def settings():
    """The stubbed settings object the worker module captured."""
    return _settings_mock.settings


@pytest.fixture(autouse=True)
def _default_settings(settings):
    """Reset the two relevant settings before each test."""
    settings.DEEP_BROWSER_TIMEZONE = ""
    settings.DEEP_BROWSER_GEOIP = False
    settings.browser_timezone.return_value = "America/New_York"
    _mod._geoip_probe_result = False
    yield
    _mod._geoip_probe_result = None


class TestFingerprintProfiles:
    def test_every_profile_declares_a_timezone(self):
        """The whole bug was a profile that declared none."""
        for profile in _mod.CAMOUFOX_FINGERPRINT_PROFILES:
            assert profile.get("timezone"), f"profile missing timezone: {profile}"

    def test_no_profile_leaves_the_clock_at_utc(self):
        for profile in _mod.CAMOUFOX_FINGERPRINT_PROFILES:
            assert profile["timezone"] != "UTC"

    def test_timezones_are_iana_names(self):
        for profile in _mod.CAMOUFOX_FINGERPRINT_PROFILES:
            tz = str(profile["timezone"])
            assert "/" in tz, f"{tz} is not an IANA region/city name"

    def test_locale_and_timezone_are_coherent(self):
        """A locale's language/region must not sit on an unrelated continent."""
        expected = {
            "en-US": "America/",
            "en-GB": "Europe/",
            "de-DE": "Europe/",
            "fr-FR": "Europe/",
            "es-ES": "Europe/",
            "ja-JP": "Asia/",
            "pt-BR": "America/",
        }
        for profile in _mod.CAMOUFOX_FINGERPRINT_PROFILES:
            locale = str(profile["locale"])
            assert locale in expected, f"unmapped locale {locale}"
            assert str(profile["timezone"]).startswith(expected[locale]), (
                f"{locale} paired with {profile['timezone']}"
            )

    def test_profiles_still_rotate(self):
        first = _mod._get_camoufox_profile(0)
        second = _mod._get_camoufox_profile(1)
        assert first is not second
        # Wrap-around must return to the start rather than drift off the end.
        assert _mod._get_camoufox_profile(len(_mod.CAMOUFOX_FINGERPRINT_PROFILES)) is first


class TestLaunchKwargsWithoutGeoip:
    def test_applies_the_profile_timezone_explicitly(self, settings):
        profile = _mod._get_camoufox_profile(0)
        kwargs = _mod._camoufox_launch_kwargs(profile, None)
        assert kwargs["config"]["timezone"] == profile["timezone"]
        assert kwargs["config"]["timezone"] != "UTC"

    def test_applies_the_profile_locale(self):
        profile = _mod._get_camoufox_profile(1)
        kwargs = _mod._camoufox_launch_kwargs(profile, None)
        assert kwargs["locale"] == profile["locale"]

    def test_never_omits_the_timezone(self, settings):
        """Whatever the inputs, the launch must not fall back to the system zone."""
        for i in range(len(_mod.CAMOUFOX_FINGERPRINT_PROFILES)):
            kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(i), None)
            assert kwargs["config"]["timezone"]

    def test_operator_timezone_wins_over_the_profile(self, settings):
        settings.DEEP_BROWSER_TIMEZONE = "Asia/Tokyo"
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None)
        assert kwargs["config"]["timezone"] == "Asia/Tokyo"

    def test_falls_back_to_the_resolved_setting_when_a_profile_lacks_one(self, settings):
        settings.browser_timezone.return_value = "Europe/Berlin"
        kwargs = _mod._camoufox_launch_kwargs({"os": "windows", "screen": (1920, 1080),
                                               "window": (1920, 1040), "locale": "en-US"}, None)
        assert kwargs["config"]["timezone"] == "Europe/Berlin"

    def test_no_geoip_flag_when_disabled(self, settings):
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None)
        assert "geoip" not in kwargs

    def test_geometry_and_webgl_reach_the_browser(self):
        profile = _mod._get_camoufox_profile(0)
        kwargs = _mod._camoufox_launch_kwargs(profile, ("Intel", "Intel(R) UHD Graphics 630"))
        assert (kwargs["screen"].max_width, kwargs["screen"].max_height) == profile["screen"]
        assert kwargs["window"] == profile["window"]
        assert kwargs["webgl_config"] == ("Intel", "Intel(R) UHD Graphics 630")
        assert kwargs["os"] == profile["os"]

    def test_stays_headful(self):
        """Xvfb only helps if the browser is not headless."""
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None)
        assert kwargs["headless"] is False

    def test_omits_webgl_config_when_sampling_failed(self):
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None)
        assert "webgl_config" not in kwargs


class TestLaunchKwargsWithGeoip:
    def test_geoip_is_used_when_enabled_and_available(self, settings, monkeypatch):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: True)
        settings.DEEP_BROWSER_GEOIP = True
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None)
        assert kwargs["geoip"] is True

    def test_geoip_does_not_also_pin_a_conflicting_locale(self, settings, monkeypatch):
        """Two sources for locale/timezone would disagree; only one may be set."""
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: True)
        settings.DEEP_BROWSER_GEOIP = True
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None)
        assert "locale" not in kwargs
        assert "config" not in kwargs

    def test_falls_back_to_the_profile_when_geoip_is_unusable(self, settings, monkeypatch):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: False)
        settings.DEEP_BROWSER_GEOIP = True
        profile = _mod._get_camoufox_profile(0)
        kwargs = _mod._camoufox_launch_kwargs(profile, None)
        assert "geoip" not in kwargs
        assert kwargs["config"]["timezone"] == profile["timezone"]

    def test_availability_is_cached_after_one_probe(self, monkeypatch):
        """The probe does network I/O, so it must not run per attempt."""
        calls = []

        def _fake_probe():
            calls.append(1)
            return True

        monkeypatch.setattr(_mod, "camoufox_geoip_usable", _fake_probe)
        settings_geoip = True
        assert _mod.camoufox_geoip_usable() is True
        assert len(calls) == 1
        assert settings_geoip  # sanity: the helper is what the builder consults


class TestLaunchKwargsAutoGeoip:
    """`DEEP_BROWSER_GEOIP="auto"` keeps the first attempt offline and only
    escalates to the IP-derived zone once a challenge has persisted."""

    def test_first_attempt_stays_deterministic_and_offline(self, settings, monkeypatch):
        probe_calls = []
        monkeypatch.setattr(
            _mod, "camoufox_geoip_usable",
            lambda: (probe_calls.append(1) or True),
        )
        settings.DEEP_BROWSER_GEOIP = "auto"
        profile = _mod._get_camoufox_profile(0)
        kwargs = _mod._camoufox_launch_kwargs(profile, None, attempt=0)
        assert "geoip" not in kwargs
        assert kwargs["config"]["timezone"] == profile["timezone"]
        # The decisive part: no probe, so no network in the common case.
        assert probe_calls == []

    def test_later_attempt_escalates_to_geoip(self, settings, monkeypatch):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: True)
        settings.DEEP_BROWSER_GEOIP = "auto"
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(1), None, attempt=1)
        assert kwargs["geoip"] is True

    def test_auto_falls_back_when_geoip_is_unusable(self, settings, monkeypatch):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: False)
        settings.DEEP_BROWSER_GEOIP = "auto"
        profile = _mod._get_camoufox_profile(1)
        kwargs = _mod._camoufox_launch_kwargs(profile, None, attempt=1)
        assert "geoip" not in kwargs
        assert kwargs["config"]["timezone"] == profile["timezone"]

    def test_off_never_uses_geoip_even_on_later_attempts(self, settings, monkeypatch):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: True)
        settings.DEEP_BROWSER_GEOIP = "off"
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(2), None, attempt=2)
        assert "geoip" not in kwargs

    def test_on_uses_geoip_on_the_first_attempt(self, settings, monkeypatch):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: True)
        settings.DEEP_BROWSER_GEOIP = "on"
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None, attempt=0)
        assert kwargs["geoip"] is True

    @pytest.mark.parametrize("value", ["true", "1", "yes", "TRUE"])
    def test_boolean_spellings_are_read_as_on(self, settings, monkeypatch, value):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: True)
        settings.DEEP_BROWSER_GEOIP = value
        kwargs = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None, attempt=0)
        assert kwargs["geoip"] is True

    def test_unknown_value_behaves_as_auto(self, settings, monkeypatch):
        monkeypatch.setattr(_mod, "camoufox_geoip_usable", lambda: True)
        settings.DEEP_BROWSER_GEOIP = "nonsense"
        first = _mod._camoufox_launch_kwargs(_mod._get_camoufox_profile(0), None, attempt=0)
        assert "geoip" not in first


class TestGeoipProbe:
    def test_probe_failure_is_reported_not_raised(self, monkeypatch):
        """A missing extra must degrade to the profile timezone, not crash a job."""
        _mod._geoip_probe_result = None

        import builtins
        real_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name.startswith("camoufox.geolocation"):
                raise ImportError("no geoip extra")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fake_import)
        assert _mod.camoufox_geoip_usable() is False
        # And the failure is remembered, so it is attempted once per process.
        monkeypatch.undo()
        assert _mod.camoufox_geoip_usable() is False
        _mod._geoip_probe_result = None
