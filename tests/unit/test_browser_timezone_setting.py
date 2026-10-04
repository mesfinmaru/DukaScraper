"""Unit tests for the browser timezone the stealth browsers present.

The value matters because an *absent* timezone is not neutral: the browser then
reports the container's UTC clock beside a Windows/en-US user agent, which is a
combination no real machine has, and Cloudflare's managed challenge never
completes against it.

These use the real Settings class - the worker test stubs the module, so this is
the only place the resolution order itself is asserted.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from app.common.config.settings import Settings  # noqa: E402

_UTC_ALIASES = {"UTC", "GMT", "ETC/UTC", "UCT"}


class TestBrowserTimezone:
    def test_explicit_configuration_wins(self, monkeypatch):
        monkeypatch.setenv("DEEP_BROWSER_TIMEZONE", "Asia/Tokyo")
        monkeypatch.setenv("TZ", "Europe/Berlin")
        assert Settings().browser_timezone() == "Asia/Tokyo"

    def test_falls_back_to_the_process_timezone(self, monkeypatch):
        monkeypatch.delenv("DEEP_BROWSER_TIMEZONE", raising=False)
        monkeypatch.setenv("TZ", "Europe/Berlin")
        assert Settings().browser_timezone() == "Europe/Berlin"

    @pytest.mark.parametrize("utc_alias", sorted(_UTC_ALIASES))
    def test_a_utc_process_timezone_is_not_accepted(self, monkeypatch, utc_alias):
        """UTC is exactly the value that breaks the fingerprint, so it is skipped."""
        monkeypatch.delenv("DEEP_BROWSER_TIMEZONE", raising=False)
        monkeypatch.setenv("TZ", utc_alias)
        assert Settings().browser_timezone() not in _UTC_ALIASES

    def test_never_returns_empty(self, monkeypatch):
        """An empty string would restore the silent-UTC failure mode."""
        monkeypatch.delenv("DEEP_BROWSER_TIMEZONE", raising=False)
        monkeypatch.delenv("TZ", raising=False)
        assert Settings().browser_timezone()

    def test_fallback_is_coherent_with_the_default_locale(self, monkeypatch):
        monkeypatch.delenv("DEEP_BROWSER_TIMEZONE", raising=False)
        monkeypatch.delenv("TZ", raising=False)
        settings = Settings()
        assert settings.browser_timezone() == settings.DEEP_BROWSER_TIMEZONE_FALLBACK
        # The browsers default to an en-US locale, so the fallback must be a US zone.
        assert settings.browser_timezone().startswith("America/")

    def test_whitespace_process_timezone_is_ignored(self, monkeypatch):
        monkeypatch.delenv("DEEP_BROWSER_TIMEZONE", raising=False)
        monkeypatch.setenv("TZ", "   ")
        assert Settings().browser_timezone()


class TestChromiumContextWiring:
    def test_context_timezone_follows_the_resolved_value(self, monkeypatch):
        """Chromium reads timezone_id from the context, not from a profile."""
        monkeypatch.setenv("DEEP_BROWSER_TIMEZONE", "Asia/Tokyo")
        assert Settings().DEEP_BROWSER_CONTEXT_KWARGS["timezone_id"] == "Asia/Tokyo"

    def test_context_locale_and_timezone_are_a_plausible_pair(self):
        settings = Settings()
        locale = str(settings.DEEP_BROWSER_CONTEXT_KWARGS["locale"])
        timezone = str(settings.DEEP_BROWSER_CONTEXT_KWARGS["timezone_id"])
        assert locale.startswith("en-US")
        assert timezone.startswith("America/")

    def test_geoip_defaults_to_auto(self):
        """Adaptive is the default: offline deterministic pairing first, geoip
        only after a challenge persists. Never always-on, never fully off."""
        assert Settings().browser_geoip_mode() == "auto"

    @pytest.mark.parametrize(
        ("configured", "expected"),
        [
            ("auto", "auto"),
            ("", "auto"),
            ("AUTO", "auto"),
            ("bogus", "auto"),
            ("on", "on"),
            ("true", "on"),
            ("1", "on"),
            ("yes", "on"),
            ("off", "off"),
            ("false", "off"),
            ("0", "off"),
            ("no", "off"),
            (True, "on"),
            (False, "off"),
        ],
    )
    def test_geoip_mode_normalizes_its_input(self, monkeypatch, configured, expected):
        """Env vars are strings, so the boolean spellings and legacy True/False
        must all resolve; anything unrecognized is the safe 'auto'."""
        settings = Settings()
        settings.DEEP_BROWSER_GEOIP = configured
        assert settings.browser_geoip_mode() == expected

    def test_geoip_mode_reads_a_string_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("DEEP_BROWSER_GEOIP", "off")
        assert Settings().browser_geoip_mode() == "off"
