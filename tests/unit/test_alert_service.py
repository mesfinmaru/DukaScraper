"""Alert service: severity gating, read state, and email formatting.

The severity gate is the whole product decision here — everything below 4 is
deliberately not an alert — so it gets the most attention, along with the two
places where a wrong value would mislead an operator: the summary (blank when
the model produced none) and the heuristic-provenance line in the email.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.services import alert_service as svc  # noqa: E402
from app.services.alert_service import Alert  # noqa: E402


def _alert(**overrides) -> Alert:
    base = dict(
        alert_id="ALR00000000001001",
        job_id="JOB1791087313758",
        item_id="ITEM1",
        url="https://example.com/a",
        title="Attack reported",
        category="physical_threat",
        severity=5,
        language="am",
        summary="Clashes were reported in Addis Ababa.",
        entities=["TPLF"],
        analysis_source="llm",
        llm_model="openai/gpt-oss-120b",
        created_at=None,
    )
    base.update(overrides)
    return Alert(**base)


class TestSeverityGate:
    """Only 4 and 5 alert. This is the deliberate noise-floor decision."""

    @pytest.mark.parametrize("severity", [4, 5])
    def test_high_severity_qualifies(self, severity):
        assert svc.qualifies_as_alert(severity) is True

    @pytest.mark.parametrize("severity", [0, 1, 2, 3])
    def test_low_severity_does_not_qualify(self, severity):
        assert svc.qualifies_as_alert(severity) is False

    def test_out_of_range_and_garbage_are_rejected(self):
        # 6 is outside the 1-5 contract the ClickHouse column enforces; a model
        # inventing a 6 must not produce an alert the DB would then reject.
        assert svc.qualifies_as_alert(6) is False
        assert svc.qualifies_as_alert(-1) is False
        assert svc.qualifies_as_alert("high") is False
        assert svc.qualifies_as_alert(None) is False

    def test_threshold_is_configurable_but_still_bounded(self):
        assert svc.qualifies_as_alert(3, minimum=3) is True
        assert svc.qualifies_as_alert(6, minimum=3) is False


class TestAlertPriority:
    def test_labels(self):
        assert svc.alert_priority(5) == "critical"
        assert svc.alert_priority(4) == "high"
        assert svc.alert_priority(3) == "low"
        assert svc.alert_priority("nonsense") == "low"


class TestSummarize:
    def test_uses_the_summary(self):
        assert svc.summarize_alert(_alert()) == "Clashes were reported in Addis Ababa."

    def test_falls_back_to_the_url_when_the_model_wrote_nothing(self):
        # A blank row is not actionable; showing the source link is strictly
        # better and still honest about what is known.
        assert svc.summarize_alert(_alert(summary="")) == "https://example.com/a"

    def test_collapses_whitespace_and_truncates(self):
        out = svc.summarize_alert(_alert(summary="word   \n\n  " * 200), max_chars=50)
        assert len(out) <= 50
        assert out.endswith("…")
        assert "  " not in out

    def test_amharic_summary_is_not_mangled(self):
        text = "በአዲስ አበባ ከተማሪ የተከሰተ አደጋ ተወጭቷል።"
        assert svc.summarize_alert(_alert(summary=text)) == text


class TestEmail:
    def test_subject_carries_priority_and_category(self):
        subject, _ = svc.build_alert_email(_alert())
        assert "CRITICAL" in subject
        assert "physical threat" in subject  # underscores humanised

    def test_body_is_short_and_contains_the_essentials(self):
        _, body = svc.build_alert_email(_alert())
        # "Short" is the point: the email nudges, the page carries the detail.
        assert len(body.splitlines()) < 15
        assert "5/5" in body
        assert "JOB1791087313758" in body
        assert "TPLF" in body

    def test_deep_link_uses_the_hash_router(self):
        _, body = svc.build_alert_email(_alert(), "http://localhost:5173")
        # The SPA uses HashRouter, so a link without the hash lands on the root
        # and silently drops the alert id.
        assert "#/alerts?alert=ALR00000000001001" in body

    def test_heuristic_provenance_is_disclosed(self):
        # A heuristic label must never read like a model verdict.
        _, body = svc.build_alert_email(_alert(analysis_source="fallback"))
        assert "heuristic" in body
        _, llm_body = svc.build_alert_email(_alert(analysis_source="llm"))
        assert "heuristic" not in llm_body

    def test_entity_overflow_is_summarised(self):
        alert = _alert(entities=[f"E{i}" for i in range(20)])
        _, body = svc.build_alert_email(alert)
        assert "+12 more" in body


class TestRecordAlert:
    """The gate has to hold at the write boundary, not just in the helper."""

    async def test_low_severity_never_reaches_the_database(self, monkeypatch):
        pg = SimpleNamespace(upsert_alert=AsyncMock())
        monkeypatch.setattr(
            "app.storage.postgres.client.pg_client", pg, raising=True
        )
        assert await svc.record_alert(job_id="J", item_id="I", url="u", severity=3) is None
        pg.upsert_alert.assert_not_called()

    async def test_high_severity_is_persisted(self, monkeypatch):
        pg = SimpleNamespace(upsert_alert=AsyncMock(return_value="ALR1"))
        monkeypatch.setattr("app.storage.postgres.client.pg_client", pg, raising=True)
        got = await svc.record_alert(
            job_id="J", item_id="I", url="u", severity=5, entities=["A"]
        )
        assert got == "ALR1"
        pg.upsert_alert.assert_awaited_once()


class TestNotifyAlertEmail:
    async def test_disabled_switch_is_a_no_op(self, monkeypatch):
        from app.common.config.settings import settings

        monkeypatch.setattr(settings, "ALERT_EMAILS_ENABLED", False)
        sent: list[str] = []
        monkeypatch.setattr(svc, "send_alert_email", AsyncMock(side_effect=sent.append))
        assert await svc.notify_alert_email("JOB1", "ALR1") is False
        assert sent == []

    async def test_resolves_the_job_owner_and_sends(self, monkeypatch):
        from app.common.config.settings import settings

        monkeypatch.setattr(settings, "ALERT_EMAILS_ENABLED", True)
        pg = SimpleNamespace(
            get_alert=AsyncMock(return_value={"alert_id": "ALR1", "job_id": "JOB1",
                                              "item_id": "I", "url": "u", "title": "t",
                                              "category": "cyber_threat", "severity": 5,
                                              "language": "am", "summary": "s",
                                              "entities": [], "analysis_source": "llm",
                                              "llm_model": "m", "created_at": None}),
            get_job=AsyncMock(return_value={"user_id": "USR1"}),
            get_user=AsyncMock(return_value={"email": "ops@example.com"}),
        )
        monkeypatch.setattr("app.storage.postgres.client.pg_client", pg, raising=True)
        sent: dict = {}
        monkeypatch.setattr(
            svc, "send_alert_email",
            AsyncMock(side_effect=lambda e, s, b: sent.update(email=e, subject=s, body=b)),
        )
        assert await svc.notify_alert_email("JOB1", "ALR1") is True
        assert sent["email"] == "ops@example.com"
        assert "CRITICAL" in sent["subject"]

    async def test_missing_alert_row_is_skipped_not_raised(self, monkeypatch):
        from app.common.config.settings import settings

        monkeypatch.setattr(settings, "ALERT_EMAILS_ENABLED", True)
        pg = SimpleNamespace(get_alert=AsyncMock(return_value=None))
        monkeypatch.setattr("app.storage.postgres.client.pg_client", pg, raising=True)
        assert await svc.notify_alert_email("JOB1", "ALR1") is False

    async def test_crossed_job_ids_are_refused(self, monkeypatch):
        from app.common.config.settings import settings

        monkeypatch.setattr(settings, "ALERT_EMAILS_ENABLED", True)
        pg = SimpleNamespace(
            get_alert=AsyncMock(return_value={"alert_id": "ALR1", "job_id": "OTHER_JOB",
                                              "item_id": "I", "url": "u", "title": "t",
                                              "category": "c", "severity": 5, "language": "",
                                              "summary": "", "entities": [],
                                              "analysis_source": "llm", "llm_model": "",
                                              "created_at": None}),
        )
        monkeypatch.setattr("app.storage.postgres.client.pg_client", pg, raising=True)
        sent = AsyncMock()
        monkeypatch.setattr(svc, "send_alert_email", sent)
        assert await svc.notify_alert_email("JOB1", "ALR1") is False
        sent.assert_not_called()


class TestRowConversion:
    def test_json_entities_string_is_parsed(self):
        alert = svc._row_to_alert({
            "alert_id": "A", "job_id": "J", "item_id": "I", "url": "u",
            "title": "", "category": "", "severity": 4, "language": "am",
            "summary": "", "entities": '["X", "Y"]', "analysis_source": "llm",
            "llm_model": "", "created_at": None, "is_read": True,
        })
        assert alert.entities == ["X", "Y"]
        assert alert.read is True

    def test_unparseable_entities_degrade_to_empty(self):
        alert = svc._row_to_alert({
            "alert_id": "A", "job_id": "J", "item_id": "I", "url": "u",
            "title": "", "category": "", "severity": 4, "language": "",
            "summary": "", "entities": "not json", "analysis_source": "llm",
            "llm_model": "", "created_at": None, "is_read": False,
        })
        assert alert.entities == []