"""Alerts API: paging, badge counts, read state and owner scoping.

Scoping is the part worth asserting. Alerts hang off ``job_id`` like every other
piece of pipeline output, so a non-admin must see only alerts raised by jobs
they own — including for the *write* endpoints, where a missing scope check
would let any authenticated user clear (or, worse, silently acknowledge) an
alert they cannot read.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.security.scope import visible_job_ids  # noqa: E402


ADMIN = {"user_id": "USRADMIN", "role": "admin"}
MEMBER = {"user_id": "USRMEM", "role": "user"}


def _alert_row(alert_id: str, job_id: str, severity: int, read: bool = False) -> dict:
    return {
        "alert_id": alert_id,
        "job_id": job_id,
        "item_id": f"ITEM{alert_id[-3:]}",
        "url": f"https://example.com/{alert_id}",
        "title": f"Alert {alert_id}",
        "category": "cyber_threat",
        "severity": severity,
        "language": "am",
        "summary": "A serious development was reported.",
        "entities": ["ENDFEU"],
        "analysis_source": "llm",
        "llm_model": "openai/gpt-oss-120b",
        "created_at": None,
        "is_read": read,
    }


class TestOwnerScope:
    # ``app.security.scope`` imports ``pg_client`` by name at module load, so
    # the patch has to target *that* binding, not the one in the storage module.
    @staticmethod
    def _patch(monkeypatch, rows):
        from app.security import scope as scope_module

        pg = SimpleNamespace(get_job_ids_for_user=AsyncMock(return_value=rows))
        monkeypatch.setattr(scope_module, "pg_client", pg)

    async def test_admin_is_unrestricted(self, monkeypatch):
        self._patch(monkeypatch, [])
        assert await visible_job_ids(ADMIN) is None

    async def test_member_gets_their_jobs(self, monkeypatch):
        self._patch(monkeypatch, [{"job_id": "JOB1"}])
        assert await visible_job_ids(MEMBER) == {"JOB1"}

    async def test_member_with_no_jobs_gets_an_empty_set_not_none(self, monkeypatch):
        """None means "no filter"; an empty set must stay concrete, otherwise a
        brand-new account reads the entire system."""
        self._patch(monkeypatch, [])
        scope = await visible_job_ids(MEMBER)
        assert scope == set()
        assert scope is not None


class TestAlertScopeHelper:
    """The SQL builder that turns a scope into a clause."""

    def test_admin_gets_no_clause(self):
        from app.storage.postgres.client import PostgreSQLClient

        clause, params = PostgreSQLClient._alert_scope(None, start_index=4)
        assert clause == ""
        assert params == ()

    def test_member_gets_a_bound_clause(self):
        from app.storage.postgres.client import PostgreSQLClient

        clause, params = PostgreSQLClient._alert_scope({"JOB2", "JOB1"}, start_index=4)
        assert "$4" in clause
        assert params == (["JOB1", "JOB2"],)

    def test_empty_set_still_produces_a_real_clause(self):
        from app.storage.postgres.client import PostgreSQLClient

        clause, params = PostgreSQLClient._alert_scope(set(), start_index=4)
        # An empty `= ANY('{}')` matches nothing, which is the safe outcome.
        assert clause != ""
        assert params == ([],)


class TestFeedPayload:
    def test_rows_are_serialised_with_priority_and_summary(self):
        from app.services.alert_service import Alert

        alert = Alert(
            alert_id="A1", job_id="J1", item_id="I1", url="u", title="t",
            category="cyber_threat", severity=5, language="am", summary="s",
            entities=[], analysis_source="llm", llm_model="m", created_at=None,
        )
        payload = alert.to_dict()
        assert payload["alert_id"] == "A1"
        assert payload["severity"] == 5
        assert payload["read"] is False

    def test_heuristic_source_survives_to_the_client(self):
        """The UI needs analysis_source to warn that a label is not a verdict."""
        from app.services.alert_service import _row_to_alert

        row = _alert_row("A1", "J1", 4)
        row["analysis_source"] = "fallback"
        assert _row_to_alert(row).analysis_source == "fallback"