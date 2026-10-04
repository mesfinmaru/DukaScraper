"""High-severity alerting: materialise severity 4/5 findings and track reads.

The intelligence rows live in ClickHouse, which has no notion of "this user has
already read this". Alerting needs three things ClickHouse cannot give us:

* a durable per-item record so an alert is raised **once** even though the
  llm-worker can re-analyse an item (Kafka redelivery, a queue retry, a second
  pass after a fallback label);
* per-user read state, so two operators of the same job each get their own
  unread badge;
* an ordered, filterable feed that does not scan ClickHouse on every poll.

So this service mirrors qualifying intelligence into ``duka_system.alerts`` and
keeps ``duka_system.alert_reads`` for the read markers.

Why severity 4 and 5 only: at 3 and below the feed is dominated by routine
conflict/news reporting and nobody reads it, so the badge becomes noise and
operators stop looking. ``MIN_ALERT_SEVERITY`` is the single knob.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.common.logger.logger import logger
from app.services.email_service import send_alert_email

#: Only severity at or above this becomes an alert.
MIN_ALERT_SEVERITY = 4

#: Severity of the most urgent alert, used for the badge/"critical" styling.
MAX_SEVERITY = 5
SYSTEM_ALERT_SEVERITY = 3


@dataclass(frozen=True)
class Alert:
    """One high-severity intelligence finding awaiting operator attention."""

    alert_id: str
    job_id: str | None
    item_id: str
    url: str
    title: str
    category: str
    severity: int
    language: str
    summary: str
    entities: list[str]
    analysis_source: str
    llm_model: str
    created_at: str | None
    read: bool = False
    alert_type: str = "threat"

    def to_dict(self) -> dict:
        return {
            "alert_id": self.alert_id,
            "job_id": self.job_id,
            "item_id": self.item_id,
            "url": self.url,
            "title": self.title,
            "category": self.category,
            "severity": self.severity,
            "language": self.language,
            "summary": self.summary,
            "entities": self.entities,
            "analysis_source": self.analysis_source,
            "llm_model": self.llm_model,
            "created_at": self.created_at,
            "read": self.read,
            "alert_type": self.alert_type,
        }


def qualifies_as_alert(severity: int, minimum: int = MIN_ALERT_SEVERITY) -> bool:
    """True when a finding is severe enough to surface.

    Guards the ``>= 4`` rule in one place so the worker's writer and the API's
    reader can never disagree about what an alert is.
    """
    try:
        value = int(severity)
    except (TypeError, ValueError):
        return False
    return minimum <= value <= MAX_SEVERITY


def alert_priority(severity: int, alert_type: str = "threat") -> str:
    """Human label for a severity value: critical / high / low."""
    if alert_type == "system":
        return "system"
    try:
        value = int(severity)
    except (TypeError, ValueError):
        return "low"
    if value >= 5:
        return "critical"
    if value == 4:
        return "high"
    return "low"


def summarize_alert(alert: Alert, max_chars: int = 240) -> str:
    """One-line summary for the list view and the email subject line.

    Falls back to the URL when the LLM produced no summary — an alert with a
    blank body is not actionable, and hiding that is worse than showing the
    source link.
    """
    text = (alert.summary or "").strip()
    if not text:
        text = alert.url
    text = " ".join(text.split())
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def build_alert_email(alert: Alert, base_url: str = "") -> tuple[str, str]:
    """Return ``(subject, body)`` for the alert email.

    Deliberately short. The email is a nudge that pulls the operator into the
    app; the full finding, its entities and its source article live on the
    Alerts page. A wall of pasted text in an inbox gets ignored, which is the
    same outcome as not sending it.
    """
    label = alert_priority(alert.severity).upper()
    category = (alert.category or "uncategorised").replace("_", " ")
    subject = f"[DukaScraper {label}] {category} — {alert.title or alert.url}"

    lines = [
        f"Severity : {alert.severity}/5 ({label})",
        f"Category : {category}",
        f"Job      : {alert.job_id or 'platform'}",
        f"Language : {alert.language or 'unknown'}",
        "",
        summarize_alert(alert),
    ]
    if alert.entities:
        shown = ", ".join(alert.entities[:8])
        more = f" (+{len(alert.entities) - 8} more)" if len(alert.entities) > 8 else ""
        lines += ["", f"Entities : {shown}{more}"]
    if alert.analysis_source and alert.analysis_source != "llm":
        # A heuristic label presented as a threat finding is misleading; say so
        # in the email exactly as the Analytics page does.
        lines += ["", f"Note: classified by heuristic ({alert.analysis_source}), not by the model."]
    if base_url:
        lines += ["", f"Open: {base_url.rstrip('/')}/#/alerts?alert={alert.alert_id}"]
    return subject, "\n".join(lines)


async def notify_alert_email(job_id: str, alert_id: str) -> bool:
    """Email a short summary of one alert to the job owner.

    Reuses the SMTP settings that already deliver password resets, so no new
    credentials are introduced. Gated on ``ALERT_EMAILS_ENABLED`` because the
    account this runs against may not be able to send mail in every
    environment; when it is off this is a no-op and the alert still lands in the
    feed and the badge.

    Resolves the recipient from ``jobs.user_id`` — the single ownership
    relationship the whole system is scoped by — rather than from a configured
    address list, so an operator is told about the jobs they actually ran.
    """
    from app.common.config.settings import settings
    from app.storage.postgres.client import pg_client

    if not getattr(settings, "ALERT_EMAILS_ENABLED", False):
        logger.info(
            "Alert %s raised; email notification disabled (ALERT_EMAILS_ENABLED=false)",
            alert_id,
        )
        return False

    row = await pg_client.get_alert(alert_id)
    if row is None:
        logger.warning("Alert %s has no row to email; skipping", alert_id)
        return False
    if row["job_id"] != job_id:
        # The worker's upsert is keyed on item_id, so the alert it just wrote
        # is the right row; anything else means the ids were crossed.
        logger.warning("Alert %s belongs to job %s, not %s; skipping", alert_id, row["job_id"], job_id)
        return False
    alert = _row_to_alert(row)

    job = await pg_client.get_job(job_id)
    if not job:
        return False
    owner = await pg_client.get_user(job["user_id"])
    recipient = (owner["email"] if owner else "") or ""
    if not recipient:
        logger.warning("Job %s has no owner email; alert %s not emailed", job_id, alert_id)
        return False

    subject, body = build_alert_email(alert, getattr(settings, "UI_BASE_URL", ""))
    await send_alert_email(recipient, subject, body)
    logger.info("Emailed alert %s to %s", alert_id, recipient)
    return True


# ── Persistence ───────────────────────────────────────────────


async def record_alert(
    *,
    job_id: str,
    item_id: str,
    url: str,
    title: str = "",
    category: str = "",
    severity: int = MIN_ALERT_SEVERITY,
    language: str = "",
    summary: str = "",
    entities: list[str] | None = None,
    analysis_source: str = "llm",
    llm_model: str = "",
) -> str | None:
    """Mirror one intelligence row into ``alerts``.

    Returns the alert_id, or None when the finding does not qualify. Idempotent
    on ``item_id``: a re-analysis of the same item updates the existing alert
    rather than raising a duplicate notification, which is what happens today
    whenever Kafka redelivers a parsed item.
    """
    from app.storage.postgres.client import pg_client

    if not qualifies_as_alert(severity):
        return None

    alert_id = await pg_client.upsert_alert(
        job_id=job_id,
        item_id=item_id,
        url=url,
        title=title or "",
        category=category or "",
        severity=int(severity),
        language=language or "",
        summary=summary or "",
        entities=entities or [],
        analysis_source=analysis_source or "llm",
        llm_model=llm_model or "",
    )
    if alert_id:
        logger.info(
            "Raised alert %s (severity=%s, job=%s, item=%s)",
            alert_id, severity, job_id, item_id,
        )
    return alert_id


async def record_system_alert(
    *,
    dedupe_key: str,
    title: str,
    summary: str,
    category: str = "system",
    severity: int = SYSTEM_ALERT_SEVERITY,
    entities: list[str] | None = None,
) -> str | None:
    """Persist one operational alert, returning None for duplicate events."""
    from app.storage.postgres.client import pg_client

    alert_id = await pg_client.insert_system_alert(
        dedupe_key=dedupe_key,
        title=title,
        summary=summary,
        category=category,
        severity=severity,
        entities=entities or [],
    )
    if alert_id:
        logger.warning("Raised system alert %s (%s)", alert_id, title)
    return alert_id


async def notify_system_alert_email(alert_id: str) -> int:
    """Email a newly-created system alert to every active administrator."""
    from app.common.config.settings import settings
    from app.storage.postgres.client import pg_client

    if not getattr(settings, "ALERT_EMAILS_ENABLED", False):
        return 0
    row = await pg_client.get_alert(alert_id)
    if row is None:
        return 0
    alert = _row_to_alert(row)
    subject, body = build_alert_email(alert, getattr(settings, "UI_BASE_URL", ""))
    sent = 0
    for email in await pg_client.list_admin_emails():
        try:
            await send_alert_email(email, subject, body)
            sent += 1
        except Exception:
            logger.exception("System alert %s email delivery failed for %s", alert_id, email)
    return sent


async def list_alerts(
    *,
    user_id: str,
    job_ids: set[str] | None,
    unread_only: bool = False,
    minimum_severity: int = MIN_ALERT_SEVERITY,
    limit: int = 50,
    offset: int = 0,
    alert_type: str = "all",
) -> tuple[list[Alert], int]:
    """Return ``(alerts, total)`` for one user, newest first.

    ``job_ids=None`` means unrestricted (admin); an empty set means "no jobs",
    which the query treats as matching nothing rather than everything.
    """
    from app.storage.postgres.client import pg_client

    rows, total = await pg_client.list_alerts(
        user_id=user_id,
        job_ids=job_ids,
        unread_only=unread_only,
        minimum_severity=minimum_severity,
        limit=limit,
        offset=offset,
        alert_type=alert_type,
    )
    return [_row_to_alert(r) for r in rows], total


async def unread_count(user_id: str, job_ids: set[str] | None) -> int:
    from app.storage.postgres.client import pg_client

    return await pg_client.count_unread_alerts(user_id=user_id, job_ids=job_ids)


async def alert_visible_to(alert_id: str, job_ids: set[str] | None) -> bool:
    """True when the alert exists and belongs to a job in the caller's scope.

    Guards the single-alert read endpoint: without it, any authenticated user
    could clear any alert by guessing its id, because the read marker is keyed
    on the caller and would succeed for an alert they cannot see.
    """
    from app.storage.postgres.client import pg_client

    return await pg_client.alert_in_scope(alert_id, job_ids)


async def mark_read(user_id: str, alert_id: str) -> bool:
    from app.storage.postgres.client import pg_client

    return await pg_client.mark_alert_read(user_id, alert_id)


async def mark_all_read(user_id: str, job_ids: set[str] | None) -> int:
    from app.storage.postgres.client import pg_client

    return await pg_client.mark_all_alerts_read(user_id, job_ids)


def _row_to_alert(row) -> Alert:
    data = dict(row)
    entities = data.get("entities") or []
    if isinstance(entities, str):
        import json

        try:
            entities = json.loads(entities)
        except (TypeError, ValueError):
            entities = []
    created = data.get("created_at")
    return Alert(
        alert_id=data["alert_id"],
        job_id=data["job_id"],
        item_id=data["item_id"],
        url=data.get("url") or "",
        title=data.get("title") or "",
        category=data.get("category") or "",
        severity=int(data.get("severity") or 0),
        language=data.get("language") or "",
        summary=data.get("summary") or "",
        entities=list(entities),
        analysis_source=data.get("analysis_source") or "llm",
        llm_model=data.get("llm_model") or "",
        created_at=created.isoformat() if created else None,
        read=bool(data.get("is_read")),
        alert_type=data.get("alert_type") or "threat",
    )