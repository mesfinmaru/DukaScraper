"""Human-label model evaluation endpoints and measured classification metrics."""

from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.common.constants.content_topics import ALL_CONTENT_TOPICS
from app.common.constants.intelligence_categories import ALL_INTELLIGENCE_CATEGORIES
from app.common.constants.source_types import ALL_SOURCE_TYPES
from app.security.auth import get_current_user
from app.storage.clickhouse.client import ch_client

router = APIRouter()


def _num(value, ndigits: int = 1):
    """JSON-safe float: ClickHouse avg() returns nan on empty groups, and
    FastAPI's JSON encoder rejects nan/inf ("Out of range float values are
    not JSON compliant") — turning a whole dashboard endpoint into a 500."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if f != f or f in (float("inf"), float("-inf")):  # nan / inf
        return None
    return round(f, ndigits)


class EvaluationRequest(BaseModel):
    item_id: str
    job_id: str
    evaluated_by: str = Field(min_length=1, max_length=128)
    expected_source_type: str
    expected_topic: str
    expected_category: str


def _validate_label(value: str, valid_values: set[str], name: str) -> str:
    normalized = value.strip().lower()
    if normalized not in valid_values:
        raise HTTPException(status_code=422, detail=f"Invalid {name}: {value}")
    return normalized


@router.post("/evaluations", status_code=201)
async def record_evaluation(request: EvaluationRequest):
    """Record a human label against the model's current prediction."""
    if not ch_client.client:
        raise HTTPException(status_code=503, detail="Analytics database unavailable")

    expected_source = _validate_label(request.expected_source_type, ALL_SOURCE_TYPES, "source type")
    expected_topic = _validate_label(request.expected_topic, ALL_CONTENT_TOPICS, "topic")
    expected_category = _validate_label(request.expected_category, ALL_INTELLIGENCE_CATEGORIES, "category")
    result = ch_client.client.query(
        "SELECT source_type, topic, category FROM intelligence_analytics "
        "WHERE item_id = {item_id:String} AND job_id = {job_id:String} ORDER BY created_at DESC LIMIT 1",
        parameters={"item_id": request.item_id, "job_id": request.job_id},
    )
    if not result.result_rows:
        raise HTTPException(status_code=404, detail="No model analysis found for this item")

    predicted_source, predicted_topic, predicted_category = result.result_rows[0]
    evaluation_id = str(uuid4())
    ch_client.client.insert(
        "model_evaluations",
        [[evaluation_id, request.item_id, request.job_id, request.evaluated_by, expected_source, predicted_source,
          expected_topic, predicted_topic, expected_category, predicted_category]],
        column_names=["evaluation_id", "item_id", "job_id", "evaluated_by", "expected_source_type",
                      "predicted_source_type", "expected_topic", "predicted_topic", "expected_category", "predicted_category"],
    )
    return {"evaluation_id": evaluation_id, "item_id": request.item_id, "job_id": request.job_id}


def _metrics(rows: list[tuple], expected_index: int, predicted_index: int) -> dict:
    total = len(rows)
    if not total:
        return {"samples": 0, "accuracy": None, "macro_f1": None}
    expected = [row[expected_index] for row in rows]
    predicted = [row[predicted_index] for row in rows]
    labels = sorted(set(expected) | set(predicted))
    correct = sum(e == p for e, p in zip(expected, predicted))
    f1_scores = []
    for label in labels:
        tp = sum(e == label and p == label for e, p in zip(expected, predicted))
        fp = sum(e != label and p == label for e, p in zip(expected, predicted))
        fn = sum(e == label and p != label for e, p in zip(expected, predicted))
        precision = tp / (tp + fp) if tp + fp else 0.0
        recall = tp / (tp + fn) if tp + fn else 0.0
        f1_scores.append((2 * precision * recall / (precision + recall)) if precision + recall else 0.0)
    return {"samples": total, "accuracy": round(correct / total, 4), "macro_f1": round(sum(f1_scores) / len(f1_scores), 4)}


@router.get("/evaluations/metrics")
async def evaluation_metrics():
    """Return measured accuracy and macro F1 from human-labeled examples."""
    if not ch_client.client:
        raise HTTPException(status_code=503, detail="Analytics database unavailable")
    result = ch_client.client.query(
        "SELECT expected_source_type, predicted_source_type, expected_topic, predicted_topic, "
        "expected_category, predicted_category FROM model_evaluations"
    )
    rows = result.result_rows
    return {
        "source_type": _metrics(rows, 0, 1),
        "topic": _metrics(rows, 2, 3),
        "category": _metrics(rows, 4, 5),
        "note": "Metrics are based only on human-labeled evaluations; no per-response confidence is inferred.",
    }


# ---------------------------------------------------------------------------
# Threat intelligence analytics (ClickHouse intelligence_analytics)
# ---------------------------------------------------------------------------

_FLAGGED_FILTER = "category != 'other'"


def _dt(value) -> str | None:
    return value.isoformat() if value is not None else None


@router.get("/threats")
async def threat_analytics(limit: int = 50):
    """Aggregate LLM-classified threat items from ClickHouse.

    Counts and the recent list cover "flagged" items only (category != 'other'),
    i.e. data_leak / gov_issue / cyber_threat / physical_threat / misinformation.
    """
    if not ch_client.client:
        raise HTTPException(status_code=503, detail="Analytics database unavailable")

    total = ch_client.client.query(
        f"SELECT count() FROM intelligence_analytics WHERE {_FLAGGED_FILTER}"
    ).result_rows[0][0]

    by_severity_rows = ch_client.client.query(
        f"SELECT threat_severity, count() FROM intelligence_analytics "
        f"WHERE {_FLAGGED_FILTER} GROUP BY threat_severity"
    ).result_rows
    by_severity = {int(sev): int(cnt) for sev, cnt in by_severity_rows}
    by_severity_list = [
        {"severity": level, "count": int(by_severity.get(level, 0))}
        for level in (1, 2, 3, 4, 5)
    ]

    by_category_rows = ch_client.client.query(
        f"SELECT category, count() FROM intelligence_analytics "
        f"WHERE {_FLAGGED_FILTER} GROUP BY category ORDER BY count() DESC"
    ).result_rows
    by_category_list = [{"category": cat, "count": int(cnt)} for cat, cnt in by_category_rows]

    recent_rows = ch_client.client.query(
        "SELECT item_id, job_id, url, source_type, category, threat_severity, summary, created_at "
        f"FROM intelligence_analytics WHERE {_FLAGGED_FILTER} "
        "ORDER BY created_at DESC LIMIT {limit:UInt32}",
        parameters={"limit": int(limit)},
    ).result_rows
    recent = [
        {
            "item_id": row[0],
            "job_id": row[1],
            "url": row[2],
            "source_type": row[3],
            "category": row[4],
            "severity": int(row[5]),
            "summary": row[6],
            "created_at": _dt(row[7]),
        }
        for row in recent_rows
    ]

    return {
        "total": int(total),
        "by_severity": by_severity_list,
        "by_category": by_category_list,
        "recent": recent,
    }


@router.get("/intelligence/entities")
async def intelligence_entities(
    limit: int = 100,
    user: dict = Depends(get_current_user),
):
    """Most frequently mentioned entities across all analyzed content.

    Reads the flattened intelligence_entities table written by the llm-worker
    (entity values are typed: 'email:x@y.z', 'ip:1.2.3.4', 'domain:example.com',
    'other:Name'). Enables "every page mentioning X" OSINT lookups.
    """
    if not ch_client.client:
        raise HTTPException(status_code=503, detail="Analytics database unavailable")

    try:
        rows = ch_client.client.query(
            """
            SELECT entity, uniqExact(item_id) AS items,
                   groupUniqArray(5)(url) AS sample_urls
            FROM intelligence_entities
            GROUP BY entity
            ORDER BY items DESC, entity
            LIMIT {limit:UInt32}
            """,
            parameters={"limit": max(1, min(int(limit), 500))},
        ).result_rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Entity query failed: {e}") from e

    entities = []
    for row in rows:
        raw = row[0]
        entity_type, _, value = raw.partition(":")
        if not value:  # malformed row without a type prefix
            entity_type, value = "other", raw
        entities.append({
            "entity": value,
            "entity_type": entity_type,
            "occurrences": int(row[1]),
            "sample_urls": list(row[2]) if row[2] else [],
        })

    return {
        "total": len(entities),
        "entities": entities,
    }


@router.get("/intelligence/entities/search")
async def search_entity_mentions(
    entity: str = Query(..., min_length=2, description="Entity value to look up, e.g. '1.2.3.4' or 'example.com'"),
    limit: int = 50,
    user: dict = Depends(get_current_user),
):
    """Find every analyzed page mentioning a given entity (OSINT pivot).

    Matches on the raw entity value regardless of its typed prefix, so
    searching 'example.com' matches 'domain:example.com'.
    """
    if not ch_client.client:
        raise HTTPException(status_code=503, detail="Analytics database unavailable")

    needle = entity.strip()
    try:
        rows = ch_client.client.query(
            """
            SELECT item_id, job_id, url, entity, category, language, threat_severity, created_at
            FROM intelligence_entities
            WHERE positionCaseInsensitive(entity, {needle:String}) > 0
            ORDER BY created_at DESC
            LIMIT {limit:UInt32}
            """,
            parameters={"needle": needle, "limit": max(1, min(int(limit), 500))},
        ).result_rows
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Entity query failed: {e}") from e

    return {
        "entity": needle,
        "total": len(rows),
        "mentions": [
            {
                "item_id": row[0],
                "job_id": row[1],
                "url": row[2],
                "entity": row[3],
                "category": row[4],
                "language": row[5],
                "severity": int(row[6]),
                "created_at": _dt(row[7]),
            }
            for row in rows
        ],
    }


@router.get("/performance")
async def crawler_performance(limit: int = 25):
    """Worker latency/reliability aggregates + recent attempts from ClickHouse."""
    if not ch_client.client:
        raise HTTPException(status_code=503, detail="Analytics database unavailable")

    def _row_result(rows, index: int, default=None):
        """Return column `index` of the first result row (aggregate queries)."""
        if not rows:
            return default
        row = rows[0]
        try:
            return row[index]
        except (IndexError, TypeError):
            return default

    # Per-worker aggregates
    worker_rows = ch_client.client.query(
        "SELECT worker, count(), avg(latency_ms), quantile(0.95)(latency_ms), "
        "max(latency_ms), sum(retry_count), "
        "countIf(status_code >= 400 OR status_code = 0), avg(payload_size_bytes) "
        "FROM crawler_performance GROUP BY worker ORDER BY count() DESC"
    ).result_rows
    workers = []
    for row in worker_rows:
        worker, requests, avg_lat, p95_lat, max_lat, retries, errors, avg_payload = row
        requests = int(requests)
        error_rate = (errors / requests) if requests else None
        workers.append({
            "worker": worker,
            "requests": requests,
            "avg_latency_ms": _num(avg_lat),
            "p95_latency_ms": _num(p95_lat),
            "max_latency_ms": _num(max_lat),
            "retries": int(retries),
            "error_rate": _num(error_rate, 4),
            "avg_payload_bytes": _num(avg_payload),
        })

    # Overall aggregates
    overall_rows = ch_client.client.query(
        "SELECT count(), avg(latency_ms), quantile(0.95)(latency_ms), "
        "countIf(status_code >= 400 OR status_code = 0) "
        "FROM crawler_performance"
    ).result_rows
    total_requests = int(_row_result(overall_rows, 0, 0))
    errors_total = int(_row_result(overall_rows, 3, 0))
    overall = {
        "requests": total_requests,
        "avg_latency_ms": _num(_row_result(overall_rows, 1)),
        "p95_latency_ms": _num(_row_result(overall_rows, 2)),
        "error_rate": round(errors_total / total_requests, 4) if total_requests else None,
    }

    # Recent attempts
    attempt_rows = ch_client.client.query(
        "SELECT job_id, worker, status_code, latency_ms, retry_count, payload_size_bytes, created_at "
        "FROM crawler_performance ORDER BY created_at DESC LIMIT {limit:UInt32}",
        parameters={"limit": int(limit)},
    ).result_rows
    recent_attempts = [
        {
            "job_id": row[0],
            "worker": row[1],
            "status_code": int(row[2]),
            "latency_ms": round(float(row[3]), 1) if row[3] is not None else 0,
            "retry_count": int(row[4]),
            "payload_size_bytes": int(row[5]),
            "created_at": _dt(row[6]),
        }
        for row in attempt_rows
    ]

    return {"workers": workers, "overall": overall, "recent_attempts": recent_attempts}
