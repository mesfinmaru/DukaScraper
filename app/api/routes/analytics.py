"""Human-label model evaluation endpoints and measured classification metrics."""

from uuid import uuid4

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.common.constants.content_topics import ALL_CONTENT_TOPICS
from app.common.constants.intelligence_categories import ALL_INTELLIGENCE_CATEGORIES
from app.common.constants.source_types import ALL_SOURCE_TYPES
from app.storage.clickhouse.client import ch_client

router = APIRouter()


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
