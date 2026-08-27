"""Safely migrate legacy intelligence JSON stored in `summary` into real columns.

The original table is retained as `intelligence_analytics_legacy` for rollback.
Run: python scripts/repair_intelligence_columns.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.common.constants.content_topics import ALL_CONTENT_TOPICS, DEFAULT_CONTENT_TOPIC
from app.common.constants.intelligence_categories import ALL_INTELLIGENCE_CATEGORIES, DEFAULT_INTELLIGENCE_CATEGORY
from app.common.constants.source_types import ALL_SOURCE_TYPES, DEFAULT_SOURCE_TYPE
from app.storage.clickhouse.client import ch_client

TABLE = "intelligence_analytics"
STAGING = "intelligence_analytics_repaired"
BACKUP = "intelligence_analytics_legacy"


def valid(value: object, allowed: set[str], default: str) -> str:
    candidate = str(value or "").strip().lower()
    return candidate if candidate in allowed else default


def normalized(row: tuple) -> list:
    job_id, item_id, url, source_type, topic, category, severity, entities, summary, language, model, score, created_at = row
    try:
        data = json.loads(summary) if str(summary).lstrip().startswith("{") else None
    except (TypeError, json.JSONDecodeError):
        data = None
    if isinstance(data, dict) and data.get("summary"):
        source_type = valid(data.get("source_type"), ALL_SOURCE_TYPES, DEFAULT_SOURCE_TYPE)
        topic = valid(data.get("topic"), ALL_CONTENT_TOPICS, DEFAULT_CONTENT_TOPIC)
        category = valid(data.get("category"), ALL_INTELLIGENCE_CATEGORIES, DEFAULT_INTELLIGENCE_CATEGORY)
        severity = max(1, min(5, int(data.get("threat_severity", 1))))
        raw_entities = data.get("entities", [])
        entities = [str(entity).strip() for entity in raw_entities if str(entity).strip()][:100] if isinstance(raw_entities, list) else []
        summary = str(data["summary"]).strip()
    return [job_id, item_id, url, source_type, topic, category, severity, entities, summary, language, model, score, created_at]


def main() -> None:
    ch_client.connect()
    client = ch_client.client
    if not client:
        raise RuntimeError("ClickHouse is unavailable")
    if client.query(f"EXISTS TABLE {BACKUP}").result_rows[0][0]:
        raise RuntimeError(f"{BACKUP} already exists; migration was already run or needs review")

    rows = client.query(
        f"SELECT job_id, item_id, url, source_type, topic, category, threat_severity, entities, summary, "
        f"language, llm_model, llm_score, created_at FROM {TABLE}"
    ).result_rows
    client.command(f"DROP TABLE IF EXISTS {STAGING}")
    client.command(
        f"""CREATE TABLE {STAGING} (
            job_id String, item_id String, url String, source_type String,
            topic LowCardinality(String) DEFAULT 'other', category LowCardinality(String),
            threat_severity UInt8, entities Array(String), summary String,
            language LowCardinality(String), llm_model String, llm_score Nullable(Float32),
            created_at DateTime
        ) ENGINE = MergeTree() ORDER BY (created_at, item_id)"""
    )
    cleaned = [normalized(row) for row in rows]
    if cleaned:
        client.insert(
            STAGING, cleaned,
            column_names=["job_id", "item_id", "url", "source_type", "topic", "category", "threat_severity",
                          "entities", "summary", "language", "llm_model", "llm_score", "created_at"],
        )
    # Metadata rename preserves the old data for rollback and is atomic on one server.
    client.command(f"RENAME TABLE {TABLE} TO {BACKUP}, {STAGING} TO {TABLE}")
    print(f"Migrated {len(cleaned)} rows. Backup retained as {BACKUP}.")


if __name__ == "__main__":
    main()
