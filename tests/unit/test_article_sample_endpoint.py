"""Regression tests for the parsed-item sample endpoint.

Two bugs made "show me the text that was actually captured" fail for every
single item:

1. `get_article` handed the raw `asyncpg.Record` to `_fetch_parsed_content`,
   which enriches the row in place. asyncpg records are immutable, so every
   request died with
   `TypeError: 'asyncpg.protocol.record.Record' object does not support item
   assignment` and the UI rendered "An unexpected error occurred".

2. The parsed object nests the extraction under a `data` key, but the endpoint
   read `extracted_text` only at the top level, so even once the 500 was gone
   every sample came back empty.

`_fetch_parsed_content` now flattens the envelope so every consumer sees one
consistent shape; these tests pin that contract.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from app.api.routes import articles


class _FakeMinioResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def close(self) -> None:
        pass

    def release_conn(self) -> None:
        pass


class _FakeMinioClient:
    def __init__(self, objects: dict[tuple[str, str], bytes]):
        self._objects = objects

    def get_object(self, bucket: str, name: str):
        try:
            return _FakeMinioResponse(self._objects[(bucket, name)])
        except KeyError:
            raise Exception(f"no such object {bucket}/{name}")


@pytest.fixture
def nested_payload() -> bytes:
    """A parsed object in the real shape the parser writes today."""
    return json.dumps(
        {
            "job_id": "JOB1",
            "item_id": "ITEM1",
            "url": "https://example.test/a",
            "status": "completed",
            "data": {
                "extracted_text": "Ask not what your country can do for you.",
                "character_count": 46,
            },
        }
    ).encode("utf-8")


class TestFetchParsedContent:
    async def test_enriches_a_plain_dict(self, monkeypatch, nested_payload):
        monkeypatch.setattr(
            articles,
            "minio_client",
            type(
                "M",
                (),
                {
                    "client": _FakeMinioClient(
                        {
                            ("duka-parsed-data", "site/JOB1_ITEM1.json"): nested_payload,
                            ("duka-raw-data", "site/JOB1_ITEM1.html"): b"<html/>",
                        }
                    )
                },
            )(),
        )
        row = {
            "parsed_json_path": "s3://duka-parsed-data/site/JOB1_ITEM1.json",
            "raw_html_path": "s3://duka-raw-data/site/JOB1_ITEM1.html",
        }
        await articles._fetch_parsed_content(row)

        assert row["parsed_object"] == "site/JOB1_ITEM1.json"
        assert row["raw_object"] == "site/JOB1_ITEM1.html"
        # parsed_content is flattened: the on-disk envelope nests the payload
        # under "data", and consumers expect the flat shape. Reading the wrong
        # level is what rendered the quality score as "NaN%".
        assert row["parsed_content"]["extracted_text"].startswith("Ask not")
        assert row["parsed_content"]["character_count"] == 46
        assert "data" not in row["parsed_content"]
        # Envelope fields survive the merge.
        assert row["parsed_content"]["job_id"] == "JOB1"
        assert row["parsed_content"]["status"] == "completed"

    async def test_missing_objects_degrade_to_none(self, monkeypatch):
        """A missing object must not fail the request; metadata still matters."""
        monkeypatch.setattr(
            articles,
            "minio_client",
            type("M", (), {"client": _FakeMinioClient({})})(),
        )
        row = {
            "parsed_json_path": "s3://duka-parsed-data/gone.json",
            "raw_html_path": "s3://duka-raw-data/gone.html",
        }
        await articles._fetch_parsed_content(row)
        assert row["parsed_content"] is None
        assert row["raw_html"] is None

    async def test_paths_are_optional(self, monkeypatch):
        monkeypatch.setattr(
            articles, "minio_client", type("M", (), {"client": _FakeMinioClient({})})()
        )
        row = {"parsed_json_path": None, "raw_html_path": None}
        await articles._fetch_parsed_content(row)
        assert row["parsed_content"] is None


class TestExtractedTextLookup:
    """The endpoint must read the nested shape as well as the flat one."""

    def _resolve(self, payload: dict) -> str:
        content = payload or {}
        return (
            content.get("extracted_text")
            or (content.get("data") or {}).get("extracted_text")
            or ""
        )

    def test_reads_nested_data_block(self):
        payload = json.loads(
            json.dumps({"data": {"extracted_text": "nested transcript"}})
        )
        assert self._resolve(payload) == "nested transcript"

    def test_reads_top_level_for_older_objects(self):
        assert self._resolve({"extracted_text": "flat text"}) == "flat text"

    def test_missing_text_is_empty_not_an_error(self):
        assert self._resolve({}) == ""
        assert self._resolve({"data": {}}) == ""

    def test_nested_null_does_not_raise(self):
        assert self._resolve({"data": None}) == ""


class TestAsyncpgRecordImmutability:
    async def test_endpoint_converts_record_to_dict_before_enriching(self, monkeypatch):
        """Guard the exact bug: an immutable row must never reach the mutator.

        asyncpg.Record refuses item assignment; a plain dict accepts it. The
        endpoint must therefore hand `_fetch_parsed_content` a real dict.
        """
        monkeypatch.setattr(
            articles,
            "minio_client",
            type("M", (), {"client": _FakeMinioClient({})})(),
        )

        class _Record(dict):
            """Behaves like asyncpg.Record: reads work, writes raise."""

            def __setitem__(self, key, value):  # type: ignore[override]
                raise TypeError(
                    "'asyncpg.protocol.record.Record' object does not support "
                    "item assignment"
                )

        captured: dict = {}

        async def _capture(item, *, include_raw: bool = True):
            captured["type"] = type(item)
            captured["mutable"] = isinstance(item, dict) and not isinstance(item, _Record)
            captured["include_raw"] = include_raw
            return None

        monkeypatch.setattr(articles, "_fetch_parsed_content", _capture)

        async def _fake_get(item_id):
            return _Record(
                item_id=item_id,
                job_id="JOB1",
                source_url="https://x.test",
                language="en",
                title=None,
                publish_date=None,
                character_count=10,
                word_count=2,
                raw_html_path="s3://duka-raw-data/site/J1_I1.html",
                parsed_json_path="s3://duka-parsed-data/site/J1_I1.json",
                parsed_at=None,
                is_exported=False,
                intelligence_processed=True,
            )

        async def _fake_access(job_id, user):
            return None

        monkeypatch.setattr(articles.pg_client, "get_parsed_item", _fake_get)
        monkeypatch.setattr(articles, "_require_job_access", _fake_access)

        await articles.get_article("ITEM1", user={"user_id": "u1", "role": "admin"})

        assert captured["mutable"] is True, (
            "get_article must convert the asyncpg Record to a plain dict before "
            "passing it to _fetch_parsed_content"
        )
        # The single-item endpoint only needs the parsed text for its sample.
        assert captured["include_raw"] is False