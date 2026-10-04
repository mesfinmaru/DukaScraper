"""Unit tests: raw-binary retention.

The raw bucket is supposed to hold the bytes the server sent. These tests pin
the naming and metadata decisions that make that true — a wrong extension or a
silently-skipped upload means the file is unrecoverable later.
"""

import pytest

from app.common.utils.minio_naming import raw_name, raw_suffix
from app.services.raw_object_store import (
    RawObjectStore,
    default_content_type,
    split_path,
)


class TestRawSuffix:
    @pytest.mark.parametrize(
        "content_type,expected",
        [
            ("application/pdf", ".pdf"),
            ("application/pdf; charset=binary", ".pdf"),
            (
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ".docx",
            ),
            ("audio/flac", ".flac"),
            ("audio/mpeg", ".mp3"),
            ("text/html; charset=utf-8", ".html"),
        ],
    )
    def test_content_type_wins(self, content_type, expected):
        assert raw_suffix(url="http://x.test/a", content_type=content_type) == expected

    def test_falls_back_to_url_extension_when_type_is_generic(self):
        assert (
            raw_suffix(url="http://x.test/report.pdf", content_type="application/octet-stream")
            == ".pdf"
        )

    def test_falls_back_to_magic_bytes_when_type_and_url_are_useless(self):
        assert raw_suffix(url="http://x.test/download", content_type="", payload=b"%PDF-1.7\n") == ".pdf"
        assert raw_suffix(url="http://x.test/download", content_type=None, payload=b"fLaC\x00") == ".flac"
        assert raw_suffix(url="http://x.test/download", content_type="binary/octet-stream", payload=b"ID3\x03") == ".mp3"

    def test_unknown_payload_is_named_bin_not_html(self):
        # Naming an unknown binary ".html" is what made the raw bucket lie.
        assert raw_suffix(url="http://x.test/thing", content_type="application/x-weird") == ".bin"

    def test_query_string_does_not_leak_into_the_extension(self):
        # "?download=1" must not become part of the name (".pdf?download=1").
        assert raw_suffix(url="http://x.test/a.pdf?download=1", content_type="") == ".pdf"


class TestRawName:
    def test_uses_the_real_extension(self):
        name = raw_name("surface", "JOB1", "ITEM2", "example.com", ".flac")
        assert name == "example.com/JOB00000001_ITEM00000002.flac"

    def test_defaults_to_html_for_plain_crawls(self):
        assert raw_name("parser", "JOB1", "ITEM2", "example.com") == (
            "example.com/JOB00000001_ITEM00000002.html"
        )


class TestSplitPath:
    def test_splits_bucket_and_object(self):
        assert split_path("s3://duka-raw-data/a.com/JOB1_ITEM2.pdf") == (
            "duka-raw-data",
            "a.com/JOB1_ITEM2.pdf",
        )

    @pytest.mark.parametrize("bad", ["", "http://x/y", "s3://", "s3://bucket", None])
    def test_rejects_non_s3_paths(self, bad):
        assert split_path(bad) is None


class TestBuildRef:
    def test_ref_describes_the_payload_without_storing_it(self):
        store = RawObjectStore()
        ref = store.build_ref(
            job_id="JOB7",
            item_id="ITEM8",
            url="http://example.com/audio",
            worker="surface",
            payload=b"fLaC" + b"\x00" * 32,
            content_type="audio/flac",
        )
        assert ref.object_name == "example.com/JOB00000007_ITEM00000008.flac"
        assert ref.content_type == "audio/flac"
        assert ref.size_bytes == 36
        assert ref.path == "s3://duka-raw-data/example.com/JOB00000007_ITEM00000008.flac"
        assert len(ref.sha256) == 64
        assert ref.extension == "flac"

    def test_sha256_is_the_payload_digest_not_the_text_digest(self):
        import hashlib

        store = RawObjectStore()
        payload = b"%PDF-1.7 real bytes"
        ref = store.build_ref(
            job_id="JOB7",
            item_id="ITEM8",
            url="http://example.com/a.pdf",
            worker="deep",
            payload=payload,
            content_type="application/pdf",
        )
        assert ref.sha256 == hashlib.sha256(payload).hexdigest()

    def test_empty_payload_is_never_stored(self):
        import asyncio

        store = RawObjectStore()
        result = asyncio.run(
            store.store(
                job_id="JOB7",
                item_id="ITEM8",
                url="http://example.com/x",
                worker="surface",
                payload=b"",
            )
        )
        assert result is None


class TestDefaultContentType:
    def test_round_trips_with_raw_suffix(self):
        for suffix in (".pdf", ".flac", ".mp3", ".docx", ".html"):
            mime = default_content_type(suffix)
            assert raw_suffix(url="http://x.test/f", content_type=mime) == suffix