"""Unit tests: the Groq Batch API client.

The request format and the result parsing are the two places a silent mistake
would cost money or lose results, so both are pinned here without a network or
an API key.
"""

import json

import pytest

from app.services.groq_batch import (
    BATCH_FAILED_STATES,
    CHAT_COMPLETIONS_URL,
    MAX_BATCH_LINES,
    GroqBatchClient,
    GroqBatchError,
    build_batch_lines,
    encode_batch_file,
    parse_result_file,
    parse_result_line,
)


class TestBuildBatchLines:
    def test_line_matches_the_documented_groq_shape(self):
        (line,) = build_batch_lines([("ITEM1", "be careful", "summarise this")])
        record = json.loads(line)
        assert record["custom_id"] == "ITEM1"
        assert record["method"] == "POST"
        assert record["url"] == CHAT_COMPLETIONS_URL
        assert record["body"]["messages"][0] == {
            "role": "system",
            "content": "be careful",
        }
        assert record["body"]["messages"][1]["content"] == "summarise this"

    def test_one_line_per_request(self):
        lines = build_batch_lines(
            [(f"ITEM{i}", "", f"text {i}") for i in range(5)]
        )
        assert len(lines) == 5
        assert [json.loads(l)["custom_id"] for l in lines] == [
            f"ITEM{i}" for i in range(5)
        ]

    def test_amharic_is_written_as_unicode_not_escapes(self):
        (line,) = build_batch_lines([("ITEM1", "", "የኢትዮጵያ ዜና")])
        assert "የኢትዮጵያ" in line
        assert "\\u12a8" not in line

    def test_omits_the_system_turn_when_there_is_no_system_prompt(self):
        for absent in (None, ""):
            (line,) = build_batch_lines([("ITEM1", absent, "just the text")])
            messages = json.loads(line)["body"]["messages"]
            assert [m["role"] for m in messages] == ["user"]

    def test_prompt_text_survives_verbatim(self):
        prompt = "Line one\nLine two with \"quotes\" and a \\ backslash"
        (line,) = build_batch_lines([("ITEM1", "", prompt)])
        assert json.loads(line)["body"]["messages"][0]["content"] == prompt


class TestEncodeBatchFile:
    def test_is_newline_terminated_jsonl(self):
        payload = encode_batch_file(build_batch_lines([("A", "", "a"), ("B", "", "b")]))
        text = payload.decode("utf-8")
        assert text.endswith("\n")
        assert len(text.strip().splitlines()) == 2

    def test_refuses_an_empty_batch(self):
        with pytest.raises(GroqBatchError, match="empty"):
            encode_batch_file([])

    def test_refuses_more_than_groq_allows(self):
        too_many = [f"line-{i}" for i in range(MAX_BATCH_LINES + 1)]
        with pytest.raises(GroqBatchError, match="line limit"):
            encode_batch_file(too_many)

    def test_accepts_exactly_the_line_limit(self):
        at_limit = [f"line-{i}" for i in range(MAX_BATCH_LINES)]
        assert encode_batch_file(at_limit).count(b"\n") == MAX_BATCH_LINES


class TestParseResultLine:
    def test_reads_the_content_out_of_a_successful_result(self):
        line = json.dumps(
            {
                "custom_id": "ITEM1",
                "response": {
                    "body": {
                        "choices": [
                            {"message": {"content": '{"category": "news"}'}}
                        ]
                    }
                },
            }
        )
        custom_id, content, error = parse_result_line(line)
        assert (custom_id, error) == ("ITEM1", None)
        assert json.loads(content)["category"] == "news"

    def test_surfaces_a_per_request_error_without_losing_the_id(self):
        line = json.dumps({"custom_id": "ITEM2", "error": {"message": "rate limited"}})
        custom_id, content, error = parse_result_line(line)
        assert custom_id == "ITEM2"
        assert content is None
        assert "rate limited" in error

    def test_a_string_error_is_handled_too(self):
        custom_id, _, error = parse_result_line(
            json.dumps({"custom_id": "I", "error": "bad request"})
        )
        assert custom_id == "I" and error == "bad request"

    def test_no_choices_is_an_error_not_an_empty_success(self):
        """Silently marking an item analysed when the model returned nothing
        would be the worst possible failure mode here."""
        custom_id, content, error = parse_result_line(
            json.dumps({"custom_id": "I", "response": {"body": {"choices": []}}})
        )
        assert custom_id == "I"
        assert content is None
        assert "no choices" in error

    def test_malformed_json_raises_rather_than_returning_empty(self):
        with pytest.raises(GroqBatchError, match="unparseable"):
            parse_result_line("{oops")


class TestParseResultFile:
    def test_maps_every_line_to_its_item(self):
        content = "\n".join(
            [
                json.dumps(
                    {
                        "custom_id": "A",
                        "response": {"body": {"choices": [{"message": {"content": "1"}}]}},
                    }
                ),
                json.dumps(
                    {
                        "custom_id": "B",
                        "response": {"body": {"choices": [{"message": {"content": "2"}}]}},
                    }
                ),
            ]
        ).encode("utf-8")
        results = parse_result_file(content)
        assert results["A"][0] == "1"
        assert results["B"][0] == "2"

    def test_blank_lines_are_skipped(self):
        content = b'\n\n{"custom_id":"A","response":{"body":{"choices":[{"message":{"content":"x"}}]}}}\n\n'
        assert list(parse_result_file(content)) == ["A"]

    def test_amharic_result_text_is_not_mangled(self):
        content = json.dumps(
            {
                "custom_id": "A",
                "response": {"body": {"choices": [{"message": {"content": "ዜና ማጠቃለያ"}}]}},
            },
            ensure_ascii=False,
        ).encode("utf-8")
        assert parse_result_file(content)["A"][0] == "ዜና ማጠቃለያ"


class TestClientConfig:
    def test_unconfigured_without_a_key(self):
        assert GroqBatchClient("").configured is False
        assert GroqBatchClient("gsk_x").configured is True

    def test_uses_the_openai_compatible_base_url(self):
        client = GroqBatchClient("k", base_url="https://api.groq.com/openai/v1/")
        assert client.base_url == "https://api.groq.com/openai/v1"


class TestCollect:
    def _client(self, status_payload, output=b""):
        client = GroqBatchClient("key")
        state = {"status": status_payload}

        async def _status(_batch_id):
            return state

        async def _download(_file_id):
            return output

        client.status = _status
        client.download_results = _download
        return client

    @pytest.mark.parametrize("status", ["validating", "in_progress", "finalizing"])
    async def test_pending_batch_returns_nothing_and_does_not_download(self, status):
        client = self._client(status)

        async def _no_download(_file_id):
            raise AssertionError("must not fetch results before the batch completes")

        client.download_results = _no_download
        assert await client.collect("batch_x") == {}

    @pytest.mark.parametrize("status", sorted(BATCH_FAILED_STATES))
    async def test_terminal_failure_raises_so_items_can_be_re_planned(self, status):
        client = self._client(status)
        with pytest.raises(GroqBatchError, match="re-planned"):
            await client.collect("batch_x")

    async def test_completed_batch_returns_parsed_results(self):
        output = json.dumps(
            {
                "custom_id": "A",
                "response": {"body": {"choices": [{"message": {"content": "ok"}}]}},
            }
        ).encode("utf-8")
        client = self._client("completed", output)

        async def _status(_batch_id):
            return {"status": "completed", "output_file_id": "file_out"}

        client.status = _status
        results = await client.collect("batch_x")
        assert results == {"A": ("ok", None)}

    async def test_completed_without_an_output_file_is_an_error(self):
        client = self._client("completed")

        async def _status(_batch_id):
            return {"status": "completed", "output_file_id": None}

        client.status = _status
        with pytest.raises(GroqBatchError, match="without an output file"):
            await client.collect("batch_x")

    async def test_unknown_status_is_treated_as_still_running(self):
        """A status this code has not seen must not be mistaken for failure."""
        client = self._client("something_new")
        assert await client.collect("batch_x") == {}


class TestSubmit:
    async def test_a_failed_submission_raises_rather_than_returning_a_ghost_id(self):
        client = GroqBatchClient("key")
        lines = build_batch_lines([("A", "", "x")])

        async def _upload(_content, _filename="batch.jsonl"):
            raise GroqBatchError("batch file upload failed: 503")

        client.upload_batch_file = _upload
        with pytest.raises(GroqBatchError):
            await client.submit(lines, model="llama-3.3-70b-versatile")

    async def test_stamps_the_model_onto_every_request(self):
        client = GroqBatchClient("key")
        captured = {}

        async def _upload(content, _filename="batch.jsonl"):
            captured["lines"] = content.decode("utf-8").strip().splitlines()
            return "file_1"

        async def _create(_file_id):
            return "batch_1"

        client.upload_batch_file = _upload
        client.create_batch = _create

        submission = await client.submit(
            build_batch_lines([("A", "", "x")]), model="llama-3.3-70b-versatile"
        )

        assert submission.batch_id == "batch_1"
        assert json.loads(captured["lines"][0])["body"]["model"] == "llama-3.3-70b-versatile"

    async def test_a_missing_model_is_refused_before_any_upload(self):
        client = GroqBatchClient("key")

        async def _no_upload(*_a, **_k):
            raise AssertionError("must validate before uploading")

        client.upload_batch_file = _no_upload
        with pytest.raises(GroqBatchError, match="model id"):
            await client.submit(build_batch_lines([("A", "", "x")]), model="")
