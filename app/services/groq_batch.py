"""Groq Batch API: half-price bulk analysis for a large backlog.

Why this exists separately from the synchronous path
-----------------------------------------------------
The durable queue drives *immediate* analysis: claim an item, call Groq, write
the result. That is what you want while a job is still being worked on, and it
is what the UI reflects within seconds.

But at scale it is the wrong shape. Per-item synchronous calls cost full price
and burn the standard rate-limit budget that interactive traffic needs. Groq's
Batch API takes the same requests as a JSONL file and runs them asynchronously
at **50% of the synchronous price, with no impact on standard rate limits**.
The trade is latency: the completion window is **24 hours to 7 days** (you
choose, and Groq recommends the longer end), so a batched item is analysed
tomorrow, not now.

So both paths share one queue and one set of results, and which one an item
takes depends on whether you want it soon or merely want it done cheaply.

The API, as implemented (verified against Groq's published Batch docs)
----------------------------------------------------------------------
1. ``POST /v1/files``    ``purpose=batch``, a ``.jsonl`` file, max 50,000 lines
                         and 200 MB. Returns a ``file_...`` id.
2. ``POST /v1/batches``  ``input_file_id``, ``endpoint="/v1/chat/completions"``,
                         ``completion_window`` in ``24h``..``7d``. Returns a
                         ``batch_...`` id and a status.
3. ``GET  /v1/batches/{batch_id}``  poll until ``completed``; the response then
                         carries ``output_file_id`` (and ``error_file_id``).
4. ``GET  /v1/files/{file_id}/content``  the output as JSONL, one result per
                         line, each tagged with the ``custom_id`` we chose.

Statuses: validating -> in_progress -> finalizing -> completed, with failed /
expired / cancelled as terminal failures.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Iterable

logger = logging.getLogger(__name__)

#: Groq's documented limits for a batch input file.
MAX_BATCH_LINES = 50_000
MAX_BATCH_BYTES = 200 * 1024 * 1024

#: Terminal states from ``GET /v1/batches/{id}``. ``completed`` is the only good
#: one; the rest mean the batch will never produce results and must be
#: re-planned (typically the payload was rejected).
BATCH_COMPLETED = "completed"
BATCH_FAILED_STATES = frozenset({"failed", "expired", "cancelled"})

#: In-flight states; poll again later.
BATCH_PENDING_STATES = frozenset({"validating", "in_progress", "finalizing"})

CHAT_COMPLETIONS_URL = "/v1/chat/completions"


class GroqBatchError(RuntimeError):
    """Raised when a batch cannot be submitted or its results cannot be read."""


@dataclass(frozen=True)
class BatchSubmission:
    batch_id: str
    input_file_id: str
    line_count: int

    def __str__(self) -> str:  # pragma: no cover - logging convenience
        return f"{self.batch_id} ({self.line_count} requests)"


def build_batch_lines(
    requests: Iterable[tuple[str, str, dict[str, Any]]],
) -> list[str]:
    """Turn ``(custom_id, system_prompt, user_prompt)`` triples into JSONL lines.

    Each line is exactly the shape Groq's Files endpoint expects::

        {"custom_id": ..., "method": "POST", "url": "/v1/chat/completions",
         "body": {"model": ..., "messages": [...]}}

    Kept as a pure function so the request format can be asserted in tests
    without a network or an API key.
    """
    lines: list[str] = []
    for custom_id, system_prompt, user_prompt in requests:
        messages: list[dict[str, Any]] = []
        # An empty system prompt is the same as no system prompt: sending a
        # blank system turn is just noise that costs tokens on every request.
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        body: dict[str, Any] = {"messages": messages}
        lines.append(
            json.dumps(
                {
                    "custom_id": custom_id,
                    "method": "POST",
                    "url": CHAT_COMPLETIONS_URL,
                    "body": body,
                },
                ensure_ascii=False,
            )
        )
    return lines


def encode_batch_file(lines: list[str]) -> bytes:
    """Serialize JSONL, refusing anything Groq would reject at upload time."""
    if not lines:
        raise GroqBatchError("refusing to upload an empty batch file")
    if len(lines) > MAX_BATCH_LINES:
        raise GroqBatchError(
            f"batch has {len(lines)} requests, over Groq's {MAX_BATCH_LINES} line limit; "
            "split it into several batches"
        )
    payload = ("\n".join(lines) + "\n").encode("utf-8")
    if len(payload) > MAX_BATCH_BYTES:
        raise GroqBatchError(
            f"batch file is {len(payload)} bytes, over Groq's {MAX_BATCH_BYTES} byte limit"
        )
    return payload


def parse_result_line(line: str) -> tuple[str, str | None, str | None]:
    """One output line -> ``(custom_id, content, error)``.

    Groq echoes the ``custom_id`` we supplied, which is how a result is matched
    back to the item it belongs to. A per-request error comes back in the same
    line, and must not be confused with a whole-batch failure.
    """
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        raise GroqBatchError(f"unparseable batch result line: {exc}") from exc

    custom_id = record.get("custom_id") or ""
    error = record.get("error")
    if isinstance(error, dict):
        error = error.get("message") or json.dumps(error)
    if error:
        return custom_id, None, str(error)

    response = record.get("response") or {}
    body = response.get("body") or {}
    choices = body.get("choices") or []
    if not choices:
        # A successful batch job can still contain an individual request that
        # produced nothing usable (empty choices, content filter). Surface it as
        # an error for that one item rather than silently marking it analysed.
        return custom_id, None, "batch result contained no choices"
    message = choices[0].get("message") or {}
    return custom_id, message.get("content"), None


def parse_result_file(content: bytes) -> dict[str, tuple[str | None, str | None]]:
    """Parse a whole output file into ``{custom_id: (content, error)}``."""
    results: dict[str, tuple[str | None, str | None]] = {}
    for line in content.decode("utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        custom_id, text, error = parse_result_line(line)
        if custom_id:
            results[custom_id] = (text, error)
    return results


class GroqBatchClient:
    """Thin async wrapper over Groq's Files + Batches endpoints.

    Uses plain HTTP rather than the SDK: the SDK is synchronous, and the batch
    calls happen on the same event loop that serves health checks and metrics.
    """

    def __init__(self, api_key: str, *, base_url: str = "https://api.groq.com/openai/v1",
                 timeout: float = 120.0, completion_window: str = "24h") -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.completion_window = completion_window

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    def _headers(self, *, multipart: bool = False) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if not multipart:
            headers["Content-Type"] = "application/json"
        return headers

    async def upload_batch_file(self, content: bytes, filename: str = "batch.jsonl") -> str:
        """POST the JSONL file to ``/v1/files``; returns the file id."""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/files",
                    headers=self._headers(multipart=True),
                    files={"file": (filename, content, "application/jsonl")},
                    data={"purpose": "batch"},
                )
                response.raise_for_status()
                return response.json()["id"]
        except Exception as exc:
            raise GroqBatchError(f"batch file upload failed: {exc}") from exc

    async def create_batch(self, input_file_id: str) -> str:
        """POST ``/v1/batches``; returns the batch id."""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    f"{self.base_url}/batches",
                    headers=self._headers(),
                    json={
                        "input_file_id": input_file_id,
                        "endpoint": CHAT_COMPLETIONS_URL,
                        "completion_window": self.completion_window,
                    },
                )
                response.raise_for_status()
                return response.json()["id"]
        except Exception as exc:
            raise GroqBatchError(f"batch creation failed: {exc}") from exc

    async def submit(self, lines: list[str], *, model: str) -> BatchSubmission:
        """Upload and create a batch in one step."""
        if not model:
            raise GroqBatchError("a model id is required to submit a batch")
        # Stamp the model onto every request. This has to be written back into
        # the serialized line, not just into a parsed copy: without it the file
        # goes out with no model and Groq rejects the whole batch at validation.
        stamped: list[str] = []
        for line in lines:
            record = json.loads(line)
            record["body"]["model"] = model
            stamped.append(json.dumps(record, ensure_ascii=False))
        content = encode_batch_file(stamped)
        file_id = await self.upload_batch_file(content)
        batch_id = await self.create_batch(file_id)
        logger.info(
            "Submitted Groq batch %s (%d requests, model=%s, window=%s)",
            batch_id, len(stamped), model, self.completion_window,
        )
        return BatchSubmission(batch_id=batch_id, input_file_id=file_id, line_count=len(stamped))

    async def status(self, batch_id: str) -> dict[str, Any]:
        """GET the current state of a batch."""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/batches/{batch_id}", headers=self._headers()
                )
                response.raise_for_status()
                return response.json()
        except Exception as exc:
            raise GroqBatchError(f"batch status check failed: {exc}") from exc

    async def download_results(self, file_id: str) -> bytes:
        """GET an output file's contents."""
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.get(
                    f"{self.base_url}/files/{file_id}/content",
                    headers=self._headers(),
                )
                response.raise_for_status()
                return response.content
        except Exception as exc:
            raise GroqBatchError(f"batch result download failed: {exc}") from exc

    async def collect(self, batch_id: str) -> dict[str, tuple[str | None, str | None]]:
        """Poll once and return results if the batch is done, else ``{}``.

        Non-blocking by design: the caller loops on its own schedule, so this
        never turns a 24-hour batch into a 24-hour ``await``.
        """
        state = await self.status(batch_id)
        status = str(state.get("status") or "")
        if status != BATCH_COMPLETED:
            if status in BATCH_FAILED_STATES:
                raise GroqBatchError(
                    f"batch {batch_id} ended as {status}; requests must be re-planned"
                )
            logger.debug("Batch %s still %s", batch_id, status or "unknown")
            return {}
        output_file_id = state.get("output_file_id")
        if not output_file_id:
            raise GroqBatchError(f"batch {batch_id} completed without an output file")
        return parse_result_file(await self.download_results(output_file_id))
