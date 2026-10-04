"""
Transcribe Worker — audio, off the crawl fast path.

Consumes ``audio.requests`` (``AudioTranscriptionRequest``), which any crawl
worker (surface/deep/dark) publishes when it encounters an audio payload.
Because transcription is far heavier than HTML/PDF/DOCX extraction, it is
deliberately isolated in its own process: the crawl workers hand the URL off
and keep crawling, so transcription can never block or slow them.

Flow:
    crawl worker → audio.requests → transcribe-worker → crawl.raw → parser → ...

The worker emits an ordinary ``CrawlResult`` (``content_kind="audio"``) so
everything downstream is unchanged, and settles the outstanding task the crawl
worker registered before handing off.

The transcription backend is pluggable and lazy-imported: ``faster-whisper`` is
NOT a core dependency, so a missing install degrades to a clear per-item
failure instead of crashing worker startup. Enable/disable via
``TRANSCRIBE_ENABLED``; model via ``TRANSCRIBE_MODEL``.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import tempfile

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import ValidationError

# --- Path Setup ---
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

# --- Environment-aware settings ---
APP_ENV = os.getenv("APP_ENV")
if APP_ENV == "wsl":
    from app.common.config import wsl_settings  # noqa

from app.common.config.settings import settings
from app.common.job_events import install_job_log_relay, publish_job_stage
from app.common.logger.logger import setup_logging as _setup_worker_logging
from app.pipeline.schemas import AudioTranscriptionRequest, CrawlResult
from app.services.raw_object_store import RawObjectRef, RawObjectStore
from app.storage.postgres.client import pg_client

RAW_STORE = RawObjectStore()
from workers.health import HealthServer
from workers.metrics import WorkerMetrics

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
if APP_ENV == "docker":
    _setup_worker_logging(level=logging.INFO, json_output=True)
else:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kafka configuration
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS: str = settings.KAFKA_BOOTSTRAP_SERVERS
CONSUME_TOPIC: str = settings.audio_request_topic
PRODUCE_TOPIC: str = settings.crawl_raw_topic
WORKER_TYPE: str = "transcribe"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DukaScraperTranscribe/1.0)",
    "Accept": "audio/*,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# Transcription backend (pluggable, lazy)
# ---------------------------------------------------------------------------


_AUDIO_SUFFIX_BY_MIME = {
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/wave": ".wav",
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/flac": ".flac",
    "audio/x-flac": ".flac",
    "audio/aac": ".aac",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
    "video/mp4": ".mp4",
    "video/webm": ".webm",
    "video/ogg": ".ogv",
}


def _decode_to_pcm16k(payload: bytes, content_type: str | None = None) -> "numpy.ndarray":
    """Decode any audio container to the mono 16 kHz float32 array whisper wants.

    Uses the ffmpeg CLI (already installed in this image) instead of
    faster-whisper's PyAV decoder, and hands the result over as a NumPy array.

    Why not just pass the file to ``whisper.transcribe(path)``: that routes
    through ``av.open(..., metadata_errors="ignore")``, and the PyAV release
    that resolves on this image (19.x) removed that keyword — so every item
    died with ``open() got an unexpected keyword argument 'metadata_errors'``
    while the model itself was perfectly happy. Pinning an older PyAV is not
    available for this Python version. Decoding here sidesteps the version
    coupling entirely: faster-whisper skips its decoder when handed an array.
    """
    import subprocess

    import numpy

    # ffmpeg picks the demuxer from the file extension/content, so a generic
    # ".audio" suffix makes it bail with "Invalid data found" before it ever
    # looks at the bytes. Give it the real container extension.
    mime = (content_type or "").split(";", 1)[0].strip().lower()
    suffix = _AUDIO_SUFFIX_BY_MIME.get(mime, ".bin")

    with tempfile.NamedTemporaryFile(suffix=suffix, delete=True) as tmp:
        tmp.write(payload)
        tmp.flush()
        result = subprocess.run(
            [
                "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error",
                "-i", tmp.name,
                "-f", "s16le", "-acodec", "pcm_s16le",
                "-ac", "1", "-ar", "16000",
                "-",
            ],
            capture_output=True,
            check=False,
        )
    if result.returncode != 0 or not result.stdout:
        raise RuntimeError(
            "ffmpeg could not decode the audio payload: "
            f"{result.stderr.decode('utf-8', 'ignore')[:200] or 'no output'}"
        )
    return numpy.frombuffer(result.stdout, dtype=numpy.int16).astype(numpy.float32) / 32768.0


def transcribe_bytes(payload: bytes, *, model: str, content_type: str | None = None) -> str:
    """Transcribe an audio payload with faster-whisper.

    Lazily imports the backend so the worker (and its tests) do not require a
    GPU/whisper install. Raises ``RuntimeError`` with an actionable message when
    the backend is unavailable — the caller reports it per item.
    """
    try:
        from faster_whisper import WhisperModel
    except ImportError as exc:  # pragma: no cover - depends on install
        raise RuntimeError(
            "faster-whisper is not installed; install it in the transcribe "
            "worker image (or set TRANSCRIBE_ENABLED=false) to transcribe audio"
        ) from exc

    audio = _decode_to_pcm16k(payload, content_type)
    whisper = WhisperModel(model, device="auto", compute_type="auto")
    segments, _info = whisper.transcribe(audio)
    return " ".join(segment.text.strip() for segment in segments).strip()


# ---------------------------------------------------------------------------
# Message processing
# ---------------------------------------------------------------------------


async def _fetch_audio(url: str, *, attempts: int = 4) -> tuple[bytes, str, int]:
    """Download the audio payload. Returns (payload, content_type, status).

    Retried with exponential backoff. Every other network hop in this pipeline
    retries, and the audio hop was the exception: a single transient connect
    failure ("All connection attempts failed") failed the whole job, even though
    the identical URL succeeded seconds later. The payload is fetched fresh each
    attempt rather than cached, so a partial body is never reused.
    """
    last_error: Exception | None = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            async with httpx.AsyncClient(
                headers=DEFAULT_HEADERS,
                follow_redirects=True,
                timeout=settings.TRANSCRIBE_FETCH_TIMEOUT_SECONDS,
                trust_env=False,
            ) as client:
                response = await client.get(url)
                response.raise_for_status()
                return (
                    response.content,
                    response.headers.get("content-type", ""),
                    response.status_code,
                )
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            delay = min(2 ** attempt, 8)
            logger.warning(
                "Audio download attempt %d/%d failed for %s (%s: %s); retrying in %ds",
                attempt, attempts, url, type(exc).__name__, exc, delay,
            )
            await asyncio.sleep(delay)
    raise RuntimeError(
        f"could not download {url} after {attempts} attempts: {last_error}"
    )


async def _obtain_audio(
    request: AudioTranscriptionRequest,
) -> tuple[bytes, str, int, "RawObjectRef | None"]:
    """Return ``(payload, content_type, status_code, stored_ref)``.

    Reads the raw bucket first when this exact audio was archived before — on a
    Kafka redelivery, a worker restart, or a job retried after a downstream
    failure. That is the point of retaining the original: audio is the largest
    thing this pipeline moves, re-downloading it is the single biggest waste,
    and the origin may well be gone or rate-limiting by the second attempt.
    Downloads only when nothing is stored.
    """
    if request.raw_object_path:
        cached = await RAW_STORE.get(request.raw_object_path)
        if cached:
            logger.info(
                "[%s] Reusing stored audio %s (%d bytes) — no re-download",
                request.job_id, request.raw_object_path, len(cached),
            )
            # Rebuild the reference from the stored object so the digest and the
            # real content type reach the item row rather than arriving blank.
            return (
                cached,
                request.content_type or RAW_STORE.build_ref(
                    job_id=request.job_id,
                    item_id=request.item_id,
                    url=request.url,
                    worker=request.worker_type,
                    payload=cached,
                    content_type=request.content_type,
                ).content_type,
                200,
                RAW_STORE.build_ref(
                    job_id=request.job_id,
                    item_id=request.item_id,
                    url=request.url,
                    worker=request.worker_type,
                    payload=cached,
                    content_type=request.content_type,
                ),
            )

    payload, content_type, status_code = await _fetch_audio(request.url)

    stored = await RAW_STORE.store(
        job_id=request.job_id,
        item_id=request.item_id,
        url=request.url,
        worker=request.worker_type,
        payload=payload,
        content_type=content_type or request.content_type,
    )
    return payload, content_type, status_code, stored


async def process_request(
    producer: AIOKafkaProducer, request: AudioTranscriptionRequest
) -> None:
    """Transcribe one audio URL and emit a CrawlResult, or record the failure.

    Always settles the outstanding task exactly once so the job can complete.
    """
    await publish_job_stage(
        job_id=request.job_id, url=request.url, stage="transcribing", state="active",
    )
    try:
        if not settings.TRANSCRIBE_ENABLED:
            raise RuntimeError("transcription disabled (TRANSCRIBE_ENABLED=false)")

        payload, content_type, status_code, stored_ref = await _obtain_audio(request)
        if len(payload) > settings.TRANSCRIBE_MAX_PAYLOAD_BYTES:
            raise RuntimeError(
                f"audio payload {len(payload)} bytes exceeds "
                f"{settings.TRANSCRIBE_MAX_PAYLOAD_BYTES}"
            )

        text = await asyncio.to_thread(
            transcribe_bytes,
            payload,
            model=settings.TRANSCRIBE_MODEL,
            content_type=content_type or request.content_type,
        )

        result = CrawlResult(
            job_id=request.job_id,
            item_id=request.item_id,
            url=request.url,
            worker=request.worker_type,
            language=request.language,
            html=text,
            status_code=status_code,
            network=request.network,
            content_kind="audio",
            content_type=content_type or request.content_type,
            raw_object_path=stored_ref.path if stored_ref else None,
            raw_content_type=(stored_ref.content_type if stored_ref else (content_type or request.content_type)),
            raw_size_bytes=stored_ref.size_bytes if stored_ref else len(payload),
            raw_sha256=stored_ref.sha256 if stored_ref else None,
        )
        await producer.send_and_wait(
            PRODUCE_TOPIC,
            value=result.model_dump_json().encode("utf-8"),
            key=request.job_id.encode("utf-8"),
        )
        # Mirror the crawl workers' synchronous-extraction bookkeeping. The job
        # finalizer decides "completed" vs "failed" from the presence of a
        # crawl_log row with status='completed', and it runs the moment the
        # outstanding-task counter reaches zero. Without this row the audio path
        # only ever logged 'audio_handoff/passed', so every transcribed job was
        # reported as failed ("No pages were stored") even though the transcript
        # was published and parsed moments later.
        await pg_client.record_crawl_log(
            request.job_id,
            request.item_id,
            request.url,
            request.worker_type,
            "audio_transcribed",
            "completed",
        )
        await publish_job_stage(
            job_id=request.job_id, url=request.url, stage="site_finished", state="passed",
            detail=f"audio transcribed ({len(text)} chars)",
        )
        logger.info(
            "[%s] Transcribed %s (%d chars)", request.job_id, request.url, len(text)
        )
    except Exception as exc:
        # A failed transcription must not fail the whole crawl; record it and
        # settle the task so the job can still complete.
        logger.error("[%s] Transcription failed for %s: %s", request.job_id, request.url, exc)
        await publish_job_stage(
            job_id=request.job_id, url=request.url, stage="site_finished", state="failed",
            detail=f"transcription failed: {str(exc)[:200]}",
        )
    finally:
        await pg_client.complete_job_task(request.job_id)


async def process_message_safely(
    producer: AIOKafkaProducer, message_value: bytes
) -> None:
    try:
        request = AudioTranscriptionRequest(**json.loads(message_value))
    except (json.JSONDecodeError, ValidationError, TypeError) as exc:
        logger.error("Skipping malformed audio request: %s", exc)
        return
    try:
        await process_request(producer, request)
    except Exception as exc:
        # process_request settles the task itself; this is a last-resort guard.
        logger.error("[%s] Transcribe task crashed: %s", request.job_id, exc, exc_info=True)
        await pg_client.complete_job_task(request.job_id)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def main() -> None:
    health = HealthServer(
        worker_name=WORKER_TYPE,
        port=int(os.getenv("HEALTH_PORT", "8080")),
    )
    metrics = WorkerMetrics(worker_name=WORKER_TYPE, topic=CONSUME_TOPIC)
    await health.start()
    await metrics.start()

    await pg_client.connect()
    install_job_log_relay()

    consumer = AIOKafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"{WORKER_TYPE}-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        session_timeout_ms=30_000,
        heartbeat_interval_ms=10_000,
        max_poll_interval_ms=600_000,  # transcription can be slow
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        acks="all",
    )

    await producer.start()
    await consumer.start()
    health.mark_ready()
    logger.info(
        "TRANSCRIBE WORKER ONLINE — consuming '%s' → publishing '%s' (model=%s, enabled=%s)",
        CONSUME_TOPIC, PRODUCE_TOPIC, settings.TRANSCRIBE_MODEL, settings.TRANSCRIBE_ENABLED,
    )

    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()

    def request_shutdown() -> None:
        logger.info("Shutdown signal received.")
        shutdown_event.set()

    for sig_name in ("SIGTERM", "SIGINT"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, request_shutdown)
            except (NotImplementedError, RuntimeError):
                logger.debug("Signal handler unavailable for %s", sig_name)

    tasks: set[asyncio.Task] = set()

    def _task_done(t: asyncio.Task) -> None:
        tasks.discard(t)
        try:
            t.result()
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Transcribe task failed: %s", exc, exc_info=True)

    try:
        while not shutdown_event.is_set():
            try:
                message = await asyncio.wait_for(consumer.getone(), timeout=1.0)
            except TimeoutError:
                continue
            metrics.record_consumed()
            task = asyncio.create_task(process_message_safely(producer, message.value))
            tasks.add(task)
            task.add_done_callback(_task_done)
    except asyncio.CancelledError:
        logger.info("Transcribe worker cancellation requested.")
    except Exception as exc:
        logger.error("Transcribe worker stopped: %s", exc, exc_info=True)
    finally:
        shutdown_event.set()
        try:
            await health.stop()
            await metrics.stop()
        except Exception as exc:
            logger.error("Health/metrics server shutdown error: %s", exc)
        if tasks:
            logger.info("Waiting for %d active tasks …", len(tasks))
            await asyncio.gather(*tasks, return_exceptions=True)
        for name, closer in (
            ("consumer", consumer.stop),
            ("producer", producer.stop),
            ("postgres", pg_client.close),
        ):
            try:
                await closer()
                logger.info("%s stopped.", name)
            except Exception as exc:
                logger.error("%s shutdown error: %s", name, exc)
        logger.info("TRANSCRIBE WORKER SHUTDOWN COMPLETE")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Worker execution interrupted by user.")
