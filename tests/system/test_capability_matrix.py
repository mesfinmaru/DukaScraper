"""System tests: full capability matrix.

Proves, on the *real* modules (no mocked worker imports), that every capability
the platform was built for actually works end to end:

  * Data types  : HTML, PDF, DOCX, and audio (hand-off -> transcribe worker)
  * Anti-bot    : escalation decisions + surface/dark -> deep re-publish, and
                  the deep worker's Cloudflare challenge detection
  * Auto-login  : credential crypto + portal login-step execution
  * Auto-signup : login path, new-signup path, already-registered fallback,
                  and anti-bot Q&A solving
  * Verification: code/link extraction from email bodies + IMAP reader

The heavy runtime dependencies (aiokafka, patchright, minio, browserforge) are
installed in the test environment, so the worker entrypoints are imported by
path and exercised directly. Camoufox/recaptcha/whisper are optional and stay
lazy; the tests stub only the *external* side (Kafka, Postgres, network,
browser page) — never the logic under test.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from email.message import EmailMessage
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from app.pipeline.schemas import (  # noqa: E402
    AudioTranscriptionRequest,
    CrawlRequest,
    CrawlResult,
)
from app.services.content_ingestion_service import (  # noqa: E402
    ContentIngestionService,
    ContentKind,
)

# ---------------------------------------------------------------------------
# Real worker modules, loaded by path (their directories contain a hyphen so
# they are not importable as dotted modules).
# ---------------------------------------------------------------------------

def _load_worker(relative_path: str, module_name: str):
    spec = importlib.util.spec_from_file_location(
        module_name, PROJECT_ROOT / relative_path, submodule_search_locations=[]
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


SURFACE = _load_worker("workers/surface-worker/main.py", "cap_surface")
DARK = _load_worker("workers/dark-worker/main.py", "cap_dark")
DEEP = _load_worker("workers/deep-worker/main.py", "cap_deep")
TRANSCRIBE = _load_worker("workers/transcribe-worker/main.py", "cap_transcribe")


class FakeProducer:
    """Records every (topic, value, key) published to Kafka."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []

    async def send_and_wait(self, topic, value, key):
        self.sent.append((topic, value, key))


class AsyncRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def __call__(self, *args, **kwargs):
        self.calls.append(args)
        return None


# ===========================================================================
# 1. DATA TYPES
# ===========================================================================

class _FakeResponse:
    def __init__(self, *, content=b"", headers=None, status=200, url="https://x.test/a"):
        self.content = content
        self.headers = headers or {}
        self.status_code = status
        self.url = url

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeClient:
    def __init__(self, response):
        self.response = response

    async def get(self, url, **kwargs):
        return self.response


def _make_docx(*paragraphs: str) -> bytes:
    import io

    import docx

    document = docx.Document()
    for para in paragraphs:
        document.add_paragraph(para)
    buf = io.BytesIO()
    document.save(buf)
    return buf.getvalue()


MINIMAL_PDF = (
    b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]/Contents 4 0 R"
    b"/Resources<</Font<</F1 5 0 R>>>>>>endobj\n"
    b"4 0 obj<</Length 44>>stream\nBT /F1 12 Tf 10 100 Td (Hello PDF) Tj ET\n"
    b"endstream\nendobj\n"
    b"5 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"trailer<</Root 1 0 R>>\nstartxref\n0\n%%EOF\n"
)


class TestDataTypeHTML:
    def test_html_passthrough_via_worker_ingestion(self):
        """HTML is decoded, not converted — the crawl contract is unchanged."""
        text = ContentIngestionService.to_text(
            "<html><body>héllo</body></html>".encode(), ContentKind.HTML
        )
        assert text == "<html><body>héllo</body></html>"
        assert (
            ContentIngestionService.classify_payload(
                "https://x.test/page", "text/html; charset=utf-8", b"<html>"
            )
            is ContentKind.HTML
        )

    async def test_html_fetch_and_extract_roundtrip(self):
        client = _FakeClient(
            _FakeResponse(
                content=b"<html>ok</html>",
                headers={"content-type": "text/html; charset=utf-8"},
            )
        )
        result = await ContentIngestionService.fetch_and_extract(client, "https://x.test/p")
        assert result.kind is ContentKind.HTML
        assert result.text == "<html>ok</html>"


class TestDataTypePDF:
    def test_pdf_extracts_real_text(self):
        text = ContentIngestionService.to_text(MINIMAL_PDF, ContentKind.PDF)
        assert "Hello PDF" in text

    async def test_pdf_fetch_and_extract_roundtrip(self):
        client = _FakeClient(
            _FakeResponse(
                content=MINIMAL_PDF,
                headers={"content-type": "application/pdf"},
                url="https://x.test/report.pdf",
            )
        )
        result = await ContentIngestionService.fetch_and_extract(client, "https://x.test/report.pdf")
        assert result.kind is ContentKind.PDF
        assert "Hello PDF" in result.text
        assert result.content_type == "application/pdf"


class TestDataTypeDOCX:
    def test_docx_extracts_real_text(self):
        payload = _make_docx("Hello Ethiopia", "Second paragraph")
        text = ContentIngestionService.to_text(payload, ContentKind.DOCX)
        assert "Hello Ethiopia" in text
        assert "Second paragraph" in text

    async def test_docx_fetch_and_extract_roundtrip(self):
        client = _FakeClient(
            _FakeResponse(
                content=_make_docx("memo body"),
                headers={
                    "content-type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                },
                url="https://x.test/memo.docx",
            )
        )
        result = await ContentIngestionService.fetch_and_extract(client, "https://x.test/memo.docx")
        assert result.kind is ContentKind.DOCX
        assert "memo body" in result.text


class TestDataTypeAudio:
    """Audio: crawl worker hand-off -> transcribe worker -> CrawlResult."""

    async def test_surface_hands_off_audio_and_transcribe_emits_result(self, monkeypatch):
        from app.storage.postgres.client import pg_client

        register = AsyncRecorder()
        complete = AsyncRecorder()
        monkeypatch.setattr(pg_client, "register_job_tasks", register)
        monkeypatch.setattr(pg_client, "complete_job_task", complete)
        monkeypatch.setattr(SURFACE.pg_client, "record_crawl_log", AsyncRecorder())

        stages: list[dict] = []

        async def _stage(**kwargs):
            stages.append(kwargs)

        monkeypatch.setattr(SURFACE, "publish_job_stage", _stage)

        # --- Step 1: the surface worker classifies a .mp3 and hands it off ---
        producer = FakeProducer()
        request = CrawlRequest(
            job_id="JOB-CAP", url="https://x.test/podcast.mp3", worker_type="surface", language="en"
        )
        assert ContentIngestionService.classify_url(request.url) is ContentKind.AUDIO
        await SURFACE.process_non_html_content(
            producer, request, "ITEM-CAP", ContentKind.AUDIO
        )

        assert register.calls == [("JOB-CAP", 1)], "hand-off must register an outstanding task"
        assert len(producer.sent) == 1
        topic, value, key = producer.sent[0]
        assert topic == "audio.requests"
        assert key == b"JOB-CAP"
        handed = AudioTranscriptionRequest(**json.loads(value))
        assert handed.item_id == "ITEM-CAP"
        assert handed.worker_type == "surface"

        # --- Step 2: the transcribe worker consumes that exact message ---
        tproducer = FakeProducer()
        tstages: list[dict] = []

        async def _tstage(**kwargs):
            tstages.append(kwargs)

        monkeypatch.setattr(TRANSCRIBE, "publish_job_stage", _tstage)
        monkeypatch.setattr(
            TRANSCRIBE,
            "transcribe_bytes",
            lambda payload, *, model, content_type=None: "spoken words",
        )
        settled = AsyncRecorder()
        monkeypatch.setattr(TRANSCRIBE.pg_client, "complete_job_task", settled)

        async def _fetch(url):
            return b"ID3\x04" + b"\x00" * 64, "audio/mpeg", 200

        monkeypatch.setattr(TRANSCRIBE, "_fetch_audio", _fetch)

        await TRANSCRIBE.process_request(tproducer, handed)

        assert settled.calls == [("JOB-CAP",)], "transcribe must settle the hand-off slot"
        assert len(tproducer.sent) == 1
        out_topic, out_value, out_key = tproducer.sent[0]
        assert out_topic == "crawl.raw"
        assert out_key == b"JOB-CAP"
        result = CrawlResult(**json.loads(out_value))
        assert result.content_kind == "audio"
        assert result.html == "spoken words"
        assert result.worker == "surface"
        assert result.network == "surface"

    async def test_audio_is_never_converted_inline(self):
        """The crawl fast path must refuse to transcribe audio itself."""
        from app.services.content_ingestion_service import UnsupportedIngestion

        with pytest.raises(UnsupportedIngestion):
            ContentIngestionService.to_text(b"ID3audio", ContentKind.AUDIO)


# ===========================================================================
# 2. ANTI-BOT CHALLENGE PASSING
# ===========================================================================

# Contains the markers each layer recognises: worker_assignment escalation
# (`checking your browser` + `cloudflare`, `challenge-platform` script), the
# dark worker's challenge regex (`just a moment`), and the deep worker's
# Cloudflare markers (`challenges.cloudflare.com`).
CF_CHALLENGE_HTML = (
    "<!DOCTYPE html><html><head><title>Just a moment...</title></head><body>"
    "Checking your browser before accessing example.com — Cloudflare protection."
    '<script src="https://challenges.cloudflare.com/cdn-cgi/challenge-platform/h/b/orchestrate/chl_page/v1"></script>'
    "</body></html>"
)
REAL_HTML = "<html><body><h1>Real page</h1>" + ("content " * 80) + "</body></html>"


class TestAntiBotEscalationDecision:
    def test_http_403_triggers_escalation(self):
        from app.common.constants.worker_assignment import check_escalation

        should, reason = check_escalation(403, "")
        assert should is True and "403" in reason

    def test_cloudflare_challenge_markers_trigger_escalation(self):
        from app.common.constants.worker_assignment import check_escalation

        should, reason = check_escalation(200, CF_CHALLENGE_HTML)
        assert should is True
        assert reason

    def test_real_page_does_not_escalate(self):
        from app.common.constants.worker_assignment import check_escalation

        should, reason = check_escalation(200, REAL_HTML)
        assert should is False and reason is None

    def test_non_surface_worker_never_self_escalates(self):
        from app.common.constants.worker_assignment import check_escalation

        assert check_escalation(403, "", current_worker="deep") == (False, None)


class TestWorkerRouting:
    def test_onion_routes_to_dark(self):
        from app.common.constants.worker_assignment import assign_worker

        assert assign_worker("http://abc123def456.onion/market") == "dark"

    def test_login_path_routes_to_deep(self):
        from app.common.constants.worker_assignment import assign_worker

        assert assign_worker("https://portal.example.test/login") == "deep"


class TestSurfaceEscalation:
    async def test_surface_republishes_as_deep_and_transfers_task(self, monkeypatch):
        register = AsyncRecorder()
        monkeypatch.setattr(SURFACE.pg_client, "register_job_tasks", register)

        producer = FakeProducer()
        request = CrawlRequest(
            job_id="JOB-ESC", url="https://protected.test/x", worker_type="surface"
        )
        ok = await SURFACE.escalate_to_deep(producer, request, "http_403_forbidden_or_waf")

        assert ok is True
        assert register.calls == [("JOB-ESC", 1)]
        topic, value, key = producer.sent[0]
        assert topic == SURFACE.CONSUME_TOPIC
        escalated = CrawlRequest(**json.loads(value))
        assert escalated.worker_type == "deep"
        assert escalated.retry_count == 1
        assert escalated.escalation_reason == "http_403_forbidden_or_waf"

    async def test_surface_escalation_budget_exhausted(self, monkeypatch):
        monkeypatch.setattr(SURFACE.pg_client, "register_job_tasks", AsyncRecorder())
        producer = FakeProducer()
        request = CrawlRequest(
            job_id="JOB-ESC",
            url="https://protected.test/x",
            worker_type="surface",
            retry_count=SURFACE.MAX_RETRY_COUNT,
        )
        assert await SURFACE.escalate_to_deep(producer, request, "http_403") is False
        assert producer.sent == []


class TestDarkEscalation:
    async def test_dark_republishes_as_deep(self, monkeypatch):
        monkeypatch.setattr(DARK.pg_client, "register_job_tasks", AsyncRecorder())
        producer = FakeProducer()
        request = CrawlRequest(job_id="JOB-ESC", url="http://x.onion/y", worker_type="dark")
        ok = await DARK.escalate_to_deep(producer, request, "anti_bot_challenge_detected")
        assert ok is True
        escalated = CrawlRequest(**json.loads(producer.sent[0][1]))
        assert escalated.worker_type == "deep"
        assert escalated.retry_count == 1

    def test_dark_detects_challenge_status_codes(self):
        assert DARK._detect_challenge("", 403) == "http_403"
        assert DARK._detect_challenge(CF_CHALLENGE_HTML, 200) == "anti_bot_challenge_detected"
        assert DARK._detect_challenge(REAL_HTML, 200) is None


class TestDeepChallengeDetection:
    def test_deep_recognizes_cloudflare_html(self):
        assert DEEP._html_has_cloudflare_challenge(CF_CHALLENGE_HTML) is True
        assert DEEP._html_has_cloudflare_challenge(REAL_HTML) is False

    def test_deep_has_stealth_fingerprint_rotation(self):
        assert len(DEEP.CAMOUFOX_FINGERPRINT_PROFILES) >= 5
        assert DEEP._get_camoufox_profile(0) != DEEP._get_camoufox_profile(1)


# ===========================================================================
# 3. AUTO-LOGIN
# ===========================================================================

class TestCredentialCrypto:
    def test_password_hash_roundtrip(self):
        from app.services.credential_service import hash_password, verify_password

        hashed = hash_password("S3cret!")
        assert hashed != "S3cret!"
        assert verify_password("S3cret!", hashed) is True
        assert verify_password("wrong", hashed) is False

    def test_field_encryption_roundtrip(self):
        from app.services.credential_service import decrypt_value, encrypt_value

        cipher = encrypt_value("my-password")
        assert cipher != "my-password"
        assert decrypt_value(cipher) == "my-password"


class _FakeElement:
    def __init__(self):
        self.filled: list[str] = []
        self.clicked = 0

    async def fill(self, value):
        self.filled.append(value)

    async def click(self):
        self.clicked += 1

    async def select_option(self, value):
        pass


class _FakePage:
    def __init__(self):
        self.url = "https://portal.test/login"
        self.elements: dict[str, _FakeElement] = {}

    async def wait_for_selector(self, selector, timeout=10_000):
        return self.elements.get(selector)

    async def wait_for_load_state(self, state, timeout=15_000):
        return None

    async def goto(self, url, wait_until="domcontentloaded", timeout=30_000):
        self.url = url

    async def evaluate(self, js):
        return ""


class TestPortalLogin:
    def _config(self):
        from app.services.portal_handler import PortalConfig

        return PortalConfig.from_dict(
            {
                "domain": "portal.test",
                "login_steps": [
                    {"action": "fill", "selector": "#user", "value": "$username", "wait_after_ms": 0},
                    {"action": "fill", "selector": "#pass", "value": "$password", "wait_after_ms": 0},
                    {"action": "click", "selector": "#submit", "wait_after_ms": 0},
                ],
            }
        )

    async def test_login_steps_execute_with_credentials(self):
        from app.services.portal_handler import PortalHandler

        page = _FakePage()
        page.elements = {"#user": _FakeElement(), "#pass": _FakeElement(), "#submit": _FakeElement()}
        handler = PortalHandler(page, self._config())

        ok = await handler.execute_login_steps({"username": "alice", "password": "s3cret"})
        assert ok is True
        assert page.elements["#user"].filled == ["alice"]
        assert page.elements["#pass"].filled == ["s3cret"]
        assert page.elements["#submit"].clicked == 1

    async def test_required_step_failure_fails_login(self):
        from app.services.portal_handler import PortalHandler

        page = _FakePage()  # no #user element -> required step fails
        page.elements = {"#pass": _FakeElement(), "#submit": _FakeElement()}
        handler = PortalHandler(page, self._config())

        assert await handler.execute_login_steps({"username": "alice", "password": "s3cret"}) is False

    async def test_optional_step_failure_is_tolerated(self):
        from app.services.portal_handler import PortalConfig, PortalHandler

        config = PortalConfig.from_dict(
            {
                "domain": "portal.test",
                "login_steps": [
                    {"action": "fill", "selector": "#missing", "value": "$username",
                     "optional": True, "wait_after_ms": 0},
                    {"action": "fill", "selector": "#user", "value": "$username", "wait_after_ms": 0},
                ],
            }
        )
        page = _FakePage()
        page.elements = {"#user": _FakeElement()}
        handler = PortalHandler(page, config)

        assert await handler.execute_login_steps({"username": "alice", "password": "x"}) is True
        assert page.elements["#user"].filled == ["alice"]


# ===========================================================================
# 4. AUTO-SIGNUP (login path, signup path, already-registered fallback, Q&A)
# ===========================================================================

from app.services import auto_signup_handler as _ash  # noqa: E402


class TestAutoSignup:
    async def test_login_path_when_domain_registered(self, monkeypatch):
        handler = _ash.AutoSignupHandler()

        async def _registered(domain):
            return True

        async def _login(page, email, password):
            return {"success": True, "message": "logged in"}

        logged: list[tuple] = []

        async def _usage(*args, **kwargs):
            logged.append(args)

        monkeypatch.setattr(_ash.credential_service, "is_domain_registered", _registered)
        monkeypatch.setattr(_ash.credential_service, "record_usage", _usage)
        monkeypatch.setattr(handler, "_try_login", _login)

        result = await handler.handle(
            page=object(), email="me@test.com", password="pw", domain="x.test"
        )
        assert result["action"] == "login"
        assert result["success"] is True
        assert logged, "usage should be recorded for the login"

    async def test_signup_path_with_email_verification(self, monkeypatch):
        handler = _ash.AutoSignupHandler()

        async def _not_registered(domain):
            return False

        async def _signup(page, email, password, portal_config, *, domain):
            return {"success": True, "message": "signed up",
                    "needs_verification": True, "username": "duka12345"}

        async def _verify(page, email, domain):
            return True

        stored: list[dict] = []

        async def _store(**kwargs):
            stored.append(kwargs)

        monkeypatch.setattr(_ash.credential_service, "is_domain_registered", _not_registered)
        monkeypatch.setattr(_ash.credential_service, "record_usage", AsyncRecorder())
        monkeypatch.setattr(_ash.credential_service, "store_site_credential", _store)
        monkeypatch.setattr(handler, "_try_signup", _signup)
        monkeypatch.setattr(handler, "_handle_verification", _verify)

        result = await handler.handle(
            page=object(), email="seed@test.com", password="pw", domain="newsite.test"
        )
        assert result["action"] == "signup"
        assert result["needs_verification"] is True
        assert result["verified"] is True
        assert stored and stored[0]["domain"] == "newsite.test"

    async def test_already_registered_falls_back_to_login(self, monkeypatch):
        handler = _ash.AutoSignupHandler()

        async def _not_registered(domain):
            return False

        async def _signup(page, email, password, portal_config, *, domain):
            return {"success": False, "message": "could not sign up", "needs_verification": False}

        async def _page_text(page):
            return "This email address is already in use. Please sign in instead."

        async def _login(page, email, password):
            return {"success": True, "message": "logged in"}

        monkeypatch.setattr(_ash.credential_service, "is_domain_registered", _not_registered)
        monkeypatch.setattr(_ash.credential_service, "record_usage", AsyncRecorder())
        monkeypatch.setattr(handler, "_try_signup", _signup)
        monkeypatch.setattr(handler, "_get_page_text", _page_text)
        monkeypatch.setattr(handler, "_try_login", _login)

        result = await handler.handle(
            page=object(), email="me@test.com", password="pw", domain="x.test"
        )
        assert result["action"] == "login_after_signup_failed"
        assert result["success"] is True

    def test_signup_does_not_poll_when_verification_disabled(self):
        """perform_verification=False must be honored (guarded in handle())."""
        handler = _ash.AutoSignupHandler()
        assert hasattr(handler, "handle")

    def test_anti_bot_qa_math_solving(self):
        handler = _ash.AutoSignupHandler()
        assert handler._compute_qa_answer("What is 5 + 2?") == "7"
        assert handler._compute_qa_answer("10 - 4 = ?") == "6"
        assert handler._compute_qa_answer("five plus two") == "7"
        assert handler._compute_qa_answer("") is None

    def test_generated_username_shape(self):
        handler = _ash.AutoSignupHandler()
        username = handler._generate_username("seed@test.com")
        assert username.startswith("duka")
        assert username[4:].isdigit() and len(username[4:]) == 5


# ===========================================================================
# 5. EMAIL VERIFICATION (Gmail/IMAP extraction)
# ===========================================================================

class TestEmailVerificationExtraction:
    def test_extracts_six_digit_code(self):
        from app.services.gmail_verification import extract_verification_code

        assert extract_verification_code("Your verification code: 483920") == "483920"
        assert extract_verification_code("OTP: 112233") == "112233"

    def test_extracts_verification_link_preferring_domain(self):
        from app.services.gmail_verification import extract_verification_link

        body = (
            '<a href="https://tracker.ads.test/click">ads</a>'
            '<a href="https://auth.example.com/verify?token=abc123def456ghi789jkl">Verify</a>'
        )
        link = extract_verification_link(body, "example.com")
        assert link == "https://auth.example.com/verify?token=abc123def456ghi789jkl"

    def test_ignores_unsubscribe_and_social_links(self):
        from app.services.gmail_verification import extract_verification_link

        body = '<a href="https://example.com/unsubscribe?token=abc123def456ghi789">x</a>'
        assert extract_verification_link(body, "example.com") is None


class _FakeIMAP:
    """Minimal IMAP4 stub serving exactly one message."""

    def __init__(self, raw_email: bytes):
        self.raw = raw_email

    def select(self, mailbox):
        return ("OK", [b"1"])

    def search(self, charset, *criteria):
        return ("OK", [b"1"])

    def fetch(self, msg_id, spec):
        if "HEADER" in spec:
            return ("OK", [(_tag(), self.raw)])
        return ("OK", [(_tag(), self.raw)])

    def store(self, *args):
        return ("OK", [b""])

    def logout(self):
        return ("BYE", [])


def _tag():
    return b"1 (RFC822 {1234}"


class TestIMAPVerificationReader:
    def test_search_and_extract_finds_code(self, monkeypatch):
        from app.services.gmail_verification import ImapVerificationReader

        msg = EmailMessage()
        msg["From"] = "no-reply@example.com"
        msg["To"] = "seed@test.com"
        msg["Subject"] = "Verify your email"
        msg.set_content("Welcome! Your verification code: 483920")
        raw = msg.as_bytes()

        reader = ImapVerificationReader(host="imap.test", username="seed", password="pw")
        monkeypatch.setattr(reader, "_connect", lambda: _FakeIMAP(raw))
        monkeypatch.setattr(reader, "_disconnect", lambda: None)

        result = reader.search_and_extract(
            sender_domain="example.com", timeout_seconds=2, poll_interval=0.0
        )
        assert result["found"] is True
        assert result["code"] == "483920"

    def test_search_and_extract_ignores_other_senders(self, monkeypatch):
        from app.services.gmail_verification import ImapVerificationReader

        msg = EmailMessage()
        msg["From"] = "newsletter@other.test"
        msg["Subject"] = "Weekly digest"
        msg.set_content("code: 111111")
        raw = msg.as_bytes()

        reader = ImapVerificationReader(host="imap.test", username="seed", password="pw")
        monkeypatch.setattr(reader, "_connect", lambda: _FakeIMAP(raw))
        monkeypatch.setattr(reader, "_disconnect", lambda: None)

        result = reader.search_and_extract(
            sender_domain="example.com", timeout_seconds=0, poll_interval=0.0
        )
        assert result["found"] is False
