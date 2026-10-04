"""
Pydantic schemas for DukaScraper platform
Data contracts for API, Kafka messages, and databases
TEXT-ONLY crawling pipeline with source_type POST-ANALYSIS

NOTE on ID types:
PostgreSQL IDs are auto-generated STRINGS (not integers):
  - duka_system.users.user_id      -> VARCHAR(8)  e.g. "USR12345"
  - duka_system.jobs.job_id        -> VARCHAR(11) e.g. "JOB00000001"
  - duka_system.parsed_items.item_id -> VARCHAR(12) e.g. "ITEM00000001"
  - duka_system.exports.export_id  -> VARCHAR(11) e.g. "EXP00000001"
All schemas below use `str` for these IDs to match the real database schema.

NOTE on source_type:
REMOVED from initial job creation. Source type is determined AFTER crawling and parsing,
by LLM analysis during the intelligence extraction phase (llm-worker consumer).
This allows dynamic classification based on actual content rather than pre-specification.

NOTE on recursive crawling:
All workers (SURFACE, DEEP, DARK) now support depth-limited recursive crawling.
New fields: depth, max_depth, parent_url, recursive_config.
"""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from app.common.constants.content_topics import DEFAULT_CONTENT_TOPIC
from app.common.constants.intelligence_categories import DEFAULT_INTELLIGENCE_CATEGORY

# ============================================================================
# KAFKA MESSAGE SCHEMAS
# ============================================================================


class SearchRequest(BaseModel):
    """
    Schema for search.requests Kafka topic.

    Input for the discovery-worker: turns a topic/query into seed URLs by
    querying the search engines configured for ``network`` (configs/
    search_engines.json). This is deliberately network-agnostic - surface,
    deep, and dark all use the exact same message shape; ``network`` only
    selects which engine list is queried (dark engines go through Tor).

    The OUTPUT is NOT a new message type: the discovery-worker emits ordinary
    CrawlRequest messages on crawl.requests, so the three crawl workers need
    zero changes to consume a topic-derived seed instead of a hand-entered URL.
    """

    job_id: str = Field(..., description="Job identifier, e.g. 'JOB00000001'")
    query: str = Field(..., description="Topic / search query to discover seed URLs for")
    network: str = Field(
        default="surface",
        description="Which engine list to query: 'surface', 'deep', or 'dark'",
    )
    language: str = Field(default="en", description="Language: 'en', 'am', or 'all'")
    max_results: int = Field(
        default=10, description="Maximum seed URLs to fan out as CrawlRequests"
    )
    engines: list[str] | None = Field(
        default=None,
        description="Optional engine-name allowlist; None uses every enabled engine for the network",
    )
    max_depth: int = Field(
        default=5, description="Recursion ceiling applied to the emitted CrawlRequests"
    )
    recursive_config: dict = Field(
        default_factory=lambda: {"enable_extraction": True},
        description="Recursion rules copied onto every emitted CrawlRequest",
    )
    job_params: dict = Field(default_factory=dict, description="Additional per-job worker parameters")


class CrawlRequest(BaseModel):
    """
    Schema for crawl.requests Kafka topic
    Input for all crawl workers (surface, deep, dark)
    NOTE: source_type removed (determined post-parsing by LLM)
    """

    job_id: str = Field(..., description="Job identifier, e.g. 'JOB00000001'")
    url: str = Field(..., description="The URL to be crawled")
    language: str = Field(default="en", description="Language: 'en', 'am', or 'all'")
    worker_type: str = Field(
        ..., description="Worker type: 'surface', 'deep', or 'dark' (assigned by rules engine)"
    )
    job_params: dict = Field(default_factory=dict, description="Additional per-job worker parameters")

    # ========================================================================
    # RECURSIVE CRAWLING FIELDS (ALL WORKERS SUPPORT)
    # ========================================================================
    depth: int = Field(default=0, description="Current recursion level (0 = seed URL)")
    max_depth: int = Field(
        default=5, description="Circuit-breaker ceiling; standard recursion depth is 5"
    )
    parent_url: str | None = Field(
        default=None, description="Lineage tracking: URL that led to this child task"
    )
    recursive_config: dict = Field(
        default_factory=lambda: {"enable_extraction": True},
        description="Recursion rules: {enable_extraction: bool, link_filter_patterns: [...], skip_domains: [...], same_domain_only: bool, scope_prefix: str|None}",
    )

    # ========================================================================
    # WORKER ESCALATION (SURFACE → DEEP AUTO-REQUEUE)
    # ========================================================================
    retry_count: int = Field(default=0, description="Number of retries (incremented on escalation)")
    escalation_reason: str | None = Field(
        default=None, description="Why escalated (e.g., 'http_403', 'cloudflare_challenge', 'empty_html')"
    )

    # ========================================================================
    # AUTO-SIGNUP FREDENTIALS
    # ========================================================================
    auto_signup: bool = Field(
        default=False,
        description="Enable auto-signup: detect login vs signup, handle email verification",
    )
    credential_email: str | None = Field(
        default=None,
        description="Explicit credential email to use. If None and auto_signup=True, auto-assigns one.",
    )


class AudioTranscriptionRequest(BaseModel):
    """
    Schema for audio.requests Kafka topic.

    Published by ANY crawl worker (surface/deep/dark) when it fetches an audio
    payload. Audio transcription is far heavier than HTML/PDF/DOCX extraction,
    so it is deliberately NOT done inline: the crawl worker hands the URL off
    to the transcribe-worker and keeps crawling. The transcribe-worker fetches
    the audio, transcribes it, and emits a normal CrawlResult on crawl.raw, so
    everything downstream of the crawl workers is unchanged.
    """

    job_id: str = Field(..., description="Job identifier, e.g. 'JOB00000001'")
    item_id: str = Field(..., description="Item id allocated by the crawl worker")
    url: str = Field(..., description="URL of the audio payload")
    language: str = Field(default="en", description="Language: 'en', 'am', or 'all'")
    worker_type: str = Field(..., description="Worker that fetched it: 'surface', 'deep', or 'dark'")
    network: str = Field(default="surface", description="Network used: surface, deep, dark")
    content_type: str | None = Field(default=None, description="Reported audio content type")
    payload_size_bytes: int | None = Field(default=None, description="Size of the audio payload in bytes")
    raw_object_path: str | None = Field(
        default=None,
        description=(
            "s3:// path of the audio already stored in the raw bucket. The "
            "transcribe-worker reads from here instead of re-downloading."
        ),
    )


class CrawlResult(BaseModel):
    """
    Schema for crawl.raw Kafka topic
    Raw output produced by crawl workers
    NOTE: source_type removed; only url + html for downstream parsing

    NOTE on multi-format content:
    ``html`` remains the single content carrier for backward compatibility.
    For non-HTML payloads (PDF/DOCX) it holds the text extracted by the shared
    ContentIngestionService, and ``content_kind`` records what it was. Plain
    HTML crawls leave both defaults untouched.
    """

    job_id: str = Field(..., description="job_id of the originating CrawlRequest")
    item_id: str = Field(..., description="Per-page item identifier, e.g. 'ITEM00000001'")
    url: str
    html: str  # Raw HTML content, or extracted text for non-HTML (content_kind != 'html')
    status_code: int
    worker: str  # Which worker produced this ('surface', 'deep', 'dark')
    language: str
    network: str = Field(default="surface", description="Network used: surface, deep, dark")
    fetch_duration: float | None = None  # Crawl time in seconds

    # --- Multi-format content (additive; defaults preserve HTML behavior) ---
    content_kind: str = Field(
        default="html",
        description="Content type of `html`: 'html', 'pdf', 'docx', or 'audio'",
    )
    content_type: str | None = Field(
        default=None, description="Reported HTTP content-type, when known"
    )

    # ------------------------------------------------------------------
    # Raw retention
    # ------------------------------------------------------------------
    # When the producing worker already archived the ORIGINAL payload (a PDF,
    # a DOCX, an audio file) in the raw bucket, it says so here so the parser
    # reuses that object instead of overwriting it with the text it derives.
    # ``html`` still carries the extracted text either way.
    raw_object_path: str | None = Field(
        default=None,
        description="s3:// path of the already-stored original payload, when one exists",
    )
    raw_content_type: str | None = Field(
        default=None, description="Content type of the stored original payload"
    )
    raw_size_bytes: int | None = Field(
        default=None, description="Byte size of the stored original payload"
    )
    raw_sha256: str | None = Field(
        default=None, description="SHA-256 of the stored original payload"
    )

    # ========================================================================
    # RECURSIVE CRAWLING FIELDS
    # ========================================================================
    depth: int = Field(default=0, description="Recursion depth of this crawl")
    extracted_links: list[str] = Field(
        default_factory=list, description="Normalized absolute URLs extracted from HTML"
    )
    child_tasks_queued: int = Field(default=0, description="Count of child tasks enqueued")
    duplicate_links_skipped: int = Field(
        default=0, description="Count of links rejected by deduplication filter"
    )

    # ========================================================================
    # WORKER ESCALATION METADATA
    # ========================================================================
    was_escalated: bool = Field(default=False, description="Was this escalated from another worker?")
    escalation_reason: str | None = Field(default=None, description="Reason for escalation")


class ParsedItemData(BaseModel):
    """
    Schema for parsed item data field
    Extracted text and metadata (POST-PARSING, PRE-LLM)
    """

    extracted_text: str = Field(..., description="Clean extracted text")
    character_count: int = Field(..., description="Number of characters")
    original_status_code: int = Field(..., description="HTTP status code")
    title: str | None = Field(default=None, description="Extracted article title")
    publish_date: str | None = Field(
        default=None, description="Extracted publish date in ISO format"
    )
    detected_language: str | None = Field(
        default=None, description="Detected language from parsed content"
    )
    fetch_duration: float | None = Field(
        default=None, description="Time taken to fetch the data in seconds"
    )
    payload_size_bytes: int | None = Field(
        default=None, description="Size of the raw HTML payload in bytes"
    )
    proxy_ip: str | None = Field(
        default=None, description="The active proxy endpoint used during the request"
    )
    retry_count: int | None = Field(
        default=None, description="Number of retries triggered before successful ingestion"
    )
    portal_structured_data: dict | None = Field(
        default=None,
        description="Structured data extracted from portal pages (grades, student info, etc.)",
    )
    requested_language: str | None = Field(
        default=None,
        description="Language the job was requested in (e.g. 'am', 'en')",
    )
    language_mismatch: bool = Field(
        default=False,
        description="True when detected_language differs from requested_language",
    )
    language_rejection_reason: str | None = Field(
        default=None,
        description="Why the content was flagged: 'language_mismatch' or 'unsupported_language'",
    )
    content_kind: str = Field(
        default="html",
        description="Source payload kind the text came from: 'html', 'pdf', 'docx', or 'audio'",
    )
    # The parser computes these for every item. They were absent here, and
    # Pydantic drops undeclared keys on validation, so they were silently
    # discarded before the parsed object reached MinIO -- which is why the UI's
    # quality bar rendered "NaN%" and document sections never appeared.
    sections: list = Field(
        default_factory=list,
        description="Headings and their text, used to render a document outline",
    )
    content_quality_score: float | None = Field(
        default=None,
        description="Heuristic text-quality score between 0 and 1",
    )
    structure_valid: bool | None = Field(
        default=None,
        description="False when the page had no usable heading structure or leaked a banner",
    )
    banner_leakage: bool | None = Field(
        default=None,
        description="True when wiki/banner edit markers leaked into the extracted text",
    )


class ParsedItem(BaseModel):
    """
    Schema for crawl.parsed Kafka topic
    Structured data extracted by parser-worker (PRE-LLM ANALYSIS)
    NOTE: source_type NOT included; added by LLM worker during intelligence extraction
    """

    job_id: str = Field(..., description="job_id of the originating CrawlRequest")
    item_id: str = Field(
        ..., description="Unique item identifier, e.g. 'ITEM00000001' from PostgreSQL"
    )
    url: str
    worker: str  # Which worker crawled it
    language: str  # resolved ISO language code or 'unknown'
    data: ParsedItemData | dict = Field(..., description="Extracted text + metadata")
    status: str = "completed"  # completed or failed
    parse_duration: float | None = None  # Parse time in seconds


class IntelligenceAnalytics(BaseModel):
    """
    Schema for ClickHouse intelligence_analytics table
    Output of LLM analysis (POST-PARSING LLM-WORKER)

    category values (see app.common.constants.intelligence_categories.IntelligenceCategory):
      - data_leak        Credentials, corporate DB dumps, PII, leaks
      - gov_issue         Regional stability, policy/political leaks, public interest
      - cyber_threat      Exploits, ransomware, malware, C2 infrastructure, DDoS
      - physical_threat   Violent extremism, illicit market contraband, sabotage
      - misinformation    Coordinated disinfo campaigns, propaganda, astroturfing
      - other             Low-value / general noise
    """

    job_id: str = Field(..., description="Source job ID")
    item_id: str = Field(..., description="Parsed item ID from PostgreSQL")
    url: str = Field(..., description="Source URL")
    source_type: str = Field(
        ..., description="Inferred by LLM: 'news', 'forum', 'blog', 'social', 'gov', 'academic', 'ecommerce', 'other', etc."
    )
    topic: str = Field(
        default=DEFAULT_CONTENT_TOPIC,
        description="Subject matter, independent of threat category: economics, politics, health, technology, security, environment, society, other",
    )
    category: str = Field(
        default=DEFAULT_INTELLIGENCE_CATEGORY,
        description="LLM classification: 'data_leak', 'gov_issue', 'cyber_threat', 'physical_threat', 'misinformation', 'other'",
    )
    threat_severity: int = Field(
        ..., description="Threat severity (1-5), 1=low, 5=critical"
    )
    entities: list[str] = Field(
        default_factory=list, description="Extracted entities (names, domains, IPs, etc.)"
    )
    summary: str = Field(..., description="LLM-generated summary of findings")
    language: str = Field(default="unknown", description="Resolved content language")
    analysis_source: str = Field(
        default="llm",
        description=(
            "Provenance of this record: 'llm' = a real model produced it; "
            "'fallback' = the rule-based heuristic did because no model was "
            "reachable; 'partial' = a model answered but its labels could not be "
            "parsed; 'unavailable' = nothing could analyze the payload; "
            "'legacy' = written before this column existed, so provenance is "
            "unknowable. Human-evaluation accuracy is only meaningful over "
            "'llm' rows."
        ),
    )
    llm_model: str = Field(default="qwen2:8b", description="Which LLM model performed analysis")
    llm_score: float | None = Field(default=None, description="Deprecated: no per-response confidence is inferred")
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC), description="Analysis timestamp")


# ============================================================================
# DATABASE SCHEMAS (duka_system - PostgreSQL)
# ============================================================================


class JobRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.jobs table.
    Jobs are an orchestration container; worker ownership is recorded at the
    parsed-item level because each job can be processed by multiple workers.
    """

    job_id: str | None = None  # Auto-generated: JOB00000001
    user_id: str  # References users.user_id, e.g. USR12345
    url: str
    language: str = "unknown"  # requested language or resolved content language
    status: str = "pending"  # pending, running, completed, failed
    created_at: datetime | None = None
    completed_at: datetime | None = None


class UserRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.users table
    """

    user_id: str | None = None  # Auto-generated: USR12345
    full_name: str
    username: str
    email: str
    password_hash: str
    created_at: datetime | None = None


class ParsedItemRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.parsed_items table.
    METADATA ONLY - actual text lives in MinIO (duka-parsed-data).
    The worker is recorded here because each item is processed by one worker,
    even though a job can flow through multiple worker stages across items.
    """

    item_id: str | None = None  # Auto-generated: ITEM00000001
    job_id: str
    source_url: str
    language: str = "unknown"
    worker_type: str = Field(
        default="surface",
        description="Worker that processed this parsed item: 'surface', 'deep', or 'dark'",
    )
    title: str | None = None
    publish_date: str | None = None
    character_count: int | None = None
    word_count: int | None = None
    raw_html_path: str
    parsed_json_path: str
    parsed_at: datetime | None = None
    is_exported: bool = False
    intelligence_processed: bool = Field(
        default=False, description="Has LLM worker processed this for intelligence?"
    )


class ExportRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.exports table
    """

    export_id: str | None = None  # Auto-generated: EXP00000001
    job_id: str
    export_type: str  # "csv", "json", "parquet"
    file_path: str
    status: str = "pending"  # pending, completed, failed
    file_size_mb: float | None = None
    item_count: int | None = None
    created_at: datetime | None = None


class ElasticsearchArticle(BaseModel):
    """
    Schema for Elasticsearch duka_articles index
    Full-text indexed documents
    """

    job_id: str
    item_id: str
    url: str
    title: str | None = None
    language: str
    extracted_text: str  # Full-text indexed
    character_count: int
    worker: str
    status: str
    source_domain: str
    created_at: datetime


# ============================================================================
# API SCHEMAS
# ============================================================================


class LoginRequest(BaseModel):
    """User login request"""

    username: str
    password: str


class Token(BaseModel):
    """JWT token response"""

    access_token: str
    token_type: str = "bearer"


class CreateJobRequest(BaseModel):
    """
    Create new crawl job request
    NOTE: source_type REMOVED; determined post-parsing by LLM
    worker_type is assigned by rules engine based on URL patterns
    """

    url: str
    language: str = "am"  # default Amharic
    job_params: dict = Field(default_factory=dict, description="Additional per-job parameters")

    # ========================================================================
    # RECURSIVE CRAWLING FIELDS (OPTIONAL)
    # ========================================================================
    max_depth: int = Field(
        default=5, description="Max recursion depth (0 = single URL, no recursion; default 5)"
    )
    recursive_config: dict = Field(
        default_factory=lambda: {"enable_extraction": True},
        description="Recursion control: {enable_extraction: bool, link_filter_patterns: list, skip_domains: list}",
    )


class JobResponse(BaseModel):
    """Job creation response."""

    job_id: str
    user_id: str
    url: str
    language: str
    status: str
    created_at: str
    assigned_worker: str | None = None


class SearchResponse(BaseModel):
    """Full-text search response"""

    query: str
    total: int
    limit: int
    offset: int
    results: list


class AnalyticsResponse(BaseModel):
    """Analytics query response"""

    metric: str
    results: list


class ExportRequest(BaseModel):
    """Export job request"""

    job_id: str
    format: str  # csv, json, parquet
