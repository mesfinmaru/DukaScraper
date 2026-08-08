"""
Pydantic schemas for DukaScraper platform
Data contracts for API, Kafka messages, and databases
TEXT-ONLY crawling pipeline with source_type POST-ANALYSIS

NOTE on ID types:
PostgreSQL IDs are auto-generated STRINGS (not integers):
  - duka_system.users.user_id    -> VARCHAR(8)  e.g. "USR12345"
  - duka_system.jobs.job_id      -> VARCHAR(11) e.g. "JOB00000001"
  - duka_db.parsed_items.item_id -> VARCHAR(12) e.g. "ITEM00000001"
  - duka_db.exports.export_id    -> VARCHAR(11) e.g. "EXP00000001"
All schemas below use `str` for these IDs to match the real database schema.

NOTE on source_type:
REMOVED from initial job creation. Source type is determined AFTER crawling and parsing,
by LLM analysis during the intelligence extraction phase (llm-worker consumer).
This allows dynamic classification based on actual content rather than pre-specification.

NOTE on recursive crawling:
All workers (SURFACE, DEEP, DARK) now support depth-limited recursive crawling.
New fields: depth, max_depth, parent_url, target_layer, recursive_config.
"""

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field

from app.common.constants.intelligence_categories import DEFAULT_INTELLIGENCE_CATEGORY

# ============================================================================
# KAFKA MESSAGE SCHEMAS
# ============================================================================


class CrawlRequest(BaseModel):
    """
    Schema for crawl.requests Kafka topic
    Input for all crawl workers (surface, deep, dark)
    NOTE: source_type removed (determined post-parsing by LLM)
    """

    job_id: str = Field(..., description="Job identifier, e.g. 'JOB00000001'")
    url: str = Field(..., description="The URL to be crawled")
    language: str = Field(default="am", description="Language: 'en' or 'am'")
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
    parent_url: Optional[str] = Field(
        default=None, description="Lineage tracking: URL that led to this child task"
    )
    target_layer: str = Field(
        default="surface", description="Execution network: 'surface', 'deep', or 'dark'"
    )
    recursive_config: dict = Field(
        default_factory=dict,
        description="Recursion rules: {enable_extraction: bool, link_filter_patterns: [...], skip_domains: [...]}",
    )

    # ========================================================================
    # WORKER ESCALATION (SURFACE → DEEP AUTO-REQUEUE)
    # ========================================================================
    retry_count: int = Field(default=0, description="Number of retries (incremented on escalation)")
    escalation_reason: Optional[str] = Field(
        default=None, description="Why escalated (e.g., 'http_403', 'cloudflare_challenge', 'empty_html')"
    )


class CrawlResult(BaseModel):
    """
    Schema for crawl.raw Kafka topic
    Raw output produced by crawl workers
    NOTE: source_type removed; only url + html for downstream parsing
    """

    job_id: str = Field(..., description="job_id of the originating CrawlRequest")
    url: str
    html: str  # Raw HTML content
    status_code: int
    worker: str  # Which worker produced this ('surface', 'deep', 'dark')
    language: str
    network: str = Field(default="surface", description="Network used: surface, deep, dark")
    fetch_duration: Optional[float] = None  # Crawl time in seconds

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
    escalation_reason: Optional[str] = Field(default=None, description="Reason for escalation")


class ParsedItemData(BaseModel):
    """
    Schema for parsed item data field
    Extracted text and metadata (POST-PARSING, PRE-LLM)
    """

    extracted_text: str = Field(..., description="Clean extracted text")
    character_count: int = Field(..., description="Number of characters")
    original_status_code: int = Field(..., description="HTTP status code")
    title: Optional[str] = Field(default=None, description="Extracted article title")
    publish_date: Optional[str] = Field(
        default=None, description="Extracted publish date in ISO format"
    )
    detected_language: Optional[str] = Field(
        default=None, description="Detected language from parsed content"
    )
    fetch_duration: Optional[float] = Field(
        default=None, description="Time taken to fetch the data in seconds"
    )
    payload_size_bytes: Optional[int] = Field(
        default=None, description="Size of the raw HTML payload in bytes"
    )
    proxy_ip: Optional[str] = Field(
        default=None, description="The active proxy endpoint used during the request"
    )
    retry_count: Optional[int] = Field(
        default=None, description="Number of retries triggered before successful ingestion"
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
    language: str  # 'en' or 'am'
    data: ParsedItemData | dict = Field(..., description="Extracted text + metadata")
    status: str = "completed"  # completed or failed
    parse_duration: Optional[float] = None  # Parse time in seconds


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
    language: str = Field(default="am", description="Original language")
    llm_model: str = Field(default="qwen2.5:14b", description="Which LLM model performed analysis")
    llm_score: float = Field(default=0.0, description="Model confidence (0-1)")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Analysis timestamp")


# ============================================================================
# DATABASE SCHEMAS (duka_system + duka_db - PostgreSQL)
# ============================================================================


class JobRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.jobs table
    Tracks all crawl jobs
    NOTE: source_type REMOVED (determined post-analysis)
    """

    job_id: Optional[str] = None  # Auto-generated: JOB00000001
    user_id: str  # References users.user_id, e.g. USR12345
    url: str
    language: str = "am"  # "am" or "en"
    worker_type: str  # "surface", "deep", or "dark" (assigned by rules engine)
    status: str = "pending"  # pending, running, completed, failed
    created_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class UserRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.users table
    """

    user_id: Optional[str] = None  # Auto-generated: USR12345
    full_name: str
    username: str
    email: str
    password_hash: str
    created_at: Optional[datetime] = None


class ParsedItemRecord(BaseModel):
    """
    Schema for PostgreSQL duka_db.parsed_items table
    METADATA ONLY - actual text lives in MinIO (duka-parsed-data)
    NOTE: source_type REMOVED; item_id added for LLM worker reference
    """

    item_id: Optional[str] = None  # Auto-generated: ITEM00000001
    job_id: str
    source_url: str
    language: str = "am"
    title: Optional[str] = None
    publish_date: Optional[str] = None
    character_count: Optional[int] = None
    word_count: Optional[int] = None
    raw_html_path: str
    parsed_json_path: str
    parsed_at: Optional[datetime] = None
    is_exported: bool = False
    intelligence_processed: bool = Field(
        default=False, description="Has LLM worker processed this for intelligence?"
    )


class ExportRecord(BaseModel):
    """
    Schema for PostgreSQL duka_db.exports table
    """

    export_id: Optional[str] = None  # Auto-generated: EXP00000001
    job_id: str
    export_type: str  # "csv", "json", "parquet"
    file_path: str
    status: str = "pending"  # pending, completed, failed
    file_size_mb: Optional[float] = None
    item_count: Optional[int] = None
    created_at: Optional[datetime] = None


class ElasticsearchArticle(BaseModel):
    """
    Schema for Elasticsearch duka_articles index
    Full-text indexed documents
    """

    job_id: str
    item_id: str
    url: str
    title: Optional[str] = None
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
        default_factory=dict,
        description="Recursion control: {enable_extraction: bool, link_filter_patterns: list, skip_domains: list}",
    )


class JobResponse(BaseModel):
    """Job creation response"""

    job_id: str
    user_id: str
    url: str
    language: str
    worker_type: str  # Assigned by rules engine
    status: str
    created_at: str


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
