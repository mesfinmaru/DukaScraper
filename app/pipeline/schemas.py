"""
Pydantic schemas for DukaScraper platform
Data contracts for API, Kafka messages, and databases
TEXT-ONLY crawling pipeline

NOTE on ID types:
PostgreSQL IDs are auto-generated STRINGS (not integers):
  - duka_system.users.user_id    -> VARCHAR(8)  e.g. "USR12345"
  - duka_system.jobs.job_id      -> VARCHAR(11) e.g. "JOB00000001"
  - duka_db.parsed_items.item_id -> VARCHAR(12) e.g. "ITEM00000001"
  - duka_db.exports.export_id    -> VARCHAR(11) e.g. "EXP00000001"
All schemas below use `str` for these IDs to match the real database schema
and the existing worker implementations (surface-worker, parser-worker,
exporter-worker), which already pass job_id/source_job_id as strings.
"""

from datetime import datetime

from pydantic import BaseModel, Field

# ============================================================================
# KAFKA MESSAGE SCHEMAS
# ============================================================================


class CrawlRequest(BaseModel):
    """
    Schema for crawl.requests Kafka topic
    Input for all crawl workers (surface, deep, dark)
    """

    job_id: str = Field(..., description="Job identifier, e.g. 'JOB00000001'")
    url: str = Field(..., description="The URL to be crawled")
    language: str = Field(default="en", description="Language: 'en' or 'am'")
    worker_type: str = Field(..., description="Worker type: 'surface', 'deep', or 'dark'")
    job_params: dict = Field(default_factory=dict, description="Additional per-job worker parameters")


class CrawlResult(BaseModel):
    """
    Schema for crawl.raw Kafka topic
    Raw output produced by crawl workers
    """

    source_job_id: str = Field(..., description="job_id of the originating CrawlRequest")
    url: str
    html: str  # Raw HTML content
    status_code: int
    worker: str  # Which worker produced this ('surface', 'deep', 'dark')
    language: str
    network: str = Field(default="surface", description="Network used: surface, deep, dark")
    fetch_duration: float | None = None  # Crawl time in seconds


class ParsedItemData(BaseModel):
    """
    Schema for parsed item data field
    Extracted text and metadata
    """

    extracted_text: str = Field(..., description="Clean extracted text")
    character_count: int = Field(..., description="Number of characters")
    original_status_code: int = Field(..., description="HTTP status code")
    title: str | None = Field(default=None, description="Extracted article title")
    publish_date: str | None = Field(default=None, description="Extracted publish date in ISO format")
    detected_language: str | None = Field(default=None, description="Detected language from parsed content")


class ParsedItem(BaseModel):
    """
    Schema for crawl.parsed Kafka topic
    Structured data extracted by parser-worker
    """

    source_job_id: str = Field(..., description="job_id of the originating CrawlRequest")
    url: str
    worker: str  # Which worker crawled it
    language: str  # 'en' or 'am'
    data: ParsedItemData | dict = Field(..., description="Extracted text + metadata")
    status: str = "completed"  # completed or failed
    parse_duration: float | None = None  # Parse time in seconds


# ============================================================================
# DATABASE SCHEMAS (duka_system + duka_db - PostgreSQL, unchanged schema)
# ============================================================================


class JobRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.jobs table
    Tracks all crawl jobs
    """

    job_id: str | None = None  # Auto-generated: JOB00000001
    user_id: str  # References users.user_id, e.g. USR12345
    url: str
    language: str = "am"  # "am" or "en"
    worker_type: str  # "surface", "deep", or "dark"
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
    Schema for PostgreSQL duka_db.parsed_items table
    METADATA ONLY - actual text lives in MinIO (duka-parsed-data)
    """

    item_id: str | None = None  # Auto-generated: ITEM00000001
    job_id: str
    source_url: str
    language: str = "am"
    title: str | None = None
    publish_date: str | None = None
    character_count: int | None = None
    word_count: int | None = None
    raw_html_path: str
    parsed_json_path: str
    parsed_at: datetime | None = None
    is_exported: bool = False


class ExportRecord(BaseModel):
    """
    Schema for PostgreSQL duka_db.exports table
    """

    export_id: str | None = None  # Auto-generated: EXP00000001
    job_id: str
    export_type: str  # "csv", "json", "parquet"
    file_path: str
    status: str = "pending"  # pending, completed, failed
    file_size_mb: float | None = None
    item_count: int | None = None
    created_at: datetime | None = None


class ClickHouseAnalyticsRecord(BaseModel):
    """
    Schema for ClickHouse duka_analytics table
    Analytics and time-series data
    """

    job_id: str
    language: str
    worker: str
    source_domain: str
    character_count: int
    status: str
    crawl_date: str
    created_at: datetime


class ElasticsearchArticle(BaseModel):
    """
    Schema for Elasticsearch duka_articles index
    Full-text indexed documents
    """

    job_id: str
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
    """Create new crawl job request"""

    url: str
    language: str = "am"  # default Amharic
    worker_type: str = "surface"  # default surface worker


class JobResponse(BaseModel):
    """Job creation response"""

    job_id: str
    user_id: str
    url: str
    language: str
    worker_type: str
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
