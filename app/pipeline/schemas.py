"""
Pydantic schemas for DukaScraper platform
Data contracts for API, Kafka messages, and databases
TEXT-ONLY crawling pipeline
"""

from typing import Any, Optional
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
    job_id: int = Field(..., description="Unique identifier for this crawl job")
    user_id: int = Field(..., description="User who submitted the job")
    url: str = Field(..., description="The URL to be crawled")
    language: str = Field(default="en", description="Language: 'en' or 'am'")
    worker_type: str = Field(..., description="Worker type: 'surface', 'deep', or 'dark'")


class CrawlResult(BaseModel):
    """
    Schema for crawl.result Kafka topic
    Raw output from crawl workers
    """
    job_id: int
    url: str
    html: str  # Raw HTML content
    status_code: int
    worker: str  # Which worker produced this
    language: str
    fetch_duration: Optional[float] = None  # Crawl time in seconds


class ParsedItemData(BaseModel):
    """
    Schema for parsed item data field
    Extracted text and metadata
    """
    extracted_text: str = Field(..., description="Clean extracted text")
    character_count: int = Field(..., description="Number of characters")
    original_status_code: int = Field(..., description="HTTP status code")


class ParsedItem(BaseModel):
    """
    Schema for crawl.parsed Kafka topic
    Structured data extracted by parser-worker
    """
    job_id: int
    url: str
    topic: str  # Title of the article
    language: str  # 'en' or 'am'
    extracted_text: str  # Full extracted text for search indexing
    character_count: int
    worker: str  # Which worker crawled it
    status: str = "completed"  # completed or failed
    parse_duration: Optional[float] = None  # Parse time in seconds


# ============================================================================
# DATABASE SCHEMAS
# ============================================================================

class JobRecord(BaseModel):
    """
    Schema for PostgreSQL duka_system.jobs table
    Tracks all crawl jobs
    """
    job_id: Optional[int] = None  # Auto-increment
    user_id: int
    url: str
    language: str  # "am" or "en"
    worker_type: str  # "surface", "deep", or "dark"
    status: str = "pending"  # pending, running, completed, failed
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ParsedItemRecord(BaseModel):
    """
    Schema for PostgreSQL duka_db.parsed_items table
    METADATA ONLY - NO FULL TEXT
    """
    parsed_item_id: Optional[int] = None
    job_id: int
    topic: str
    url: str
    language: str
    worker: str
    character_count: int
    status: str
    source_domain: str
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


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
    user_id: int
    created_at: datetime


class ElasticsearchArticle(BaseModel):
    """
    Schema for Elasticsearch duka_articles index
    Full-text indexed documents
    """
    job_id: str
    url: str
    topic: str
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
    language: str = "en"  # default English
    worker_type: str = "surface"  # default surface worker


class JobResponse(BaseModel):
    """Job creation response"""
    job_id: int
    user_id: int
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
    job_id: int
    format: str  # pdf, csv, txt, json
