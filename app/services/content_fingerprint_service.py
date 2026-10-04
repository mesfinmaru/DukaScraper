"""
3-Tier Content Deduplication Service.

Professional-grade deduplication for web scraping pipelines:

  Tier 1 — URL Fingerprint:
    SHA-256 of normalized URL (stripped query params, fragments,
    trailing slashes, case-normalized). Catches same-page different-URLs.

  Tier 2 — Content Hash:
    SHA-256 of normalized visible text. Catches identical content
    served at different URLs (e.g. paginated mirrors, syndicated articles).

  Tier 3 — SimHash (near-duplicate):
    64-bit locality-sensitive hash of text shingles. Catches pages that
    are *almost* identical (e.g. news articles with one paragraph updated).
    Uses Hamming distance threshold for comparison.

Used by: surface-worker, deep-worker, dark-worker.

Flow:
  1. Before fetching: check URL fingerprint → if exact match and fresh, return cached
  2. After parsing: generate content hash + SimHash → store in content_fingerprints
  3. On next crawl of same/similar content: detect duplicate, return cached parsed data

Professional reference:
  - Google uses SimHash for near-duplicate detection at web scale
  - Common Crawl stores WARC + content hash per page
  - Scrapy caches by URL fingerprint + ETag/Last-Modified
"""

from __future__ import annotations

import hashlib
import re
import struct
from datetime import UTC
from typing import Any

# ============================================================
# URL NORMALIZATION
# ============================================================

def normalize_url(url: str) -> str:
    """Normalize URL for fingerprinting.

    Strips: query params, fragments, trailing slashes,
    normalizes scheme/host to lowercase, removes default ports.
    """
    from urllib.parse import urlparse, urlunparse

    parsed = urlparse(url.lower().strip())

    # Normalize scheme to https (http/https are equivalent for dedup)
    scheme = "https"

    # Remove default ports
    host = parsed.hostname or ""

    # Strip www. prefix (www.example.com == example.com for dedup)
    if host.startswith("www."):
        host = host[4:]

    port = parsed.port
    if (parsed.scheme == "https" and port == 443) or \
       (parsed.scheme == "http" and port == 80):
        port = None

    # Rebuild without query/fragment
    netloc = host
    if port:
        netloc = f"{host}:{port}"

    # Normalize trailing slashes: always strip them
    path = parsed.path.rstrip("/") or "/"

    normalized = urlunparse((
        scheme,
        netloc,
        path,
        "",  # params
        "",  # query (stripped)
        "",  # fragment (stripped)
    ))

    return normalized


def url_fingerprint(url: str) -> str:
    """SHA-256 of normalized URL for Tier 1 dedup."""
    normalized = normalize_url(url)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# ============================================================
# CONTENT NORMALIZATION
# ============================================================

# Boilerplate patterns to strip before hashing.
# These vary across pages on the same site but don't represent content.
_BOILERPLATE_PATTERNS: list[re.Pattern] = [
    re.compile(r"cookie[s]?\s*(?:policy|notice|consent|banner|accept|decline)", re.I),
    re.compile(r"privacy\s*(?:policy|notice|statement)", re.I),
    re.compile(r"terms\s*(?:of\s*(?:use|service|conditions))?", re.I),
    re.compile(r"©\s*\d{4}", re.I),
    re.compile(r"all\s*rights\s*reserved", re.I),
    re.compile(r"powered\s*by\s*\w+", re.I),
    re.compile(r"sign\s*(?:up|in|out|log)", re.I),
    re.compile(r"subscribe\s*(?:to|for|newsletter)", re.I),
    re.compile(r"follow\s*(?:us|me|them)\s*(?:on|at)", re.I),
    re.compile(r"share\s*(?:this|on|via)", re.I),
    re.compile(r"(?:facebook|twitter|instagram|linkedin|youtube|tiktok)\s*(?:follow|share|like)", re.I),
    re.compile(r"skip\s*to\s*(?:content|main|navigation)", re.I),
    re.compile(r"(?:home|about|contact|faq|help|support)\s*(?:\||•|-)", re.I),
]


def normalize_content(text: str) -> str:
    """Normalize text content for content hashing.

    Steps:
      1. Lowercase
      2. Collapse whitespace
      3. Strip common boilerplate (navigation, footers, cookie banners)
      4. Remove non-alphanumeric noise (keep spaces for tokenization)
      5. Strip leading/trailing whitespace
    """
    if not text:
        return ""

    # Lowercase
    text = text.lower()

    # Strip HTML entities that may have leaked through
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"&#\d+;", " ", text)

    # Collapse whitespace
    text = re.sub(r"\s+", " ", text)

    # Remove boilerplate lines
    for pattern in _BOILERPLATE_PATTERNS:
        text = pattern.sub(" ", text)

    # Remove very short segments (likely navigation crumbs)
    # Split on common delimiters, filter, rejoin
    raw_segments = [s.strip() for s in re.split(r"[|•–—/\\]", text)]
    segments = [s for s in raw_segments if len(s) > 3]
    if not segments:
        # Every segment fell below the crumb threshold, so the content really is
        # that short (e.g. a one-word audio transcript). Normalizing it away would
        # hash it identically to an empty body and make every such item an exact
        # duplicate of every other one, so keep the short text instead.
        segments = [s for s in raw_segments if s]
    text = " ".join(segments)

    # Remove special characters, keep alphanumeric + spaces
    text = re.sub(r"[^a-z0-9\s]", " ", text)

    # Collapse whitespace again
    text = re.sub(r"\s+", " ", text).strip()

    return text


def content_hash(text: str) -> str:
    """SHA-256 of normalized content for Tier 2 dedup."""
    normalized = normalize_content(text)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


# ============================================================
# SIMHASH (Tier 3 — Near-Duplicate Detection)
# ============================================================

def _tokenize(text: str) -> list[str]:
    """Tokenize normalized text into words."""
    return text.split()


def _get_shingles(tokens: list[str], k: int = 3) -> list[str]:
    """Generate k-word shingles from token list.

    Shingles are contiguous k-grams that capture local context.
    k=3 is a good balance: small enough for sensitivity, large enough
    for noise resistance.
    """
    if len(tokens) < k:
        return [" ".join(tokens)] if tokens else []
    return [" ".join(tokens[i:i + k]) for i in range(len(tokens) - k + 1)]


def _hash_shingle(shingle: str) -> int:
    """Hash a shingle to a 64-bit integer."""
    h = hashlib.md5(shingle.encode("utf-8")).digest()
    return struct.unpack("<Q", h[:8])[0]


def simhash(text: str, num_bits: int = 64) -> int:
    """Compute SimHash fingerprint of text.

    SimHash is a locality-sensitive hash: similar texts produce
    similar hashes. The Hamming distance between two SimHash
    fingerprints indicates their similarity.

    Algorithm:
      1. Tokenize text into words
      2. Generate k-word shingles (k=3)
      3. Hash each shingle to 64-bit
      4. For each bit position, add +1 if shingle hash has bit=1,
         -1 if bit=0
      5. Final fingerprint: bit=1 where vote > 0, else bit=0
    """
    tokens = _tokenize(text)
    if not tokens:
        return 0

    shingles = _get_shingles(tokens, k=3)
    if not shingles:
        return 0

    # Vote vector: +1 for each bit set in each shingle hash
    votes = [0] * num_bits

    for shingle in shingles:
        h = _hash_shingle(shingle)
        for bit in range(num_bits):
            if h & (1 << bit):
                votes[bit] += 1
            else:
                votes[bit] -= 1

    # Build fingerprint: 1 where positive vote, 0 otherwise
    fingerprint = 0
    for bit in range(num_bits):
        if votes[bit] > 0:
            fingerprint |= (1 << bit)

    # Convert unsigned 64-bit to signed for PostgreSQL BIGINT storage
    if fingerprint >= (1 << 63):
        fingerprint -= (1 << 64)

    return fingerprint


def hamming_distance(hash1: int, hash2: int) -> int:
    """Compute Hamming distance between two 64-bit integers.

    Handles both signed (PostgreSQL BIGINT) and unsigned values.

    Distance 0 = identical content
    Distance 1-3 = very similar (likely near-duplicate)
    Distance 4-5 = somewhat similar
    Distance 6+ = different content
    """
    # Convert signed to unsigned for XOR comparison
    u1 = hash1 % (1 << 64)
    u2 = hash2 % (1 << 64)
    xor = u1 ^ u2
    distance = 0
    while xor:
        distance += 1
        xor &= xor - 1  # Clear lowest set bit
    return distance


def is_near_duplicate(
    hash1: int,
    hash2: int,
    threshold: int = 3,
) -> bool:
    """Check if two SimHash fingerprints are near-duplicates.

    Professional systems typically use threshold 3-5:
    - Threshold 3: Very strict (catches nearly identical pages)
    - Threshold 5: Relaxed (catches pages with moderate differences)
    """
    return hamming_distance(hash1, hash2) <= threshold


# ============================================================
# HIGH-LEVEL DEDUP API
# ============================================================

class ContentFingerprint:
    """Result of fingerprinting a page's content."""

    __slots__ = (
        "url_fp",
        "content_fp",
        "simhash_val",
        "word_count",
        "char_count",
        "text_preview",
    )

    def __init__(
        self,
        url_fp: str,
        content_fp: str,
        simhash_val: int,
        word_count: int = 0,
        char_count: int = 0,
        text_preview: str = "",
    ):
        self.url_fp = url_fp
        self.content_fp = content_fp
        self.simhash_val = simhash_val
        self.word_count = word_count
        self.char_count = char_count
        self.text_preview = text_preview

    def to_dict(self) -> dict[str, Any]:
        return {
            "url_fingerprint": self.url_fp,
            "content_fingerprint": self.content_fp,
            "simhash": self.simhash_val,
            "word_count": self.word_count,
            "char_count": self.char_count,
        }


def generate_fingerprint(
    url: str,
    content_text: str,
) -> ContentFingerprint:
    """Generate all 3 fingerprint tiers for a page.

    Args:
        url: The page URL
        content_text: The visible text content (post-parsing)

    Returns:
        ContentFingerprint with url_fp, content_fp, simhash_val
    """
    u_fp = url_fingerprint(url)
    c_fp = content_hash(content_text)
    s_hash = simhash(normalize_content(content_text))

    word_count = len(content_text.split()) if content_text else 0
    char_count = len(content_text) if content_text else 0
    text_preview = (content_text[:200] if content_text else "")

    return ContentFingerprint(
        url_fp=u_fp,
        content_fp=c_fp,
        simhash_val=s_hash,
        word_count=word_count,
        char_count=char_count,
        text_preview=text_preview,
    )


# ============================================================
# STALENESS CHECK
# ============================================================

def is_stale(
    created_at: Any,
    stale_hours: float = 24.0,
) -> bool:
    """Check if a fingerprint record is stale (older than threshold).

    Args:
        created_at: datetime object or ISO timestamp string
        stale_hours: Hours after which content is considered stale

    Returns:
        True if the record is older than stale_hours
    """
    from datetime import datetime

    if created_at is None:
        return True

    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return True

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=UTC)

    now = datetime.now(UTC)
    age_hours = (now - created_at).total_seconds() / 3600.0

    return age_hours > stale_hours
