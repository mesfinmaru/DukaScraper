"""
Source type definitions for LLM-based classification of content origin.

This module defines the possible `source_type` values that the llm-worker
will use to classify the origin of scraped content. This is distinct from
`intelligence_categories` which classify the *type of intelligence* found.
"""

from enum import StrEnum


class SourceType(StrEnum):
    """Enumeration of content source types."""

    NEWS = "news"
    FORUM = "forum"
    BLOG = "blog"
    SOCIAL = "social"
    GOVERNMENT = "government"
    ACADEMIC = "academic"
    ENCYCLOPEDIA = "encyclopedia"
    ECOMMERCE = "ecommerce"
    OTHER = "other"


# Full set of valid source type values
ALL_SOURCE_TYPES = {member.value for member in SourceType}

DEFAULT_SOURCE_TYPE = SourceType.OTHER

# Human-readable descriptions (used in LLM prompt construction)
SOURCE_TYPE_DESCRIPTIONS = {
    SourceType.NEWS: "News articles, press releases, journalistic content",
    SourceType.FORUM: "Discussion forums, message boards, community platforms",
    SourceType.BLOG: "Personal or corporate blogs, opinion pieces",
    SourceType.SOCIAL: "Social media platforms (e.g., Twitter, Facebook, Instagram)",
    SourceType.GOVERNMENT: "Official government websites, public records, legislative documents",
    SourceType.ACADEMIC: "Research papers, university websites, academic journals",
    SourceType.ENCYCLOPEDIA: "Reference works and collaboratively maintained encyclopedias",
    SourceType.ECOMMERCE: "Online stores, product pages, retail sites",
    SourceType.OTHER: "Any other type of content source not fitting the above categories",
}
