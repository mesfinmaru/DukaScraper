"""
Intelligence category definitions for LLM-based threat/content classification.

Single source of truth for the 6 categories used by the llm-worker consumer
when analyzing parsed content and writing to ClickHouse intelligence_analytics.
"""

from enum import StrEnum


class IntelligenceCategory(StrEnum):
    """Top-level intelligence classification categories."""

    DATA_LEAK = "data_leak"               # Credentials, corporate DB dumps, PII, leaks
    GOV_ISSUE = "gov_issue"               # Regional stability, policy/political leaks, public interest
    CYBER_THREAT = "cyber_threat"         # Exploits, ransomware, malware, C2 infrastructure, DDoS
    PHYSICAL_THREAT = "physical_threat"   # Violent extremism, illicit market contraband, sabotage
    MISINFORMATION = "misinformation"     # Coordinated disinfo campaigns, propaganda, astroturfing
    OTHER = "other"                       # Low-value / general noise


# Full set of valid category values (enforced by CHECK constraint /
# validation in llm-worker and ClickHouse ingestion path)
ALL_INTELLIGENCE_CATEGORIES = {member.value for member in IntelligenceCategory}

DEFAULT_INTELLIGENCE_CATEGORY = IntelligenceCategory.OTHER

# Human-readable descriptions (used in LLM prompt construction)
CATEGORY_DESCRIPTIONS = {
    IntelligenceCategory.DATA_LEAK: "Credentials, corporate DB dumps, PII, leaks",
    IntelligenceCategory.GOV_ISSUE: "Regional stability, policy/political leaks, public interest",
    IntelligenceCategory.CYBER_THREAT: "Exploits, ransomware, malware, C2 infrastructure, DDoS",
    IntelligenceCategory.PHYSICAL_THREAT: "Violent extremism, illicit market contraband, sabotage",
    IntelligenceCategory.MISINFORMATION: "Coordinated disinfo campaigns, propaganda, astroturfing",
    IntelligenceCategory.OTHER: "Low-value / general noise",
}