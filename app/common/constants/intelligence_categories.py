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


# Canonical anchor texts per category. The llm-worker embeds these once and
# compares them against each article embedding, giving the classifier
# similarity-grounded hints that are independent of previously scraped pages
# (fixes the self-referential RAG loop). Keep them descriptive and distinct.
CATEGORY_ANCHOR_TEXTS = {
    IntelligenceCategory.DATA_LEAK: (
        "A post or document exposing stolen credentials, email and password "
        "lists, database dumps, personally identifiable information, leaked "
        "API keys, or confidential corporate records made public without "
        "authorization. Typical language mentions account logins, password "
        "combolists, customer databases, or internal files published by "
        "hackers or whistleblowers."
    ),
    IntelligenceCategory.GOV_ISSUE: (
        "Reporting or discussion of government policy decisions, political "
        "instability, ministerial statements, elections, new legislation, "
        "court rulings, public administration, or regional affairs of public "
        "interest. Typical language cites officials, ministries, parliament, "
        "agencies, or diplomatic developments."
    ),
    IntelligenceCategory.CYBER_THREAT: (
        "Technical disclosure or discussion of malware, ransomware, phishing "
        "campaigns, software exploits and vulnerabilities, botnets, "
        "command-and-control infrastructure, distributed denial-of-service "
        "attacks, or other digital threats to computer systems and networks."
    ),
    IntelligenceCategory.PHYSICAL_THREAT: (
        "Content describing violent extremism, armed conflict, sabotage, "
        "trafficking of weapons or contraband, kidnapping, or credible plans "
        "for physical harm against people or critical infrastructure."
    ),
    IntelligenceCategory.MISINFORMATION: (
        "Coordinated disinformation, state propaganda, fabricated news "
        "stories, astroturfed social campaigns, or false claims presented as "
        "fact in order to manipulate public opinion or obscure verified "
        "events."
    ),
    IntelligenceCategory.OTHER: (
        "General news, sports, weather, entertainment, lifestyle, product "
        "reviews, or everyday community content with no intelligence value "
        "and no connection to security, politics, or threats."
    ),
}


def get_category_anchor_texts() -> dict[str, str]:
    """Return {category_value: anchor_text} for embedding-based grounding."""
    return {str(cat): text for cat, text in CATEGORY_ANCHOR_TEXTS.items()}