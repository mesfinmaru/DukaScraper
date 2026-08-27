"""Content topics, deliberately separate from intelligence/threat categories."""

from enum import StrEnum


class ContentTopic(StrEnum):
    ECONOMICS = "economics"
    POLITICS = "politics"
    HEALTH = "health"
    TECHNOLOGY = "technology"
    SECURITY = "security"
    ENVIRONMENT = "environment"
    SOCIETY = "society"
    OTHER = "other"


ALL_CONTENT_TOPICS = {topic.value for topic in ContentTopic}
DEFAULT_CONTENT_TOPIC = ContentTopic.OTHER

TOPIC_DESCRIPTIONS = {
    ContentTopic.ECONOMICS: "Markets, finance, inflation, trade, employment, business, or economic policy",
    ContentTopic.POLITICS: "Government, elections, public policy, diplomacy, or political actors",
    ContentTopic.HEALTH: "Medicine, public health, disease, healthcare, or wellbeing",
    ContentTopic.TECHNOLOGY: "Software, hardware, science, digital services, or innovation",
    ContentTopic.SECURITY: "Cybersecurity, crime, safety, conflict, or defence",
    ContentTopic.ENVIRONMENT: "Climate, agriculture, natural resources, weather, or pollution",
    ContentTopic.SOCIETY: "Culture, education, religion, community, or human-interest issues",
    ContentTopic.OTHER: "Content not covered by another topic",
}