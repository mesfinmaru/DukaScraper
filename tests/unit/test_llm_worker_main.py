import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[2] / "workers" / "llm-worker" / "main.py"
SPEC = importlib.util.spec_from_file_location("llm_worker_main", MODULE_PATH)
llm_worker_main = importlib.util.module_from_spec(SPEC)
assert SPEC is not None and SPEC.loader is not None
SPEC.loader.exec_module(llm_worker_main)
HostedLLMClient = llm_worker_main.HostedLLMClient


class TestHostedLLMClient:
    def test_parse_intelligence_response_valid_json(self):
        text = '{"source_type":"news","category":"gov_issue","threat_severity":3,"entities":["Ethiopia","Prime Minister"],"summary":"Political report."}'
        parsed = HostedLLMClient._parse_intelligence_response(text)

        assert parsed["source_type"] == "news"
        assert parsed["category"] == "gov_issue"
        assert parsed["threat_severity"] == 3
        assert parsed["entities"] == ["Ethiopia", "Prime Minister"]
        assert parsed["summary"] == "Political report."

    def test_parse_intelligence_response_with_extra_text(self):
        text = 'Result:\n{"source_type":"blog","category":"other","threat_severity":1,"entities":"foo,bar","summary":"General content."}\nThanks.'
        parsed = HostedLLMClient._parse_intelligence_response(text)

        assert parsed["category"] == "other"
        assert parsed["summary"] == "General content."

    def test_validate_intelligence_normalizes_string_entities(self):
        data = {
            "source_type": "forum",
            "category": "cyber_threat",
            "threat_severity": 4,
            "entities": "192.168.0.1, example.com; user@example.com",
            "summary": "Detected suspicious indicators.",
        }
        validated = HostedLLMClient._validate_intelligence(data)

        assert validated["entities"] == ["192.168.0.1", "example.com", "user@example.com"]
        assert validated["summary"].startswith("Detected suspicious")

    def test_validate_intelligence_moves_default_category(self):
        data = {
            "source_type": "unknown",
            "category": "invalid_category",
            "threat_severity": 10,
            "entities": [],
            "summary": "Fallback summary.",
        }
        validated = HostedLLMClient._validate_intelligence(data)

        assert validated["category"] != "invalid_category"
        assert validated["threat_severity"] == 5
        assert validated["summary"].startswith("Fallback summary")

    def test_validate_intelligence_preserves_source_type_and_long_summary(self):
        data = {
            "source_type": "news",
            "category": "gov_issue",
            "threat_severity": 3,
            "entities": ["Ethiopia", "Parliament"],
            "summary": "This article explains the policy proposal, the reaction from public institutions, and the broader economic impact on citizens across the region in a full and detailed summary that highlights the underlying political tension, fiscal implications, and public response over several weeks.",
        }
        validated = HostedLLMClient._validate_intelligence(data)

        assert validated["source_type"] == "news"
        assert len(validated["summary"]) > 200
        assert "economic impact" in validated["summary"].lower()


def test_hosted_llm_extracts_groq_content_text():
    payload = {
        "choices": [{
            "message": {
                "content": '{"category":"other","threat_severity":1,"entities":["test"],"summary":"ok"}'
            }
        }]
    }
    text = llm_worker_main.HostedLLMClient._extract_text_from_payload(payload)
    assert '"category"' in text
    assert '"summary":"ok"' in text


def test_fallback_intelligence_includes_source_type():
    result = llm_worker_main.HostedLLMClient._fallback_analyze(
        parsed_text="The government announced a new policy and agencies are responding.",
        url="https://example.com/news/policy-update",
    )
    assert "source_type" in result
    assert result["source_type"] in llm_worker_main.ALL_SOURCE_TYPES


def test_detect_content_language_prefers_amharic_when_dominant():
    detected = llm_worker_main.HostedLLMClient._detect_content_language(
        "የኢትዮጵያ መንግሥት አዲስ ፖሊሲ አዋጅ አውጥቷል። እስካሁን በሀገሪቱ ውስጥ ሰፊ ውይይት ተካሄዷል።"
    )
    assert detected == "am"
