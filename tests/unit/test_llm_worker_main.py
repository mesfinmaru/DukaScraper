import pytest

from workers.llm-worker.main import OllamaLLMClient


class TestOllamaLLMClient:
    def test_parse_intelligence_response_valid_json(self):
        text = '{"source_type":"news","category":"gov_issue","threat_severity":3,"entities":["Ethiopia","Prime Minister"],"summary":"Political report."}'
        parsed = OllamaLLMClient._parse_intelligence_response(text)

        assert parsed["source_type"] == "news"
        assert parsed["category"] == "gov_issue"
        assert parsed["threat_severity"] == 3
        assert parsed["entities"] == ["Ethiopia", "Prime Minister"]
        assert parsed["summary"] == "Political report."

    def test_parse_intelligence_response_with_extra_text(self):
        text = 'Result:\n{"source_type":"blog","category":"other","threat_severity":1,"entities":"foo,bar","summary":"General content."}\nThanks.'
        parsed = OllamaLLMClient._parse_intelligence_response(text)

        assert parsed["category"] == "other"
        assert parsed["summary"] == "General content."

    def test_validate_intelligence_normalizes_string_entities(self):
        data = {
            "source_type": "forum",
            "category": "cyber_threat",
            "threat_severity": 4,
            "entities": "192.168.0.1, example.com; user@example.com",
 "summary": "Detected suspicious indicators."
        }
        validated = OllamaLLMClient._validate_intelligence(data)

        assert validated["entities"] == ["192.168.0.1", "example.com", "user@example.com"]
        assert validated["summary"] == "Detected suspicious indicators."

    def test_validate_intelligence_moves_default_category(self):
        data = {
            "source_type": "unknown",
            "category": "invalid_category",
            "threat_severity": 10,
            "entities": [],
            "summary": "Fallback summary.",
        }
        validated = OllamaLLMClient._validate_intelligence(data)

        assert validated["category"] != "invalid_category"
        assert validated["threat_severity"] == 5
        assert validated["summary"] == "Fallback summary."
