"""Regression tests: two bugs that made every LLM analysis silently fake.

1. ``llm_fallback_items`` is a Counter with a ``source`` label but was called as
   ``inc()`` with no arguments. That raises ``ValueError`` at *runtime*, and it
   raised before the ClickHouse write - so every item that fell back to the
   heuristic had its analysis thrown away and retried until it ran out of
   attempts. Nothing was logged at error level; the item just never appeared.

2. ``gpt-oss-120b`` is a reasoning model. Its completion tokens include a
   ``reasoning`` block, so a 1024-token budget was spent thinking and the
   response came back ``finish_reason="length"`` with ``content=""``. The code
   read that as "the model answered but the JSON was unreadable" and stored a
   heuristic classification with ``analysis_source="fallback"`` - which looks
   like a successful run in every dashboard.
"""

from __future__ import annotations

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest

WORKER = Path(__file__).resolve().parents[2] / "workers" / "llm-worker" / "main.py"
SOURCE = WORKER.read_text(encoding="utf-8")


def _load_worker():
    """Import the worker once, with a clean Prometheus registry.

    The worker registers collectors at import time and other test modules load it
    too; Prometheus refuses duplicate registrations, so the registry is emptied
    first or merely collecting this file fails depending on import order.
    """
    try:
        from prometheus_client import REGISTRY

        for collector in list(getattr(REGISTRY, "_collector_to_names", {})):
            REGISTRY.unregister(collector)
    except Exception:  # pragma: no cover
        pass

    spec = importlib.util.spec_from_file_location(
        "llm_regression_probe", WORKER, submodule_search_locations=[]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["llm_regression_probe"] = module
    spec.loader.exec_module(module)
    return module


LLM = _load_worker()


class TestLabelledCounters:
    """A labelled Counter called without its label raises at runtime."""

    def _labelled_counters(self) -> dict[str, list[str]]:
        found: dict[str, list[str]] = {}
        for node in ast.walk(ast.parse(SOURCE)):
            if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
                continue
            func = node.value.func
            if not (isinstance(func, ast.Name) and func.id == "Counter"):
                continue
            name = getattr(node.targets[0], "id", None)
            if not name or len(node.value.args) < 3:
                continue
            labels = node.value.args[2]
            if isinstance(labels, ast.List):
                found[name] = [
                    el.value for el in labels.elts if isinstance(el, ast.Constant)
                ]
        return found

    def test_the_fallback_counter_is_labelled(self):
        assert self._labelled_counters()["llm_fallback_items"] == ["source"]

    @pytest.mark.parametrize("counter", ["llm_fallback_items", "llm_request_errors"])
    def test_no_labelled_counter_is_incremented_without_its_labels(self, counter):
        pattern = re.compile(rf"\b{re.escape(counter)}\s*\.\s*inc\s*\(")
        bare = [
            SOURCE[: SOURCE.index(line)].count("\n") + 1
            for line in SOURCE.splitlines()
            if pattern.search(line) and ".labels(" not in line
        ]
        assert not bare, (
            f"{counter}.inc() with no labels on line(s) {bare} — a labelled "
            "Counter raises ValueError without them, at a point where the "
            "analysis has not yet been written anywhere"
        )


class TestTruncationDetection:
    def _client(self):
        return LLM.HostedLLMClient("https://api.groq.com/openai/v1", "k", "m")

    def test_a_truncated_reasoning_answer_raises_instead_of_falling_back(self):
        client = self._client()
        with pytest.raises(LLM.LLMResponseTruncated):
            client._raise_if_truncated(
                {
                    "choices": [
                        {
                            "finish_reason": "length",
                            "message": {"content": "", "reasoning": "thinking..."},
                        }
                    ]
                }
            )

    def test_truncation_raises_the_budget_for_the_retry(self):
        client = self._client()
        before = client._max_completion_tokens
        with pytest.raises(LLM.LLMResponseTruncated):
            client._raise_if_truncated(
                {"choices": [{"finish_reason": "length", "message": {"content": ""}}]}
            )
        assert client._max_completion_tokens > before

    def test_a_normal_answer_is_not_flagged(self):
        self._client()._raise_if_truncated(
            {"choices": [{"finish_reason": "stop", "message": {"content": "{}"}}]}
        )

    def test_a_complete_answer_with_reasoning_is_not_flagged(self):
        """Reasoning output alongside content is normal for these models."""
        self._client()._raise_if_truncated(
            {
                "choices": [
                    {
                        "finish_reason": "stop",
                        "message": {"content": '{"category":"news"}', "reasoning": "x"},
                    }
                ]
            }
        )

    def test_an_empty_untruncated_answer_still_raises(self):
        client = self._client()
        with pytest.raises(LLM.LLMResponseTruncated, match="empty"):
            client._raise_if_truncated(
                {"choices": [{"finish_reason": "stop", "message": {"content": "  "}}]}
            )

    def test_a_response_with_no_choices_is_not_flagged(self):
        """A shape problem is the parser's business, not a token-budget one."""
        client = self._client()
        client._raise_if_truncated({"choices": []})
        client._raise_if_truncated({})


class TestTruncationIsRetryable:
    def test_truncation_counts_as_transient(self):
        assert LLM._looks_transient(LLM.LLMResponseTruncated("hit the limit"))

    def test_an_auth_error_does_not(self):
        class _Auth(Exception):
            status_code = 401

        assert not LLM._looks_transient(_Auth("invalid api key"))

    def test_truncation_gets_its_own_metric_label(self):
        assert LLM._classify_llm_error(LLM.LLMResponseTruncated("x")) == "truncated"


class TestTokenBudgets:
    def test_the_default_budget_leaves_room_for_reasoning(self):
        """1024 is what caused this: reasoning consumed the entire budget."""
        assert LLM.GROQ_MAX_COMPLETION_TOKENS >= 4096

    def test_the_retry_budget_is_larger_than_the_first(self):
        assert LLM.GROQ_TRUNCATED_RETRY_TOKENS > LLM.GROQ_MAX_COMPLETION_TOKENS

    def test_the_budget_resets_after_a_successful_call(self):
        """One hard document must not permanently double everyone's token spend."""
        client = LLM.HostedLLMClient("https://api.groq.com/openai/v1", "k", "m")
        client._max_completion_tokens = LLM.GROQ_TRUNCATED_RETRY_TOKENS

        class _Completion:
            @staticmethod
            def model_dump():
                return {
                    "choices": [
                        {"finish_reason": "stop", "message": {"content": '{"a":1}'}}
                    ]
                }

        import asyncio

        client.groq_client = type(
            "_C",
            (),
            {
                "chat": type(
                    "_Chat",
                    (),
                    {"completions": type("_Comp", (), {"create": staticmethod(
                        lambda **kw: _Completion())})},
                )()
            },
        )()
        asyncio.run(client.extract_intelligence("x" * 200, "https://example.com/a"))
        assert client._max_completion_tokens == LLM.GROQ_MAX_COMPLETION_TOKENS