"""Unit tests for app.common.logger.logger."""

import json
import logging
from io import StringIO

from app.common.logger.logger import (
    CustomFormatter,
    JSONFormatter,
    get_request_id,
    set_request_id,
    setup_logging,
)


class TestRequestIDContext:
    def test_default_is_none(self):
        set_request_id(None)
        assert get_request_id() is None

    def test_set_and_get(self):
        set_request_id("req-123")
        assert get_request_id() == "req-123"
        set_request_id(None)

    def test_isolation(self):
        set_request_id("a")
        assert get_request_id() == "a"
        set_request_id(None)
        assert get_request_id() is None


class TestJSONFormatter:
    def setup_method(self):
        set_request_id(None)

    def test_valid_json_output(self):
        formatter = JSONFormatter()
        record = logging.LogRecord("test", logging.INFO, "t.py", 1, "hello", (), None)
        output = formatter.format(record)
        parsed = json.loads(output)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "hello"
        assert "timestamp" in parsed

    def test_includes_request_id(self):
        set_request_id("rid-abc")
        formatter = JSONFormatter()
        record = logging.LogRecord("test", logging.INFO, "t.py", 1, "msg", (), None)
        parsed = json.loads(formatter.format(record))
        assert parsed["request_id"] == "rid-abc"
        set_request_id(None)

    def test_excludes_request_id_when_none(self):
        formatter = JSONFormatter()
        record = logging.LogRecord("test", logging.INFO, "t.py", 1, "msg", (), None)
        parsed = json.loads(formatter.format(record))
        assert "request_id" not in parsed

    def test_includes_exception(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("boom")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
        record = logging.LogRecord("test", logging.ERROR, "t.py", 1, "err", (), exc_info)
        parsed = json.loads(formatter.format(record))
        assert "exception" in parsed
        assert "boom" in parsed["exception"]

    def test_all_log_levels(self):
        formatter = JSONFormatter()
        for name in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            level = getattr(logging, name)
            record = logging.LogRecord("test", level, "t.py", 1, f"msg {name}", (), None)
            parsed = json.loads(formatter.format(record))
            assert parsed["level"] == name


class TestCustomFormatter:
    def test_returns_string(self):
        formatter = CustomFormatter()
        record = logging.LogRecord("test", logging.INFO, "t.py", 1, "hello", (), None)
        output = formatter.format(record)
        assert isinstance(output, str)
        assert "hello" in output

    def test_includes_truncated_request_id(self):
        set_request_id("req-terminal-id")
        formatter = CustomFormatter()
        record = logging.LogRecord("test", logging.INFO, "t.py", 1, "hello", (), None)
        output = formatter.format(record)
        assert "[req-term" in output  # Truncated to 8 chars
        set_request_id(None)


class TestSetupLogging:
    def test_json_mode(self):
        setup_logging(level=logging.DEBUG, json_output=True)
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JSONFormatter)

    def test_console_mode(self):
        setup_logging(level=logging.WARNING, json_output=False)
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, CustomFormatter)

    def test_clears_existing_handlers(self):
        root = logging.getLogger()
        root.addHandler(logging.StreamHandler(StringIO()))
        setup_logging(level=logging.INFO, json_output=False)
        assert len(root.handlers) == 1

    def test_suppresses_noisy_loggers(self):
        setup_logging(level=logging.DEBUG, json_output=False)
        for name in ("httpx", "httpcore", "aiokafka", "elasticsearch", "urllib3"):
            assert logging.getLogger(name).level == logging.WARNING
