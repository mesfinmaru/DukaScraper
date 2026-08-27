"""
Structured logging for Duka Scraper.

Provides:
  - Custom formatter with colored output for development
  - JSON formatter for production/containerized environments
  - Request context propagation via contextvars
"""

import logging
import sys
from contextvars import ContextVar
from datetime import UTC

from colorama import Fore, Style, init

# Initialize colorama for Windows support
init(autoreset=True)

# Context variable for request ID propagation
_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)


def set_request_id(request_id: str | None) -> None:
    """Set the current request ID in the context."""
    _request_id.set(request_id)


def get_request_id() -> str | None:
    """Get the current request ID from the context."""
    return _request_id.get()


class CustomFormatter(logging.Formatter):
    """Custom formatting for beautiful terminal logs with request ID."""

    format_str = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s"

    FORMATS = {
        logging.DEBUG: Fore.BLUE + format_str + Style.RESET_ALL,
        logging.INFO: Fore.GREEN + format_str + Style.RESET_ALL,
        logging.WARNING: Fore.YELLOW + format_str + Style.RESET_ALL,
        logging.ERROR: Fore.RED + format_str + Style.RESET_ALL,
        logging.CRITICAL: Fore.RED + Style.BRIGHT + format_str + Style.RESET_ALL,
    }

    def format(self, record):
        # Inject request_id if available
        rid = get_request_id()
        if rid:
            record.msg = f"[{rid[:8]}] {record.msg}"

        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


class JSONFormatter(logging.Formatter):
    """JSON formatter for production/containerized environments.

    Outputs one JSON object per line, suitable for log aggregation
    services (ELK, Datadog, CloudWatch, etc.).
    """

    def format(self, record):
        import json
        from datetime import datetime

        rid = get_request_id()
        log_entry = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if rid:
            log_entry["request_id"] = rid
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, default=str)


def setup_logging(level: int = logging.INFO, json_output: bool = False) -> None:
    """Configure the root logger.

    Args:
        level: Logging level (default INFO)
        json_output: If True, use JSON formatter (for production)
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    root_logger.handlers.clear()

    # Console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)

    if json_output:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(CustomFormatter())

    root_logger.addHandler(handler)

    # Suppress noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("elasticsearch").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


# Setup default logger (backward compatible)
logger = logging.getLogger("DukaScraper")
logger.setLevel(logging.INFO)

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(CustomFormatter())

# Add handler to logger
if not logger.handlers:
    logger.addHandler(ch)
