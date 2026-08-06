"""
Shared utilities for all workers (SURFACE, DEEP, DARK).

This module contains common dependencies that all workers use:
- Link extraction and deduplication (for recursive crawling)
- Worker assignment rules
- Shared configuration
"""

from app.services.dedup_service import DedupService
from app.services.link_extraction_service import LinkExtractionService
from app.services.recursive_crawl_service import extract_and_queue_children
from app.common.constants.worker_assignment import (
    WorkerAssignmentEngine,
    assign_worker,
    assign_worker_with_reason,
    check_escalation,
)

__all__ = [
    "DedupService",
    "LinkExtractionService",
    "extract_and_queue_children",
    "WorkerAssignmentEngine",
    "assign_worker",
    "assign_worker_with_reason",
    "check_escalation",
]