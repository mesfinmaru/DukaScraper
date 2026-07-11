"""Worker type definitions and routing rules."""

from enum import StrEnum


class WorkerType(StrEnum):
    SURFACE = "surface"
    DEEP = "deep"
    DARK = "dark"


# Which worker handles a crawl job based on URL/job metadata
CRAWL_WORKERS = {WorkerType.SURFACE, WorkerType.DEEP, WorkerType.DARK}
