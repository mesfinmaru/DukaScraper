"""Worker type definitions and routing rules."""

from enum import StrEnum


class WorkerType(StrEnum):
    SURFACE = "surface"
    BROWSER = "browser"
    DEEP = "deep"
    DARK = "dark"
    RSS = "rss"
    PARSER = "parser"
    EXPORTER = "exporter"


# Which worker handles a crawl job based on URL/job metadata
CRAWL_WORKERS = {WorkerType.SURFACE, WorkerType.BROWSER, WorkerType.DEEP, WorkerType.DARK}
PIPELINE_WORKERS = {WorkerType.RSS, WorkerType.PARSER, WorkerType.EXPORTER}
