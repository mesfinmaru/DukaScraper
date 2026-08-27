"""
Shared text normalization and deduplication helpers.

These are lightweight, dependency-free utilities for cleaning extracted
text.  For full HTML-to-text extraction with language-aware filtering
see ``app.language.cleaning.cleaner``.
"""

import re


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted text.

    Handles ``\\r\\n`` → ``\\n``, tabs, non-breaking spaces, and
    collapses repeated whitespace.  Does **not** touch HTML entities
    (``HTMLParser(convert_charrefs=True)`` already handles those).
    """
    if not text:
        return ""

    text = text.replace("\r\n", "\n")
    text = text.replace("\r", "\n")
    text = text.replace("\t", " ")
    text = text.replace("\u00a0", " ")  # non-breaking space

    # Collapse runs of spaces.
    text = re.sub(r"[ ]{2,}", " ", text)
    # Strip spaces around newlines.
    text = re.sub(r"[ ]*\n[ ]*", "\n", text)
    # Collapse 3+ blank lines to 2.
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def remove_duplicate_lines(text: str) -> str:
    """Remove repeated lines from *text*, preserving order.

    Each line is normalized (lower-cased, whitespace-collapsed) before
    the duplicate check.  Lines of 1 character or fewer are discarded
    as noise.
    """
    if not text:
        return ""

    result: list[str] = []
    seen: set[str] = set()

    for line in text.splitlines():
        line = clean_text(line)
        if not line:
            continue
        if len(line) <= 1:
            continue

        normalized = re.sub(r"\s+", " ", line.lower()).strip()
        if normalized in seen:
            continue

        seen.add(normalized)
        result.append(line)

    return "\n\n".join(result)
