"""Single source of truth for which URL extensions are fetchable content.

Kept dependency-free (no httpx/kafka/db imports) so both
``app.services.link_extraction_service`` (the crawl front door) and
``app.services.content_ingestion_service`` (the conversion gate) can import it
without pulling in heavyweight modules or creating an import cycle.

HTML, PDF, DOCX/ODT and audio are fetchable. Images, scripts, archives,
executables, video and fonts are not. Audio is fetchable but is handed off for
transcription rather than converted inline.
"""

HTML_EXTENSIONS = {
    "", ".html", ".htm", ".xhtml", ".shtml", ".php", ".asp", ".aspx", ".jsp", ".cgi",
}
PDF_EXTENSIONS = {".pdf"}
DOCX_EXTENSIONS = {".docx", ".doc", ".odt", ".rtf"}
AUDIO_EXTENSIONS = {
    ".mp3", ".wav", ".m4a", ".flac", ".ogg", ".oga", ".opus", ".aac", ".weba", ".wma",
}

#: Everything that is NOT an article-bearing payload.
NON_CONTENT_EXTENSIONS = {
    # images
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".bmp",
    ".tiff", ".avif", ".heic", ".heif",
    # markup-adjacent / code
    ".css", ".js", ".json", ".map", ".wasm", ".xml", ".rss",
    # archives
    ".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz",
    # executables
    ".exe", ".msi", ".dll", ".bin", ".apk", ".dmg", ".iso", ".torrent",
    # video
    ".mp4", ".avi", ".mov", ".mkv", ".webm", ".m4v",
    # fonts
    ".woff", ".woff2", ".ttf", ".eot", ".otf",
}

#: Extensions whose payload can be turned into text or handed off for transcription.
FETCHABLE_EXTENSIONS = (
    HTML_EXTENSIONS | PDF_EXTENSIONS | DOCX_EXTENSIONS | AUDIO_EXTENSIONS
)
