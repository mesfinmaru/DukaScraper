"""Regression tests for short non-HTML content being silently discarded.

Two independent bugs dropped short-but-valid payloads on the way to storage:

1. ``clean_and_extract_text`` filtered out every "paragraph" shorter than 8
   characters. That filter exists to sift navigation crumbs out of HTML, but it
   also erased an audio transcript like "You" wholesale, so a transcribed job
   stored an item with ``extracted_text=""``.

2. ``normalize_content`` dropped every whitespace segment shorter than 4
   characters, so a one-word transcript normalized to the empty string and
   hashed identically to an empty body. Every short item therefore became an
   "exact duplicate" of every other short item, and the audio job could never
   be stored at all.

Both are covered here, along with the guards that keep the surrounding
boilerplate filtering intact.
"""

from __future__ import annotations

from app.language.cleaning.cleaner import clean_and_extract_text
from app.services.content_fingerprint_service import (
    content_hash,
    generate_fingerprint,
    normalize_content,
    simhash,
)


class TestCleanAndExtractTextPlainText:
    def test_html_mode_still_drops_short_fragments(self):
        """The default HTML path keeps its short-fragment noise filter."""
        assert clean_and_extract_text("You", "en") == ""

    def test_plain_text_mode_keeps_a_one_word_transcript(self):
        assert clean_and_extract_text("You", "en", plain_text=True) == "You"

    def test_plain_text_mode_keeps_short_lines_of_prose(self):
        text = "Yes.\nOkay.\nThe meeting starts at nine."
        extracted = clean_and_extract_text(text, "en", plain_text=True)
        assert "Yes." in extracted
        assert "Okay." in extracted
        assert "nine" in extracted

    def test_plain_text_mode_returns_normalized_text(self):
        extracted = clean_and_extract_text("You", "en", plain_text=True)
        assert extracted == extracted.strip()
        assert "\n\n" not in extracted

    def test_plain_text_mode_bypasses_the_script_ratio_filter(self):
        """A short transcript must survive even when its language is 'unknown'.

        The Latin/Ethiopic script-ratio passes return "" when nothing clears the
        threshold, which is correct for noisy HTML but wrong for prose.
        """
        assert clean_and_extract_text("Hello world", "unknown", plain_text=True)

    def test_plain_text_still_drops_interstitial_placeholder_lines(self):
        """The anti-bot placeholder filter must apply to plain text too."""
        extracted = clean_and_extract_text(
            "Please Wait... for Checking\nThe real transcript follows here.",
            "en",
            plain_text=True,
        )
        assert "real transcript" in extracted
        assert "Checking" not in extracted

    def test_plain_text_mode_preserves_amharic(self):
        extracted = clean_and_extract_text("ሰላም", "am", plain_text=True)
        assert "ሰላም" in extracted

    def test_html_mode_behaviour_is_unchanged_for_real_content(self):
        html = "<html><body><p>The quick brown fox jumps over the lazy dog.</p></body></html>"
        assert "quick brown fox" in clean_and_extract_text(html, "en")


class TestNormalizeContentShortText:
    def test_short_word_survives_normalization(self):
        assert normalize_content("You") == "you"

    def test_boilerplate_segments_are_still_filtered(self):
        # The navigation-crumb filter must keep working when real content exists.
        assert "home" not in normalize_content("Home | About | Contact")

    def test_content_hash_of_short_text_differs_from_empty(self):
        assert content_hash("You") != content_hash("")

    def test_different_short_texts_hash_differently(self):
        assert content_hash("You") != content_hash("Yes")

    def test_empty_text_still_normalizes_to_empty(self):
        assert normalize_content("") == ""

    def test_simhash_of_short_text_differs_from_empty(self):
        assert simhash(normalize_content("You")) != simhash(normalize_content(""))


class TestFingerprintShortContent:
    def test_short_pages_are_not_collapsing_to_one_fingerprint(self):
        """Two unrelated short pages must not look like exact duplicates."""
        fp_a = generate_fingerprint("https://a.example/x", "You")
        fp_b = generate_fingerprint("https://b.example/y", "Yes")
        assert fp_a.content_fp != fp_b.content_fp

    def test_identical_short_content_still_deduplicates(self):
        """The fix must not weaken real dedup of genuinely identical content."""
        fp_a = generate_fingerprint("https://a.example/x", "You")
        fp_b = generate_fingerprint("https://a.example/x", "You")
        assert fp_a.content_fp == fp_b.content_fp

    def test_url_tier_still_discriminates(self):
        fp_a = generate_fingerprint("https://a.example/x", "You")
        fp_b = generate_fingerprint("https://b.example/y", "You")
        assert fp_a.url_fp != fp_b.url_fp

    def test_short_content_reports_its_real_counts(self):
        fp = generate_fingerprint("https://a.example/x", "You")
        assert fp.word_count == 1
        assert fp.char_count == 3
        assert fp.text_preview == "You"