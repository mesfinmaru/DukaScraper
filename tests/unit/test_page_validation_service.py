from app.services.page_validation_service import PageValidationService


def test_security_article_mentioning_captcha_is_not_a_challenge_page():
    html = "<html><title>Security report</title><body>" + ("This report explains CAPTCHA protection in a ransomware campaign. " * 20) + "</body></html>"

    result = PageValidationService.validate("https://example.test/report", html, 200, "en")

    assert result.status == "completed"


def test_clear_challenge_phrase_is_held_for_review():
    html = "<html><title>Please wait</title><body>" + ("Verify you are human before continuing. " * 8) + "</body></html>"

    result = PageValidationService.validate("https://example.test/report", html, 200, "en")

    assert result.status == "needs_review"


def test_document_language_recovers_english_page_with_mixed_navigation_text():
    html = '<html lang="en"><title>Report</title><body>' + ("English vulnerability report content. " * 8) + "</body></html>"

    result = PageValidationService.validate("https://example.test/report", html, 200, "en")

    assert result.status == "completed"
    assert result.language == "en"


def test_registration_page_is_not_reported_as_insufficient_content():
    html = "<html><title>Register</title><body>Create Account</body></html>"

    result = PageValidationService.validate("https://signup.com/Organizer/Register/", html, 200, "en")

    assert result.status == "skipped"
    assert result.reason == "registration_page"
