"""Regression tests for the signup required-field sweep.

Found by driving the real AutoSignupHandler against a registration form with a
split firstName/lastName layout: the handler filled `firstName` and left
`lastName` empty. Because `lastName` was `required`, the browser refused to
submit the form. Nothing navigated, no error was shown, and the page kept
looking like the form was still present -- so signup was reported as a plain
failure with no clue why, and no account was ever created.

`_fill_missing_required_fields` closes that gap. These tests pin the behaviour
that matters, including the deliberate decision NOT to invent values for
fields where a guess would create a genuinely wrong account.
"""

from __future__ import annotations

import pytest

from app.services import auto_signup_handler as ash


class _StubElement:
    """Minimal stand-in for a Playwright ElementHandle."""

    def __init__(
        self,
        *,
        tag: str = "input",
        name: str = "",
        id_: str = "",
        placeholder: str = "",
        input_type: str = "text",
        value: str = "",
        options: list[str] | None = None,
    ):
        self.tag = tag
        self.name = name
        self.id_ = id_
        self.placeholder = placeholder
        self.input_type = input_type
        self.value = value
        self.options = options or []
        self.filled: list[str] = []
        self.selected: list[str] = []

    async def get_attribute(self, attr: str) -> str:
        return {
            "name": self.name,
            "id": self.id_,
            "placeholder": self.placeholder,
            "type": self.input_type,
        }.get(attr, "")

    async def evaluate(self, js: str):
        if "tagName" in js:
            return self.tag
        if "e.value" in js:
            return self.value
        if "options" in js:
            return self.options
        return None

    async def fill(self, value: str):
        self.value = value
        self.filled.append(value)

    async def select_option(self, value: str):
        self.value = value
        self.selected.append(value)


class _StubPage:
    def __init__(self, elements: list[_StubElement]):
        self.elements = elements

    async def query_selector_all(self, selector: str):
        return self.elements


async def _run(elements: list[_StubElement]) -> None:
    handler = ash.AutoSignupHandler()
    await handler._fill_missing_required_fields(
        _StubPage(elements), "user@example.com", "Sup3rSecret!42"
    )


class TestRequiredFieldSweep:
    async def test_fills_empty_required_last_name(self):
        el = _StubElement(name="lastName")
        await _run([el])
        assert el.filled == [ash._SIGNUP_LAST_NAME]

    async def test_fills_empty_required_first_name(self):
        el = _StubElement(name="firstName")
        await _run([el])
        assert el.filled == [ash._SIGNUP_FIRST_NAME]

    async def test_fills_required_email_and_password_with_real_values(self):
        email_el = _StubElement(name="email", input_type="email")
        pw_el = _StubElement(name="password", input_type="password")
        await _run([email_el, pw_el])
        assert email_el.filled == ["user@example.com"]
        assert pw_el.filled == ["Sup3rSecret!42"]

    async def test_matches_on_id_and_placeholder_when_name_is_absent(self):
        by_id = _StubElement(id_="user_surname")
        by_placeholder = _StubElement(placeholder="Last name")
        await _run([by_id, by_placeholder])
        assert by_id.filled == [ash._SIGNUP_LAST_NAME]
        assert by_placeholder.filled == [ash._SIGNUP_LAST_NAME]

    async def test_fills_generic_text_field_with_full_name(self):
        el = _StubElement(name="city")
        await _run([el])
        assert el.filled == [ash._SIGNUP_FULL_NAME]

    async def test_leaves_already_filled_fields_untouched(self):
        el = _StubElement(name="firstName", value="Already")
        await _run([el])
        assert el.filled == []

    async def test_selects_first_option_for_required_select(self):
        el = _StubElement(tag="select", name="country", options=["ET", "US"])
        await _run([el])
        assert el.selected == ["ET"]

    async def test_skips_required_select_with_no_usable_options(self):
        el = _StubElement(tag="select", name="country", options=[])
        await _run([el])
        assert el.selected == []

    @pytest.mark.parametrize(
        "input_type,name",
        [
            ("tel", "phone"),
            ("date", "dob"),
            ("datetime-local", "appointment"),
            ("month", "startMonth"),
            ("week", "startWeek"),
            ("time", "preferredTime"),
            ("file", "avatar"),
            ("number", "quantity"),
        ],
    )
    async def test_does_not_invent_values_for_unguessable_fields(self, input_type, name):
        """A fabricated phone number or DOB would create a wrong real account."""
        el = _StubElement(name=name, input_type=input_type)
        await _run([el])
        assert el.filled == []

    @pytest.mark.parametrize("input_type", ["checkbox", "radio", "hidden", "submit"])
    async def test_skips_non_text_controls(self, input_type):
        el = _StubElement(name=f"x_{input_type}", input_type=input_type)
        await _run([el])
        assert el.filled == []