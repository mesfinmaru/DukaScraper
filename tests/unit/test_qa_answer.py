"""Tests for AutoSignupHandler._compute_qa_answer — anti-bot Q&A solver."""

import pytest

from app.services.auto_signup_handler import auto_signup_handler


class TestNumericMath:
    """Numeric arithmetic: 5+2→7, 10-3→7, 4*3→12, 8/2→4, 10%3→1"""

    def test_addition(self):
        assert auto_signup_handler._compute_qa_answer("What is 4+2?") == "6"

    def test_addition_with_spaces(self):
        assert auto_signup_handler._compute_qa_answer("What is 5 + 3 ?") == "8"

    def test_subtraction(self):
        assert auto_signup_handler._compute_qa_answer("What is 10-3?") == "7"

    def test_multiplication(self):
        assert auto_signup_handler._compute_qa_answer("What is 4*3?") == "12"

    def test_division(self):
        assert auto_signup_handler._compute_qa_answer("What is 8/2?") == "4"

    def test_division_unicode(self):
        assert auto_signup_handler._compute_qa_answer("What is 12÷3?") == "4"

    def test_modulo(self):
        assert auto_signup_handler._compute_qa_answer("What is 10%3?") == "1"

    def test_addition_in_sentence(self):
        assert auto_signup_handler._compute_qa_answer(
            "Please compute 15 + 7 to prove you are human"
        ) == "22"

    def test_subtraction_in_sentence(self):
        assert auto_signup_handler._compute_qa_answer(
            "Enter the result of 20 - 8"
        ) == "12"

    def test_zero_result(self):
        assert auto_signup_handler._compute_qa_answer("What is 5-5?") == "0"

    def test_large_numbers(self):
        assert auto_signup_handler._compute_qa_answer("What is 123+456?") == "579"


class TestWordMath:
    """Word-based math: 'five plus two' → 7"""

    def test_word_addition(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is five plus two?"
        ) == "7"

    def test_word_subtraction(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is twelve minus three?"
        ) == "9"

    def test_word_multiplication(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is three times four?"
        ) == "12"

    def test_word_multiplication_long(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is six multiplied by two?"
        ) == "12"

    def test_word_division(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is ten divided by two?"
        ) == "5"

    def test_word_division_over(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is eight over four?"
        ) == "2"

    def test_word_higher_numbers(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is fifteen plus five?"
        ) == "20"


class TestColorQA:
    """Color-based questions: sky → blue, grass → green, etc."""

    def test_sky(self):
        assert auto_signup_handler._compute_qa_answer("What color is the sky?") == "blue"

    def test_grass(self):
        assert auto_signup_handler._compute_qa_answer("What color is grass?") == "green"

    def test_snow(self):
        assert auto_signup_handler._compute_qa_answer("What color is snow?") == "white"

    def test_fire(self):
        assert auto_signup_handler._compute_qa_answer("What color is fire?") == "red"

    def test_orange_fruit(self):
        assert auto_signup_handler._compute_qa_answer(
            "What color is an orange?"
        ) == "orange"

    def test_generic_color_question(self):
        assert auto_signup_handler._compute_qa_answer("What color?") == "blue"


class TestCapitalsQA:
    """World capital questions."""

    def test_france(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is the capital of France?"
        ) == "paris"

    def test_japan(self):
        assert auto_signup_handler._compute_qa_answer(
            "capital of japan"
        ) == "tokyo"

    def test_ethiopia(self):
        assert auto_signup_handler._compute_qa_answer(
            "capital of ethiopia"
        ) == "addis ababa"

    def test_nigeria(self):
        assert auto_signup_handler._compute_qa_answer(
            "capital of nigeria"
        ) == "abuja"

    def test_kenya(self):
        assert auto_signup_handler._compute_qa_answer(
            "capital of kenya"
        ) == "nairobi"

    def test_uk(self):
        assert auto_signup_handler._compute_qa_answer(
            "capital of uk"
        ) == "london"

    def test_germany(self):
        assert auto_signup_handler._compute_qa_answer(
            "capital of germany"
        ) == "berlin"

    def test_case_insensitive(self):
        assert auto_signup_handler._compute_qa_answer(
            "Capital of FRANCE"
        ) == "paris"


class TestGeneralKnowledge:
    """General knowledge Q&A."""

    def test_largest_ocean(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is the largest ocean?"
        ) == "pacific"

    def test_highest_mountain(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is the highest mountain?"
        ) == "everest"

    def test_days_in_week(self):
        assert auto_signup_handler._compute_qa_answer(
            "How many days in a week?"
        ) == "7"

    def test_months_in_year(self):
        assert auto_signup_handler._compute_qa_answer(
            "How many months in a year?"
        ) == "12"

    def test_longest_river(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is the longest river?"
        ) == "nile"


class TestDayOfWeek:
    """Day-of-week questions: 'after Monday' → tuesday."""

    def test_after_monday(self):
        assert auto_signup_handler._compute_qa_answer(
            "What day comes after Monday?"
        ) == "tuesday"

    def test_after_friday(self):
        assert auto_signup_handler._compute_qa_answer(
            "What day comes after friday?"
        ) == "saturday"

    def test_after_sunday_wraps(self):
        assert auto_signup_handler._compute_qa_answer(
            "day after sunday"
        ) == "monday"


class TestSmartFallback:
    """When nothing else matches, try to extract numbers or return None."""

    def test_empty_string(self):
        assert auto_signup_handler._compute_qa_answer("") is None

    def test_none_input(self):
        assert auto_signup_handler._compute_qa_answer(None) is None

    def test_unrecognized_returns_none(self):
        assert auto_signup_handler._compute_qa_answer(
            "What is the meaning of life?"
        ) is None

    def test_number_in_unrecognized_question(self):
        # Falls back to extracting first number found
        assert auto_signup_handler._compute_qa_answer(
            "Enter the number 42 to continue"
        ) == "42"
