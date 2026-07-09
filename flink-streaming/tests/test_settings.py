"""Unit tests for config/settings.py env parsing helpers."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from config.settings import parse_subkeywords


def test_parse_subkeywords_multiple_keywords():
    result = parse_subkeywords("apple:technology,fruit;jaguar:car,animal")
    assert result == {
        "apple": ["technology", "fruit"],
        "jaguar": ["car", "animal"],
    }


def test_parse_subkeywords_empty_string():
    assert parse_subkeywords("") == {}


def test_parse_subkeywords_lowercases_and_strips():
    result = parse_subkeywords(" Apple : Technology , Fruit ")
    assert result == {"apple": ["technology", "fruit"]}


def test_parse_subkeywords_skips_malformed_segments():
    # missing ":", empty keyword, and empty subkeyword list are all skipped
    result = parse_subkeywords("apple:fruit;noColonHere;:car,animal;jaguar:")
    assert result == {"apple": ["fruit"]}
