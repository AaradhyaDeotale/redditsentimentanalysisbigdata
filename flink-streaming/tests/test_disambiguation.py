"""Unit tests for the context-word disambiguation heuristic."""

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from flink_job.operators.disambiguation import resolve_sense


def test_company_context_wins():
    text = "Just got the new Apple iPhone, love iOS"
    assert resolve_sense("apple", text) == "company"


def test_fruit_context_wins():
    text = "I ate an apple from the tree, best fruit ever"
    assert resolve_sense("apple", text) == "fruit"


def test_no_context_is_ambiguous():
    text = "Apple was mentioned today"
    assert resolve_sense("apple", text) == "ambiguous"


def test_tied_context_is_ambiguous():
    text = "Apple: eat the iphone"  # one fruit word, one company word
    assert resolve_sense("apple", text) == "ambiguous"


def test_case_insensitive_context_match():
    text = "Got a new APPLE IPHONE today"
    assert resolve_sense("apple", text) == "company"


def test_unknown_keyword_is_ambiguous():
    assert resolve_sense("android", "anything") == "ambiguous"
