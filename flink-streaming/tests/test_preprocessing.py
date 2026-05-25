"""Unit tests for emoji-safe preprocessing (no Flink cluster required)."""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from flink_job.preprocessing.cleaner import TextCleaner
from flink_job.preprocessing.tokenizer import tokenize


@pytest.fixture
def cleaner():
    return TextCleaner(remove_urls=True, remove_markdown=True, lowercase=False)


def test_preserves_emojis(cleaner):
    body = "Love this 🔥💯 great post!!"
    assert "🔥" in cleaner.clean(body)
    assert "💯" in cleaner.clean(body)
    tokens = tokenize(cleaner.clean(body))
    assert "🔥" in tokens or "🔥💯" in tokens or any("🔥" in t for t in tokens)


def test_removes_urls(cleaner):
    body = "Check https://example.com/foo and www.reddit.com/r/test"
    cleaned = cleaner.clean(body)
    assert "https://" not in cleaned
    assert "www." not in cleaned


def test_reddit_markdown(cleaner):
    body = "See [link](https://x.com) and /u/someone in /r/python"
    cleaned = cleaner.clean(body)
    assert "https://" not in cleaned
    assert "/u/" not in cleaned


def test_tokenizer_keeps_emoticons():
    text = "I am happy :-) and sad :("
    tokens = tokenize(text)
    assert any(":-)" in t or ":)" in t for t in tokens)


def test_stopwords_optional():
    text = "the quick brown fox"
    with_sw = tokenize(text, remove_stopwords=True)
    without_sw = tokenize(text, remove_stopwords=False)
    assert "the" not in with_sw
    assert "the" in without_sw


def test_utf8_non_ascii():
    body = "Café naïve résumé 🎉"
    cleaned = TextCleaner().clean(body)
    assert "Café" in cleaned
    assert "🎉" in cleaned
