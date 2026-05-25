"""
UTF-8 aware tokenization that preserves emojis and sentiment symbols.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable

# Split on whitespace; keep emoji blocks, emoticons, and word tokens
_TOKEN_RE = re.compile(
    r"[\w]+(?:'[\w]+)?|"  # words / contractions
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF]+|"  # emoji ranges
    r"[:;=8][-oO]*[)\]\(\[dDpP/\\|]+|"  # emoticons :-) :D
    r"[<>]=?|>=?|<=?|"  # comparison arrows (sentiment-ish)
    r"[!?]+|"  # emphasis
    r"[^\W\d_]+",  # other unicode letter runs (accented words)
    re.UNICODE,
)

_DEFAULT_STOPWORDS = frozenset(
    {
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "is", "it", "this", "that", "with", "as", "by", "from", "be",
        "are", "was", "were", "been", "have", "has", "had", "do", "does", "did",
        "i", "you", "he", "she", "we", "they", "my", "your", "his", "her", "our",
    }
)


@lru_cache(maxsize=1)
def _get_stemmer():
    try:
        from nltk.stem import SnowballStemmer

        return SnowballStemmer("english")
    except Exception:
        return None


def _apply_stopwords(tokens: Iterable[str], enabled: bool) -> list[str]:
    if not enabled:
        return list(tokens)
    return [t for t in tokens if t.lower() not in _DEFAULT_STOPWORDS]


def _apply_stem(tokens: Iterable[str], enabled: bool) -> list[str]:
    if not enabled:
        return list(tokens)
    stemmer = _get_stemmer()
    if stemmer is None:
        return list(tokens)
    return [stemmer.stem(t) if t.isascii() and t.isalpha() else t for t in tokens]


def tokenize(
    text: str,
    *,
    remove_stopwords: bool = False,
    stem: bool = False,
) -> list[str]:
    if not text:
        return []

    raw = _TOKEN_RE.findall(text)
    tokens = _apply_stopwords(raw, remove_stopwords)
    return _apply_stem(tokens, stem)
