"""
UTF-8 aware tokenization that preserves emojis and sentiment symbols.

The pipeline is English-only (non-English comments are dropped during
preprocessing), so stop-word removal and stemming are English-only too.
Each emoji is its own token.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable

_TOKEN_RE = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F900-\U0001F9FF]+"
    r"|[\w]+(?:'[\w]+)?"
    r"|[:;=8][-oO]*[)\]\(\[dDpP/\\|]+"
    r"|[!?]+"
    r"|[^\W\d_]+",
    re.UNICODE,
)

_STOPWORDS: frozenset[str] = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "is", "it", "this", "that", "with", "as", "by", "from", "be",
    "are", "was", "were", "been", "have", "has", "had", "do", "does", "did",
    "i", "you", "he", "she", "we", "they", "my", "your", "his", "her", "our",
    "not", "no", "so", "if", "up", "out", "about", "just", "get", "would",
    "could", "should", "will", "can", "its", "also", "than", "then", "when",
})


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
    return [t for t in tokens if t.lower() not in _STOPWORDS]


def _apply_stem(tokens: Iterable[str], enabled: bool) -> list[str]:
    if not enabled:
        return list(tokens)
    stemmer = _get_stemmer()
    if stemmer is None:
        return list(tokens)
    return [stemmer.stem(t) if t.isalpha() else t for t in tokens]


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
