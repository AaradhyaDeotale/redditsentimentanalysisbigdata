"""
UTF-8 aware tokenization that preserves emojis and sentiment symbols.

Improvements over original:
  - Multi-language stop-word support (en, de, fr, es, nl, it, pt)
  - Language-aware stemming
  - Each emoji is its own token
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

_STOPWORDS: dict[str, frozenset[str]] = {
    "en": frozenset({
        "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "is", "it", "this", "that", "with", "as", "by", "from", "be",
        "are", "was", "were", "been", "have", "has", "had", "do", "does", "did",
        "i", "you", "he", "she", "we", "they", "my", "your", "his", "her", "our",
        "not", "no", "so", "if", "up", "out", "about", "just", "get", "would",
        "could", "should", "will", "can", "its", "also", "than", "then", "when",
    }),
    "de": frozenset({
        "der", "die", "das", "ein", "eine", "und", "oder", "aber", "in", "auf",
        "an", "zu", "für", "von", "mit", "ist", "sind", "war", "waren", "ich",
        "du", "er", "sie", "wir", "ihr", "nicht", "auch", "sich", "es",
    }),
    "fr": frozenset({
        "le", "la", "les", "un", "une", "des", "et", "ou", "mais", "dans",
        "sur", "à", "de", "du", "pour", "par", "avec", "est", "sont", "je",
        "tu", "il", "elle", "nous", "vous", "ils", "elles", "ne", "pas", "aussi",
    }),
    "es": frozenset({
        "el", "la", "los", "las", "un", "una", "y", "o", "pero", "en", "a",
        "de", "del", "para", "por", "con", "es", "son", "yo", "no", "también",
    }),
    "nl": frozenset({
        "de", "het", "een", "en", "of", "maar", "in", "op", "aan", "te",
        "van", "met", "is", "zijn", "ik", "jij", "hij", "zij", "wij", "niet",
    }),
    "it": frozenset({
        "il", "la", "i", "le", "un", "una", "e", "o", "ma", "in", "a",
        "di", "del", "per", "con", "è", "sono", "io", "tu", "non", "anche",
    }),
    "pt": frozenset({
        "o", "a", "os", "as", "um", "uma", "e", "ou", "mas", "em",
        "de", "do", "da", "para", "por", "com", "é", "são", "eu", "não",
    }),
}

_DEFAULT_LANG = "en"


@lru_cache(maxsize=8)
def _get_stemmer(language: str):
    try:
        from nltk.stem import SnowballStemmer
        lang_map = {
            "en": "english", "de": "german", "fr": "french",
            "es": "spanish", "nl": "dutch", "it": "italian", "pt": "portuguese",
        }
        return SnowballStemmer(lang_map.get(language, "english"))
    except Exception:
        return None


def _get_stopwords(language: str) -> frozenset[str]:
    return _STOPWORDS.get(language, _STOPWORDS[_DEFAULT_LANG])


def _apply_stopwords(tokens: Iterable[str], enabled: bool, language: str = _DEFAULT_LANG) -> list[str]:
    if not enabled:
        return list(tokens)
    sw = _get_stopwords(language)
    return [t for t in tokens if t.lower() not in sw]


def _apply_stem(tokens: Iterable[str], enabled: bool, language: str = _DEFAULT_LANG) -> list[str]:
    if not enabled:
        return list(tokens)
    stemmer = _get_stemmer(language)
    if stemmer is None:
        return list(tokens)
    return [stemmer.stem(t) if t.isalpha() else t for t in tokens]


def tokenize(
    text: str,
    *,
    remove_stopwords: bool = False,
    stem: bool = False,
    language: str = _DEFAULT_LANG,
) -> list[str]:
    if not text:
        return []
    raw = _TOKEN_RE.findall(text)
    tokens = _apply_stopwords(raw, remove_stopwords, language)
    return _apply_stem(tokens, stem, language)
