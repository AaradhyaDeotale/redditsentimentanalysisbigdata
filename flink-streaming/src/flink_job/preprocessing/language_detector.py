"""
Language detection for Reddit comments.

Detects the language of each comment so the pipeline can:
  - Tag records with their language code (e.g. "en", "de", "fr")
  - Apply the correct stop-word list per language
  - Support multilingual sentiment analysis (Stage 2 extension)

Uses langdetect (lightweight, no external API calls).
Falls back to "unknown" on short or ambiguous text.
"""

from __future__ import annotations

import logging
from functools import lru_cache

log = logging.getLogger("flink_job.language")

MIN_DETECT_LENGTH = 20

SUPPORTED_LANGUAGES = frozenset({"en", "de", "fr", "es", "nl", "it", "pt"})


@lru_cache(maxsize=1)
def _get_detect_fn():
    try:
        from langdetect import detect, LangDetectException
        return detect, LangDetectException
    except ImportError:
        log.warning("langdetect not installed — all records tagged as 'unknown'")
        return None, None


def detect_language(text: str) -> str:
    if not text or len(text.strip()) < MIN_DETECT_LENGTH:
        return "unknown"

    detect_fn, LangDetectException = _get_detect_fn()
    if detect_fn is None:
        return "unknown"

    try:
        lang = detect_fn(text)
        return lang if isinstance(lang, str) else "unknown"
    except Exception:
        return "unknown"


def is_supported_language(lang_code: str) -> bool:
    return lang_code in SUPPORTED_LANGUAGES
