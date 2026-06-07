"""
Keyword filtering for Reddit comments.

The project spec (Stage 1) requires an operator that tracks sentiment
for specific keywords like Apple vs Android.

This operator tags each comment with which keywords it matched.
Records are not dropped and even non-matching ones flow through.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

log = logging.getLogger("flink_job.keyword_filter")


def _compile_keyword_patterns(keywords: list[str]) -> dict[str, re.Pattern]:
    patterns = {}
    for kw in keywords:
        kw_clean = kw.strip().lower()
        if kw_clean:
            patterns[kw_clean] = re.compile(
                rf"\b{re.escape(kw_clean)}\b", re.IGNORECASE
            )
    return patterns


def load_keywords_from_env() -> list[str]:
    raw = os.getenv("KEYWORD_FILTER", "").strip()
    if not raw:
        return []
    return [kw.strip() for kw in raw.split(",") if kw.strip()]


def _do_map(record: dict[str, Any], patterns: dict) -> dict[str, Any]:
    if not patterns:
        return {**record, "matched_keywords": []}
    search_text = record.get("cleaned_body", "") or " ".join(record.get("tokens", []))
    matched = [kw for kw, pattern in patterns.items() if pattern.search(search_text)]
    return {**record, "matched_keywords": matched}


try:
    from pyflink.datastream.functions import MapFunction

    class KeywordFilterFunction(MapFunction):
        def __init__(self, keywords: list[str] | None = None):
            self._keywords_init = keywords
            self._patterns: dict[str, re.Pattern] = {}

        def open(self, runtime_context):
            keywords = self._keywords_init
            if keywords is None:
                keywords = load_keywords_from_env()
            if keywords:
                self._patterns = _compile_keyword_patterns(keywords)
                log.info("KeywordFilter: tracking %d keyword(s): %s",
                         len(self._patterns), list(self._patterns.keys()))
            else:
                log.info("KeywordFilter: no keywords configured")

        def map(self, record: dict[str, Any]) -> dict[str, Any]:
            return _do_map(record, self._patterns)

except ImportError:
    # Running outside Flink (e.g. unit tests)
    class KeywordFilterFunction:
        def __init__(self, keywords: list[str] | None = None):
            self._keywords_init = keywords or []
            self._patterns = _compile_keyword_patterns(self._keywords_init)

        def map(self, record: dict[str, Any]) -> dict[str, Any]:
            return _do_map(record, self._patterns)
