"""
Keyword filtering for Reddit comments.

The project spec (Stage 1) requires an operator that tracks sentiment
for specific keywords like "Apple" vs "Android".

This operator tags each comment with which keywords it matched.
Records are NOT dropped - even non-matching ones flow through.

Keywords are dynamic: the dashboard writes the tracked set into Redis
(SET key ``flink:keywords``) and this operator re-reads it periodically so
keywords can be added/removed while the job is running. If Redis is empty or
unavailable the operator falls back to the static KEYWORD_FILTER env var.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any

from config.settings import parse_subkeywords
from flink_job.operators.disambiguation import AMBIGUOUS_KEYWORDS, resolve_sense

log = logging.getLogger("flink_job.keyword_filter")

KEYWORD_REDIS_KEY = os.getenv("KEYWORD_REDIS_KEY", "flink:keywords")
KEYWORD_REFRESH_SEC = float(os.getenv("KEYWORD_REFRESH_SEC", "5"))
DEFAULT_W2V_MODEL_PATH = "/models/word2vec_subset/word2vec.model"


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


def load_subkeywords_from_env() -> dict[str, list[str]]:
    return parse_subkeywords(os.getenv("SUBKEYWORDS", ""))


def load_w2v_resolver(model_path: str | None = None):
    """Load a W2VSenseResolver from disk, or return None on any failure.

    Never raises: the caller falls back to the hard-coded disambiguation
    path (or "ambiguous") when the model isn't present or fails to load.
    """
    path = model_path or os.getenv("W2V_MODEL_PATH", DEFAULT_W2V_MODEL_PATH)
    try:
        from gensim.models import Word2Vec

        from flink_job.preprocessing.w2v_sense_resolver import W2VSenseResolver

        model = Word2Vec.load(path)
        log.info("KeywordFilter: loaded W2V sense model from %s", path)
        return W2VSenseResolver(model)
    except Exception as exc:  # noqa: BLE001 - any failure means run without it
        log.warning(
            "KeywordFilter: W2V model unavailable at %s (%s), "
            "falling back to hard-coded disambiguation", path, exc,
        )
        return None


def _connect_redis():
    """Return a decode_responses Redis client, or None if unavailable."""
    url = os.getenv("REDIS_URL")
    if not url:
        return None
    try:
        import redis

        client = redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=1,
            socket_timeout=1,
        )
        client.ping()
        return client
    except Exception as exc:  # noqa: BLE001 - any failure means run without Redis
        log.warning("KeywordFilter: Redis unavailable, using env keywords: %s", exc)
        return None


def _do_map(
    record: dict[str, Any],
    patterns: dict,
    subkeywords: dict[str, list[str]] | None = None,
    resolver: Any | None = None,
) -> dict[str, Any]:
    if not patterns:
        return {**record, "matched_keywords": [], "keyword_senses": {}}
    subkeywords = subkeywords or {}
    search_text = record.get("cleaned_body", "") or " ".join(record.get("tokens", []))
    matched = [kw for kw, pattern in patterns.items() if pattern.search(search_text)]

    senses: dict[str, str] = {}
    for kw in matched:
        kw_subkeywords = subkeywords.get(kw)
        if kw_subkeywords:
            if resolver is not None:
                tokens = record.get("tokens", [])
                senses[kw] = resolver.resolve(tokens, kw_subkeywords)
            elif kw in AMBIGUOUS_KEYWORDS:
                senses[kw] = resolve_sense(kw, search_text)
            else:
                senses[kw] = "ambiguous"
        elif kw in AMBIGUOUS_KEYWORDS:
            senses[kw] = resolve_sense(kw, search_text)

    return {**record, "matched_keywords": matched, "keyword_senses": senses}


try:
    from pyflink.datastream.functions import MapFunction

    class KeywordFilterFunction(MapFunction):
        def __init__(
            self,
            keywords: list[str] | None = None,
            subkeywords: dict[str, list[str]] | None = None,
            resolver: Any | None = None,
        ):
            self._keywords_init = keywords
            self._patterns: dict[str, re.Pattern] = {}
            self._current: set[str] = set()
            self._redis = None
            self._last_refresh = 0.0
            self._subkeywords_init = subkeywords
            self._resolver_init = resolver
            self._subkeywords: dict[str, list[str]] = {}
            self._resolver: Any | None = None

        def open(self, runtime_context):
            self._redis = _connect_redis()
            initial = self._read_keywords()
            self._apply(initial)
            self._last_refresh = time.monotonic()

            self._subkeywords = (
                self._subkeywords_init
                if self._subkeywords_init is not None
                else load_subkeywords_from_env()
            )
            if self._resolver_init is not None:
                self._resolver = self._resolver_init
            elif self._subkeywords:
                self._resolver = load_w2v_resolver()
            else:
                self._resolver = None

            log.info("KeywordFilter: initial keyword(s): %s", sorted(self._current))

        def _read_keywords(self) -> list[str]:
            """Current tracked set: Redis if reachable & non-empty, else env/init."""
            if self._redis is not None:
                try:
                    members = self._redis.smembers(KEYWORD_REDIS_KEY)
                    if members:
                        return list(members)
                except Exception as exc:  # noqa: BLE001 - degrade to fallback
                    log.warning("KeywordFilter: Redis read failed: %s", exc)
            if self._keywords_init is not None:
                return self._keywords_init
            return load_keywords_from_env()

        def _apply(self, keywords: list[str]) -> None:
            """Recompile patterns only when the keyword set actually changes."""
            new_set = {k.strip().lower() for k in keywords if k.strip()}
            if new_set != self._current:
                self._current = new_set
                self._patterns = _compile_keyword_patterns(sorted(new_set))
                log.info("KeywordFilter: now tracking %d keyword(s): %s",
                         len(new_set), sorted(new_set))

        def _maybe_refresh(self) -> None:
            # Re-read the tracked set on a timer (not on every record - map()
            # runs thousands of times/sec). On a Redis hiccup, keep the current
            # set rather than flapping back to the env defaults. An empty/unseeded
            # set is ignored for the same reason (tracking nothing is never useful).
            now = time.monotonic()
            if now - self._last_refresh < KEYWORD_REFRESH_SEC:
                return
            self._last_refresh = now
            if self._redis is None:
                return
            try:
                members = self._redis.smembers(KEYWORD_REDIS_KEY)
            except Exception as exc:  # noqa: BLE001 - transient: keep current set
                log.warning("KeywordFilter: refresh failed, keeping current: %s", exc)
                return
            if members:
                self._apply(list(members))

        def map(self, record: dict[str, Any]) -> dict[str, Any]:
            self._maybe_refresh()
            return _do_map(record, self._patterns, self._subkeywords, self._resolver)

except ImportError:
    # Running outside Flink (e.g. unit tests)
    class KeywordFilterFunction:
        def __init__(
            self,
            keywords: list[str] | None = None,
            subkeywords: dict[str, list[str]] | None = None,
            resolver: Any | None = None,
        ):
            self._keywords_init = keywords or []
            self._patterns = _compile_keyword_patterns(self._keywords_init)
            self._subkeywords = subkeywords or {}
            self._resolver = resolver

        def map(self, record: dict[str, Any]) -> dict[str, Any]:
            return _do_map(record, self._patterns, self._subkeywords, self._resolver)
