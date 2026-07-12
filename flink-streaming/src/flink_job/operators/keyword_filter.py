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
SUBKEYWORD_REDIS_KEY = os.getenv("SUBKEYWORD_REDIS_KEY", "flink:subkeywords")
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


class _KeywordFilterBase:
    """Stateful refresh machinery, kept pyflink-independent so it can be
    unit-tested without a pyflink/redis install (see subclasses below)."""

    def __init__(
        self,
        keywords: list[str] | None = None,
        subkeywords: dict[str, list[str]] | None = None,
        resolver: Any | None = None,
    ):
        self._keywords_init = keywords
        self._subkeywords_init = subkeywords
        self._resolver_init = resolver
        self._redis = None
        self._last_refresh = 0.0

        # Eagerly usable straight from the constructor args, without calling
        # open() first - this is the no-pyflink test/standalone contract the
        # old fallback class provided (construct-and-map(), no Flink runtime
        # driving open()). The real streaming job always calls open() before
        # the first map(), which re-derives everything from Redis/env on top
        # of whatever is set here.
        self._current: set[str] = {
            k.strip().lower() for k in (keywords or []) if k.strip()
        }
        self._patterns: dict[str, re.Pattern] = _compile_keyword_patterns(
            sorted(self._current)
        )
        self._subkeywords: dict[str, list[str]] = subkeywords or {}
        self._resolver: Any | None = resolver

    def open(self, runtime_context):
        self._redis = _connect_redis()
        initial = self._read_keywords()
        self._apply(initial)
        self._last_refresh = time.monotonic()

        self._apply_subkeywords(self._read_subkeywords())

        if self._resolver_init is not None:
            self._resolver = self._resolver_init
        else:
            # Load unconditionally (not gated on self._subkeywords being
            # non-empty at startup) so subkeywords added live via Redis
            # after this point still get classified instead of silently
            # degrading to "ambiguous". load_w2v_resolver() never raises -
            # a missing/broken model just leaves self._resolver as None.
            self._resolver = load_w2v_resolver()

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

    def _read_subkeywords(self) -> dict[str, list[str]]:
        """Current subkeyword map: Redis if reachable & non-empty, else env/init."""
        if self._redis is not None:
            try:
                raw = self._redis.hgetall(SUBKEYWORD_REDIS_KEY)
                if raw:
                    return {k: [s for s in v.split(",") if s] for k, v in raw.items()}
            except Exception as exc:  # noqa: BLE001 - degrade to fallback
                log.warning("KeywordFilter: Redis subkeyword read failed: %s", exc)
        if self._subkeywords_init is not None:
            return self._subkeywords_init
        return load_subkeywords_from_env()

    def _apply(self, keywords: list[str]) -> None:
        """Recompile patterns only when the keyword set actually changes."""
        new_set = {k.strip().lower() for k in keywords if k.strip()}
        if new_set != self._current:
            self._current = new_set
            self._patterns = _compile_keyword_patterns(sorted(new_set))
            log.info("KeywordFilter: now tracking %d keyword(s): %s",
                     len(new_set), sorted(new_set))

    def _apply_subkeywords(self, subkeywords: dict[str, list[str]]) -> None:
        """Update the tracked subkeyword map only when it actually changes."""
        if subkeywords != self._subkeywords:
            self._subkeywords = subkeywords
            log.info(
                "KeywordFilter: now tracking subkeywords for %d keyword(s): %s",
                len(subkeywords), sorted(subkeywords),
            )

    def _maybe_refresh(self) -> None:
        # Re-read the tracked set on a timer (not on every record - map()
        # runs thousands of times/sec). On a Redis hiccup, keep the current
        # set rather than flapping back to the env defaults. An empty/unseeded
        # set is ignored for the same reason (tracking nothing is never useful).
        # Keywords and subkeywords share this one timer/cadence.
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
        else:
            if members:
                self._apply(list(members))
        try:
            subkeywords_raw = self._redis.hgetall(SUBKEYWORD_REDIS_KEY)
        except Exception as exc:  # noqa: BLE001 - transient: keep current set
            log.warning(
                "KeywordFilter: subkeyword refresh failed, keeping current: %s", exc
            )
        else:
            if subkeywords_raw:
                subs = {
                    k: [s for s in v.split(",") if s]
                    for k, v in subkeywords_raw.items()
                }
                self._apply_subkeywords(subs)

    def map(self, record: dict[str, Any]) -> dict[str, Any]:
        self._maybe_refresh()
        return _do_map(record, self._patterns, self._subkeywords, self._resolver)


try:
    from pyflink.datastream.functions import MapFunction

    class KeywordFilterFunction(_KeywordFilterBase, MapFunction):
        pass

except ImportError:
    # Running outside Flink (e.g. unit tests, or no pyflink/redis installed)
    class KeywordFilterFunction(_KeywordFilterBase):
        pass
