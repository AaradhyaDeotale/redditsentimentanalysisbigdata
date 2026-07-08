"""
keywords.py
-----------
The tracked-keyword registry: the single source of truth for *which* keywords
the pipeline scores. Stored as a Redis SET (key ``flink:keywords``) so the
running Flink job can re-read it live (see flink_job/operators/keyword_filter.py).

The dashboard is the only writer; Flink only reads. The SET is seeded once from
the DEFAULT_KEYWORDS (or KEYWORD_FILTER) env var if it does not exist yet, so a
fresh stack still starts with apple/android.

If Redis is unreachable we degrade to an in-memory set so the API keeps working
(changes just won't reach Flink until Redis is back).
"""

from __future__ import annotations

import os
import re
import threading

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
KEYWORD_REDIS_KEY = os.getenv("KEYWORD_REDIS_KEY", "flink:keywords")
_DEFAULTS = os.getenv("DEFAULT_KEYWORDS", os.getenv("KEYWORD_FILTER", "apple,android"))

# A tracked keyword: letters/digits/space/+/#/-, 1-40 chars (e.g. "c++", "node.js"
# stay simple; this guards the SET and the regex the filter builds from it).
_VALID = re.compile(r"^[a-z0-9 .+#-]{1,40}$")


def normalize(raw: str) -> str | None:
    """Lowercase + trim a user-supplied keyword; return None if invalid."""
    kw = (raw or "").strip().lower()
    if not kw or not _VALID.match(kw):
        return None
    return kw


def connect_redis(url: str = REDIS_URL):
    """decode_responses Redis client, or None if unreachable.

    Shared by the keyword registry and the control panel's replay-cursor
    persistence so connection settings live in one place.
    """
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
    except Exception:  # noqa: BLE001 - caller runs degraded without Redis
        return None


class KeywordRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._fallback: set[str] = set()  # used only when Redis is down
        self._redis = connect_redis()
        self._seed_defaults()

    def _seed_defaults(self) -> None:
        seeds = {normalize(k) for k in _DEFAULTS.split(",")}
        seeds.discard(None)
        if not seeds:
            return
        if self._redis is not None:
            try:
                # Only seed when the set has never been created.
                if not self._redis.exists(KEYWORD_REDIS_KEY):
                    self._redis.sadd(KEYWORD_REDIS_KEY, *seeds)
                return
            except Exception:  # noqa: BLE001 - fall through to in-memory
                self._redis = None
        self._fallback = set(seeds)

    def list(self) -> list[str]:
        if self._redis is not None:
            try:
                return sorted(self._redis.smembers(KEYWORD_REDIS_KEY))
            except Exception:  # noqa: BLE001 - degrade
                self._redis = None
        with self._lock:
            return sorted(self._fallback)

    def add(self, keyword: str) -> list[str]:
        kw = normalize(keyword)
        if kw is None:
            raise ValueError(f"invalid keyword: {keyword!r}")
        if self._redis is not None:
            try:
                self._redis.sadd(KEYWORD_REDIS_KEY, kw)
                return self.list()
            except Exception:  # noqa: BLE001 - degrade
                self._redis = None
        with self._lock:
            self._fallback.add(kw)
            return sorted(self._fallback)

    def remove(self, keyword: str) -> list[str]:
        kw = normalize(keyword)
        if kw is None:
            raise ValueError(f"invalid keyword: {keyword!r}")
        if self._redis is not None:
            try:
                self._redis.srem(KEYWORD_REDIS_KEY, kw)
                return self.list()
            except Exception:  # noqa: BLE001 - degrade
                self._redis = None
        with self._lock:
            self._fallback.discard(kw)
            return sorted(self._fallback)


registry = KeywordRegistry()
