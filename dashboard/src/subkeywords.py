"""
subkeywords.py
---------------
Per-keyword sub-keyword lists: the finer-grained terms a tracked keyword can
be broken into (e.g. "apple" -> ["iphone", "macbook"]). Stored as a Redis
HASH (key ``flink:subkeywords``, field=parent keyword, value=comma-separated
sub-keywords) so a future Flink job can re-read it live, mirroring how
keywords.py stores the tracked-keyword SET.

Stage 2 of the custom-subkeywords feature: this module + the routes in
main.py. The Flink side reading this hash is a later stage.

If Redis is unreachable we degrade to an in-memory dict so the API keeps
working (changes just won't reach Flink until Redis is back).
"""

from __future__ import annotations

import os
import threading

from .keywords import _VALID, connect_redis, normalize

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SUBKEYWORD_REDIS_KEY = os.getenv("SUBKEYWORD_REDIS_KEY", "flink:subkeywords")


def normalize_one(raw: str) -> str | None:
    """Lowercase + trim a single sub-keyword; return None if invalid.

    Reuses keywords._VALID, which already excludes commas - a sub-keyword
    containing one would break the comma-join used to store the list.
    """
    sk = (raw or "").strip().lower()
    if not sk or not _VALID.match(sk):
        return None
    return sk


def normalize_list(raw: list[str]) -> list[str]:
    """Normalize a whole sub-keyword list; raise ValueError on any bad entry.

    An empty list is valid (it clears the sub-keywords for a keyword).
    De-dupes while preserving first-seen order.
    """
    seen: list[str] = []
    for item in raw:
        sk = normalize_one(item)
        if sk is None:
            raise ValueError(f"invalid subkeyword: {item!r}")
        if sk not in seen:
            seen.append(sk)
    return seen


class SubkeywordRegistry:
    def __init__(self):
        self._lock = threading.Lock()
        self._fallback: dict[str, list[str]] = {}  # used only when Redis is down
        self._redis = connect_redis(REDIS_URL)

    def get(self, keyword: str) -> list[str]:
        kw = normalize(keyword)
        if kw is None:
            raise ValueError(f"invalid keyword: {keyword!r}")
        if self._redis is not None:
            try:
                raw = self._redis.hget(SUBKEYWORD_REDIS_KEY, kw)
                return [s for s in raw.split(",") if s] if raw else []
            except Exception:  # noqa: BLE001 - degrade
                self._redis = None
        with self._lock:
            return list(self._fallback.get(kw, []))

    def set(self, keyword: str, subkeywords: list[str]) -> list[str]:
        kw = normalize(keyword)
        if kw is None:
            raise ValueError(f"invalid keyword: {keyword!r}")
        subs = normalize_list(subkeywords)
        if self._redis is not None:
            try:
                if subs:
                    self._redis.hset(SUBKEYWORD_REDIS_KEY, kw, ",".join(subs))
                else:
                    self._redis.hdel(SUBKEYWORD_REDIS_KEY, kw)
                return subs
            except Exception:  # noqa: BLE001 - degrade
                self._redis = None
        with self._lock:
            if subs:
                self._fallback[kw] = subs
            else:
                self._fallback.pop(kw, None)
            return subs

    def get_all(self) -> dict[str, list[str]]:
        if self._redis is not None:
            try:
                raw = self._redis.hgetall(SUBKEYWORD_REDIS_KEY)
                return {k: [s for s in v.split(",") if s] for k, v in raw.items()}
            except Exception:  # noqa: BLE001 - degrade
                self._redis = None
        with self._lock:
            return {k: list(v) for k, v in self._fallback.items()}

    def remove_keyword(self, keyword: str) -> None:
        """Drop a keyword's sub-keyword field entirely (keyword untracked)."""
        kw = normalize(keyword)
        if kw is None:
            return
        if self._redis is not None:
            try:
                self._redis.hdel(SUBKEYWORD_REDIS_KEY, kw)
                return
            except Exception:  # noqa: BLE001 - degrade
                self._redis = None
        with self._lock:
            self._fallback.pop(kw, None)


registry = SubkeywordRegistry()
