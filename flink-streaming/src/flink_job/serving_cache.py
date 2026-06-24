"""Redis-backed prediction cache.

Maps a comment's tokens to a previously computed sentiment result so repeated or
identical comments skip the model. Keys are namespaced by model version, so a
retrain starts a fresh cache and a stale answer is never served. Every Redis
call degrades gracefully: if the cache is unreachable, ``get`` misses and
``put`` is a no-op, and scoring continues normally.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Sequence


class PredictionCache:
    def __init__(self, client, *, version: str, ttl_sec: int = 86400):
        self._client = client
        self._version = version
        self._ttl = ttl_sec

    def _key(self, tokens: Sequence[str]) -> str:
        digest = hashlib.sha1(" ".join(str(t) for t in tokens).encode("utf-8")).hexdigest()
        return f"pred:{self._version}:{digest}"

    def get(self, tokens: Sequence[str]) -> dict[str, Any] | None:
        try:
            raw = self._client.get(self._key(tokens))
        except Exception:
            return None
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except (TypeError, ValueError):
            return None

    def put(self, tokens: Sequence[str], result: dict[str, Any]) -> None:
        try:
            self._client.setex(self._key(tokens), self._ttl, json.dumps(result))
        except Exception:
            pass
