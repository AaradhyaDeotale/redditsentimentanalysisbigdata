"""
sentiment_ml.py
---------------
Real sentiment scorer that replaces NullSentimentScorer. It loads a trained
SentimentModel from the model store (produced by ml-model/train.py) and scores
each comment's tokens. Implements the SentimentScorer interface so it is a
drop-in swap in reddit_stream_job.py.

Requires the `ml-model` package and its dependencies to be importable inside the
Flink image, and the model directory (MODEL_DIR) to be mounted/available.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from flink_job.operators.sentiment_placeholder import SentimentScorer

log = logging.getLogger("flink_job.sentiment_ml")


class RealSentimentScorer(SentimentScorer):
    def __init__(self, model_dir: str | None = None, min_tokens: int | None = None,
                 reload_every: int = 500):
        self._model_dir = model_dir or os.getenv("MODEL_DIR", "/models")
        self._min_tokens = int(min_tokens if min_tokens is not None
                               else os.getenv("MIN_TOKENS", "2"))
        self._reload_every = reload_every
        self._scorer = None
        self._since_reload = 0

    def _ensure_loaded(self) -> None:
        if self._scorer is None:
            from ml_model.serving.scorer import ModelScorer
            # Don't eager-load: if no model exists yet, score() returns
            # "no_model_available" and picks it up as soon as it appears.
            self._scorer = ModelScorer(self._model_dir, min_tokens=self._min_tokens)
            log.info("Sentiment scorer ready (model dir: %s)", self._model_dir)

    def score(self, cleaned_body: str, tokens: list[str]) -> dict[str, Any]:
        self._ensure_loaded()
        self._since_reload += 1
        if self._reload_every and self._since_reload >= self._reload_every:
            self._since_reload = 0
            if self._scorer.maybe_reload():
                log.info("Hot-reloaded model -> version '%s'", self._scorer.model_version)
        return self._scorer.score(tokens)

    def score_batch(self, token_lists: list[list[str]]) -> list[dict[str, Any]]:
        self._ensure_loaded()
        self._since_reload += len(token_lists)
        if self._reload_every and self._since_reload >= self._reload_every:
            self._since_reload = 0
            if self._scorer.maybe_reload():
                log.info("Hot-reloaded model -> version '%s'", self._scorer.model_version)
        return self._scorer.score_batch(token_lists)

    def reload_now(self) -> bool:
        return self._scorer is not None and self._scorer.maybe_reload()

    @property
    def model_version(self):
        return self._scorer.model_version if self._scorer is not None else None


try:
    from pyflink.datastream.functions import MapFunction

    class SentimentMLFunction(MapFunction):
        """Drop-in replacement for SentimentPlaceholderFunction."""

        def __init__(self, model_dir: str | None = None, min_tokens: int | None = None):
            self._model_dir = model_dir
            self._min_tokens = min_tokens
            self._scorer = None

        def open(self, runtime_context):
            self._scorer = RealSentimentScorer(self._model_dir, self._min_tokens)

        def map(self, value: dict) -> dict:
            meta = self._scorer.score(value.get("cleaned_body", ""), value.get("tokens", []))
            return {**value, **meta}

    from pyflink.datastream.functions import FlatMapFunction

    from flink_job.operators.batch_buffer import BatchBuffer

    class SentimentMLBatchFunction(FlatMapFunction):
        """Micro-batching scorer with an optional Redis prediction cache and
        model-reload pub/sub.

        Cache hits are emitted immediately; misses are buffered and scored in one
        vectorized call when the buffer fills (BATCH_MAX_SIZE) or BATCH_MAX_MS
        has elapsed. A FlatMapFunction (not a keyed ProcessFunction) is used so
        no keyed-timer context is needed — the time-based flush is driven off
        arriving records, which is sufficient for a continuous replay.
        """

        def __init__(self, model_dir=None, min_tokens=None, max_size=None, max_ms=None):
            self._model_dir = model_dir
            self._min_tokens = min_tokens
            self._max_size_init = max_size
            self._max_ms_init = max_ms

        def open(self, runtime_context):
            import os
            import time

            self._scorer = RealSentimentScorer(self._model_dir, self._min_tokens)
            self._max_ms = int(self._max_ms_init if self._max_ms_init is not None
                               else os.getenv("BATCH_MAX_MS", "200"))
            max_size = int(self._max_size_init if self._max_size_init is not None
                           else os.getenv("BATCH_MAX_SIZE", "256"))
            self._emit = []
            self._buffer = BatchBuffer(max_size=max_size, on_flush=self._score_batch_into_emit)
            self._last_flush = time.monotonic()
            self._ttl = int(os.getenv("CACHE_TTL_SEC", "86400"))
            self._redis = None
            self._listener = None
            url = os.getenv("REDIS_URL")
            if url:
                try:
                    import redis

                    from flink_job.reload_signal import ReloadListener
                    self._redis = redis.from_url(url, decode_responses=True)
                    self._listener = ReloadListener()
                    self._start_reload_subscriber()
                    log.info("Prediction cache + reload pub/sub enabled (%s)", url)
                except Exception as exc:
                    log.warning("Redis unavailable, running without cache: %s", exc)
                    self._redis = None

        def _start_reload_subscriber(self):
            import threading

            from flink_job.reload_signal import RELOAD_CHANNEL

            def run():
                try:
                    pubsub = self._redis.pubsub()
                    pubsub.subscribe(RELOAD_CHANNEL)
                    for msg in pubsub.listen():
                        if msg.get("type") == "message":
                            self._listener.notify(msg.get("data"))
                except Exception:
                    pass

            threading.Thread(target=run, daemon=True).start()

        def _cache(self):
            if self._redis is None:
                return None
            from flink_job.serving_cache import PredictionCache
            return PredictionCache(self._redis, version=self._scorer.model_version or "none",
                                   ttl_sec=self._ttl)

        def _score_batch_into_emit(self, batch):
            import time

            token_lists = [r.get("tokens", []) for r in batch]
            metas = self._scorer.score_batch(token_lists)
            cache = self._cache()
            for record, meta in zip(batch, metas):
                if cache is not None and meta.get("sentiment_status") == "scored":
                    cache.put(record.get("tokens", []), meta)
                self._emit.append({**record, **meta, "cache_hit": False})
            self._last_flush = time.monotonic()

        def flat_map(self, value):
            import time

            if self._listener is not None and self._listener.take_pending():
                self._scorer.reload_now()

            cache = self._cache()
            hit = cache.get(value.get("tokens", [])) if cache is not None else None
            if hit is not None:
                yield {**value, **hit, "cache_hit": True}
            else:
                self._buffer.add(value)

            if self._buffer.pending and (time.monotonic() - self._last_flush) * 1000 >= self._max_ms:
                self._buffer.flush_now()

            if self._emit:
                pending, self._emit = self._emit, []
                for out in pending:
                    yield out

except ImportError:
    SentimentMLFunction = None
    SentimentMLBatchFunction = None