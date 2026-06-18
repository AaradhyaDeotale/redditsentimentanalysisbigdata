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
            from ml_model.serving.scorer import ModelScorer  # imported lazily on the worker
            self._scorer = ModelScorer(self._model_dir, min_tokens=self._min_tokens).load()
            log.info("Loaded sentiment model '%s' from %s",
                     self._scorer.model_version, self._model_dir)

    def score(self, cleaned_body: str, tokens: list[str]) -> dict[str, Any]:
        self._ensure_loaded()
        self._since_reload += 1
        if self._reload_every and self._since_reload >= self._reload_every:
            self._since_reload = 0
            if self._scorer.maybe_reload():
                log.info("Hot-reloaded model -> version '%s'", self._scorer.model_version)
        return self._scorer.score(tokens)


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

except ImportError:
    SentimentMLFunction = None