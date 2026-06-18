"""
scorer.py
---------
Runtime scoring of streamed comments using a trained SentimentModel loaded from
the model store. This is the logic the Flink operator delegates to.

Returns the four sentiment fields the pipeline reserves on each record:
  sentiment_label, sentiment_score, sentiment_model, sentiment_status

Supports hot-reloading: when tracking "latest", maybe_reload() swaps in a newer
model version produced by the retraining loop.
"""

from __future__ import annotations

from typing import Any, Sequence

from ml_model.model.model_store import ModelStore

STATUS_SCORED = "scored"
STATUS_SKIPPED = "skipped_too_short"
STATUS_NO_MODEL = "no_model_available"


class ModelScorer:
    def __init__(self, model_dir: str = "models", version: str = "latest", min_tokens: int = 1):
        self._store = ModelStore(model_dir)
        self._requested = version
        self._min_tokens = min_tokens
        self._model = None
        self._version: str | None = None

    def load(self) -> "ModelScorer":
        self._model = self._store.load(self._requested)
        self._version = self._store.resolve_version(self._requested)
        return self

    @property
    def model_version(self) -> str | None:
        return self._version

    def is_loaded(self) -> bool:
        return self._model is not None

    def maybe_reload(self) -> bool:
        """Pick up a model that did not exist at startup, or a newer 'latest'."""
        if self._requested != "latest":
            if self._model is None:
                try:
                    self.load()
                    return True
                except FileNotFoundError:
                    return False
            return False
        latest = self._store.latest_version()
        if latest and latest != self._version:
            self._model = self._store.load("latest")
            self._version = latest
            return True
        return False

    def score(self, tokens: Sequence[str]) -> dict[str, Any]:
        if self._model is None:
            try:
                self.load()
            except FileNotFoundError:
                return self._result(None, None, STATUS_NO_MODEL)
        toks = [str(t) for t in (tokens or []) if str(t).strip()]

    def _result(self, label, score, status) -> dict[str, Any]:
        return {
            "sentiment_label": label,
            "sentiment_score": score,
            "sentiment_model": self._version,
            "sentiment_status": status,
        }