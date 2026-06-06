"""
Placeholder sentiment scoring interface for future ML integration.
No model training or inference is performed here.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from pyflink.datastream.functions import MapFunction


class SentimentScorer(ABC):
    """Contract for plugging in a trained sentiment model later."""

    @abstractmethod
    def score(self, cleaned_body: str, tokens: list[str]) -> dict[str, Any]:
        """Return sentiment metadata to attach to each record."""


class NullSentimentScorer(SentimentScorer):
    """Default no-op scorer until ML teammate wires a real model."""

    def score(self, cleaned_body: str, tokens: list[str]) -> dict[str, Any]:
        return {
            "sentiment_label": None,
            "sentiment_score": None,
            "sentiment_model": None,
            "sentiment_status": "pending_ml_integration",
        }


class SentimentPlaceholderFunction(MapFunction):
    """
    Flink MapFunction that reserves output fields for future sentiment analysis.
    """

    def __init__(self):
        self._scorer: SentimentScorer | None = None

    def open(self, runtime_context):
        self._scorer = NullSentimentScorer()

    def map(self, value: dict) -> dict:
        meta = self._scorer.score(value.get("cleaned_body", ""), value.get("tokens", []))
        return {**value, **meta}
