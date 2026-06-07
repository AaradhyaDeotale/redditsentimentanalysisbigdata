"""
Placeholder sentiment scoring interface for future ML integration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class SentimentScorer(ABC):
    @abstractmethod
    def score(self, cleaned_body: str, tokens: list[str]) -> dict[str, Any]:
        pass


class NullSentimentScorer(SentimentScorer):
    def score(self, cleaned_body: str, tokens: list[str]) -> dict[str, Any]:
        return {
            "sentiment_label": None,
            "sentiment_score": None,
            "sentiment_model": None,
            "sentiment_status": "pending_ml_integration",
        }


try:
    from pyflink.datastream.functions import MapFunction

    class SentimentPlaceholderFunction(MapFunction):
        def __init__(self):
            self._scorer = None

        def open(self, runtime_context):
            self._scorer = NullSentimentScorer()

        def map(self, value: dict) -> dict:
            meta = self._scorer.score(
                value.get("cleaned_body", ""),
                value.get("tokens", [])
            )
            return {**value, **meta}

except ImportError:
    SentimentPlaceholderFunction = None
