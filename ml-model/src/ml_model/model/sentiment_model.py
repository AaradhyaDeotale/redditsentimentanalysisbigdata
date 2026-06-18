"""
sentiment_model.py
------------------
The trained sentiment artifact: a feature extractor (Phase 3) bundled with a
classifier we trained ourselves (Phase 4). This is the object the real-time
scorer (Phase 5) loads to turn a comment's tokens into a sentiment label.

predict_one() returns the (label, score) pair the Flink scorer needs:
  - label: "positive" / "negative"
  - score: probability of the positive class, in [0.0, 1.0]
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import numpy as np

from ml_model.features.base import FeatureExtractor
from ml_model.labeling.lexicon_labeler import POSITIVE


@dataclass(frozen=True)
class Prediction:
    label: str
    score: float   # P(positive), in [0, 1]


class SentimentModel:
    def __init__(
        self,
        feature_extractor: FeatureExtractor,
        classifier: Any,
        feature_type: str,
        metadata: dict | None = None,
    ):
        self.feature_extractor = feature_extractor
        self.classifier = classifier
        self.feature_type = feature_type
        self.metadata = metadata or {}
        self.classes_ = list(classifier.classes_)
        # Column of predict_proba that corresponds to the positive class.
        self._pos_index = (
            self.classes_.index(POSITIVE) if POSITIVE in self.classes_ else len(self.classes_) - 1
        )

    def predict_one(self, tokens: Sequence[str]) -> Prediction:
        """Predict sentiment for a single comment's tokens."""
        features = self.feature_extractor.transform([list(tokens)])
        label = str(self.classifier.predict(features)[0])
        score = float(self.classifier.predict_proba(features)[0][self._pos_index])
        return Prediction(label=label, score=score)

    def predict_batch(self, corpus: Sequence[Sequence[str]]) -> list[str]:
        """Predict labels for many comments at once (used in evaluation)."""
        if len(corpus) == 0:
            return []
        features = self.feature_extractor.transform([list(t) for t in corpus])
        return [str(p) for p in self.classifier.predict(features)]