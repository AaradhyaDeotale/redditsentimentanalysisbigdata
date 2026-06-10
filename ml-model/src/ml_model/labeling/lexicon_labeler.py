"""
lexicon_labeler.py
------------------
Auto-labels Reddit comments as positive / negative / neutral using the VADER
sentiment lexicon.


VADER is used **only** to generate training labels here. It is NOT the final
classifier. The model we train ourselves learns from these labels.

Why VADER for labelling: it is rule-based and tuned for social-media text, so
it handles capitalisation, punctuation, negation and emojis well which makes
it a cheap, reasonable source of weak labels for an unlabelled corpus.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

POSITIVE = "positive"
NEGATIVE = "negative"
NEUTRAL = "neutral"


@dataclass(frozen=True)
class LabelResult:
    """Outcome of labelling one comment."""
    label: str          # POSITIVE | NEGATIVE | NEUTRAL
    compound: float     # VADER compound score in [-1.0, 1.0]


def pick_text(record: dict[str, Any]) -> str:
    """Pick the richest available text for VADER.

    VADER benefits from capitalisation, punctuation and emojis, so we prefer
    the original comment body, then the cleaned body, then the token list.
    """
    for field in ("original_body", "cleaned_body"):
        value = record.get(field)
        if isinstance(value, str) and value.strip():
            return value
    tokens = record.get("tokens")
    if isinstance(tokens, list) and tokens:
        return " ".join(str(t) for t in tokens)
    return ""


class LexiconLabeler:
    """Maps a piece of text to a sentiment label using VADER's compound score.

    A comment is POSITIVE if its compound score >= neutral_band, NEGATIVE if
    <= -neutral_band, and NEUTRAL otherwise. Neutral comments are typically
    dropped from the training set (a binary positive/negative model).
    """

    def __init__(self, neutral_band: float = 0.05):
        if neutral_band < 0:
            raise ValueError("neutral_band must be >= 0")
        self._neutral_band = neutral_band
        self._analyzer = SentimentIntensityAnalyzer()

    @property
    def neutral_band(self) -> float:
        return self._neutral_band

    def score(self, text: str) -> float:
        """Return VADER's compound score for the text (0.0 for empty input)."""
        if not text or not text.strip():
            return 0.0
        return self._analyzer.polarity_scores(text)["compound"]

    def label(self, text: str) -> LabelResult:
        """Label a raw piece of text."""
        compound = self.score(text)
        if compound >= self._neutral_band:
            label = POSITIVE
        elif compound <= -self._neutral_band:
            label = NEGATIVE
        else:
            label = NEUTRAL
        return LabelResult(label=label, compound=compound)

    def label_record(self, record: dict[str, Any]) -> LabelResult:
        """Label a cleaned-comment record (as produced by Flink / P3)."""
        return self.label(pick_text(record))