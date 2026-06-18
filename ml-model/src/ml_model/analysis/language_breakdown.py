"""
language_breakdown.py
---------------------
Bonus analysis (Stage 2): break sentiment down by comment language, so the
dashboard / report can show e.g. that German Redditors feel differently about a
keyword than English ones. Pure function — reuses scored records.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

from ml_model.labeling.lexicon_labeler import POSITIVE


def language_breakdown(records: Iterable[dict]) -> dict[str, dict[str, Any]]:
    """Aggregate sentiment per language.

    Each record needs a "language" and a "sentiment_label". Returns
    {language: {"positive_ratio": float, "comment_count": int}}.
    """
    counts: dict[str, list[str]] = defaultdict(list)
    for rec in records:
        label = rec.get("sentiment_label")
        if not label:
            continue
        lang = rec.get("language") or "unknown"
        counts[lang].append(label)

    breakdown: dict[str, dict[str, Any]] = {}
    for lang, labels in counts.items():
        total = len(labels)
        positives = sum(1 for l in labels if l == POSITIVE)
        breakdown[lang] = {
            "positive_ratio": round(positives / total, 4) if total else 0.0,
            "comment_count": total,
        }
    return breakdown