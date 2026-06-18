"""Phase 2 — lexicon labelling """
"""lexicon labelling (generate training labels ONLY; not the final
classifier.)"""

from ml_model.labeling.lexicon_labeler import (
    LabelResult,
    LexiconLabeler,
    NEGATIVE,
    NEUTRAL,
    POSITIVE,
    pick_text,
)

__all__ = [
    "LexiconLabeler",
    "LabelResult",
    "pick_text",
    "POSITIVE",
    "NEGATIVE",
    "NEUTRAL",
]