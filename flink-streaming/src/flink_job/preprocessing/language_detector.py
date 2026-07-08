"""
English gate for Reddit comments.

The pipeline is English-only: comments confidently detected as another
language are dropped during preprocessing, so every downstream operator
(sentiment, trending, reach) only ever sees English text.

Short or ambiguous texts (langdetect needs ~20 chars to be reliable) are
given the benefit of the doubt and kept.

Uses langdetect (lightweight, no external API calls); if it is not
installed, everything is kept.
"""

from __future__ import annotations

import logging
from functools import lru_cache

log = logging.getLogger("flink_job.language")

MIN_DETECT_LENGTH = 20


@lru_cache(maxsize=1)
def _get_detect_fn():
    try:
        from langdetect import DetectorFactory, detect

        # langdetect is probabilistic and seeds randomly per process, so the
        # same borderline text can flip languages between runs/workers - and
        # a flipped comment is silently DROPPED from the whole pipeline.
        # Fixing the seed makes keep/drop deterministic everywhere.
        DetectorFactory.seed = 0
        return detect
    except ImportError:
        log.warning("langdetect not installed — keeping all records")
        return None


def is_english(text: str) -> bool:
    """Whether a cleaned comment should be kept in the English-only stream."""
    if not text or len(text.strip()) < MIN_DETECT_LENGTH:
        return True  # too short to judge reliably - keep

    detect_fn = _get_detect_fn()
    if detect_fn is None:
        return True

    try:
        return detect_fn(text) == "en"
    except Exception:
        return True  # detector choked - keep rather than lose data
