"""
-------------
Pure (Flink-free) aggregation logic shared by the Flink window operator (P5) so
the math is unit-tested independently of any cluster.

Produces the per-keyword, per-window record the dashboard (P5) consumes:
  {keyword, window_start, window_end, positive_ratio, comment_count}
window_start / window_end are unix SECONDS.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Iterable

from ml_model.labeling.lexicon_labeler import POSITIVE


def summarize_window(labels: Iterable[str]) -> tuple[float, int]:
    """Return (positive_ratio, comment_count) for a window's labels."""
    labels = [l for l in labels if l]
    count = len(labels)
    if count == 0:
        return 0.0, 0
    positives = sum(1 for l in labels if l == POSITIVE)
    return positives / count, count


def build_result_record(
    keyword: str,
    labels: Iterable[str],
    window_start_ms: int | None,
    window_end_ms: int,
) -> dict[str, Any]:
    ratio, count = summarize_window(labels)
    return {
        "keyword": keyword,
        "window_start": int(window_start_ms // 1000) if window_start_ms is not None else None,
        "window_end": int(window_end_ms // 1000),
        "positive_ratio": round(ratio, 4),
        "comment_count": count,
    }


class WindowedSentimentAggregator:
    """In-memory tumbling-window aggregator (non-Flink path / tests).

    Buckets (timestamp_seconds, keyword, label) into fixed windows and emits
    result records. Useful for local runs and for verifying the math.
    """

    def __init__(self, window_size_sec: int = 3600):
        if window_size_sec <= 0:
            raise ValueError("window_size_sec must be > 0")
        self._size = window_size_sec
        self._buckets: dict[tuple[str, int], list[str]] = defaultdict(list)

    def add(self, keyword: str, label: str, timestamp_sec: int) -> None:
        window_start = (timestamp_sec // self._size) * self._size
        self._buckets[(keyword, window_start)].append(label)

    def results(self) -> list[dict[str, Any]]:
        out = []
        for (keyword, start), labels in sorted(self._buckets.items()):
            out.append(
                build_result_record(
                    keyword, labels,
                    window_start_ms=start * 1000,
                    window_end_ms=(start + self._size) * 1000,
                )
            )
        return out