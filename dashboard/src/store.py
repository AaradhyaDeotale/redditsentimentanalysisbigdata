"""
store.py
--------
A tiny in-memory store for sentiment results, keyed by keyword.

Each record describes the average sentiment for a keyword over one time
window (as produced by the ML model, P4, on the `sentiment-results` topic).

For the project this in-memory store is enough: the dashboard only needs to
show recent sentiment-over-time per keyword. If you later want persistence
across restarts, swap the dict for SQLite — the public methods can stay the
same so nothing else has to change.
"""

import threading
from collections import defaultdict


class SentimentStore:
    def __init__(self, max_points_per_keyword: int = 500):
        # keyword -> list of records (each record is a dict)
        self._data: dict[str, list[dict]] = defaultdict(list)
        self._max = max_points_per_keyword
        self._lock = threading.Lock()

    def add(self, record: dict) -> None:
        """Add one sentiment record. Expected keys: keyword, window_end,
        positive_ratio, comment_count (see consumer.py for the schema)."""
        keyword = record.get("keyword")
        if not keyword:
            return
        with self._lock:
            series = self._data[keyword.lower()]
            series.append(record)
            # keep only the most recent N points so memory stays bounded
            if len(series) > self._max:
                del series[: len(series) - self._max]

    def timeseries(self, keyword: str) -> list[dict]:
        """Return all stored records for a keyword, oldest first."""
        with self._lock:
            return list(self._data.get(keyword.lower(), []))

    def latest(self, keyword: str) -> dict | None:
        """Return the most recent record for a keyword, or None."""
        with self._lock:
            series = self._data.get(keyword.lower(), [])
            return series[-1] if series else None

    def keywords(self) -> list[str]:
        """All keywords currently known to the store."""
        with self._lock:
            return sorted(self._data.keys())


# A single shared store instance used by both the API and the consumer.
store = SentimentStore()
