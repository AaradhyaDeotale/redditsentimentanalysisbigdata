"""
analytics_store.py
------------------
In-memory store for the sketch-based analytics records (P1) coming from the
`analytics-results` Kafka topic. Powers the Trends tab.

Two record shapes share the topic, discriminated by `type` (both carry the
tracked keyword they were computed for):

  {"type": "trending", "keyword": "apple", "window_start", "window_end",
   "items": [{"token": "apple watch", "count": 5000}, ...], "sketch": {...}}

  {"type": "reach", "keyword": "apple", "window_start", "window_end",
   "unique_authors": 1200000, "comment_count": 143, "sketch": {...}}

A third shape carries no keyword: counts of records the Flink event-time
windows REJECTED as late (usually a re-replay of an already-processed slice
of the dump). They power the "your replay is behind the watermark" warning:

  {"type": "late_drop", "pipeline": "trending", "count": 1234,
   "emitted_at": 1751900000}

Mirrors store.py's design: thread-safe and bounded. Both kinds keep a short
per-keyword window history; trending keeps the last few windows so the
overview can compare a term against the previous window (momentum: NEW /
rising / cooling), and the overview is filtered by the CURRENTLY tracked
keyword set - untracking a keyword removes its trends immediately.
"""

import threading

# Momentum: relative change vs the previous window below this is "flat".
_MOMENTUM_EPS = 0.10


class AnalyticsStore:
    def __init__(self, max_reach_points: int = 200, max_trend_windows: int = 8):
        self._trending: dict[str, list[dict]] = {}  # keyword -> windows, oldest first
        self._reach: dict[str, list[dict]] = {}
        self._late: dict[str, dict] = {}  # pipeline -> {count, last_at}
        self._max_reach = max_reach_points
        self._max_trend = max_trend_windows
        self._lock = threading.Lock()

    def add(self, record: dict) -> None:
        """Route one analytics record by its `type` field."""
        kind = record.get("type")
        if kind == "late_drop":
            with self._lock:
                entry = self._late.setdefault(
                    record.get("pipeline") or "unknown", {"count": 0, "last_at": 0}
                )
                entry["count"] += int(record.get("count") or 0)
                entry["last_at"] = max(entry["last_at"],
                                       int(record.get("emitted_at") or 0))
            return
        keyword = record.get("keyword")
        if not keyword:
            return  # legacy/global records carry no keyword - not renderable
        keyword = keyword.lower()
        if kind == "trending":
            with self._lock:
                series = self._trending.setdefault(keyword, [])
                end = record.get("window_end", 0)
                # replace a re-emitted window, keep the rest ordered by time
                series[:] = [r for r in series if r.get("window_end", 0) != end]
                series.append(record)
                series.sort(key=lambda r: r.get("window_end", 0))
                if len(series) > self._max_trend:
                    del series[: len(series) - self._max_trend]
        elif kind == "reach":
            with self._lock:
                series = self._reach.setdefault(keyword, [])
                end = record.get("window_end", 0)
                # replace a re-emitted window (replay), keep ordered by time
                series[:] = [r for r in series if r.get("window_end", 0) != end]
                series.append(record)
                series.sort(key=lambda r: r.get("window_end", 0))
                if len(series) > self._max_reach:
                    del series[: len(series) - self._max_reach]

    def trending_overview(self, tracked: set[str] | None = None,
                          top_k: int = 20) -> dict:
        """Merged trending view across the currently tracked keywords.

        Takes each tracked keyword's latest window, sums counts for terms
        shared between keywords, and tags every term with momentum vs that
        keyword's previous window: "new", "up", "down" or "flat" (plus the
        relative change). Keywords outside `tracked` are excluded, so the
        panel follows the tracked set live.
        """
        tracked = {k.lower() for k in tracked} if tracked is not None else None
        with self._lock:
            series_by_kw = {
                kw: list(series) for kw, series in self._trending.items()
                if series and (tracked is None or kw in tracked)
            }

        merged: dict[str, dict] = {}
        window_end = None
        stream_total = 0
        sketch = None
        for kw, series in series_by_kw.items():
            latest = series[-1]
            prev = series[-2] if len(series) >= 2 else None
            prev_counts = {i["token"]: i["count"]
                           for i in (prev or {}).get("items", [])}
            end = latest.get("window_end")
            if end is not None:
                window_end = end if window_end is None else max(window_end, end)
            sk = latest.get("sketch") or {}
            stream_total += sk.get("stream_total", 0)
            sketch = sketch or sk
            for item in latest.get("items", []):
                entry = merged.setdefault(item["token"], {
                    "count": 0, "score": 0.0, "keywords": [], "prev": 0,
                    "cur_hist": 0, "has_history": False,
                })
                entry["count"] += item["count"]
                # score = count x distinctiveness (computed by the Flink
                # job); records from before the scoring change carry no
                # score, so fall back to the raw count.
                entry["score"] += item.get("score", item["count"])
                entry["keywords"].append(kw)
                if prev is not None:
                    # Momentum must compare like with like: only keywords
                    # that HAVE a previous window contribute to both sides,
                    # otherwise a keyword's very first window inflates the
                    # "current" side against a baseline it never had.
                    entry["has_history"] = True
                    entry["prev"] += prev_counts.get(item["token"], 0)
                    entry["cur_hist"] += item["count"]

        items = []
        for token, entry in merged.items():
            momentum, change = _momentum(entry)
            items.append({
                "token": token,
                "count": entry["count"],
                "score": round(entry["score"], 2),
                "keywords": sorted(entry["keywords"]),
                "momentum": momentum,
                "change": change,
            })
        items.sort(key=lambda i: (-i["score"], i["token"]))

        if sketch:
            sketch = {**sketch, "stream_total": stream_total}
        return {
            "type": "trending",
            "window_end": window_end,
            "keywords": sorted(series_by_kw),
            "items": items[:top_k],
            "sketch": sketch,
        }

    def trending_history(self, tracked: set[str] | None = None,
                         top_k: int = 5) -> dict:
        """Per-window counts for the latest window's top terms.

        Merges the stored windows across the scoped keywords (summing counts
        for terms shared between keywords, like `trending_overview`), picks
        the top_k terms of the LATEST merged window, and returns each term's
        count in every stored window. A term absent from a window's stored
        top list gets None, not 0 - the sketch only ships each window's
        heaviest terms, so absence means "below that window's cutoff".
        """
        tracked = {k.lower() for k in tracked} if tracked is not None else None
        with self._lock:
            series_list = [
                list(series) for kw, series in self._trending.items()
                if series and (tracked is None or kw in tracked)
            ]

        # window_end -> token -> [count, score]; the chart PLOTS counts, but
        # its terms are CHOSEN by score, matching the ranked list's order.
        windows: dict[int, dict[str, list[float]]] = {}
        for series in series_list:
            for record in series:
                end = record.get("window_end")
                if end is None:
                    continue
                counts = windows.setdefault(end, {})
                for item in record.get("items", []):
                    entry = counts.setdefault(item["token"], [0, 0.0])
                    entry[0] += item["count"]
                    entry[1] += item.get("score", item["count"])
        if not windows:
            return {"windows": [], "series": []}

        ends = sorted(windows)
        latest = windows[ends[-1]]
        top = sorted(latest.items(), key=lambda kv: (-kv[1][1], kv[0]))[:top_k]
        return {
            "windows": ends,
            "series": [
                {"token": token,
                 "points": [
                     (windows[end].get(token) or [None])[0] for end in ends
                 ]}
                for token, _ in top
            ],
        }

    def reach_latest(self) -> list[dict]:
        """Most recent reach record per keyword, biggest reach first."""
        with self._lock:
            latest = [series[-1] for series in self._reach.values() if series]
        return sorted(latest, key=lambda r: -r.get("unique_authors", 0))

    def reach_series(self, keyword: str) -> list[dict]:
        """Full reach history for one keyword, oldest first."""
        with self._lock:
            return list(self._reach.get(keyword.lower(), []))

    def clear_late(self) -> None:
        """Forget late-drop counts (after a pipeline reset they're history)."""
        with self._lock:
            self._late.clear()

    def clear(self) -> None:
        """Drop everything - this store is a materialized view of the
        analytics-results topic, so it must be emptied when that topic is
        (pipeline reset), or the Trends tab keeps showing pre-reset windows."""
        with self._lock:
            self._trending.clear()
            self._reach.clear()
            self._late.clear()

    def late_status(self) -> dict:
        """How many records the Flink windows dropped as late, and when last.

        A non-zero, recent total means the replayed data's event time is
        behind the watermark - windowed results (trends, sentiment graph)
        will not update until the pipeline is reset and the data replayed.
        """
        with self._lock:
            total = sum(e["count"] for e in self._late.values())
            last_at = max((e["last_at"] for e in self._late.values()), default=None)
            by_pipeline = {p: dict(e) for p, e in self._late.items()}
        return {"total": total, "last_at": last_at, "by_pipeline": by_pipeline}


def _momentum(entry: dict) -> tuple[str, float | None]:
    """Classify a merged term against the previous window's counts.

    A term with no previous window to compare against (first window ever for
    all of its keywords) is "flat" rather than "new" - everything being NEW
    on the first window would be meaningless. The comparison uses only the
    counts from keywords that have history (`cur_hist`), so a keyword's
    first-ever window cannot masquerade as growth.
    """
    if not entry["has_history"]:
        return "flat", None
    if entry["prev"] <= 0:
        return "new", None
    change = (entry["cur_hist"] - entry["prev"]) / entry["prev"]
    if change > _MOMENTUM_EPS:
        return "up", round(change, 3)
    if change < -_MOMENTUM_EPS:
        return "down", round(change, 3)
    return "flat", round(change, 3)


# Shared instance used by the consumer and the REST API.
analytics_store = AnalyticsStore()
