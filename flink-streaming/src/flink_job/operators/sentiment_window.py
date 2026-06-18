"""
sentiment_window.py
-------------------
Windowed aggregation (P5 output). Fans each scored comment out to one record per
matched keyword, then aggregates per keyword over tumbling event-time windows
into the dashboard schema:
    {keyword, window_start, window_end, positive_ratio, comment_count}

The aggregation math lives in ml_model.serving.aggregator (unit-tested there);
these classes are the thin Flink wrappers.
"""

from __future__ import annotations

import logging

from ml_model.serving.aggregator import build_result_record

log = logging.getLogger("flink_job.sentiment_window")


def fanout_record(record: dict) -> list[dict]:
    """One item per matched keyword for a scored record (empty if unscored)."""
    label = record.get("sentiment_label")
    if not label:
        return []
    keywords = record.get("matched_keywords") or []
    return [{"keyword": kw, "sentiment_label": label} for kw in keywords]


try:
    from pyflink.datastream.functions import FlatMapFunction, ProcessWindowFunction

    class KeywordFanoutFunction(FlatMapFunction):
        def flat_map(self, value: dict):
            for item in fanout_record(value):
                yield item

    class SentimentWindowFunction(ProcessWindowFunction):
        def process(self, key: str, context: "ProcessWindowFunction.Context", elements):
            labels = [e["sentiment_label"] for e in elements]
            window = context.window()           # TimeWindow; bounds in ms
            yield build_result_record(key, labels, window.start, window.end)

except ImportError:
    KeywordFanoutFunction = None
    SentimentWindowFunction = None