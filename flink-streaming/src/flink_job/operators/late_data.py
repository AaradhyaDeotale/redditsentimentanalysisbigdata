"""
late_data.py
------------
Visibility for LATE events dropped by the event-time windows.

Event-time windows only accept records whose timestamp is ahead of the
current watermark. Replaying a slice of the dump that was already processed
(e.g. after adding a new tracked keyword) re-sends events whose created_utc
the watermark has long passed - every windowed operator (sentiment windows,
trending Count-Min, reach HyperLogLog) silently drops them, so the dashboard
looks frozen even though the unwindowed comment feed keeps flowing.

This operator makes that visible: the job routes each window's late records
to a side output, and this counter aggregates them over short
PROCESSING-time windows (late events, by definition, cannot use event time)
into small records on the analytics topic:

    {"type": "late_drop", "pipeline": "trending", "count": 1234,
     "emitted_at": 1751900000}

The dashboard sums these and warns the user to reset the pipeline (fresh
watermark) before re-replaying old data.
"""

from __future__ import annotations

import os
import time
from typing import Any

# How often (wall clock) the late counter flushes a count record.
LATE_REPORT_WINDOW_SEC = int(os.getenv("LATE_REPORT_WINDOW_SEC", "10"))


def build_late_drop_record(pipeline: str, count: int,
                           emitted_at: int | None = None) -> dict[str, Any]:
    """One analytics record summarizing late drops for one windowed pipeline."""
    return {
        "type": "late_drop",
        "pipeline": pipeline,
        "count": int(count),
        "emitted_at": int(time.time() if emitted_at is None else emitted_at),
    }


try:
    from pyflink.datastream.functions import ProcessWindowFunction

    class LateCountWindowFunction(ProcessWindowFunction):
        """Counts late elements per pipeline over a processing-time window.

        The stream is keyed by pipeline name (each late element was mapped to
        the name of the window that rejected it), so `key` names the pipeline.
        """

        def process(self, key: str, context: "ProcessWindowFunction.Context",
                    elements):
            count = sum(1 for _ in elements)
            if count:
                yield build_late_drop_record(key, count)

except ImportError:
    # Running outside Flink (e.g. unit tests) - the builder above still works
    LateCountWindowFunction = None
