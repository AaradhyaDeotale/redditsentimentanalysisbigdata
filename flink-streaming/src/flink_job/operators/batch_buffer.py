"""Count-based micro-batch buffer.

Accumulates items and flushes them as one batch when ``max_size`` is reached.
A processing-time timer in the Flink operator calls ``flush_now()`` so slow
trickles still drain within a bounded delay. Keeping this logic framework-free
makes it unit-testable without a Flink cluster.
"""

from __future__ import annotations

from typing import Any, Callable


class BatchBuffer:
    def __init__(self, max_size: int, on_flush: Callable[[list[Any]], None]):
        if max_size < 1:
            raise ValueError("max_size must be >= 1")
        self._max_size = max_size
        self._on_flush = on_flush
        self._buf: list[Any] = []

    def add(self, item: Any) -> None:
        self._buf.append(item)
        if len(self._buf) >= self._max_size:
            self.flush_now()

    def flush_now(self) -> None:
        if not self._buf:
            return
        batch, self._buf = self._buf, []
        self._on_flush(batch)

    @property
    def pending(self) -> int:
        return len(self._buf)
