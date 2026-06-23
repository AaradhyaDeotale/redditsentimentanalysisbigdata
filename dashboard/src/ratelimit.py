"""
ratelimit.py
------------
A per-key token-bucket rate limiter.

Used by the WebSocket hub to cap how many comments per keyword each client
receives, so a high-throughput replay of the dump can't flood the browser.
The clock is injectable so the behaviour is deterministically testable.
"""

import time


class RateLimiter:
    def __init__(self, rate: float, burst: int = 1, clock=time.monotonic):
        """`rate` tokens refill per second; `burst` is the bucket capacity."""
        self._rate = rate
        self._burst = burst
        self._clock = clock
        self._tokens: dict[str, float] = {}
        self._last: dict[str, float] = {}

    def allow(self, key: str) -> bool:
        """Consume one token for `key`. Returns True if one was available."""
        now = self._clock()
        tokens = self._tokens.get(key, float(self._burst))
        tokens = min(self._burst, tokens + (now - self._last.get(key, now)) * self._rate)
        self._last[key] = now
        allowed = tokens >= 1
        self._tokens[key] = tokens - 1 if allowed else tokens
        return allowed
