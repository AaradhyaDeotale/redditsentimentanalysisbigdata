"""
ws_hub.py
---------
The live data plane. Kafka consumers run in blocking daemon threads, but
WebSockets live on the async event loop - so records cross the boundary via a
thread-safe hand-off:

    consumer thread --publish_threadsafe--> asyncio.Queue --broadcaster--> sockets

Each client subscribes to a set of (base) keywords; the hub sends only
matching records and rate-limits both streams per client so the browser is
never flooded. Window records are keyed by base_keyword so ambiguous
keywords fanned out into senses ("apple:company", "apple:fruit", ...) are
throttled as one budget, not one budget per sense - otherwise a keyword with
3 senses would triple its effective rate. The window limiter is set very
generously since window messages were previously unthrottled and are still
low volume in the single-sense case.
"""

import asyncio

from .ratelimit import RateLimiter

COMMENT_RATE = 5      # comments/sec/keyword delivered to each client
COMMENT_BURST = 3
WINDOW_RATE = 20       # window updates/sec/base-keyword delivered to each client
WINDOW_BURST = 10
QUEUE_MAXSIZE = 10_000


class _Client:
    __slots__ = ("ws", "keywords", "limiter", "window_limiter")

    def __init__(self, ws):
        self.ws = ws
        self.keywords: set[str] = set()
        self.limiter = RateLimiter(rate=COMMENT_RATE, burst=COMMENT_BURST)
        self.window_limiter = RateLimiter(rate=WINDOW_RATE, burst=WINDOW_BURST)


class Hub:
    def __init__(self):
        self._clients: set[_Client] = set()
        self._queue: asyncio.Queue | None = None
        self._loop: asyncio.AbstractEventLoop | None = None

    # --- startup -----------------------------------------------------------
    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Capture the running loop + create the hand-off queue (at startup)."""
        self._loop = loop
        self._queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    # --- producer side (called from Kafka consumer threads) ----------------
    def publish_threadsafe(self, msg: dict) -> None:
        loop, queue = self._loop, self._queue
        if loop is None or queue is None:
            return
        try:
            loop.call_soon_threadsafe(self._enqueue, msg)
        except RuntimeError:
            pass  # loop already closed during shutdown

    def _enqueue(self, msg: dict) -> None:
        try:
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            pass  # best-effort feed: drop on overload rather than block

    # --- consumer side (the async broadcaster task) ------------------------
    async def broadcaster(self) -> None:
        assert self._queue is not None, "bind_loop() must run first"
        while True:
            msg = await self._queue.get()
            await self._fanout(msg)

    async def _fanout(self, msg: dict) -> None:
        for client in list(self._clients):
            if not self._should_send(client, msg):
                continue
            try:
                await client.ws.send_json(msg)
            except Exception:
                self._clients.discard(client)

    @staticmethod
    def _should_send(client: _Client, msg: dict) -> bool:
        # A window belongs to one (possibly sense-qualified) keyword; a
        # comment may match several (always plain, never sense-qualified).
        if msg.get("type") == "comment":
            matched = {str(k).lower() for k in (msg.get("matched_keywords") or [])}
            hits = matched & client.keywords
            if not hits:
                return False
            return client.limiter.allow(sorted(hits)[0])  # throttle per keyword
        keyword = str(msg.get("keyword") or "")
        base = str(msg.get("base_keyword") or keyword.split(":", 1)[0]).lower()
        if not base or base not in client.keywords:
            return False
        return client.window_limiter.allow(base)  # throttle per base keyword

    # --- connection management ---------------------------------------------
    async def connect(self, ws) -> _Client:
        await ws.accept()
        client = _Client(ws)
        self._clients.add(client)
        return client

    def disconnect(self, client: _Client) -> None:
        self._clients.discard(client)

    @staticmethod
    def update_subscription(client: _Client, message: dict) -> None:
        subs = message.get("subscribe")
        if isinstance(subs, list):
            client.keywords = {str(k).lower() for k in subs}

    @property
    def client_count(self) -> int:
        return len(self._clients)


# Shared hub instance.
hub = Hub()
