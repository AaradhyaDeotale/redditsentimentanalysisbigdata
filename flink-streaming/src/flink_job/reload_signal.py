"""Model-reload signalling over Redis pub/sub.

A background thread subscribed to the reload channel calls ``ReloadListener.notify``
on each message; the scorer operator checks ``take_pending()`` at flush time and
reloads the model when a signal arrived. This lets a freshly retrained model be
picked up near-instantly instead of waiting for the periodic LATEST-file poll
(which stays as a fallback).
"""

from __future__ import annotations

import json

RELOAD_CHANNEL = "reddit:model-reload"
RELOAD_EVENT = "model_ready"


def is_reload_message(message) -> bool:
    if message is None:
        return False
    if isinstance(message, (bytes, bytearray)):
        message = message.decode("utf-8", errors="replace")
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except ValueError:
            return False
    return isinstance(message, dict) and message.get("event") == RELOAD_EVENT


class ReloadListener:
    def __init__(self):
        self._pending = False

    def notify(self, message=None) -> None:
        if is_reload_message(message):
            self._pending = True

    def take_pending(self) -> bool:
        was, self._pending = self._pending, False
        return was
