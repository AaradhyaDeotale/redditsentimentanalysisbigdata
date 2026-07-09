"""
JSON parsing, validation, and preprocessing for Reddit Kafka messages.

The pipeline is English-only: comments confidently detected as another
language are dropped here, so downstream operators (sentiment, trending,
reach) only ever see English text. The author field is preserved in the
output for the reach (unique-authors) analytics.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from config.settings import REQUIRED_FIELDS
from flink_job.preprocessing.cleaner import TextCleaner
from flink_job.preprocessing.language_detector import is_english
from flink_job.preprocessing.tokenizer import tokenize

log = logging.getLogger("flink_job.parse")

DELETED_BODIES = frozenset({"[deleted]", "[removed]"})

# Flink imports are optional so unit tests can run without pyflink installed
try:
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import OutputTag
    from pyflink.datastream.functions import ProcessFunction
    MALFORMED_TAG = OutputTag("malformed-records", Types.STRING())

    class ParseCommentFunction(ProcessFunction):
        MALFORMED_TAG = MALFORMED_TAG

        def __init__(self, preprocess_config: dict):
            self._preprocess_config = preprocess_config

        def open(self, runtime_context):
            cfg = self._preprocess_config
            self._cleaner = TextCleaner(
                remove_urls=cfg["remove_urls"],
                remove_markdown=cfg["remove_markdown"],
                lowercase=cfg["lowercase"],
            )
            self._remove_stopwords = cfg["remove_stopwords"]
            self._stem = cfg["stem"]

        def process_element(self, value, ctx: ProcessFunction.Context):
            raw = value if isinstance(value, str) else value.decode("utf-8", errors="replace")
            record, err = parse_comment_payload(raw)

            if record is None:
                malformed = {
                    "raw": raw[:2000],
                    "error": err,
                    "ingest_ts": ctx.timestamp() if ctx.timestamp() is not None else 0,
                }
                # PyFlink emits side outputs by yielding (tag, value) - the Java
                # ctx.output(tag, value) API does not exist on the Python context.
                yield self.MALFORMED_TAG, json.dumps(malformed, ensure_ascii=False)
                log.debug("Malformed record: %s", err)
                return

            cleaned = build_cleaned_record(
                record,
                self._cleaner,
                remove_stopwords=self._remove_stopwords,
                stem=self._stem,
            )
            if cleaned is None:  # non-English comment - English-only pipeline
                return
            yield cleaned

except ImportError:
    # Running outside Flink (e.g. unit tests) — Flink classes not available
    MALFORMED_TAG = None
    ParseCommentFunction = None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def parse_comment_payload(raw: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"json_decode_error: {exc}"

    if not isinstance(data, dict):
        return None, "root_not_object"

    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        return None, f"missing_fields: {sorted(missing)}"

    body = data.get("body")
    if not isinstance(body, str) or not body.strip():
        return None, "empty_body"
    if body in DELETED_BODIES:
        return None, "deleted_body"

    created_utc = _safe_int(data.get("created_utc"), default=-1)
    if created_utc < 0:
        return None, "invalid_created_utc"

    return {
        "id": str(data["id"]),
        "author": str(data.get("author", "")),
        "created_utc": created_utc,
        "body": body,
        "score": _safe_int(data.get("score"), 0),
        "subreddit": str(data.get("subreddit", "")),
        "controversiality": _safe_int(data.get("controversiality"), 0),
    }, None


def build_cleaned_record(
    comment: dict[str, Any],
    cleaner: TextCleaner,
    *,
    remove_stopwords: bool = False,
    stem: bool = False,
) -> dict[str, Any] | None:
    """The cleaned record, or None if the comment is not English."""
    original = comment["body"]
    cleaned = cleaner.clean(original)

    # Check language AFTER cleaning to avoid URL/markdown noise
    if not is_english(cleaned):
        return None

    tokens = tokenize(cleaned, remove_stopwords=remove_stopwords, stem=stem)

    return {
        "id": comment["id"],
        "author": comment["author"],
        "created_utc": comment["created_utc"],
        "subreddit": comment["subreddit"],
        "original_body": original,
        "cleaned_body": cleaned,
        "tokens": tokens,
        "score": comment["score"],
        "controversiality": comment["controversiality"],
        # Everything that survives the gate is English by pipeline policy
        # (short/ambiguous texts are kept and assumed English). Downstream
        # consumers (ml-model's per-language breakdown) still read this field.
        "language": "en",
    }