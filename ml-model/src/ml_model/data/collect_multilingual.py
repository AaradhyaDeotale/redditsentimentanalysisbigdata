"""
collect_multilingual.py
-----------------------
Harvest non-English comments from a raw Reddit dump (RC_YYYY-MM.zst) to
enrich the training corpus. The live pipeline sees mostly English traffic,
so the multilingual labeler has almost nothing to label; this collector
mines the same dump the producer replays, but targeted at non-English
subreddits.

For each language we keep a list of subreddits where that language is
spoken, cheaply pre-filter dump lines by subreddit *before* JSON parsing,
then run the exact same preprocessing as the Flink job (cleaner ->
langdetect -> tokenizer via ``build_cleaned_record``) so output records are
byte-compatible with ``cleaned_comments.jsonl``. langdetect must CONFIRM the
expected language — mixed subreddits (r/spain, r/belgium) emit plenty of
English, which is dropped.

Run from the repo root (needs the flink-streaming sources on PYTHONPATH,
and `pip install zstandard langdetect` — collector-only deps, deliberately
not in requirements.txt so they don't bloat the training image):

    PYTHONPATH=ml-model/src:flink-streaming/src:flink-streaming \\
    ml-model/.venv/bin/python -m ml_model.data.collect_multilingual \\
        --file RC_2019-04.zst \\
        --output ml-model/pipeline-data/multilingual_comments.jsonl \\
        --exclude ml-model/pipeline-data/cleaned_comments.jsonl \\
        --per-lang 4000

Merge the output into the training corpus with a plain `cat`.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
from pathlib import Path

from flink_job.operators.parse import parse_comment_payload, build_cleaned_record
from flink_job.preprocessing.cleaner import TextCleaner

log = logging.getLogger("ml_model.data.collect_multilingual")

# Subreddits per target language (April 2019 activity). Lowercased for
# matching; langdetect still has to confirm, so mixed/mostly-English subs
# only cost parse time, never pollute the output.
SUBREDDIT_LANG: dict[str, str] = {
    # German
    "de": "de", "ich_iel": "de", "fragreddit": "de", "austria": "de",
    "schweiz": "de", "german": "de", "finanzen": "de", "germanrap": "de",
    # French
    "france": "fr", "rance": "fr", "quebec": "fr", "moi_dlvv": "fr",
    "askfrance": "fr", "jeuxvideo": "fr",
    # Spanish
    "es": "es", "espanol": "es", "mexico": "es", "argentina": "es",
    "chile": "es", "uruguay": "es", "vzla": "es", "colombia": "es",
    "spain": "es", "puertorico": "es",
    # Portuguese
    "brasil": "pt", "portugal": "pt", "desabafos": "pt", "futebol": "pt",
    # Italian
    "italy": "it", "italia": "it",
    # Dutch
    "thenetherlands": "nl", "cirkeltrek": "nl", "belgium": "nl",
    "nederlands": "nl",
}

_SUBREDDIT_RE = re.compile(r'"subreddit"\s*:\s*"([^"]+)"')


def _open_dump(path: Path) -> io.TextIOWrapper:
    """Stream-decompress a pushshift .zst dump (needs the long window)."""
    import zstandard

    fh = open(path, "rb")
    reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(fh)
    return io.TextIOWrapper(reader, encoding="utf-8", errors="replace")


def _load_exclude_ids(path: Path | None) -> set[str]:
    ids: set[str] = set()
    if path is None:
        return ids
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            try:
                rid = json.loads(line).get("id")
            except json.JSONDecodeError:
                continue
            if rid:
                ids.add(str(rid))
    log.info("Loaded %d ids to exclude (already in corpus)", len(ids))
    return ids


def collect(
    dump: Path,
    output: Path,
    per_lang: int,
    min_tokens: int,
    exclude_ids: set[str],
    max_lines: int = 0,
) -> dict[str, int]:
    # Same settings as the Flink pipeline defaults (config/settings.py),
    # so records match the corpus collected through Kafka.
    cleaner = TextCleaner(remove_urls=True, remove_markdown=True, lowercase=False)

    targets = set(SUBREDDIT_LANG.values())
    counts: dict[str, int] = {lang: 0 for lang in targets}
    scanned = matched = 0

    with output.open("w", encoding="utf-8") as out, _open_dump(dump) as lines:
        for line in lines:
            scanned += 1
            if max_lines and scanned > max_lines:
                log.info("Stopping at --max-lines=%d", max_lines)
                break
            if scanned % 5_000_000 == 0:
                log.info("Scanned %dM lines, kept %s", scanned // 1_000_000, counts)

            m = _SUBREDDIT_RE.search(line)
            if m is None:
                continue
            expected = SUBREDDIT_LANG.get(m.group(1).lower())
            if expected is None or counts[expected] >= per_lang:
                continue

            record, _err = parse_comment_payload(line)
            if record is None or record["id"] in exclude_ids:
                continue

            cleaned = build_cleaned_record(record, cleaner)
            # langdetect must agree with the subreddit's language
            if cleaned["language"] != expected:
                continue
            if len(cleaned["tokens"]) < min_tokens:
                continue

            out.write(json.dumps(cleaned, ensure_ascii=False) + "\n")
            counts[expected] += 1
            matched += 1
            if all(c >= per_lang for c in counts.values()):
                log.info("All languages reached --per-lang=%d, stopping early", per_lang)
                break

    log.info("Scanned %d lines, kept %d comments: %s", scanned, matched, counts)
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Collect non-English comments from a Reddit dump."
    )
    parser.add_argument("--file", required=True, type=Path, help="RC_*.zst dump")
    parser.add_argument("--output", required=True, type=Path, help="output JSONL")
    parser.add_argument(
        "--per-lang", type=int, default=4000,
        help="max comments to keep per language (default 4000)",
    )
    parser.add_argument(
        "--min-tokens", type=int, default=3,
        help="skip comments with fewer tokens (default 3)",
    )
    parser.add_argument(
        "--exclude", type=Path, default=None,
        help="existing cleaned corpus JSONL — skip ids already in it",
    )
    parser.add_argument(
        "--max-lines", type=int, default=0,
        help="stop after scanning N dump lines (0 = whole dump; for testing)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s")

    if not args.file.exists():
        log.error("Dump not found: %s", args.file)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)

    exclude_ids = _load_exclude_ids(args.exclude)
    collect(
        dump=args.file,
        output=args.output,
        per_lang=args.per_lang,
        min_tokens=args.min_tokens,
        exclude_ids=exclude_ids,
        max_lines=args.max_lines,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
