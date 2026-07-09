#!/usr/bin/env python3
"""
Train a Word2Vec model on a subset of comments from a Reddit .zst dump.

Stage 0a: standalone training script only. Does not touch the streaming
pipeline, dashboard, or any operator.

Usage:
    python ml-model/scripts/train_word2vec_subset.py \
        [--input ~/Downloads/RC_2019-04.zst] \
        [--max-comments 2000000] \
        [--output-dir ml-model/models/word2vec_subset]
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import zstandard as zstd

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "flink-streaming" / "src"))
sys.path.insert(0, str(_REPO_ROOT / "ml-model" / "src"))

from flink_job.preprocessing.cleaner import TextCleaner  # noqa: E402
from flink_job.preprocessing.tokenizer import tokenize  # noqa: E402
from gensim.models import Word2Vec  # noqa: E402
from ml_model.features.word2vec_embedder import Word2VecFeatureExtractor  # noqa: E402

DEFAULT_INPUT = "~/Downloads/RC_2019-04.zst"
DEFAULT_MAX_COMMENTS = 2_000_000
DEFAULT_OUTPUT_DIR = _REPO_ROOT / "ml-model" / "models" / "word2vec_subset"
LOG_EVERY = 100_000

# Mirrors flink_job.operators.parse.DELETED_BODIES
_DELETED_BODIES = frozenset({"[deleted]", "[removed]"})

# Mirrors PreprocessSettings defaults in flink-streaming/config/settings.py, so
# tokens produced here land in the same vocabulary as the production pipeline.
# (`language` is intentionally omitted from the tokenize() call below: with
# remove_stopwords=False and stem=False it has no effect on the output, so
# skipping langdetect keeps this script dependency-light.)
_CLEANER = TextCleaner(remove_urls=True, remove_markdown=True, lowercase=False)


def _fmt_duration(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m{secs:02d}s" if minutes else f"{secs}s"


def iter_comment_bodies(input_path: Path):
    """Stream a Reddit .zst dump line by line, yielding usable comment bodies.

    Decompresses incrementally via zstandard's streaming reader - never holds
    the compressed or decompressed file in memory at once, only one line.
    """
    with open(input_path, "rb") as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2**31)
        with dctx.stream_reader(fh) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8", errors="replace")
            for line in text_stream:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                body = record.get("body")
                if not isinstance(body, str) or not body.strip():
                    continue
                if body in _DELETED_BODIES:
                    continue
                yield body


def build_tokenized_cache(input_path: Path, cache_path: Path, max_comments: int) -> int:
    """First pass: clean + tokenize comments, writing one whitespace-joined
    sentence per line to `cache_path`.

    Only ever holds one comment's tokens in memory at a time (plus a buffered
    file writer), so RAM stays flat regardless of max_comments. Comments are
    later re-read from this on-disk cache via gensim's `corpus_file` mode,
    which streams line-by-line rather than materializing a token list.
    """
    start = time.perf_counter()
    used = 0
    with open(cache_path, "w", encoding="utf-8") as out:
        for body in iter_comment_bodies(input_path):
            if used >= max_comments:
                break
            cleaned = _CLEANER.clean(body)
            tokens = tokenize(cleaned, remove_stopwords=False, stem=False)
            if not tokens:
                continue
            out.write(" ".join(tokens) + "\n")
            used += 1
            if used % LOG_EVERY == 0:
                elapsed = time.perf_counter() - start
                print(f"[tokenize] {used:,} comments processed ({_fmt_duration(elapsed)} elapsed)", flush=True)
    elapsed = time.perf_counter() - start
    print(f"[tokenize] done: {used:,} comments processed ({_fmt_duration(elapsed)} elapsed)", flush=True)
    return used


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=DEFAULT_INPUT, help="Path to Reddit .zst dump")
    parser.add_argument("--max-comments", type=int, default=DEFAULT_MAX_COMMENTS)
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    model_path = output_dir / "word2vec.model"

    cache_fd, cache_name = tempfile.mkstemp(prefix="word2vec_subset_", suffix=".txt")
    os.close(cache_fd)
    cache_path = Path(cache_name)

    try:
        print(f"[stage 1] tokenizing comments from {input_path} (cap={args.max_comments:,})")
        total_comments = build_tokenized_cache(input_path, cache_path, args.max_comments)

        if total_comments == 0:
            raise RuntimeError("No usable comments found - check --input path and dump format.")

        print(f"[stage 2] training Word2Vec on {total_comments:,} tokenized comments")
        train_start = time.perf_counter()
        model = Word2Vec(
            corpus_file=str(cache_path),
            vector_size=100,
            window=5,
            min_count=5,
            workers=4,
            epochs=5,
            seed=42,
        )
        training_time = time.perf_counter() - train_start
        vocab_size = len(model.wv.key_to_index)

        print("[stage 2] training complete")
        print(f"  total comments used : {total_comments:,}")
        print(f"  vocabulary size     : {vocab_size:,}")
        print(f"  training time       : {_fmt_duration(training_time)}")

        model.save(str(model_path))
        print(f"[stage 3] saved model to {model_path}")

        reloaded = Word2VecFeatureExtractor.load(str(model_path))
        assert reloaded.vocabulary_size == vocab_size, "reload vocab size mismatch"
        print(f"[stage 3] verified reload via Word2VecFeatureExtractor.load() - vocabulary_size={reloaded.vocabulary_size:,}")

    finally:
        cache_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
