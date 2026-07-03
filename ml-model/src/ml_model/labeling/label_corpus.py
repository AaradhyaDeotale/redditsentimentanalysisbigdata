"""
label_corpus.py
---------------
Read a corpus of cleaned comments (JSONL, one record per line), attach a
lexicon-derived sentiment label to each (language-aware: VADER for English,
per-language word lists otherwise), drop neutral comments (unless
--keep-neutral), and write a labelled dataset for training.

Run it as a script from the ml-model/ directory:

    python src/ml_model/labeling/label_corpus.py \
        --input data/cleaned_comments.jsonl \
        --output data/labeled_comments.jsonl

The label is produced by lexicons for LABELLING ONLY — the model that actually
classifies sentiment is trained by us later (Phase 4).
"""

from __future__ import annotations

# --- make `config` and `ml_model` importable when run directly as a script ---
import os
import sys

_HERE = os.path.abspath(__file__)
_SRC = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))   # ml-model/src
_ROOT = os.path.dirname(_SRC)                                     # ml-model
for _p in (_ROOT, _SRC):
    if _p not in sys.path:
        sys.path.insert(0, _p)
# -----------------------------------------------------------------------------

import argparse
import json
import logging
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from config.settings import load_settings
from ml_model.labeling.lexicon_labeler import LabelResult, LexiconLabeler, NEUTRAL
from ml_model.labeling.multilingual_labeler import MultilingualLexiconLabeler

log = logging.getLogger("ml_model.labeling.label_corpus")

# Fields carried over from the cleaned record into the labelled dataset.
# tokens/cleaned_body feed the model; language/matched_keywords enable later
# per-language and per-keyword analysis.
CARRY_FIELDS = ("id", "tokens", "cleaned_body", "language", "matched_keywords")


def iter_records(path: Path) -> Iterator[dict[str, Any]]:
    """Yield one parsed JSON record per non-empty line, skipping bad lines."""
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                log.warning("Skipping malformed JSON on line %d", line_no)


def build_labeled_record(record: dict[str, Any], result: LabelResult) -> dict[str, Any]:
    out = {field: record.get(field) for field in CARRY_FIELDS}
    out["label"] = result.label
    # Compound score from whichever lexicon labelled it; `label_source` records
    # which one (e.g. "vader" for English, "lexicon" for other languages).
    out["vader_compound"] = round(result.compound, 4)
    out["label_source"] = result.source
    return out


def label_corpus(
    input_path: Path,
    output_path: Path,
    labeler: LexiconLabeler | MultilingualLexiconLabeler,
    keep_neutral: bool = False,
) -> tuple[Counter, int]:
    """Label every record in input_path and write kept records to output_path.

    Uses ``label_record`` so the labeler can route on each comment's detected
    ``language``. Returns (label_counts, records_written).
    """
    counts: Counter = Counter()
    written = 0
    with output_path.open("w", encoding="utf-8") as out_fh:
        for record in iter_records(input_path):
            result = labeler.label_record(record)
            counts[result.label] += 1
            if result.label == NEUTRAL and not keep_neutral:
                continue
            out_fh.write(json.dumps(build_labeled_record(record, result), ensure_ascii=False) + "\n")
            written += 1
    return counts, written


def _parse_args(argv: list[str] | None, default_band: float) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Label cleaned comments with VADER (labels only)."
    )
    parser.add_argument("--input", required=True, type=Path, help="cleaned comments JSONL")
    parser.add_argument("--output", required=True, type=Path, help="output labelled JSONL")
    parser.add_argument(
        "--neutral-band", type=float, default=default_band,
        help="abs compound score below which a comment is neutral (default from settings)",
    )
    parser.add_argument(
        "--keep-neutral", action="store_true",
        help="keep neutral comments instead of dropping them",
    )
    parser.add_argument(
        "--lexicon-dir", type=str, default=None,
        help="optional dir of <lang>.txt word lists to extend the built-in "
             "non-English lexicons (lines: 'word<TAB>pos' or 'word<TAB>neg')",
    )
    parser.add_argument(
        "--english-only", action="store_true",
        help="use plain VADER for every comment (disable per-language routing)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    args = _parse_args(argv, default_band=settings.training.neutral_band)

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.input.exists():
        log.error("Input file not found: %s", args.input)
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)

    if args.english_only:
        labeler: LexiconLabeler | MultilingualLexiconLabeler = LexiconLabeler(
            neutral_band=args.neutral_band)
    else:
        labeler = MultilingualLexiconLabeler(
            neutral_band=args.neutral_band, lexicon_dir=args.lexicon_dir)
    counts, written = label_corpus(args.input, args.output, labeler, keep_neutral=args.keep_neutral)

    total = sum(counts.values())
    log.info(
        "Labelled %d comments -> %d written (neutral %s), output: %s",
        total, written, "kept" if args.keep_neutral else "dropped", args.output,
    )
    for lbl in ("positive", "negative", "neutral"):
        c = counts.get(lbl, 0)
        pct = (100.0 * c / total) if total else 0.0
        log.info("  %-9s %8d  (%5.1f%%)", lbl, c, pct)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())