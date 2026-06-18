"""
train.py
--------
Train, evaluate, and save a sentiment model from the labelled dataset.

Run from the ml-model/ directory:

    python src/ml_model/model/train.py \
        --input data/labeled_comments.jsonl \
        --feature tfidf

The trained model is written to a new version directory under --model-dir and
the LATEST pointer is updated so the scorer can load "latest".
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
import logging

from config.settings import load_settings
from ml_model.data.dataset import load_labeled_dataset
from ml_model.model.model_store import ModelStore
from ml_model.model.trainer import FEATURE_TFIDF, FEATURE_WORD2VEC, train_model

log = logging.getLogger("ml_model.model.train")


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Train the sentiment model.")
    parser.add_argument("--input", required=True, help="labelled JSONL (Phase 2 output)")
    parser.add_argument("--feature", choices=[FEATURE_TFIDF, FEATURE_WORD2VEC],
                        default=FEATURE_TFIDF)
    parser.add_argument("--model-dir", default=settings.model.model_dir)
    parser.add_argument("--version", default=None, help="explicit version id (default: timestamp)")
    parser.add_argument("--test-size", type=float, default=settings.training.test_size)
    parser.add_argument("--min-tokens", type=int, default=settings.model.min_tokens)
    args = parser.parse_args(argv)

    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(message)s")

    dataset = load_labeled_dataset(args.input, min_tokens=args.min_tokens)
    if len(dataset) == 0:
        log.error("No usable labelled comments in %s", args.input)
        return 1
    log.info("Loaded %d labelled comments", len(dataset))

    feature_kwargs = {}
    if args.feature == FEATURE_WORD2VEC:
        feature_kwargs.update(
            vector_size=settings.training.embedding_dim,
            window=settings.training.w2v_window,
            min_count=settings.training.w2v_min_count,
            epochs=settings.training.w2v_epochs,
        )

    result = train_model(
        dataset,
        feature_type=args.feature,
        test_size=args.test_size,
        random_state=settings.training.random_state,
        **feature_kwargs,
    )

    log.info("Trained on %d, tested on %d comments (%s features)",
             result.train_size, result.test_size, args.feature)
    print("\n" + result.report.format_text() + "\n")

    store = ModelStore(args.model_dir)
    version = store.save(result.model, version=args.version)
    log.info("Saved model version '%s' under %s/ (LATEST updated)", version, args.model_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())