"""
evaluate_by_language.py
-----------------------
Per-language accuracy of a saved model on the SAME held-out test split that
train.py used. The aggregate accuracy in metadata.json hides per-language
behaviour; this recreates the stratified 80/20 split (same seed, same
filtering) while carrying each record's `language` field, so the test rows are
identical to training's and can be grouped by language.

Run from the ml-model/ directory:

    python src/ml_model/model/evaluate_by_language.py \
        --input pipeline-data/labeled_comments.jsonl \
        --version latest
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
from collections import defaultdict
from pathlib import Path

from sklearn.metrics import precision_recall_fscore_support
from sklearn.model_selection import train_test_split

from config.settings import load_settings
from ml_model.data.dataset import _iter_jsonl
from ml_model.model.model_store import ModelStore


def load_with_language(path: str, min_tokens: int):
    """Same records, same order, same filters as load_labeled_dataset —
    plus each record's language, so the train/test split lines up 1:1."""
    tokens, labels, languages = [], [], []
    for record in _iter_jsonl(Path(path)):
        raw_tokens = record.get("tokens")
        label = record.get("label")
        if not isinstance(raw_tokens, list) or not label:
            continue
        clean = [str(t) for t in raw_tokens if str(t).strip()]
        if len(clean) < min_tokens:
            continue
        tokens.append(clean)
        labels.append(str(label))
        languages.append(str(record.get("language") or "unknown"))
    return tokens, labels, languages


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Per-language test accuracy.")
    parser.add_argument("--input", required=True, help="labelled JSONL used for training")
    parser.add_argument("--version", default="latest", help="model version to evaluate")
    parser.add_argument("--model-dir", default=settings.model.model_dir)
    parser.add_argument("--test-size", type=float, default=settings.training.test_size)
    parser.add_argument("--min-tokens", type=int, default=settings.model.min_tokens)
    args = parser.parse_args(argv)

    tokens, labels, languages = load_with_language(args.input, args.min_tokens)
    idx_train, idx_test = train_test_split(
        range(len(labels)),
        test_size=args.test_size,
        random_state=settings.training.random_state,
        stratify=labels,
    )

    store = ModelStore(args.model_dir)
    version = store.resolve_version(args.version)
    model = store.load(version)
    preds = model.predict_batch([tokens[i] for i in idx_test])

    by_lang: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for i, pred in zip(idx_test, preds):
        by_lang[languages[i]].append((labels[i], pred))

    print(f"model {version} — per-language metrics on the {len(idx_test)}-comment test split")
    print(f"{'language':<10} {'n':>6} {'accuracy':>9} {'precision':>10} {'recall':>7} {'f1':>7}   (macro)")
    rows = sorted(by_lang.items(), key=lambda kv: -len(kv[1]))
    summary = {}
    for lang, pairs in rows:
        n = len(pairs)
        y_true = [t for t, _ in pairs]
        y_pred = [p for _, p in pairs]
        acc = sum(t == p for t, p in pairs) / n
        prec, rec, f1, _ = precision_recall_fscore_support(
            y_true, y_pred, average="macro", zero_division=0
        )
        summary[lang] = {"n": n, "accuracy": round(acc, 4),
                         "precision_macro": round(prec, 4),
                         "recall_macro": round(rec, 4),
                         "f1_macro": round(f1, 4)}
        print(f"{lang:<10} {n:>6} {acc:>9.4f} {prec:>10.4f} {rec:>7.4f} {f1:>7.4f}")
    print(json.dumps({"version": version, "by_language": summary}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
