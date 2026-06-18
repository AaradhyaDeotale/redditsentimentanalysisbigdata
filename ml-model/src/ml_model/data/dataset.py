"""
dataset.py
----------
Load the labelled dataset produced by label_corpus.py into the
(token lists, labels) form the feature extractors and model trainer expect.

This reads the *labelled* JSONL (output of Phase 2), which is distinct from the
raw cleaned-comment dump that Phase 1 collects.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

log = logging.getLogger("ml_model.data.dataset")


@dataclass(frozen=True)
class LabeledDataset:
    tokens: list[list[str]]   # one token list per comment
    labels: list[str]         # parallel list of "positive" / "negative"

    def __len__(self) -> int:
        return len(self.labels)


def _iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                log.warning("Skipping malformed JSON on line %d", line_no)


def load_labeled_dataset(path: str | Path, min_tokens: int = 1) -> LabeledDataset:
    """Read labelled JSONL into a LabeledDataset.

    Records without a token list or label, or with fewer than `min_tokens`
    tokens, are skipped.
    """
    path = Path(path)
    tokens: list[list[str]] = []
    labels: list[str] = []
    for record in _iter_jsonl(path):
        raw_tokens = record.get("tokens")
        label = record.get("label")
        if not isinstance(raw_tokens, list) or not label:
            continue
        clean = [str(t) for t in raw_tokens if str(t).strip()]
        if len(clean) < min_tokens:
            continue
        tokens.append(clean)
        labels.append(str(label))
    return LabeledDataset(tokens=tokens, labels=labels)