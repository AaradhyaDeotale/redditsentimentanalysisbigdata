"""
retrainer.py
------------
Retraining loop. A RetrainTrigger decides WHEN to retrain (every N
processed comments); run_retrain_cycle does the retraining: load the labelled
corpus, train a fresh model, and save it as a NEW version. The scorer's
maybe_reload() then hot-swaps to it.
"""

from __future__ import annotations

from dataclasses import dataclass

from ml_model.data.dataset import load_labeled_dataset
from ml_model.model.model_store import ModelStore
from ml_model.model.trainer import FEATURE_TFIDF, train_model


class RetrainTrigger:
    """Fires once every `every_n` recorded comments (0 disables)."""

    def __init__(self, every_n: int):
        self._every = every_n
        self._count = 0

    def record(self, n: int = 1) -> bool:
        if self._every <= 0:
            return False
        self._count += n
        if self._count >= self._every:
            self._count = 0
            return True
        return False

    @property
    def pending(self) -> int:
        return self._count


@dataclass(frozen=True)
class RetrainResult:
    version: str
    accuracy: float
    train_size: int
    test_size: int


def run_retrain_cycle(
    labeled_path: str,
    model_dir: str = "models",
    feature_type: str = FEATURE_TFIDF,
    test_size: float = 0.2,
    random_state: int = 42,
    min_tokens: int = 1,
    version: str | None = None,
    **feature_kwargs,
) -> RetrainResult:
    dataset = load_labeled_dataset(labeled_path, min_tokens=min_tokens)
    result = train_model(
        dataset,
        feature_type=feature_type,
        test_size=test_size,
        random_state=random_state,
        **feature_kwargs,
    )
    saved = ModelStore(model_dir).save(result.model, version=version)
    return RetrainResult(
        version=saved,
        accuracy=result.report.accuracy,
        train_size=result.train_size,
        test_size=result.test_size,
    )