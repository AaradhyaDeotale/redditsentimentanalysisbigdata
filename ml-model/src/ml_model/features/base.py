"""
base.py
-------
Common interface for feature extractors so the model trainer can
swap between the TF-IDF baseline and self-trained Word2Vec without changing
its own code.

All extractors operate on *pre-tokenized* comments: a sequence of token lists
(the `tokens` field produced by Flink / P3), e.g. [["apple", "great", "🔥"], ...].
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

TokenList = Sequence[str]
Corpus = Sequence[TokenList]


class FeatureExtractor(ABC):
    """Turns token lists into a numeric (n_samples, dim) feature matrix."""

    @abstractmethod
    def fit(self, corpus: Corpus) -> "FeatureExtractor":
        """Learn the vocabulary / embeddings from the corpus. Returns self."""

    @abstractmethod
    def transform(self, corpus: Corpus):
        """Return an (n_samples, dim) matrix (dense ndarray or sparse matrix)."""

    def fit_transform(self, corpus: Corpus):
        """Fit on the corpus and return its feature matrix."""
        return self.fit(corpus).transform(corpus)

    @property
    @abstractmethod
    def dim(self) -> int:
        """Number of feature columns produced by transform()."""

    @abstractmethod
    def save(self, path: str) -> None:
        """Persist the fitted extractor to disk."""

    @classmethod
    @abstractmethod
    def load(cls, path: str) -> "FeatureExtractor":
        """Load a previously saved extractor from disk."""