"""
word2vec_embedder.py
--------------------
Self-trained Word2Vec embeddings — our own domain-specific vectors learned from
the Reddit corpus (compliant with the project rules: we train the embeddings
ourselves; no pre-trained or transfer-learned models).

A comment is represented as the mean of its in-vocabulary token vectors
(a simple, robust "average word embedding" sentence vector). Out-of-vocabulary
tokens are skipped; comments with no in-vocab tokens map to a zero vector.
"""

from __future__ import annotations

import numpy as np
from gensim.models import Word2Vec

from ml_model.features.base import Corpus, FeatureExtractor, TokenList


class Word2VecFeatureExtractor(FeatureExtractor):
    def __init__(
        self,
        vector_size: int = 100,
        window: int = 5,
        min_count: int = 2,
        epochs: int = 5,
        seed: int = 42,
        workers: int = 1,   # workers=1 keeps training reproducible
    ):
        self._vector_size = vector_size
        self._window = window
        self._min_count = min_count
        self._epochs = epochs
        self._seed = seed
        self._workers = workers
        self._model: Word2Vec | None = None

    def fit(self, corpus: Corpus) -> "Word2VecFeatureExtractor":
        sentences = [[str(t) for t in tokens] for tokens in corpus]
        self._model = Word2Vec(
            sentences=sentences,
            vector_size=self._vector_size,
            window=self._window,
            min_count=self._min_count,
            epochs=self._epochs,
            seed=self._seed,
            workers=self._workers,
        )
        return self

    def embed(self, tokens: TokenList) -> np.ndarray:
        """Mean of in-vocab token vectors; zero vector if none are known."""
        self._check_fitted()
        wv = self._model.wv
        vectors = [wv[str(t)] for t in tokens if str(t) in wv.key_to_index]
        if not vectors:
            return np.zeros(self._vector_size, dtype=np.float32)
        return np.mean(vectors, axis=0).astype(np.float32)

    def transform(self, corpus: Corpus) -> np.ndarray:
        self._check_fitted()
        if len(corpus) == 0:
            return np.empty((0, self._vector_size), dtype=np.float32)
        return np.vstack([self.embed(tokens) for tokens in corpus])

    @property
    def dim(self) -> int:
        return self._vector_size

    @property
    def vocabulary_size(self) -> int:
        self._check_fitted()
        return len(self._model.wv.key_to_index)

    def save(self, path: str) -> None:
        self._check_fitted()
        self._model.save(path)

    @classmethod
    def load(cls, path: str) -> "Word2VecFeatureExtractor":
        model = Word2Vec.load(path)
        instance = cls(vector_size=model.vector_size)
        instance._model = model
        return instance

    def _check_fitted(self) -> None:
        if self._model is None:
            raise RuntimeError("Word2VecFeatureExtractor must be fitted before use.")