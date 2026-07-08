"""
Embedding-based word-sense resolution using a trained Word2Vec model.

Pure Python, no Flink/file-IO dependencies - the model is injected at
construction so this stays fully unit-testable with a tiny fake embedding
table instead of the real ~114MB model.

Approach: build the comment's mean embedding from its in-vocab tokens, then
score each candidate subkeyword by cosine similarity to that mean (falling
back to literal token presence for subkeywords missing from the model's
vocabulary). The top-scoring subkeyword wins only if it clears an absolute
similarity floor AND separates from the runner-up by a minimum margin;
otherwise the result is reported as "ambiguous" rather than guessed at.
"""

from __future__ import annotations

import numpy as np

AMBIGUOUS = "ambiguous"

DEFAULT_MIN_SIMILARITY = 0.20
DEFAULT_MIN_MARGIN = 0.05


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    denom = np.linalg.norm(a) * np.linalg.norm(b)
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


class W2VSenseResolver:
    def __init__(
        self,
        model,
        min_similarity: float = DEFAULT_MIN_SIMILARITY,
        min_margin: float = DEFAULT_MIN_MARGIN,
    ):
        self._kv = model.wv if hasattr(model, "wv") else model
        self.min_similarity = min_similarity
        self.min_margin = min_margin

    def _in_vocab(self, word: str) -> bool:
        return word in self._kv.key_to_index

    def _mean_embedding(self, tokens: list[str]) -> np.ndarray | None:
        vectors = [self._kv[t] for t in tokens if self._in_vocab(t)]
        if not vectors:
            return None
        return np.mean(vectors, axis=0)

    def resolve(self, tokens: list[str], subkeywords: list[str]) -> str:
        mean_vector = self._mean_embedding(tokens)

        scores: dict[str, float] = {}
        for subkeyword in subkeywords:
            if self._in_vocab(subkeyword):
                if mean_vector is None:
                    continue
                scores[subkeyword] = _cosine_similarity(mean_vector, self._kv[subkeyword])
            elif subkeyword in tokens:
                scores[subkeyword] = 1.0

        if not scores:
            return AMBIGUOUS

        ranked = sorted(scores.values(), reverse=True)
        best_subkeyword = max(scores, key=scores.get)
        best_score = ranked[0]
        second_score = ranked[1] if len(ranked) > 1 else float("-inf")

        if best_score < self.min_similarity:
            return AMBIGUOUS
        if best_score - second_score < self.min_margin:
            return AMBIGUOUS
        return best_subkeyword
