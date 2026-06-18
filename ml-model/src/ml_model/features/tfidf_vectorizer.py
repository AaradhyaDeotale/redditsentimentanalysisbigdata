"""
tfidf_vectorizer.py
-------------------
TF-IDF feature extractor — the quick, strong baseline. It treats each comment
as a bag of its tokens and weighs them by term-frequency * inverse-document-
frequency.

We feed sklearn the *already-tokenized* tokens via an identity analyzer, so the
exact tokens from P3 are used (emojis and punctuation preserved) — sklearn never
re-tokenizes or lowercases them.
"""

from __future__ import annotations

import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from ml_model.features.base import Corpus, FeatureExtractor


def _identity_analyzer(tokens):
    """Return the tokens unchanged as the document's terms."""
    return [str(t) for t in tokens]


class TfidfFeatureExtractor(FeatureExtractor):
    def __init__(self, max_features: int | None = 20000, min_df: int = 2):
        # analyzer=callable => sklearn uses our tokens as-is (no lowercasing,
        # no regex tokenization, emojis kept).
        self._vectorizer = TfidfVectorizer(
            analyzer=_identity_analyzer,
            max_features=max_features,
            min_df=min_df,
        )
        self._fitted = False

    def fit(self, corpus: Corpus) -> "TfidfFeatureExtractor":
        self._vectorizer.fit(corpus)
        self._fitted = True
        return self

    def fit_transform(self, corpus: Corpus):
        matrix = self._vectorizer.fit_transform(corpus)  # single efficient pass
        self._fitted = True
        return matrix

    def transform(self, corpus: Corpus):
        self._check_fitted()
        return self._vectorizer.transform(corpus)  # scipy sparse CSR matrix

    @property
    def dim(self) -> int:
        self._check_fitted()
        return len(self._vectorizer.vocabulary_)

    def save(self, path: str) -> None:
        self._check_fitted()
        joblib.dump(self._vectorizer, path)

    @classmethod
    def load(cls, path: str) -> "TfidfFeatureExtractor":
        instance = cls()
        instance._vectorizer = joblib.load(path)
        instance._fitted = True
        return instance

    def _check_fitted(self) -> None:
        if not self._fitted:
            raise RuntimeError("TfidfFeatureExtractor must be fitted before use.")