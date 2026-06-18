"""Phase 3 — feature extraction (TF-IDF baseline and self-trained Word2Vec)."""

from ml_model.features.base import FeatureExtractor
from ml_model.features.tfidf_vectorizer import TfidfFeatureExtractor
from ml_model.features.word2vec_embedder import Word2VecFeatureExtractor

__all__ = [
    "FeatureExtractor",
    "TfidfFeatureExtractor",
    "Word2VecFeatureExtractor",
]