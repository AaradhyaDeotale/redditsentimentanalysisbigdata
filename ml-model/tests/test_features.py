"""Tests for the Phase 3 feature extractors (TF-IDF and Word2Vec)."""

import numpy as np
import pytest

from ml_model.features.tfidf_vectorizer import TfidfFeatureExtractor
from ml_model.features.word2vec_embedder import Word2VecFeatureExtractor

CORPUS = [
    ["apple", "phone", "great", "🔥"],
    ["android", "phone", "buggy"],
    ["apple", "great", "great"],
    ["android", "lag", "buggy"],
]


# ----------------------------- TF-IDF -----------------------------

def test_tfidf_fit_transform_shape():
    ex = TfidfFeatureExtractor(min_df=1)
    matrix = ex.fit_transform(CORPUS)
    assert matrix.shape[0] == len(CORPUS)
    assert matrix.shape[1] == ex.dim


def test_tfidf_preserves_emoji_token():
    ex = TfidfFeatureExtractor(min_df=1).fit(CORPUS)
    assert "🔥" in ex._vectorizer.vocabulary_  # emoji kept as its own term


def test_tfidf_min_df_drops_rare_terms():
    # "lag" and the emoji appear once; min_df=2 should drop them.
    ex = TfidfFeatureExtractor(min_df=2).fit(CORPUS)
    vocab = ex._vectorizer.vocabulary_
    assert "phone" in vocab        # appears twice
    assert "lag" not in vocab      # appears once


def test_tfidf_unseen_tokens_ignored():
    ex = TfidfFeatureExtractor(min_df=1).fit(CORPUS)
    out = ex.transform([["completely", "unseen", "words"]])
    assert out.shape == (1, ex.dim)
    assert out.nnz == 0            # nothing matched the vocabulary


def test_tfidf_transform_before_fit_raises():
    with pytest.raises(RuntimeError):
        TfidfFeatureExtractor().transform(CORPUS)


def test_tfidf_save_load_roundtrip(tmp_path):
    ex = TfidfFeatureExtractor(min_df=1).fit(CORPUS)
    path = tmp_path / "tfidf.joblib"
    ex.save(str(path))
    reloaded = TfidfFeatureExtractor.load(str(path))
    a = ex.transform(CORPUS).toarray()
    b = reloaded.transform(CORPUS).toarray()
    assert np.allclose(a, b)


# ---------------------------- Word2Vec ----------------------------

def _w2v():
    # small + min_count=1 so the tiny corpus produces a usable vocabulary
    return Word2VecFeatureExtractor(vector_size=16, window=2, min_count=1, epochs=20)


def test_w2v_dim_and_transform_shape():
    ex = _w2v().fit(CORPUS)
    out = ex.transform(CORPUS)
    assert ex.dim == 16
    assert out.shape == (len(CORPUS), 16)
    assert out.dtype == np.float32


def test_w2v_embed_known_token():
    ex = _w2v().fit(CORPUS)
    vec = ex.embed(["apple"])
    assert vec.shape == (16,)
    assert np.any(vec != 0.0)      # a known token has a non-zero vector


def test_w2v_all_oov_is_zero_vector():
    ex = _w2v().fit(CORPUS)
    vec = ex.embed(["zzz_not_in_vocab"])
    assert vec.shape == (16,)
    assert np.allclose(vec, 0.0)


def test_w2v_empty_tokens_is_zero_vector():
    ex = _w2v().fit(CORPUS)
    assert np.allclose(ex.embed([]), 0.0)


def test_w2v_empty_corpus_transform():
    ex = _w2v().fit(CORPUS)
    out = ex.transform([])
    assert out.shape == (0, 16)


def test_w2v_transform_before_fit_raises():
    with pytest.raises(RuntimeError):
        _w2v().transform(CORPUS)


def test_w2v_save_load_roundtrip(tmp_path):
    ex = _w2v().fit(CORPUS)
    path = tmp_path / "w2v.model"
    ex.save(str(path))
    reloaded = Word2VecFeatureExtractor.load(str(path))
    assert reloaded.dim == ex.dim
    assert np.allclose(reloaded.embed(["apple"]), ex.embed(["apple"]))