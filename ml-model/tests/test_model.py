"""Tests for the Phase 4 model: evaluation, training, prediction, and store."""

import numpy as np
import pytest

from ml_model.data.dataset import LabeledDataset
from ml_model.model.evaluation import evaluate
from ml_model.model.model_store import ModelStore
from ml_model.model.sentiment_model import Prediction
from ml_model.model.trainer import FEATURE_TFIDF, FEATURE_WORD2VEC, train_model

POS_WORDS = ["love", "great", "amazing", "best", "wonderful", "awesome"]
NEG_WORDS = ["hate", "awful", "terrible", "worst", "broken", "buggy"]


def _separable_dataset(n_per_class: int = 20) -> LabeledDataset:
    """A clearly separable synthetic dataset (positive vs negative vocab)."""
    tokens, labels = [], []
    for i in range(n_per_class):
        tokens.append([POS_WORDS[i % len(POS_WORDS)], POS_WORDS[(i + 1) % len(POS_WORDS)]])
        labels.append("positive")
        tokens.append([NEG_WORDS[i % len(NEG_WORDS)], NEG_WORDS[(i + 1) % len(NEG_WORDS)]])
        labels.append("negative")
    return LabeledDataset(tokens=tokens, labels=labels)


# --------------------------- evaluation ---------------------------

def test_evaluate_perfect_predictions():
    y = ["positive", "negative", "positive", "negative"]
    report = evaluate(y, y, labels=["negative", "positive"])
    assert report.accuracy == 1.0
    assert report.f1 == 1.0
    assert report.support == 4


def test_evaluate_confusion_matrix_shape():
    y_true = ["positive", "negative", "positive"]
    y_pred = ["positive", "positive", "positive"]
    report = evaluate(y_true, y_pred, labels=["negative", "positive"])
    assert len(report.confusion) == 2 and len(report.confusion[0]) == 2
    assert "accuracy" in report.to_dict()


# ----------------------------- training ---------------------------

def test_train_tfidf_is_accurate_on_separable_data():
    result = train_model(_separable_dataset(), feature_type=FEATURE_TFIDF,
                         test_size=0.25, random_state=0)
    assert result.report.accuracy >= 0.9
    pred = result.model.predict_one(["love", "amazing"])
    assert isinstance(pred, Prediction)
    assert pred.label == "positive"
    assert 0.0 <= pred.score <= 1.0


def test_train_word2vec_runs_and_predicts():
    result = train_model(_separable_dataset(30), feature_type=FEATURE_WORD2VEC,
                         test_size=0.25, random_state=0,
                         vector_size=32, min_count=1, epochs=30)
    assert result.report.accuracy >= 0.5          # sane, not necessarily perfect
    assert result.model.predict_one(["hate", "awful"]).label in {"positive", "negative"}


def test_train_requires_two_classes():
    one_class = LabeledDataset(tokens=[["a"], ["b"], ["c"], ["d"]],
                               labels=["positive"] * 4)
    with pytest.raises(ValueError):
        train_model(one_class)


def test_predict_batch():
    model = train_model(_separable_dataset(), random_state=0).model
    preds = model.predict_batch([["love", "great"], ["hate", "awful"]])
    assert preds == ["positive", "negative"]


# ---------------------------- model store -------------------------

def test_model_store_save_load_roundtrip(tmp_path):
    model = train_model(_separable_dataset(), random_state=0).model
    store = ModelStore(tmp_path / "models")

    version = store.save(model, version="v1")
    assert version == "v1"
    assert store.list_versions() == ["v1"]
    assert store.resolve_version("latest") == "v1"

    loaded = store.load("latest")
    # loaded model predicts the same as the original
    sample = ["amazing", "best"]
    assert loaded.predict_one(sample).label == model.predict_one(sample).label
    assert np.isclose(loaded.predict_one(sample).score,
                      model.predict_one(sample).score, atol=1e-6)
    assert loaded.feature_type == model.feature_type


def test_model_store_latest_points_to_newest(tmp_path):
    model = train_model(_separable_dataset(), random_state=0).model
    store = ModelStore(tmp_path / "models")
    store.save(model, version="v1")
    store.save(model, version="v2")
    assert store.resolve_version("latest") == "v2"
    assert set(store.list_versions()) == {"v1", "v2"}


def test_model_store_load_missing_latest_raises(tmp_path):
    store = ModelStore(tmp_path / "models")
    with pytest.raises(FileNotFoundError):
        store.load("latest")