"""Phase 4 — model training, evaluation, and the versioned model store."""

from ml_model.model.evaluation import EvaluationReport, evaluate
from ml_model.model.model_store import ModelStore
from ml_model.model.sentiment_model import Prediction, SentimentModel
from ml_model.model.trainer import (
    FEATURE_TFIDF,
    FEATURE_WORD2VEC,
    TrainResult,
    train_model,
)

__all__ = [
    "SentimentModel",
    "Prediction",
    "train_model",
    "TrainResult",
    "FEATURE_TFIDF",
    "FEATURE_WORD2VEC",
    "evaluate",
    "EvaluationReport",
    "ModelStore",
]
