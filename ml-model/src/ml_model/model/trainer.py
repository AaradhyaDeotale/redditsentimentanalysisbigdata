"""
trainer.py
----------
Train a sentiment classifier on the labelled dataset using the
features from Phase 3. We train the model OURSELVES (Logistic Regression) the
lexicon was only used to create labels, never as the classifier.

The feature extractor is fitted on the TRAIN split only and then used to
transform both splits, so no information leaks from test into training.
"""

from __future__ import annotations

from dataclasses import dataclass

from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split

from ml_model.data.dataset import LabeledDataset
from ml_model.features.base import FeatureExtractor
from ml_model.features.tfidf_vectorizer import TfidfFeatureExtractor
from ml_model.features.word2vec_embedder import Word2VecFeatureExtractor
from ml_model.model.evaluation import EvaluationReport, evaluate
from ml_model.model.sentiment_model import SentimentModel

FEATURE_TFIDF = "tfidf"
FEATURE_WORD2VEC = "word2vec"


@dataclass(frozen=True)
class TrainResult:
    model: SentimentModel
    report: EvaluationReport
    train_size: int
    test_size: int


def build_feature_extractor(feature_type: str, **kwargs) -> FeatureExtractor:
    if feature_type == FEATURE_TFIDF:
        return TfidfFeatureExtractor(
            min_df=kwargs.get("min_df", 2),
            max_features=kwargs.get("max_features", 20000),
        )
    if feature_type == FEATURE_WORD2VEC:
        return Word2VecFeatureExtractor(
            vector_size=kwargs.get("vector_size", 100),
            window=kwargs.get("window", 5),
            min_count=kwargs.get("min_count", 2),
            epochs=kwargs.get("epochs", 5),
            seed=kwargs.get("random_state", 42),
        )
    raise ValueError(f"unknown feature_type: {feature_type!r}")


def train_model(
    dataset: LabeledDataset,
    feature_type: str = FEATURE_TFIDF,
    test_size: float = 0.2,
    random_state: int = 42,
    **feature_kwargs,
) -> TrainResult:
    if len(dataset) < 4:
        raise ValueError("Need at least 4 labelled comments to train.")
    distinct = sorted(set(dataset.labels))
    if len(distinct) < 2:
        raise ValueError(f"Need at least 2 classes to train; got {distinct}.")

    X_train_tokens, X_test_tokens, y_train, y_test = train_test_split(
        dataset.tokens,
        dataset.labels,
        test_size=test_size,
        random_state=random_state,
        stratify=dataset.labels,
    )

    # Fit features on TRAIN only, then transform both splits (no leakage).
    extractor = build_feature_extractor(
        feature_type, random_state=random_state, **feature_kwargs
    )
    extractor.fit(X_train_tokens)
    X_train = extractor.transform(X_train_tokens)
    X_test = extractor.transform(X_test_tokens)

    classifier = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",   # handle class imbalance from labelling
        random_state=random_state,
    )
    classifier.fit(X_train, y_train)

    y_pred = classifier.predict(X_test)
    report = evaluate(y_test, list(y_pred), labels=distinct)

    metadata = {
        "feature_type": feature_type,
        "classes": distinct,
        "train_size": len(y_train),
        "test_size": len(y_test),
        "metrics": report.to_dict(),
    }
    model = SentimentModel(extractor, classifier, feature_type, metadata)
    return TrainResult(model, report, len(y_train), len(y_test))