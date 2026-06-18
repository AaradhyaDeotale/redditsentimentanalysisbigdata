"""
model_store.py
--------------
Versioned persistence for trained sentiment models. Each save creates a new
version directory containing the feature extractor, the classifier, and a
metadata.json. A LATEST pointer file records the most recent version so the
scorer can simply load "latest", and the retraining loop 
can hot-swap by saving a new version.

Layout:
    models/
      LATEST                      # text file holding the latest version id
      <version>/
        feature_extractor.bin     # saved via the extractor's own format
        classifier.joblib
        metadata.json
"""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import joblib

from ml_model.features.tfidf_vectorizer import TfidfFeatureExtractor
from ml_model.features.word2vec_embedder import Word2VecFeatureExtractor
from ml_model.model.sentiment_model import SentimentModel
from ml_model.model.trainer import FEATURE_TFIDF, FEATURE_WORD2VEC

LATEST = "LATEST"
_EXTRACTOR_FILE = "feature_extractor.bin"
_CLASSIFIER_FILE = "classifier.joblib"
_METADATA_FILE = "metadata.json"

_EXTRACTOR_CLASSES = {
    FEATURE_TFIDF: TfidfFeatureExtractor,
    FEATURE_WORD2VEC: Word2VecFeatureExtractor,
}


class ModelStore:
    def __init__(self, base_dir: str | Path = "models"):
        self.base_dir = Path(base_dir)

    # ---------------------------- save ----------------------------

    def save(self, model: SentimentModel, version: str | None = None) -> str:
        version = version or _timestamp_version()
        version_dir = self.base_dir / version
        version_dir.mkdir(parents=True, exist_ok=True)

        model.feature_extractor.save(str(version_dir / _EXTRACTOR_FILE))
        joblib.dump(model.classifier, version_dir / _CLASSIFIER_FILE)

        metadata = dict(model.metadata)
        metadata.update(
            {
                "version": version,
                "feature_type": model.feature_type,
                "classes": model.classes_,
                "created_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            }
        )
        (version_dir / _METADATA_FILE).write_text(
            json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        self._set_latest(version)
        return version

    # ---------------------------- load ----------------------------

    def load(self, version: str = "latest") -> SentimentModel:
        version = self.resolve_version(version)
        version_dir = self.base_dir / version
        if not version_dir.is_dir():
            raise FileNotFoundError(f"No model version at {version_dir}")

        metadata = json.loads((version_dir / _METADATA_FILE).read_text(encoding="utf-8"))
        feature_type = metadata["feature_type"]
        extractor_cls = _EXTRACTOR_CLASSES.get(feature_type)
        if extractor_cls is None:
            raise ValueError(f"unknown feature_type in metadata: {feature_type!r}")

        extractor = extractor_cls.load(str(version_dir / _EXTRACTOR_FILE))
        classifier = joblib.load(version_dir / _CLASSIFIER_FILE)
        return SentimentModel(extractor, classifier, feature_type, metadata)

    # -------------------------- versions --------------------------

    def list_versions(self) -> list[str]:
        if not self.base_dir.is_dir():
            return []
        return sorted(
            p.name for p in self.base_dir.iterdir()
            if p.is_dir() and (p / _METADATA_FILE).exists()
        )
    
    def latest_version(self) -> str | None:
        """Return the most recent version id, or None if none saved."""
        return self._get_latest()

    def resolve_version(self, version: str) -> str:
        if version != "latest":
            return version
        latest = self._get_latest()
        if latest is None:
            raise FileNotFoundError("No 'latest' model available — train one first.")
        return latest

    # -------------------------- internals -------------------------

    def _set_latest(self, version: str) -> None:
        (self.base_dir / LATEST).write_text(version, encoding="utf-8")

    def _get_latest(self) -> str | None:
        pointer = self.base_dir / LATEST
        if pointer.exists():
            return pointer.read_text(encoding="utf-8").strip()
        return None


def _timestamp_version() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("v%Y%m%d-%H%M%S")