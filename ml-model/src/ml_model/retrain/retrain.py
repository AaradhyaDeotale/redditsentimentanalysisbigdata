"""
retrain.py
----------
Run ONE retraining cycle and save a new model version. Schedule this (cron, a
loop, or a Kubernetes CronJob) to retrain periodically; the Flink scorer picks
up the new version via maybe_reload().

    python src/ml_model/retrain/retrain.py --input data/labeled_comments.jsonl
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.abspath(__file__)
_SRC = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))   # ml-model/src
_ROOT = os.path.dirname(_SRC)                                     # ml-model
for _p in (_ROOT, _SRC):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import argparse
import logging

from config.settings import load_settings
from ml_model.model.trainer import FEATURE_TFIDF, FEATURE_WORD2VEC
from ml_model.retrain.retrainer import run_retrain_cycle

log = logging.getLogger("ml_model.retrain")


def _redis_publisher():
    """Return a publish(channel, message) callable, or None if Redis is
    unavailable — retraining still proceeds and Flink falls back to LATEST polling."""
    import os

    url = os.getenv("REDIS_URL")
    if not url:
        return None
    try:
        import redis

        client = redis.from_url(url)
    except Exception as exc:
        log.warning("Redis not available, skipping reload signal: %s", exc)
        return None

    def publish(channel: str, message: str) -> None:
        try:
            client.publish(channel, message)
        except Exception as exc:
            log.warning("Could not publish reload signal: %s", exc)

    return publish


def main(argv: list[str] | None = None) -> int:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Run one retraining cycle.")
    parser.add_argument("--input", required=True, help="labelled JSONL")
    parser.add_argument("--feature", choices=[FEATURE_TFIDF, FEATURE_WORD2VEC],
                        default=FEATURE_TFIDF)
    parser.add_argument("--model-dir", default=settings.model.model_dir)
    args = parser.parse_args(argv)

    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(message)s")

    feature_kwargs = {}
    if args.feature == FEATURE_WORD2VEC:
        feature_kwargs.update(
            vector_size=settings.training.embedding_dim,
            window=settings.training.w2v_window,
            min_count=settings.training.w2v_min_count,
            epochs=settings.training.w2v_epochs,
        )

    publisher = _redis_publisher()

    result = run_retrain_cycle(
        args.input,
        model_dir=args.model_dir,
        feature_type=args.feature,
        test_size=settings.training.test_size,
        random_state=settings.training.random_state,
        min_tokens=settings.model.min_tokens,
        publisher=publisher,
        **feature_kwargs,
    )
    log.info("Retrained -> version '%s' (accuracy %.4f, train %d / test %d)",
             result.version, result.accuracy, result.train_size, result.test_size)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())