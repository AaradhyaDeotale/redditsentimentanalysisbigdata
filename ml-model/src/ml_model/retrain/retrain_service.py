"""
retrain_service.py
------------------
Continuous retraining service — the "producer" half of the model lifecycle.

It consumes cleaned comments from Kafka, labels them with VADER, accumulates a
labelled corpus, and — when a trigger fires — trains a fresh model, saves it as
a NEW version, and pings the Flink scorer over Redis so it hot-swaps the new
model live (no restart). The Flink scorer already listens for that ping.

Run inside the ml-model image:
    python -m ml_model.retrain.retrain_service

Env:
    KAFKA_BROKER          bootstrap servers (required)
    KAFKA_COMMENTS_TOPIC  input topic (default reddit-comments-cleaned)
    KAFKA_GROUP_ID        consumer group (default ml-retrainer)
    MODEL_DIR             where new model versions are written (default /models)
    LABELED_CORPUS        accumulating labelled JSONL (default /models/live_corpus.jsonl)
    RETRAIN_EVERY_N       retrain after this many labelled comments (default 5000)
    REDIS_URL             for the "model_ready" reload ping (optional)
"""

from __future__ import annotations

import json
import logging
import os

from confluent_kafka import Consumer

from ml_model.labeling.lexicon_labeler import NEUTRAL, LexiconLabeler
from ml_model.retrain.retrainer import RetrainTrigger, run_retrain_cycle

log = logging.getLogger("ml_model.retrain.service")


def _build_consumer(broker: str, group: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": group,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )


def _make_publisher(redis_url: str | None):
    """Return a (channel, message) -> None publisher, or None if no Redis."""
    if not redis_url:
        return None
    import redis

    client = redis.from_url(redis_url)

    def publish(channel: str, message: str) -> None:
        try:
            client.publish(channel, message)
        except Exception as exc:  # noqa: BLE001
            log.warning("reload ping failed: %s", exc)

    return publish


def should_retrain(trigger: RetrainTrigger, labeled_total: int) -> bool:
    """Decide whether to retrain *now*.

    Default: a simple counter — fire every N labelled comments.

    ┌─ THIS IS THE INTERESTING DECISION ────────────────────────────────────┐
    │ A counter retrains on a fixed schedule whether or not the data         │
    │ actually changed — wasteful when stable, blind when it shifts fast.    │
    │ A smarter trigger watches the stream for DRIFT (e.g. the rolling       │
    │ positive-ratio, or the model's confidence distribution via PSI) and    │
    │ retrains only when the world moves. This function is where that logic  │
    │ lives — evolve it from a counter into a drift detector.                │
    └────────────────────────────────────────────────────────────────────────┘
    """
    return trigger.record()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    broker = os.environ["KAFKA_BROKER"]
    topic = os.getenv("KAFKA_COMMENTS_TOPIC", "reddit-comments-cleaned")
    group = os.getenv("KAFKA_GROUP_ID", "ml-retrainer")
    model_dir = os.getenv("MODEL_DIR", "/models")
    corpus_path = os.getenv("LABELED_CORPUS", "/models/live_corpus.jsonl")
    every_n = int(os.getenv("RETRAIN_EVERY_N", "5000"))
    redis_url = os.getenv("REDIS_URL")

    labeler = LexiconLabeler()
    trigger = RetrainTrigger(every_n)
    publisher = _make_publisher(redis_url)
    consumer = _build_consumer(broker, group)
    consumer.subscribe([topic])

    labeled_total = 0
    log.info(
        "Retrain service up | topic=%s | retrain every %d labelled comments | model_dir=%s",
        topic, every_n, model_dir,
    )

    corpus = open(corpus_path, "a", encoding="utf-8")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue

            try:
                rec = json.loads(msg.value())
            except (ValueError, TypeError):
                continue

            # Weak-label with VADER; drop neutrals (binary positive/negative model).
            result = labeler.label_record(rec)
            if result.label == NEUTRAL:
                continue
            tokens = rec.get("tokens") or []
            if not tokens:
                continue

            corpus.write(json.dumps({"tokens": tokens, "label": result.label}) + "\n")
            corpus.flush()
            labeled_total += 1

            if should_retrain(trigger, labeled_total):
                log.info("Trigger fired at %d labelled comments — retraining ...", labeled_total)
                try:
                    r = run_retrain_cycle(corpus_path, model_dir=model_dir, publisher=publisher)
                    log.info(
                        "NEW MODEL %s | accuracy=%.3f | trained on %d | pinged Flink to hot-swap",
                        r.version, r.accuracy, r.train_size,
                    )
                except Exception as exc:  # noqa: BLE001
                    log.exception("retrain cycle failed: %s", exc)
    finally:
        corpus.close()
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
