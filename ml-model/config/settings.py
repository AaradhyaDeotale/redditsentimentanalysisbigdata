"""
Central configuration for the ML model service (P4), loaded from environment
variables. Mirrors the settings pattern used by flink-streaming so the whole
project stays consistent.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# override=False means real environment variables (e.g. in Docker) take
# priority over values in a local .env file.
load_dotenv(override=False)


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def _env_list(key: str, default: str = "") -> list[str]:
    raw = os.getenv(key, default).strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class KafkaSettings:
    broker: str
    input_topic: str
    results_topic: str
    consumer_group: str
    starting_offset: str


@dataclass(frozen=True)
class ModelSettings:
    model_dir: str
    model_version: str
    min_tokens: int


@dataclass(frozen=True)
class TrainingSettings:
    test_size: float
    random_state: int
    neutral_band: float
    embedding_dim: int
    w2v_window: int
    w2v_min_count: int
    w2v_epochs: int
    retrain_after_n: int


@dataclass(frozen=True)
class AggregationSettings:
    window_size_sec: int


@dataclass(frozen=True)
class AppSettings:
    kafka: KafkaSettings
    model: ModelSettings
    training: TrainingSettings
    aggregation: AggregationSettings
    keywords: list[str]
    log_level: str


def load_settings() -> AppSettings:
    """Build an AppSettings instance from the current environment."""
    return AppSettings(
        kafka=KafkaSettings(
            broker=os.getenv("KAFKA_BROKER", "localhost:9092"),
            input_topic=os.getenv("KAFKA_INPUT_TOPIC", "reddit-comments-cleaned"),
            results_topic=os.getenv("KAFKA_RESULTS_TOPIC", "sentiment-results"),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "ml-sentiment-scorer"),
            starting_offset=os.getenv("KAFKA_STARTING_OFFSET", "earliest").strip().lower(),
        ),
        model=ModelSettings(
            model_dir=os.getenv("MODEL_DIR", "models"),
            model_version=os.getenv("MODEL_VERSION", "latest").strip(),
            min_tokens=_env_int("MIN_TOKENS", 2),
        ),
        training=TrainingSettings(
            test_size=_env_float("TRAIN_TEST_SPLIT", 0.2),
            random_state=_env_int("RANDOM_STATE", 42),
            neutral_band=_env_float("NEUTRAL_BAND", 0.05),
            embedding_dim=_env_int("EMBEDDING_DIM", 100),
            w2v_window=_env_int("W2V_WINDOW", 5),
            w2v_min_count=_env_int("W2V_MIN_COUNT", 2),
            w2v_epochs=_env_int("W2V_EPOCHS", 5),
            retrain_after_n=_env_int("RETRAIN_AFTER_N", 50000),
        ),
        aggregation=AggregationSettings(
            window_size_sec=_env_int("WINDOW_SIZE_SEC", 3600),
        ),
        keywords=_env_list("KEYWORD_FILTER"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
