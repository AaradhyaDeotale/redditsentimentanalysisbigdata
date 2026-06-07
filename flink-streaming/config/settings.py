"""
Central configuration loaded from environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

# override=False means Docker environment variables take priority over .env file
load_dotenv(override=False)


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key, "1" if default else "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _env_list(key: str, default: str = "") -> list[str]:
    raw = os.getenv(key, default).strip()
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


@dataclass(frozen=True)
class KafkaSettings:
    broker: str
    input_topic: str
    output_topic: str
    malformed_topic: str
    consumer_group: str
    starting_offset: str


@dataclass(frozen=True)
class FlinkSettings:
    parallelism: int
    checkpoint_interval_ms: int
    watermark_max_out_of_order_sec: int
    job_name: str


@dataclass(frozen=True)
class PreprocessSettings:
    remove_urls: bool
    remove_markdown: bool
    lowercase: bool
    remove_stopwords: bool
    stem: bool


@dataclass(frozen=True)
class AppSettings:
    kafka: KafkaSettings
    flink: FlinkSettings
    preprocess: PreprocessSettings
    keywords: list[str]
    output_sink: str
    output_file_path: str
    log_level: str


REQUIRED_FIELDS = frozenset(
    {"id", "author", "created_utc", "body", "score", "subreddit", "controversiality"}
)


def load_settings() -> AppSettings:
    return AppSettings(
        kafka=KafkaSettings(
            broker=os.getenv("KAFKA_BROKER", "localhost:9092"),
            input_topic=os.getenv("KAFKA_INPUT_TOPIC", "reddit-comments"),
            output_topic=os.getenv("KAFKA_OUTPUT_TOPIC", "reddit-comments-cleaned"),
            malformed_topic=os.getenv("KAFKA_MALFORMED_TOPIC", "reddit-comments-malformed"),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "flink-reddit-preprocessor"),
            starting_offset=os.getenv("KAFKA_STARTING_OFFSET", "latest").strip().lower(),
        ),
        flink=FlinkSettings(
            parallelism=int(os.getenv("FLINK_PARALLELISM", "2")),
            checkpoint_interval_ms=int(os.getenv("FLINK_CHECKPOINT_INTERVAL_MS", "60000")),
            watermark_max_out_of_order_sec=int(os.getenv("WATERMARK_MAX_OUT_OF_ORDER_SEC", "5")),
            job_name=os.getenv("FLINK_JOB_NAME", "reddit-comment-preprocessor"),
        ),
        preprocess=PreprocessSettings(
            remove_urls=_env_bool("PREPROCESS_REMOVE_URLS", True),
            remove_markdown=_env_bool("PREPROCESS_REMOVE_MARKDOWN", True),
            lowercase=_env_bool("PREPROCESS_LOWERCASE", False),
            remove_stopwords=_env_bool("PREPROCESS_REMOVE_STOPWORDS", False),
            stem=_env_bool("PREPROCESS_STEM", False),
        ),
        keywords=_env_list("KEYWORD_FILTER"),
        output_sink=os.getenv("OUTPUT_SINK", "kafka").strip().lower(),
        output_file_path=os.getenv("OUTPUT_FILE_PATH", "output/cleaned_comments.jsonl"),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
    )
