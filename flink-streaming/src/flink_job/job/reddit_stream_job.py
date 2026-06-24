"""
Reddit comment streaming pipeline: Kafka -> parse -> keyword filter -> sentiment -> Kafka.

Pipeline stages:
  1. Kafka source       - reads raw JSON from reddit-comments topic
  2. Parse + preprocess - validates, cleans, detects language, tokenizes
  3. Keyword filter     - tags records with matching keywords e.g. apple, android
  4. Sentiment placeholder - reserves fields for P4 ML model
  5. Event-time watermark  - assigns timestamps from created_utc
  6. Kafka sink         - writes cleaned JSON to reddit-comments-cleaned topic
  7. Malformed sink     - writes bad records to reddit-comments-malformed topic
"""

from __future__ import annotations

import json
import logging
import os




from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.functions import MapFunction
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingEventTimeWindows

from config.settings import AppSettings
from flink_job.operators.keyword_filter import KeywordFilterFunction
from flink_job.operators.parse import MALFORMED_TAG, ParseCommentFunction
from flink_job.operators.sentiment_placeholder import SentimentPlaceholderFunction
from flink_job.sources.kafka_io import build_kafka_sink, build_kafka_source
from flink_job.operators.sentiment_ml import SentimentMLBatchFunction
from flink_job.operators.sentiment_window import KeywordFanoutFunction, SentimentWindowFunction

log = logging.getLogger("flink_job.pipeline")


class CreatedUtcAssigner(TimestampAssigner):
    """Event-time from Reddit created_utc (seconds to milliseconds).

    Note: the producer also stamps the Kafka record timestamp with created_utc,
    so event-time is correct even if this assigner is not invoked.
    """

    def extract_timestamp(self, value, record_timestamp) -> int:
        if isinstance(value, dict) and "created_utc" in value:
            return int(value["created_utc"]) * 1000
        return record_timestamp


class ToJsonString(MapFunction):
    """Serialize records as UTF-8 JSON (emoji-safe)."""

    def map(self, value) -> str:
        return json.dumps(value, ensure_ascii=False)


class JsonLineEncoder(Encoder):
    def encode(self, element: str) -> bytes:
        return (element + "\n").encode("utf-8")


def build_pipeline(env: StreamExecutionEnvironment, settings: AppSettings) -> None:
    kafka = settings.kafka
    flink_cfg = settings.flink
    preprocess = settings.preprocess

    env.set_parallelism(flink_cfg.parallelism)
    env.enable_checkpointing(flink_cfg.checkpoint_interval_ms)

    # Stage 1: Kafka source
    source = build_kafka_source(kafka)
    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-reddit-source",
    )

    # Stage 2: Parse, clean, detect language, tokenize
    preprocess_cfg = {
        "remove_urls": preprocess.remove_urls,
        "remove_markdown": preprocess.remove_markdown,
        "lowercase": preprocess.lowercase,
        "remove_stopwords": preprocess.remove_stopwords,
        "stem": preprocess.stem,
    }

    parsed_main = raw_stream.process(
        ParseCommentFunction(preprocess_cfg),
        output_type=Types.PICKLED_BYTE_ARRAY(),
    ).name("parse-preprocess-langdetect")

    malformed_stream = parsed_main.get_side_output(MALFORMED_TAG)

    # Stage 3: Keyword filter - tag records with matched keywords
    keyword_stream = (
        parsed_main
        .map(KeywordFilterFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("keyword-filter")
    )

    # Stage 4: Sentiment ML - micro-batched scoring with optional Redis cache
    sentiment_stream = (
        keyword_stream
        .flat_map(SentimentMLBatchFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("sentiment-ml-batch")
    )

    # Stage 5: Assign event-time watermarks from created_utc
    wm_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(flink_cfg.watermark_max_out_of_order_sec)
        )
        .with_timestamp_assigner(CreatedUtcAssigner())
        .with_idleness(Duration.of_minutes(1))
    )

    cleaned_stream = sentiment_stream.assign_timestamps_and_watermarks(
        wm_strategy
    ).name("event-time-assignment")

    # Per-keyword sentiment over tumbling event-time windows -> dashboard
    results_topic = os.getenv("KAFKA_RESULTS_TOPIC", "sentiment-results")
    window_sec = int(os.getenv("WINDOW_SIZE_SEC", "3600"))

    results_stream = (
        cleaned_stream
        .flat_map(KeywordFanoutFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("keyword-fanout")
        .key_by(lambda r: r["keyword"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(window_sec)))
        .process(SentimentWindowFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("sentiment-window-agg")
    )

    (
        results_stream
        .map(ToJsonString(), output_type=Types.STRING())
        .name("results-to-json")
        .sink_to(build_kafka_sink(kafka, topic=results_topic))
        .name("kafka-sentiment-results-sink")
    )

    # Stage 6: Serialize to JSON strings
    json_stream = (
        cleaned_stream
        .map(ToJsonString(), output_type=Types.STRING())
        .name("to-json")
    )

    # Stage 7: Sink
    if settings.output_sink == "file":
        sink_path = settings.output_file_path
        log.info("Writing to file sink: %s", sink_path)
        file_sink = (
            FileSink.for_row_format(sink_path, JsonLineEncoder())
            .with_output_file_config(OutputFileConfig.builder().with_part_prefix("cleaned").build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy().build())
            .build()
        )
        json_stream.sink_to(file_sink).name("file-sink")
    else:
        log.info("Writing to Kafka topic: %s", kafka.output_topic)
        json_stream.sink_to(
            build_kafka_sink(kafka, topic=kafka.output_topic)
        ).name("kafka-cleaned-sink")

    malformed_stream.sink_to(
        build_kafka_sink(kafka, topic=kafka.malformed_topic)
    ).name("kafka-malformed-sink")

    log.info(
        "Pipeline ready - input: %s | output: %s | malformed: %s | parallelism: %s",
        kafka.input_topic, kafka.output_topic,
        kafka.malformed_topic, flink_cfg.parallelism,
    )
