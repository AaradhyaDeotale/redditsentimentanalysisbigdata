"""
Reddit comment streaming pipeline: Kafka → parse/preprocess → Kafka/file sink.
"""

from __future__ import annotations

import json
import logging

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.functions import MapFunction

from config.settings import AppSettings
from flink_job.operators.parse import MALFORMED_TAG, ParseCommentFunction
from flink_job.operators.sentiment_placeholder import SentimentPlaceholderFunction
from flink_job.sources.kafka_io import build_kafka_sink, build_kafka_source

log = logging.getLogger("flink_job.pipeline")


class CreatedUtcAssigner(TimestampAssigner):
    """Event-time from Reddit created_utc (seconds → milliseconds)."""

    def extract_timestamp(self, value, record_timestamp) -> int:
        if isinstance(value, dict) and "created_utc" in value:
            return int(value["created_utc"]) * 1000
        return 0


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

    source = build_kafka_source(kafka)
    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-reddit-source",
    )

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
    ).name("parse-and-preprocess")

    malformed_stream = parsed_main.get_side_output(MALFORMED_TAG)

    wm_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(flink_cfg.watermark_max_out_of_order_sec)
        )
        .with_timestamp_assigner(CreatedUtcAssigner())
        .with_idleness(Duration.of_minutes(1))
    )

    cleaned_stream = (
        parsed_main.map(SentimentPlaceholderFunction())
        .name("sentiment-placeholder")
        .assign_timestamps_and_watermarks(wm_strategy)
        .name("event-time-assignment")
    )

    json_stream = cleaned_stream.map(ToJsonString(), output_type=Types.STRING()).name("to-json")

    if settings.output_sink == "file":
        sink_path = settings.output_file_path
        log.info("Writing cleaned records to file sink: %s", sink_path)
        file_sink = (
            FileSink.for_row_format(sink_path, JsonLineEncoder())
            .with_output_file_config(OutputFileConfig.builder().with_part_prefix("cleaned").build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy().build())
            .build()
        )
        json_stream.sink_to(file_sink).name("file-sink")
    else:
        log.info("Writing cleaned records to Kafka topic: %s", kafka.output_topic)
        json_stream.sink_to(build_kafka_sink(kafka, topic=kafka.output_topic)).name(
            "kafka-cleaned-sink"
        )

    malformed_stream.sink_to(
        build_kafka_sink(kafka, topic=kafka.malformed_topic)
    ).name("kafka-malformed-sink")

    log.info(
        "Pipeline configured: input=%s output=%s malformed=%s parallelism=%s",
        kafka.input_topic,
        kafka.output_topic,
        kafka.malformed_topic,
        flink_cfg.parallelism,
    )
