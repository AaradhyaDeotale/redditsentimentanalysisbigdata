"""
Reddit comment streaming pipeline: Kafka -> parse -> keyword filter -> sentiment -> Kafka.

Pipeline stages:
  1. Kafka source       - reads raw JSON from reddit-comments topic
  2. Parse + preprocess - validates, cleans, drops non-English, tokenizes
  3. Keyword filter     - tags records with matching keywords e.g. apple, android
  4. Sentiment placeholder - reserves fields for P4 ML model
  5. Event-time watermark  - assigns timestamps from created_utc
  6. Kafka sink         - writes cleaned JSON to reddit-comments-cleaned topic
  7. Malformed sink     - writes bad records to reddit-comments-malformed topic

Side pipelines off the cleaned stream:
  - sentiment-window     - per-keyword positive ratio -> sentiment-results topic
  - probabilistic sketches (P1) - Count-Min trending + HyperLogLog reach
    -> analytics-results topic (dashboard Trends tab)
"""

from __future__ import annotations

import json
import logging
import os




from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.functions import MapFunction
from pyflink.common.time import Time
from pyflink.datastream.window import (
    TumblingEventTimeWindows,
    TumblingProcessingTimeWindows,
)

from config.settings import AppSettings
from flink_job.operators.keyword_filter import KeywordFilterFunction
from flink_job.operators.late_data import (
    LATE_REPORT_WINDOW_SEC,
    LateCountWindowFunction,
)
from flink_job.operators.parse import MALFORMED_TAG, ParseCommentFunction
from flink_job.operators.sentiment_placeholder import SentimentPlaceholderFunction
from flink_job.sources.kafka_io import build_kafka_sink, build_kafka_source
from flink_job.operators.sentiment_ml import SentimentMLFunction
from flink_job.operators.sentiment_window import KeywordFanoutFunction, SentimentWindowFunction
from flink_job.operators.sketches import (
    AuthorFanoutFunction,
    ReachAggregateFunction,
    ReachWindowFunction,
    TrendingAggregateFunction,
    TrendingFanoutFunction,
    TrendingWindowFunction,
)

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

    # Stage 2: Parse, clean, drop non-English, tokenize
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
    ).name("parse-preprocess-english-only")

    malformed_stream = parsed_main.get_side_output(MALFORMED_TAG)

    # Stage 3: Keyword filter - tag records with matched keywords
    keyword_stream = (
        parsed_main
        .map(KeywordFilterFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("keyword-filter")
    )

    # Stage 4: Sentiment ML - score each comment as it arrives (1 output per
    # input). This is a plain map, not a micro-batch buffer: the buffered version
    # could strand its last partial batch at end-of-stream (a bounded replay has
    # no next record to flush it, and a FlatMapFunction can't emit from close()),
    # silently dropping the most recent comments. Per-record scoring can't lose a
    # tail. The model call is already fast (cached, vectorised internally), so the
    # batch was buying little here.
    sentiment_stream = (
        keyword_stream
        .map(SentimentMLFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("sentiment-ml")
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

    # Late side outputs: event-time windows silently DROP records behind the
    # watermark (typically: re-replaying an already-processed slice of the
    # dump). Each windowed pipeline tags its rejects so they can be counted
    # and surfaced on the dashboard instead of vanishing.
    late_sentiment_tag = OutputTag("late-sentiment", Types.PICKLED_BYTE_ARRAY())
    late_trending_tag = OutputTag("late-trending", Types.PICKLED_BYTE_ARRAY())
    late_reach_tag = OutputTag("late-reach", Types.PICKLED_BYTE_ARRAY())

    results_stream = (
        cleaned_stream
        .flat_map(KeywordFanoutFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("keyword-fanout")
        .key_by(lambda r: r["keyword"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(window_sec)))
        .side_output_late_data(late_sentiment_tag)
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

    # Probabilistic sketches (P1) -> dashboard Trends tab, via analytics topic.
    # Both use windowed AggregateFunctions whose ACCUMULATOR IS THE SKETCH:
    # each parallel worker summarizes only the records it sees, and Flink
    # merges the partial sketches (grids add cell-wise / registers take max).
    analytics_topic = os.getenv("KAFKA_ANALYTICS_TOPIC", "analytics-results")
    analytics_window_sec = int(os.getenv("ANALYTICS_WINDOW_SEC", str(window_sec)))
    analytics_window = TumblingEventTimeWindows.of(Time.seconds(analytics_window_sec))

    # Trending terms: Count-Min Sketch + heavy-hitter candidates, keyed PER
    # TRACKED KEYWORD (words + two-word phrases around each keyword). One
    # fixed-memory sketch per (keyword, window); keying by keyword is what
    # lets the dashboard react immediately when the tracked set changes.
    trending_stream = (
        cleaned_stream
        .flat_map(TrendingFanoutFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("trending-term-fanout")
        .key_by(lambda r: r["keyword"], key_type=Types.STRING())
        .window(analytics_window)
        .side_output_late_data(late_trending_tag)
        .aggregate(
            TrendingAggregateFunction(),
            window_function=TrendingWindowFunction(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .name("trending-count-min-sketch")
    )

    # Reach: HyperLogLog of distinct authors per tracked keyword. Duplicates
    # hash to the same registers, so re-commenting authors are counted once.
    reach_stream = (
        cleaned_stream
        .flat_map(AuthorFanoutFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("author-fanout")
        .key_by(lambda r: r["keyword"], key_type=Types.STRING())
        .window(analytics_window)
        .side_output_late_data(late_reach_tag)
        .aggregate(
            ReachAggregateFunction(),
            window_function=ReachWindowFunction(),
            accumulator_type=Types.PICKLED_BYTE_ARRAY(),
            output_type=Types.PICKLED_BYTE_ARRAY(),
        )
        .name("reach-hyperloglog")
    )

    # Count everything the three windows rejected as late, over short
    # PROCESSING-time windows (late events cannot advance event time), and
    # publish {"type": "late_drop", ...} records so the dashboard can warn
    # "you are replaying data the watermark already passed" instead of
    # silently showing stale trends.
    late_stream = (
        results_stream.get_side_output(late_sentiment_tag)
        .map(lambda r: "sentiment-window", output_type=Types.STRING())
        .union(
            trending_stream.get_side_output(late_trending_tag)
            .map(lambda r: "trending", output_type=Types.STRING()),
            reach_stream.get_side_output(late_reach_tag)
            .map(lambda r: "reach", output_type=Types.STRING()),
        )
        .key_by(lambda name: name, key_type=Types.STRING())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(LATE_REPORT_WINDOW_SEC)))
        .process(LateCountWindowFunction(), output_type=Types.PICKLED_BYTE_ARRAY())
        .name("late-data-counter")
    )

    analytics_sink = build_kafka_sink(kafka, topic=analytics_topic)
    (
        trending_stream.union(reach_stream, late_stream)
        .map(ToJsonString(), output_type=Types.STRING())
        .name("analytics-to-json")
        .sink_to(analytics_sink)
        .name("kafka-analytics-sink")
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
