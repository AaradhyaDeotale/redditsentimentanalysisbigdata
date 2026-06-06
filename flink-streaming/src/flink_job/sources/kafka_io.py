"""
Kafka source/sink builders for the PyFlink DataStream API.
"""

from __future__ import annotations

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    DeliveryGuarantee,
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from config.settings import KafkaSettings


def _starting_offsets(mode: str) -> KafkaOffsetsInitializer:
    if mode == "earliest":
        return KafkaOffsetsInitializer.earliest()
    return KafkaOffsetsInitializer.latest()


def build_kafka_source(kafka: KafkaSettings) -> KafkaSource:
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka.broker)
        .set_topics(kafka.input_topic)
        .set_group_id(kafka.consumer_group)
        .set_starting_offsets(_starting_offsets(kafka.starting_offset))
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def build_kafka_sink(kafka: KafkaSettings, *, topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(kafka.broker)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
