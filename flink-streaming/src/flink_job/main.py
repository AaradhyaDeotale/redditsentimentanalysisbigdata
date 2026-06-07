"""
Entry point for the PyFlink Reddit preprocessing job.
"""

from __future__ import annotations

import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pyflink.datastream import StreamExecutionEnvironment

from config.settings import load_settings
from flink_job.job.reddit_stream_job import build_pipeline
from flink_job.logging_setup import setup_logging


def main() -> None:
    settings = load_settings()
    log = setup_logging(settings.log_level)

    log.info("Kafka broker: %s", settings.kafka.broker)

    env = StreamExecutionEnvironment.get_execution_environment()
    build_pipeline(env, settings)

    log.info("Starting Flink job: %s", settings.flink.job_name)
    env.execute(settings.flink.job_name)


if __name__ == "__main__":
    main()
