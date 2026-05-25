"""
Entry point for the PyFlink Reddit preprocessing job.

Run locally (Flink cluster must be up):
    python src/flink_job/main.py

Submit to remote cluster:
    flink run -m localhost:8081 -py src/flink_job/main.py
"""

from __future__ import annotations

import os
import sys

# Ensure project root is on PYTHONPATH when running as a script
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pyflink.datastream import StreamExecutionEnvironment

from config.settings import load_settings
from flink_job.job.reddit_stream_job import build_pipeline
from flink_job.logging_setup import setup_logging


def _configure_flink_home() -> None:
    """Point PyFlink at the local Flink installation when FLINK_HOME is set."""
    flink_home = os.getenv("FLINK_HOME")
    if flink_home:
        os.environ.setdefault("FLINK_CONF_DIR", os.path.join(flink_home, "conf"))


def main() -> None:
    settings = load_settings()
    log = setup_logging(settings.log_level)
    _configure_flink_home()

    env = StreamExecutionEnvironment.get_execution_environment()
    build_pipeline(env, settings)

    log.info("Starting Flink job: %s", settings.flink.job_name)
    env.execute(settings.flink.job_name)


if __name__ == "__main__":
    main()
