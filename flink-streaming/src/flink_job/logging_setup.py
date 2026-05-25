import logging
import sys


def setup_logging(level: str = "INFO") -> logging.Logger:
    numeric = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric,
        format="%(asctime)s  [flink-reddit]  %(levelname)s  %(message)s",
        stream=sys.stdout,
        force=True,
    )
    return logging.getLogger("flink_job")
