"""
tests/test_config.py – Tests for configuration validation.
"""

import importlib
import pytest
import producer.config as config_module


def test_missing_env_exits(monkeypatch):
    """get_config() should exit if required env vars are absent."""
    monkeypatch.delenv("ZST_FILE",     raising=False)
    monkeypatch.delenv("KAFKA_BROKER", raising=False)
    monkeypatch.delenv("KAFKA_TOPIC",  raising=False)

    importlib.reload(config_module)
    with pytest.raises(SystemExit):
        config_module.get_config()


def test_valid_env_returns_dict(monkeypatch):
    monkeypatch.setenv("ZST_FILE",     "RC_2019-04.zst")
    monkeypatch.setenv("KAFKA_BROKER", "kafka-1:9094,kafka-2:9094,kafka-3:9094")
    monkeypatch.setenv("KAFKA_TOPIC",  "reddit-comments")
    monkeypatch.setenv("REPLAY_SPEED", "5.0")

    importlib.reload(config_module)
    cfg = config_module.get_config()
    assert cfg["kafka_broker"] == "kafka-1:9094,kafka-2:9094,kafka-3:9094"
    assert cfg["replay_speed"] == 5.0


def test_invalid_speed_exits(monkeypatch):
    monkeypatch.setenv("ZST_FILE",     "RC_2019-04.zst")
    monkeypatch.setenv("KAFKA_BROKER", "kafka-1:9094,kafka-2:9094,kafka-3:9094")
    monkeypatch.setenv("KAFKA_TOPIC",  "reddit-comments")
    monkeypatch.setenv("REPLAY_SPEED", "not_a_number")

    importlib.reload(config_module)
    with pytest.raises(SystemExit):
        config_module.get_config()
