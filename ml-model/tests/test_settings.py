"""Smoke tests for configuration loading — keeps the scaffold green from day one."""

import importlib

import config.settings as settings_module
from config.settings import load_settings


def test_defaults_load_without_env(monkeypatch):
    # Clear any relevant env vars so we exercise the built-in defaults.
    for key in (
        "KAFKA_BROKER", "KAFKA_INPUT_TOPIC", "KAFKA_RESULTS_TOPIC",
        "MODEL_DIR", "MODEL_VERSION", "MIN_TOKENS", "WINDOW_SIZE_SEC",
        "KEYWORD_FILTER",
    ):
        monkeypatch.delenv(key, raising=False)

    s = load_settings()

    assert s.kafka.input_topic == "reddit-comments-cleaned"
    assert s.kafka.results_topic == "sentiment-results"
    assert s.model.model_version == "latest"
    assert s.aggregation.window_size_sec == 3600
    assert s.keywords == []


def test_env_overrides_are_applied(monkeypatch):
    monkeypatch.setenv("KAFKA_RESULTS_TOPIC", "my-results")
    monkeypatch.setenv("MIN_TOKENS", "5")
    monkeypatch.setenv("KEYWORD_FILTER", "apple, android , tesla")
    # Re-import so module-level load_dotenv does not clobber monkeypatched vars.
    importlib.reload(settings_module)

    s = settings_module.load_settings()

    assert s.kafka.results_topic == "my-results"
    assert s.model.min_tokens == 5
    assert s.keywords == ["apple", "android", "tesla"]


def test_settings_are_immutable():
    s = load_settings()
    try:
        s.kafka.broker = "tampered:9092"  # frozen dataclass -> should raise
    except Exception:
        return
    raise AssertionError("KafkaSettings should be immutable (frozen dataclass)")
