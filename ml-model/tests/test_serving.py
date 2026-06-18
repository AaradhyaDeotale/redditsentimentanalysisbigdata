"""Tests for the Phase 5 runtime scorer and aggregator."""

import pytest

from ml_model.data.dataset import LabeledDataset
from ml_model.model.model_store import ModelStore
from ml_model.model.trainer import train_model
from ml_model.serving.aggregator import (
    WindowedSentimentAggregator,
    build_result_record,
    summarize_window,
)
from ml_model.serving.scorer import STATUS_SCORED, STATUS_SKIPPED, ModelScorer

POS = ["love", "great", "amazing", "best", "awesome", "perfect"]
NEG = ["hate", "awful", "terrible", "worst", "broken", "buggy"]


def _dataset(n=20):
    tokens, labels = [], []
    for i in range(n):
        tokens.append([POS[i % len(POS)], POS[(i + 1) % len(POS)]]); labels.append("positive")
        tokens.append([NEG[i % len(NEG)], NEG[(i + 1) % len(NEG)]]); labels.append("negative")
    return LabeledDataset(tokens, labels)


def _train_into(model_dir, version):
    model = train_model(_dataset(), random_state=0).model
    ModelStore(model_dir).save(model, version=version)


def test_scorer_scores_comment(tmp_path):
    md = tmp_path / "models"; _train_into(md, "v1")
    scorer = ModelScorer(str(md)).load()
    out = scorer.score(["love", "amazing", "best"])
    assert out["sentiment_label"] == "positive"
    assert 0.0 <= out["sentiment_score"] <= 1.0
    assert out["sentiment_status"] == STATUS_SCORED
    assert out["sentiment_model"] == "v1"


def test_scorer_skips_short_comment(tmp_path):
    md = tmp_path / "models"; _train_into(md, "v1")
    scorer = ModelScorer(str(md), min_tokens=2).load()
    out = scorer.score(["love"])
    assert out["sentiment_label"] is None
    assert out["sentiment_status"] == STATUS_SKIPPED


def test_scorer_lazy_loads(tmp_path):
    md = tmp_path / "models"; _train_into(md, "v1")
    scorer = ModelScorer(str(md))
    assert not scorer.is_loaded()
    scorer.score(["love", "great"])
    assert scorer.is_loaded()


def test_scorer_hot_reload_picks_up_new_version(tmp_path):
    md = tmp_path / "models"; _train_into(md, "v1")
    scorer = ModelScorer(str(md)).load()
    assert scorer.model_version == "v1"
    assert scorer.maybe_reload() is False
    _train_into(md, "v2")
    assert scorer.maybe_reload() is True
    assert scorer.model_version == "v2"


def test_summarize_window():
    assert summarize_window(["positive", "positive", "negative"]) == (2 / 3, 3)
    assert summarize_window([]) == (0.0, 0)


def test_build_result_record_schema():
    rec = build_result_record("apple", ["positive", "negative", "positive"],
                              window_start_ms=1554076800000, window_end_ms=1554080400000)
    assert rec == {"keyword": "apple", "window_start": 1554076800,
                   "window_end": 1554080400, "positive_ratio": 0.6667, "comment_count": 3}


def test_windowed_aggregator_buckets_by_keyword_and_time():
    agg = WindowedSentimentAggregator(window_size_sec=3600)
    base = 1554076800
    agg.add("apple", "positive", base + 10)
    agg.add("apple", "negative", base + 20)
    agg.add("apple", "positive", base + 3700)
    agg.add("android", "negative", base + 5)
    results = agg.results()
    apple = [r for r in results if r["keyword"] == "apple"]
    assert len(apple) == 2
    assert apple[0]["comment_count"] == 2 and apple[0]["positive_ratio"] == 0.5
    assert any(r["keyword"] == "android" and r["positive_ratio"] == 0.0 for r in results)


def test_windowed_aggregator_rejects_bad_size():
    with pytest.raises(ValueError):
        WindowedSentimentAggregator(window_size_sec=0)