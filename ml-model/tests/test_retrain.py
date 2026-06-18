"""Tests for the retraining trigger and cycle."""

import json

from ml_model.model.model_store import ModelStore
from ml_model.retrain.retrainer import RetrainTrigger, run_retrain_cycle

POS = ["love", "great", "amazing", "best", "awesome", "perfect"]
NEG = ["hate", "awful", "terrible", "worst", "broken", "buggy"]


def test_retrain_trigger_fires_every_n():
    t = RetrainTrigger(every_n=3)
    assert [t.record() for _ in range(6)] == [False, False, True, False, False, True]


def test_retrain_trigger_disabled():
    t = RetrainTrigger(every_n=0)
    assert all(t.record() is False for _ in range(10))


def test_run_retrain_cycle_writes_new_version(tmp_path):
    rows = []
    for i in range(20):
        rows.append({"tokens": [POS[i % len(POS)], POS[(i + 1) % len(POS)]], "label": "positive"})
        rows.append({"tokens": [NEG[i % len(NEG)], NEG[(i + 1) % len(NEG)]], "label": "negative"})
    path = tmp_path / "labeled.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")
    md = tmp_path / "models"
    result = run_retrain_cycle(str(path), model_dir=str(md), test_size=0.25,
                               random_state=0, version="r1")
    assert result.version == "r1"
    assert result.accuracy >= 0.9
    assert ModelStore(str(md)).resolve_version("latest") == "r1"