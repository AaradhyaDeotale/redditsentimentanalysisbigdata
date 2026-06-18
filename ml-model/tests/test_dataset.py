"""Tests for loading the labelled dataset (Phase 3 input loader)."""

import json

from ml_model.data.dataset import LabeledDataset, load_labeled_dataset


def _write(tmp_path, records):
    path = tmp_path / "labeled.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    return path


def test_load_basic(tmp_path):
    path = _write(tmp_path, [
        {"id": "1", "tokens": ["apple", "great"], "label": "positive"},
        {"id": "2", "tokens": ["android", "buggy"], "label": "negative"},
    ])
    ds = load_labeled_dataset(path)
    assert isinstance(ds, LabeledDataset)
    assert len(ds) == 2
    assert ds.tokens[0] == ["apple", "great"]
    assert ds.labels == ["positive", "negative"]


def test_skip_records_missing_tokens_or_label(tmp_path):
    path = _write(tmp_path, [
        {"id": "1", "tokens": ["ok"], "label": "positive"},
        {"id": "2", "label": "negative"},            # no tokens
        {"id": "3", "tokens": ["x"]},                # no label
    ])
    ds = load_labeled_dataset(path)
    assert len(ds) == 1


def test_min_tokens_filter(tmp_path):
    path = _write(tmp_path, [
        {"id": "1", "tokens": ["a", "b", "c"], "label": "positive"},
        {"id": "2", "tokens": ["a"], "label": "negative"},
    ])
    ds = load_labeled_dataset(path, min_tokens=2)
    assert len(ds) == 1
    assert ds.labels == ["positive"]


def test_malformed_lines_skipped(tmp_path):
    path = tmp_path / "labeled.jsonl"
    path.write_text('{"tokens":["a"],"label":"positive"}\nNOT JSON\n', encoding="utf-8")
    ds = load_labeled_dataset(path)
    assert len(ds) == 1