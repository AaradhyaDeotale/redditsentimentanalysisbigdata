"""Tests for the VADER-based corpus labeller."""

import json
from pathlib import Path

from ml_model.labeling.label_corpus import label_corpus
from ml_model.labeling.lexicon_labeler import (
    LexiconLabeler,
    NEGATIVE,
    NEUTRAL,
    POSITIVE,
    pick_text,
)


def test_clear_positive():
    assert LexiconLabeler().label("I absolutely love this, it's amazing!").label == POSITIVE


def test_clear_negative():
    assert LexiconLabeler().label("This is terrible, I hate it.").label == NEGATIVE


def test_factual_statement_is_neutral():
    assert LexiconLabeler().label("The meeting is at 3pm on Tuesday.").label == NEUTRAL


def test_empty_text_is_neutral_with_zero_score():
    result = LexiconLabeler().label("")
    assert result.label == NEUTRAL
    assert result.compound == 0.0


def test_emoji_carries_sentiment():
    # VADER ships an emoji lexicon, so emojis shift the score even when the
    # surrounding words are neutral. This guards the project's "keep emojis"
    # rule. (Note: VADER recognises many but not all emojis — e.g. it scores
    # 😍/😢/😠 but is neutral on 😡/❤️/👍, so we test ones it knows.)
    assert LexiconLabeler().label("the update 😍").label == POSITIVE
    assert LexiconLabeler().label("the update 😠").label == NEGATIVE


def test_wide_neutral_band_forces_neutral():
    text = "I love this!"
    assert LexiconLabeler(neutral_band=0.05).label(text).label == POSITIVE
    # A near-1.0 band swallows almost everything into neutral.
    assert LexiconLabeler(neutral_band=0.99).label(text).label == NEUTRAL


def test_negative_neutral_band_rejected():
    try:
        LexiconLabeler(neutral_band=-0.1)
    except ValueError:
        return
    raise AssertionError("negative neutral_band should raise ValueError")


def test_pick_text_prefers_original_body():
    rec = {"original_body": "ORIG", "cleaned_body": "clean", "tokens": ["t"]}
    assert pick_text(rec) == "ORIG"


def test_pick_text_falls_back_to_tokens():
    assert pick_text({"tokens": ["good", "great"]}) == "good great"


def test_pick_text_empty_record():
    assert pick_text({}) == ""


def test_label_corpus_drops_neutral_and_carries_fields(tmp_path: Path):
    records = [
        {"id": "1", "original_body": "I love this, it's wonderful!",
         "tokens": ["love", "wonderful"], "cleaned_body": "love wonderful",
         "language": "en", "matched_keywords": ["apple"]},
        {"id": "2", "original_body": "This is awful and a total disaster.",
         "tokens": ["awful", "disaster"], "cleaned_body": "awful disaster",
         "language": "en", "matched_keywords": []},
        {"id": "3", "original_body": "The package arrived on Tuesday.",
         "tokens": ["package", "tuesday"], "cleaned_body": "package tuesday",
         "language": "en", "matched_keywords": []},
    ]
    inp = tmp_path / "in.jsonl"
    inp.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    out = tmp_path / "out.jsonl"

    counts, written = label_corpus(inp, out, LexiconLabeler(), keep_neutral=False)

    lines = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    labels = {r["id"]: r["label"] for r in lines}

    assert labels.get("1") == POSITIVE
    assert labels.get("2") == NEGATIVE
    assert "3" not in labels            # neutral dropped
    assert written == 2
    assert counts[NEUTRAL] == 1         # but still counted
    expected_keys = {"id", "tokens", "cleaned_body", "language",
                     "matched_keywords", "label", "vader_compound"}
    assert expected_keys.issubset(lines[0].keys())


def test_label_corpus_keep_neutral(tmp_path: Path):
    rec = {"id": "9", "original_body": "It happened on Tuesday.",
           "tokens": ["tuesday"], "cleaned_body": "tuesday",
           "language": "en", "matched_keywords": []}
    inp = tmp_path / "in.jsonl"
    inp.write_text(json.dumps(rec), encoding="utf-8")
    out = tmp_path / "out.jsonl"

    _, written = label_corpus(inp, out, LexiconLabeler(), keep_neutral=True)
    assert written == 1


def test_label_corpus_skips_malformed_lines(tmp_path: Path):
    inp = tmp_path / "in.jsonl"
    inp.write_text('{"id":"1","original_body":"I love it!"}\nNOT JSON\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"

    counts, written = label_corpus(inp, out, LexiconLabeler())
    assert written == 1  # the one valid positive line