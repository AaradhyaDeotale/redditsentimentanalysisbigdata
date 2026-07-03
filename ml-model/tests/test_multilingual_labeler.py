"""Tests for the language-aware (multilingual) corpus labeller."""

import json
from pathlib import Path

from ml_model.labeling.label_corpus import label_corpus
from ml_model.labeling.lexicon_labeler import NEGATIVE, NEUTRAL, POSITIVE
from ml_model.labeling.multilingual_labeler import MultilingualLexiconLabeler


def test_english_still_uses_vader():
    r = MultilingualLexiconLabeler().label_record(
        {"language": "en", "tokens": ["love", "amazing"],
         "cleaned_body": "I love this, it's amazing!"})
    assert r.label == POSITIVE
    assert r.source == "vader"


def test_german_positive_via_lexicon():
    r = MultilingualLexiconLabeler().label_record(
        {"language": "de", "tokens": ["das", "ist", "wunderbar", "danke"]})
    assert r.label == POSITIVE
    assert r.source == "lexicon"


def test_french_negative_via_lexicon():
    r = MultilingualLexiconLabeler().label_record(
        {"language": "fr", "tokens": ["je", "déteste", "horrible"]})
    assert r.label == NEGATIVE
    assert r.source == "lexicon"


def test_spanish_positive():
    r = MultilingualLexiconLabeler().label_record(
        {"language": "es", "tokens": ["me", "encanta", "excelente"]})
    assert r.label == POSITIVE


def test_emoji_is_language_independent():
    # No lexicon word hits — polarity comes purely from the emoji token.
    pos = MultilingualLexiconLabeler().label_record(
        {"language": "it", "tokens": ["boh", "😍"]})
    neg = MultilingualLexiconLabeler().label_record(
        {"language": "it", "tokens": ["boh", "👎"]})
    assert pos.label == POSITIVE
    assert neg.label == NEGATIVE


def test_no_signal_is_neutral():
    r = MultilingualLexiconLabeler().label_record(
        {"language": "pt", "tokens": ["a", "reunião", "terça"]})
    assert r.label == NEUTRAL
    assert r.compound == 0.0


def test_unknown_language_falls_back_to_vader():
    r = MultilingualLexiconLabeler().label_record(
        {"language": "unknown", "tokens": ["terrible", "hate"],
         "cleaned_body": "This is terrible, I hate it."})
    assert r.label == NEGATIVE
    assert r.source == "vader"


def test_unsupported_language_falls_back_to_vader():
    # A language we carry no list for (e.g. Russian) routes to VADER.
    r = MultilingualLexiconLabeler().label_record(
        {"language": "ru", "tokens": ["love", "wonderful"],
         "cleaned_body": "love wonderful"})
    assert r.source == "vader"


def test_missing_tokens_falls_back_to_text():
    r = MultilingualLexiconLabeler().label_record(
        {"language": "de", "cleaned_body": "das ist schrecklich"})
    assert r.label == NEGATIVE


def test_negative_neutral_band_rejected():
    try:
        MultilingualLexiconLabeler(neutral_band=-0.1)
    except ValueError:
        return
    raise AssertionError("negative neutral_band should raise ValueError")


def test_lexicon_dir_extends_seeds(tmp_path: Path):
    (tmp_path / "de.txt").write_text(
        "zauberhaft\tpos\nabscheulich\tneg\n", encoding="utf-8")
    labeler = MultilingualLexiconLabeler(lexicon_dir=str(tmp_path))
    assert labeler.label_record(
        {"language": "de", "tokens": ["zauberhaft"]}).label == POSITIVE
    assert labeler.label_record(
        {"language": "de", "tokens": ["abscheulich"]}).label == NEGATIVE


def test_label_corpus_routes_by_language(tmp_path: Path):
    records = [
        {"id": "1", "language": "en", "tokens": ["love", "wonderful"],
         "cleaned_body": "I love this, it's wonderful!", "matched_keywords": []},
        {"id": "2", "language": "de", "tokens": ["das", "ist", "schrecklich"],
         "cleaned_body": "das ist schrecklich", "matched_keywords": []},
        {"id": "3", "language": "fr", "tokens": ["parfait", "génial"],
         "cleaned_body": "parfait génial", "matched_keywords": []},
    ]
    inp = tmp_path / "in.jsonl"
    inp.write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    out = tmp_path / "out.jsonl"

    counts, written = label_corpus(inp, out, MultilingualLexiconLabeler())

    lines = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    by_id = {r["id"]: r for r in lines}
    assert by_id["1"]["label"] == POSITIVE and by_id["1"]["label_source"] == "vader"
    assert by_id["2"]["label"] == NEGATIVE and by_id["2"]["label_source"] == "lexicon"
    assert by_id["3"]["label"] == POSITIVE and by_id["3"]["label_source"] == "lexicon"
    assert written == 3
