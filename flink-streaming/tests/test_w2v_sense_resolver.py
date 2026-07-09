"""Unit tests for the embedding-based sense resolver."""

import math
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from flink_job.preprocessing.w2v_sense_resolver import W2VSenseResolver


class FakeKeyedVectors:
    """Mimics the slice of gensim's KeyedVectors interface the resolver needs."""

    def __init__(self, vectors: dict[str, list[float]]):
        self._vectors = {word: list(map(float, vec)) for word, vec in vectors.items()}
        self.key_to_index = {word: i for i, word in enumerate(self._vectors)}

    def __getitem__(self, word):
        import numpy as np
        return np.array(self._vectors[word])


class FakeWord2VecModel:
    """Mimics gensim's Word2Vec / Word2VecFeatureExtractor: exposes vectors via `.wv`."""

    def __init__(self, vectors: dict[str, list[float]]):
        self.wv = FakeKeyedVectors(vectors)


def _v(cosine_to_ctx: float) -> list[float]:
    """A 2D unit vector whose cosine similarity to [1.0, 0.0] is exactly `cosine_to_ctx`."""
    return [cosine_to_ctx, math.sqrt(1 - cosine_to_ctx**2)]


def test_clear_winner_above_threshold_and_margin():
    model = FakeKeyedVectors({
        "iphone": _v(1.0),
        "company": _v(1.0),
        "fruit": _v(0.0),
    })
    resolver = W2VSenseResolver(model)
    result = resolver.resolve(["iphone"], ["company", "fruit"])
    assert result == "company"


def test_tie_is_ambiguous():
    model = FakeKeyedVectors({
        "iphone": _v(1.0),
        "tree": _v(0.0),
        "company": _v(1.0),
        "fruit": _v(0.0),
    })
    resolver = W2VSenseResolver(model)
    # mean of iphone/tree sits equidistant between the company and fruit axes
    result = resolver.resolve(["iphone", "tree"], ["company", "fruit"])
    assert result == "ambiguous"


def test_all_scores_below_min_similarity_is_ambiguous():
    model = FakeKeyedVectors({
        "iphone": _v(1.0),
        "quantum": _v(0.15),
        "banana": _v(0.10),
    })
    resolver = W2VSenseResolver(model)
    result = resolver.resolve(["iphone"], ["quantum", "banana"])
    assert result == "ambiguous"


def test_out_of_vocab_subkeyword_wins_via_literal_fallback():
    model = FakeKeyedVectors({
        "iphone": _v(1.0),
        "fruit": _v(0.0),
        # "slang" intentionally absent from vocab
    })
    resolver = W2VSenseResolver(model)
    result = resolver.resolve(["iphone", "slang"], ["slang", "fruit"])
    assert result == "slang"


def test_out_of_vocab_subkeyword_absent_from_tokens_is_skipped():
    model = FakeKeyedVectors({
        "iphone": _v(1.0),
        "company": _v(1.0),
        # "ghost" absent from vocab and never appears in tokens
    })
    resolver = W2VSenseResolver(model)
    result = resolver.resolve(["iphone"], ["ghost", "company"])
    assert result == "company"


def test_no_in_vocab_tokens_and_no_literal_matches_is_ambiguous():
    model = FakeKeyedVectors({
        "company": _v(1.0),
        "fruit": _v(0.0),
    })
    resolver = W2VSenseResolver(model)
    result = resolver.resolve(["unknownword1", "unknownword2"], ["company", "fruit"])
    assert result == "ambiguous"


def test_weak_winner_with_close_distractor_is_ambiguous():
    """Noisy-embedding case: the correct subkeyword clears min_similarity but
    only barely, and a distractor scores close enough behind it to fall
    within min_margin - the resolver should refuse to guess rather than
    pick the nominal top score."""
    model = FakeKeyedVectors({
        "ctx": _v(1.0),
        "target": _v(0.22),      # just above default min_similarity (0.20)
        "distractor": _v(0.19),  # only 0.03 behind "target" (< default min_margin 0.05)
    })
    resolver = W2VSenseResolver(model)
    result = resolver.resolve(["ctx"], ["target", "distractor"])
    assert result == "ambiguous"


def test_lower_min_margin_allows_close_win():
    """Same borderline geometry as above, but with a resolver tuned to accept
    a smaller margin - proves min_margin is actually wired through."""
    model = FakeKeyedVectors({
        "ctx": _v(1.0),
        "target": _v(0.22),
        "distractor": _v(0.19),
    })
    resolver = W2VSenseResolver(model, min_margin=0.02)
    result = resolver.resolve(["ctx"], ["target", "distractor"])
    assert result == "target"


def test_custom_min_similarity_allows_low_score_win():
    """Proves min_similarity is wired through: a score that fails the default
    floor passes once the floor is lowered."""
    model = FakeKeyedVectors({
        "ctx": _v(1.0),
        "target": _v(0.15),
    })
    resolver = W2VSenseResolver(model, min_similarity=0.10)
    result = resolver.resolve(["ctx"], ["target"])
    assert result == "target"


def test_accepts_model_with_wv_attribute():
    model = FakeWord2VecModel({
        "iphone": _v(1.0),
        "company": _v(1.0),
        "fruit": _v(0.0),
    })
    resolver = W2VSenseResolver(model)
    result = resolver.resolve(["iphone"], ["company", "fruit"])
    assert result == "company"
