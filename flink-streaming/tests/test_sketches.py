"""
Unit tests for the probabilistic sketches (P1).

Verifies the three properties the design leans on:
  - Count-Min never undercounts, and overcounts stay within the e/width bound
  - HyperLogLog ignores duplicates and lands within a few standard errors
  - both merge associatively (the property that lets Flink parallelize them)

No Flink cluster required - pure Python.
"""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "src"))

from flink_job.operators.sketches import (
    CountMinSketch,
    HyperLogLog,
    RecentDuplicateFilter,
    TrendingCounter,
    build_reach_record,
    build_trending_record,
    informativeness,
    trending_terms,
    should_emit_trending,
    TRENDING_MIN_TOKENS,
)


class TestCountMinSketch:
    def test_exact_when_no_collisions(self):
        cms = CountMinSketch(width=2048, depth=4)
        for _ in range(5):
            cms.add("apple")
        cms.add("tesla", count=3)
        assert cms.estimate("apple") == 5
        assert cms.estimate("tesla") == 3

    def test_never_undercounts(self):
        rng = random.Random(42)
        truth: dict[str, int] = {}
        cms = CountMinSketch(width=256, depth=4)  # small grid to force collisions
        for _ in range(20_000):
            item = f"item-{rng.randint(0, 999)}"
            truth[item] = truth.get(item, 0) + 1
            cms.add(item)
        for item, count in truth.items():
            assert cms.estimate(item) >= count

    def test_overcount_within_bound(self):
        # theoretical bound: overcount <= e/width * total w.p. 1 - e^-depth
        rng = random.Random(7)
        truth: dict[str, int] = {}
        cms = CountMinSketch(width=2048, depth=4)
        for _ in range(50_000):
            item = f"item-{rng.randint(0, 4999)}"
            truth[item] = truth.get(item, 0) + 1
            cms.add(item)
        bound = (2.718 / cms.width) * cms.total  # ~66 for this stream
        misses = sum(
            1 for item, count in truth.items() if cms.estimate(item) - count > bound
        )
        assert misses / len(truth) < 0.05  # bound holds w.p. ~98%

    def test_merge_equals_single_sketch(self):
        a = CountMinSketch(width=512, depth=4)
        b = CountMinSketch(width=512, depth=4)
        whole = CountMinSketch(width=512, depth=4)
        for i in range(3_000):
            item = f"item-{i % 50}"
            (a if i % 2 else b).add(item)
            whole.add(item)
        a.merge(b)
        for i in range(50):
            assert a.estimate(f"item-{i}") == whole.estimate(f"item-{i}")
        assert a.total == whole.total

    def test_merge_rejects_mismatched_dimensions(self):
        with pytest.raises(ValueError):
            CountMinSketch(width=512, depth=4).merge(CountMinSketch(width=256, depth=4))

    def test_memory_is_flat(self):
        cms = CountMinSketch(width=2048, depth=4)
        assert cms.memory_cells == 8192
        for i in range(10_000):
            cms.add(f"unique-{i}")
        assert cms.memory_cells == 8192  # grid never grows


class TestHyperLogLog:
    def test_duplicates_are_ignored(self):
        hll = HyperLogLog(precision=12)
        for _ in range(500):
            hll.add("same_author")
        assert hll.cardinality() == 1

    def test_small_cardinality_is_near_exact(self):
        hll = HyperLogLog(precision=12)
        for i in range(100):
            hll.add(f"author-{i}")
        assert abs(hll.cardinality() - 100) <= 2  # linear-counting regime

    @pytest.mark.parametrize("n", [1_000, 50_000])
    def test_estimate_within_error_bound(self, n):
        hll = HyperLogLog(precision=12)
        for i in range(n):
            hll.add(f"author-{i}")
        # allow 4 standard errors (~6.5% at p=12); failures would be ~1 in 16k
        assert abs(hll.cardinality() - n) / n < 4 * hll.std_error

    def test_duplicates_do_not_move_estimate(self):
        hll = HyperLogLog(precision=12)
        for i in range(1_000):
            hll.add(f"author-{i}")
        before = hll.cardinality()
        for i in range(1_000):  # same authors again
            hll.add(f"author-{i}")
        assert hll.cardinality() == before

    def test_merge_counts_union(self):
        a = HyperLogLog(precision=12)
        b = HyperLogLog(precision=12)
        for i in range(2_000):
            a.add(f"author-{i}")
        for i in range(1_000, 3_000):  # 1k overlap with a
            b.add(f"author-{i}")
        a.merge(b)
        assert abs(a.cardinality() - 3_000) / 3_000 < 4 * a.std_error

    def test_merge_rejects_mismatched_precision(self):
        with pytest.raises(ValueError):
            HyperLogLog(precision=12).merge(HyperLogLog(precision=10))

    def test_memory_is_flat(self):
        hll = HyperLogLog(precision=12)
        assert len(hll._registers) == 4096  # ~4 KB
        for i in range(100_000):
            hll.add(f"author-{i}")
        assert len(hll._registers) == 4096


class TestTrendingCounter:
    def test_heavy_hitters_surface_in_order(self):
        rng = random.Random(1)
        counter = TrendingCounter(width=2048, depth=4, top_k=3)
        stream = ["apple"] * 500 + ["tesla"] * 300 + ["brexit"] * 100
        stream += [f"noise-{rng.randint(0, 2000)}" for _ in range(2_000)]
        rng.shuffle(stream)
        for token in stream:
            counter.add(token)
        top = counter.top()
        assert [t for t, *_ in top] == ["apple", "tesla", "brexit"]
        assert top[0][1] >= 500  # CMS never undercounts

    def test_candidate_list_stays_bounded(self):
        counter = TrendingCounter(width=512, depth=4, top_k=5, capacity=20)
        for i in range(10_000):
            counter.add(f"token-{i}")  # all unique
        assert len(counter._candidates) <= 20

    def test_merge_finds_global_top(self):
        a = TrendingCounter(width=1024, depth=4, top_k=2)
        b = TrendingCounter(width=1024, depth=4, top_k=2)
        # "apple" is only #2 on each worker but #1 globally
        for _ in range(60):
            a.add("apple")
            b.add("apple")
        for _ in range(80):
            a.add("tesla")
            b.add("brexit")
        a.merge(b)
        token, count, _score = a.top()[0]
        assert (token, count) == ("apple", 120)


class TestTermExtraction:
    def test_drops_stopwords_short_and_nonalpha(self):
        record = {"matched_keywords": ["tesla"],
                  "tokens": ["the", "Apple", "at", "x1", "🚀", "market", "AND"]}
        assert list(trending_terms(record, "tesla")) == ["apple", "market"]

    def test_handles_missing_tokens(self):
        assert list(trending_terms({}, "tesla")) == []

    def test_unmatched_comments_do_not_trend(self):
        record = {"tokens": ["apple", "market"]}  # no matched_keywords
        assert list(trending_terms(record, "apple")) == []

    def test_scoped_to_the_given_keyword(self):
        record = {"matched_keywords": ["google"], "tokens": ["search", "engine"]}
        assert list(trending_terms(record, "tesla")) == []

    def test_adjacent_tokens_become_phrases(self):
        record = {"matched_keywords": ["apple"],
                  "tokens": ["battery", "life", "sucks"]}
        assert list(trending_terms(record, "apple")) == [
            "battery", "life", "battery life", "sucks", "life sucks"]

    def test_stopword_breaks_the_phrase(self):
        record = {"matched_keywords": ["tesla"],
                  "tokens": ["king", "of", "pop"]}
        assert list(trending_terms(record, "tesla")) == ["king", "pop"]

    def test_keyword_excluded_alone_but_allowed_in_phrases(self):
        record = {"matched_keywords": ["apple"],
                  "tokens": ["Apple", "watch", "rocks"]}
        assert list(trending_terms(record, "apple")) == [
            "watch", "apple watch", "rocks", "watch rocks"]

    def test_drops_stripped_contractions(self):
        record = {"matched_keywords": ["tesla"],
                  "tokens": ["dont", "thats", "brexit"]}
        assert list(trending_terms(record, "tesla")) == ["brexit"]

    def test_drops_generic_filler(self):
        record = {"matched_keywords": ["tesla"],
                  "tokens": ["people", "think", "karma", "brexit"]}
        assert list(trending_terms(record, "tesla")) == ["brexit"]

    def test_drops_low_signal_filler_seen_on_sparse_windows(self):
        """Generic verbs/adverbs that surfaced as 'trends' on near-empty
        real-data windows (already, whole, bought, ...) are stopwords."""
        record = {"matched_keywords": ["coffee"],
                  "tokens": ["already", "whole", "bottom", "moved",
                             "bought", "roast"]}
        assert list(trending_terms(record, "coffee")) == ["roast"]

    def test_short_keyword_still_anchors_phrases(self):
        """A tracked keyword below the countable length (e.g. 'ai') must
        still form phrases - phrases around the keyword are the feature."""
        record = {"matched_keywords": ["ai"],
                  "tokens": ["ai", "safety", "research"]}
        terms = list(trending_terms(record, "ai"))
        assert "ai safety" in terms
        assert "safety research" in terms
        assert "ai" not in terms  # bare keyword still excluded as a word

    def test_each_term_counts_once_per_comment(self):
        """Document frequency, not raw term frequency: a spam comment
        repeating a word ten times must contribute it once."""
        record = {"matched_keywords": ["tesla"],
                  "tokens": ["video"] * 10 + ["custom", "video"]}
        terms = list(trending_terms(record, "tesla"))
        assert terms.count("video") == 1
        assert terms.count("video video") == 1  # phrase dedupes too
        assert terms.count("custom") == 1


class TestDistinctivenessRanking:
    """Ranking is count x informativeness (Zipf vs everyday English), so a
    generic word cannot 'trend' just by being common in ALL conversation."""

    def test_topical_terms_outrank_generic_filler(self):
        pytest.importorskip("wordfreq")
        counter = TrendingCounter()
        for _ in range(3):
            counter.add("moment")     # everyday word, high Zipf
        for _ in range(2):
            counter.add("cranberry")  # topical word, rare in general English
        tokens = [t for t, *_ in counter.top()]
        assert tokens.index("cranberry") < tokens.index("moment")

    def test_phrase_scores_like_its_rarest_word(self):
        pytest.importorskip("wordfreq")
        assert informativeness("boiling water") == informativeness("boiling")

    def test_unknown_tokens_count_as_rare(self):
        pytest.importorskip("wordfreq")
        # names/slang missing from the frequency table must not be buried
        assert informativeness("xqzzyplat") > informativeness("moment")


class TestRecentDuplicateFilter:
    def test_first_occurrence_passes_repeats_blocked(self):
        f = RecentDuplicateFilter(capacity=8)
        assert f.is_duplicate("kik sessions menu") is False
        assert f.is_duplicate("kik sessions menu") is True
        assert f.is_duplicate("a genuine comment") is False

    def test_lru_eviction_keeps_memory_bounded(self):
        f = RecentDuplicateFilter(capacity=2)
        f.is_duplicate("a")
        f.is_duplicate("b")
        f.is_duplicate("c")  # evicts "a" (oldest)
        assert f.is_duplicate("a") is False  # forgotten -> passes again
        assert f.is_duplicate("c") is True

    def test_rejects_invalid_capacity(self):
        with pytest.raises(ValueError):
            RecentDuplicateFilter(capacity=0)


class TestEmitFloor:
    def test_sparse_window_is_not_emitted(self):
        counter = TrendingCounter()
        for _ in range(TRENDING_MIN_TOKENS - 1):
            counter.add("apple")
        assert not should_emit_trending(counter)

    def test_substantial_window_is_emitted(self):
        counter = TrendingCounter()
        for _ in range(TRENDING_MIN_TOKENS):
            counter.add("apple")
        assert should_emit_trending(counter)


class TestResultRecords:
    def test_trending_record_schema(self):
        counter = TrendingCounter(top_k=2)
        for _ in range(3):
            counter.add("apple")
        rec = build_trending_record(counter, "apple",
                                    1_554_076_800_000, 1_554_080_400_000)
        assert rec["type"] == "trending"
        assert rec["keyword"] == "apple"
        assert rec["window_start"] == 1_554_076_800
        assert rec["window_end"] == 1_554_080_400
        item = rec["items"][0]
        assert item["token"] == "apple"
        assert item["count"] == 3
        assert item["score"] >= item["count"]  # count x informativeness >= count
        assert rec["sketch"]["kind"] == "count-min"

    def test_singleton_terms_are_not_published(self):
        """A term one single comment used is vocabulary, not a trend: it
        must not reach the dashboard (TRENDING_MIN_COUNT floor)."""
        counter = TrendingCounter()
        counter.add("battery life")
        counter.add("battery life")
        counter.add("cranberry")  # one comment's phrase, count 1
        rec = build_trending_record(counter, "coke", 0, 60_000)
        assert [i["token"] for i in rec["items"]] == ["battery life"]

    def test_quiet_window_publishes_empty_items(self):
        """All-singleton windows publish an empty item list (the record
        still carries the window bounds so the UI can say 'too quiet')."""
        counter = TrendingCounter()
        for token in ["among", "awkward", "bear"]:
            counter.add(token)
        rec = build_trending_record(counter, "coke", 0, 60_000)
        assert rec["items"] == []
        assert rec["window_end"] == 60

    def test_reach_record_schema(self):
        hll = HyperLogLog(precision=12)
        for author in ["u1", "u2", "u2"]:
            hll.add(author)
        rec = build_reach_record("apple", hll, 3, 1_554_076_800_000, 1_554_080_400_000)
        assert rec["type"] == "reach"
        assert rec["keyword"] == "apple"
        assert rec["unique_authors"] == 2
        assert rec["comment_count"] == 3
        assert rec["sketch"]["kind"] == "hyperloglog"
