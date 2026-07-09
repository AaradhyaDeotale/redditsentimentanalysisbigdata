"""
sketches.py
-----------
Probabilistic sketches (P1): approximate stream analytics in fixed memory.

Two classic one-pass, mergeable summaries feed the dashboard's Trends tab:

  * Count-Min Sketch  - "how often?"          -> trending terms per keyword
  * HyperLogLog       - "how many different?" -> unique authors per keyword

Both trade a small, bounded error for memory that stays flat no matter how
large the stream grows, and both merge associatively - which is exactly what
lets Flink run them on parallel workers and combine per-window partial
results (AggregateFunction.merge) without any worker seeing the whole stream.

Parameter choices (the accuracy-vs-memory dial):

  Count-Min Sketch: depth=4, width=2048
      error per query  <= e/width  ~ 0.13% of total stream count,
      with probability >= 1 - e^-depth ~ 98.2%.
      Memory: 4 x 2048 counters = 8,192 ints (~64 KB) - flat forever.
      Good enough because trending only needs the ORDER of the big counts;
      heavy hitters dwarf the collision noise.

  HyperLogLog: precision p=12 -> m=4096 registers
      standard error = 1.04 / sqrt(m) ~ 1.6%.
      Memory: 4,096 single-byte registers ~ 4 KB per (keyword, window) -
      versus megabytes for an exact set of author names.

The pure-Python classes are Flink-free (unit-testable); the Flink wrappers at
the bottom follow the same try/ImportError convention as the other operators.
"""

from __future__ import annotations

import hashlib
import math
import os
from collections import OrderedDict
from functools import lru_cache
from typing import Any, Iterable

# ---------------------------------------------------------------------------
# hashing
# ---------------------------------------------------------------------------
# Python's built-in hash() is randomized per process, so two Flink workers
# would disagree about where "apple" lands - sketches built on it could not
# be merged. blake2b with a per-row salt is deterministic everywhere.


def _hash64(item: str, seed: int = 0) -> int:
    """Deterministic 64-bit hash of a string, independent per seed."""
    h = hashlib.blake2b(
        item.encode("utf-8"), digest_size=8, salt=seed.to_bytes(8, "little")
    )
    return int.from_bytes(h.digest(), "big")


# ---------------------------------------------------------------------------
# Count-Min Sketch
# ---------------------------------------------------------------------------


class CountMinSketch:
    """Approximate frequency counts in a fixed depth x width grid.

    Each row hashes the item to one column and increments that counter.
    A query reads the same cells and returns the MINIMUM - collisions can
    only inflate a cell, never deflate it, so estimates never undercount.
    """

    def __init__(self, width: int = 2048, depth: int = 4):
        if width < 1 or depth < 1:
            raise ValueError("width and depth must be >= 1")
        self.width = width
        self.depth = depth
        self.total = 0  # total additions, for relative-error context
        self._rows: list[list[int]] = [[0] * width for _ in range(depth)]

    def _columns(self, item: str) -> list[int]:
        return [_hash64(item, seed=row) % self.width for row in range(self.depth)]

    def add(self, item: str, count: int = 1) -> int:
        """Increment and return the fresh estimate (min over the updated
        cells) - one hash pass instead of an add() + estimate() pair, which
        matters in the per-term hot path."""
        self.total += count
        estimate = None
        for row, col in enumerate(self._columns(item)):
            self._rows[row][col] += count
            value = self._rows[row][col]
            if estimate is None or value < estimate:
                estimate = value
        return estimate

    def estimate(self, item: str) -> int:
        """Estimated count; may overcount by ~e/width * total, never undercounts."""
        return min(self._rows[row][col] for row, col in enumerate(self._columns(item)))

    def merge(self, other: "CountMinSketch") -> "CountMinSketch":
        """Combine two sketches of disjoint sub-streams: add grids cell-by-cell."""
        if (self.width, self.depth) != (other.width, other.depth):
            raise ValueError("cannot merge sketches with different dimensions")
        for row in range(self.depth):
            mine, theirs = self._rows[row], other._rows[row]
            for col in range(self.width):
                mine[col] += theirs[col]
        self.total += other.total
        return self

    @property
    def memory_cells(self) -> int:
        return self.width * self.depth


# ---------------------------------------------------------------------------
# HyperLogLog
# ---------------------------------------------------------------------------


class HyperLogLog:
    """Approximate distinct count using leading-zero statistics.

    Each item is hashed; the first p bits pick one of m=2^p registers and the
    register remembers the longest run of leading zeros (+1) seen in the rest
    of the hash. Duplicates hash identically, so re-adding the same author
    never moves a register - that is why it counts DISTINCT items.
    """

    def __init__(self, precision: int = 12):
        if not 4 <= precision <= 18:
            raise ValueError("precision must be in [4, 18]")
        self.precision = precision
        self.m = 1 << precision
        self._registers = bytearray(self.m)

    @property
    def std_error(self) -> float:
        """Expected relative standard error (~1.6% at p=12)."""
        return 1.04 / math.sqrt(self.m)

    def add(self, item: str) -> None:
        h = _hash64(item)
        bucket = h >> (64 - self.precision)  # first p bits pick the register
        rest = h & ((1 << (64 - self.precision)) - 1)
        # rank = leading zeros in the remaining 64-p bits, plus 1
        rank = (64 - self.precision) - rest.bit_length() + 1
        if rank > self._registers[bucket]:
            self._registers[bucket] = rank

    def cardinality(self) -> int:
        """Harmonic-mean estimate with the standard small-range correction."""
        m = self.m
        inv_sum = 0.0
        zeros = 0
        for reg in self._registers:
            inv_sum += 2.0 ** -reg
            if reg == 0:
                zeros += 1

        alpha = 0.7213 / (1 + 1.079 / m)  # bias constant for m >= 128
        estimate = alpha * m * m / inv_sum

        # Small cardinalities: harmonic mean is biased; linear counting
        # (occupancy of empty registers) is more accurate below ~2.5m.
        if estimate <= 2.5 * m and zeros:
            estimate = m * math.log(m / zeros)
        return int(round(estimate))

    def merge(self, other: "HyperLogLog") -> "HyperLogLog":
        """Combine two sketches of disjoint sub-streams: register-wise max."""
        if self.precision != other.precision:
            raise ValueError("cannot merge HLLs with different precision")
        for i, reg in enumerate(other._registers):
            if reg > self._registers[i]:
                self._registers[i] = reg
        return self


# ---------------------------------------------------------------------------
# distinctiveness weighting (TF-IDF against general English)
# ---------------------------------------------------------------------------
# Ranking by raw document frequency makes generic conversation words "trend"
# the moment a window is thin - "already" and "whole" appear in ANY topic, so
# on 8 comments they tie with everything else and win alphabetically. Instead
# of chasing them with an ever-growing stopword list, weight each term by how
# surprising it is in everyday English: wordfreq's Zipf scale puts "the" at
# ~7.7, "already" at ~5.6 and "cranberry" at ~3.2, so
#
#   score = count x max(1, ZIPF_CEILING - zipf(term))
#
# makes a common-everywhere word need several times the mentions of a
# genuinely topical one to outrank it, while the count factor keeps volume
# relevant. A phrase is as informative as its RAREST word ("boiling water"
# scores like "boiling"). wordfreq is optional by design: without it every
# term weighs 1.0 and ranking degrades to plain counts.

_ZIPF_CEILING = 7.5  # ~the most common English words ("the" is 7.7)
_ZIPF_UNKNOWN = 2.5  # unseen tokens (names, slang) count as rare = informative

try:
    from wordfreq import zipf_frequency as _zipf
except ImportError:  # keep the sketches dependency-free when absent
    _zipf = None


@lru_cache(maxsize=65536)
def informativeness(term: str) -> float:
    """How surprising a term is in general English (1.0 = pure filler)."""
    if _zipf is None:
        return 1.0
    rarest = min(
        (z if (z := _zipf(word, "en")) > 0 else _ZIPF_UNKNOWN)
        for word in term.split()
    )
    return max(1.0, _ZIPF_CEILING - rarest)


# ---------------------------------------------------------------------------
# Trending = Count-Min Sketch + a small candidate list (heavy hitters)
# ---------------------------------------------------------------------------


class TrendingCounter:
    """Top-K frequent tokens over a stream, in fixed memory.

    The CMS alone answers "how often is X?" but cannot enumerate items, so we
    keep a small candidate dict (token -> cached estimate) of the current
    front-runners. When it overflows, the weakest candidate is evicted -
    a genuinely rare token can never displace a heavy hitter because its CMS
    estimate stays small. Memory: the grid + at most `capacity` tokens.
    """

    def __init__(self, width: int = 2048, depth: int = 4, top_k: int = 20,
                 capacity: int | None = None):
        self.cms = CountMinSketch(width=width, depth=depth)
        self.top_k = top_k
        # 8x headroom: candidates are kept by raw count but RANKED by
        # count x informativeness, so a rare topical term with a modest count
        # must survive eviction long enough for the final scoring to lift it.
        self.capacity = capacity or max(8 * top_k, 128)
        self._candidates: dict[str, int] = {}

    def add(self, token: str, count: int = 1) -> None:
        est = self.cms.add(token, count)  # add() returns the fresh estimate
        if token in self._candidates or len(self._candidates) < self.capacity:
            self._candidates[token] = est
            return
        weakest = min(self._candidates, key=self._candidates.get)
        if est > self._candidates[weakest]:
            del self._candidates[weakest]
            self._candidates[token] = est

    def merge(self, other: "TrendingCounter") -> "TrendingCounter":
        self.cms.merge(other.cms)
        # Re-estimate every candidate against the merged grid, keep the best.
        union = set(self._candidates) | set(other._candidates)
        scored = {t: self.cms.estimate(t) for t in union}
        keep = sorted(scored, key=scored.get, reverse=True)[: self.capacity]
        self._candidates = {t: scored[t] for t in keep}
        return self

    def top(self, k: int | None = None) -> list[tuple[str, int, float]]:
        """[(token, estimated_count, score)] by score, descending.

        score = count x informativeness: everyday words need many times the
        mentions of a topical one to rank (see the weighting section above).
        """
        k = k or self.top_k
        ranked = sorted(
            (
                (t, est, round(est * informativeness(t), 2))
                for t, est in
                ((t, self.cms.estimate(t)) for t in self._candidates)
            ),
            key=lambda item: (-item[2], item[0]),
        )
        return ranked[:k]


# ---------------------------------------------------------------------------
# term extraction for the trending stream
# ---------------------------------------------------------------------------
# The pipeline's stop-word removal is optional (PREPROCESS_REMOVE_STOPWORDS
# defaults to false), so filter here: trending "the" is never interesting.
# Trending is scoped PER TRACKED KEYWORD - "what terms come up around apple" -
# because raw frequency over the whole stream is always generic conversation
# filler, and scoping per keyword lets the dashboard drop a keyword's trends
# the moment it is untracked. Besides single words we also count two-word
# phrases (adjacent surviving tokens), which read far better than lone words:
# "battery life" instead of "battery". The bare keyword is excluded as a
# single word (it appears in 100% of its own comments) but is allowed inside
# phrases - "apple watch" trending under apple is exactly the point.
# Non-English comments never reach this operator (dropped in parse), so the
# stopword list is English-only. It is self-contained (not imported from the
# tokenizer) so the sketches work standalone.

_TRENDING_STOPWORDS: frozenset[str] = frozenset({
    # articles / conjunctions / prepositions
    "the", "and", "but", "for", "nor", "yet", "with", "about", "against",
    "between", "into", "through", "during", "before", "after", "above",
    "below", "from", "down", "out", "off", "over", "under", "again",
    "further", "once", "because", "until", "while", "than", "then",
    # pronouns / determiners
    "you", "your", "yours", "yourself", "yourselves", "him", "his",
    "himself", "she", "her", "hers", "herself", "its", "itself", "they",
    "them", "their", "theirs", "themselves", "what", "which", "who",
    "whom", "this", "that", "these", "those", "all", "any", "both",
    "each", "few", "more", "most", "other", "some", "such", "own",
    "same", "our", "ours", "ourselves", "myself",
    # verbs / auxiliaries
    "are", "was", "were", "been", "being", "have", "has", "had", "having",
    "must", "may", "might", "shall",
    "does", "did", "doing", "will", "would", "could", "should", "can",
    "cannot", "get", "got", "gets", "getting", "make", "makes", "made",
    "say", "says", "said", "going", "gonna", "want", "wants", "wanted",
    # adverbs / fillers
    "not", "only", "very", "too", "just", "also", "here", "there",
    "when", "where", "why", "how", "now", "even", "still", "really",
    "much", "many", "well", "way", "like", "one", "something", "anything",
    "everything", "nothing", "thing", "things", "actually", "though",
    "maybe", "probably", "pretty", "lot", "yes", "yeah",
    # apostrophe-stripped contractions ("don't" -> "dont" after cleaning)
    "dont", "cant", "wont", "isnt", "arent", "wasnt", "werent", "didnt",
    "doesnt", "hasnt", "havent", "wouldnt", "couldnt", "shouldnt",
    "thats", "theres", "youre", "youve", "youll", "theyre", "theyve",
    "ive", "hes", "shes", "its", "whats", "lets", "ill", "weve", "youd",
    "theyd", "theyll", "whos", "hows",
    # ...and their stems when the apostrophe splits instead ("don't" ->
    # "don t"): fragments that are not words on their own
    "don", "didn", "doesn", "isn", "wasn", "weren", "aren", "wouldn",
    "couldn", "shouldn", "hasn", "haven", "hadn", "ain",
    # generic conversation filler - frequent in ANY topic, so never "trending"
    "people", "person", "think", "thought", "know", "knew", "good", "bad",
    "time", "times", "right", "wrong", "need", "needs", "back", "use",
    "used", "using", "never", "always", "feel", "feels", "felt", "better",
    "worse", "best", "worst", "years", "year", "day", "days", "sure",
    "someone", "anyone", "everyone", "nobody", "somebody", "said", "see",
    "seen", "look", "looks", "looking", "come", "comes", "came", "take",
    "takes", "took", "give", "gives", "gave", "tell", "told", "mean",
    "means", "meant", "point", "stuff", "guy", "guys", "man", "men",
    "woman", "women", "first", "last", "long", "little", "big", "great",
    "new", "old", "different", "keep", "kept", "put", "find", "found",
    "work", "works", "help", "try", "trying", "tried", "start", "started",
    "stop", "part", "place", "case", "fact", "least", "most", "less",
    "already", "whole", "bottom", "moved", "move", "moves", "called",
    "call", "calls", "bought", "buy", "buying", "went", "goes", "gone",
    "done", "seem", "seems", "seemed", "ever", "every", "around", "away",
    "enough", "else", "real", "since", "without", "thanks", "thank",
    "sorry", "definitely", "honestly", "literally", "basically",
    # reddit-meta noise (bot messages, subreddit housekeeping)
    "post", "posts", "posted", "comment", "comments", "reddit", "thread",
    "sub", "subreddit", "karma", "mod", "mods", "moderator", "moderators",
    "removed", "automatically", "please", "message", "action", "questions",
    "concerns", "rule", "rules", "upvote", "downvote", "edit", "deleted",
})

_MIN_TOKEN_LEN = 3

# Windows with almost no countable terms (e.g. a quiet minute, or one whose
# comments were all non-English and got filtered) are not emitted: "top k of
# 38 terms" is noise, and the CMS error bound is relative to the stream
# total, so tiny windows have no statistical mass to rank. The stream is now
# split per keyword, so the floor is lower than the old global default.
TRENDING_MIN_TOKENS = int(os.getenv("TRENDING_MIN_TOKENS", "30"))

# A term a single comment used is by definition not trending - it is that
# comment's vocabulary. Terms below this document-frequency floor are dropped
# from the published record; a quiet window can therefore publish EMPTY items
# (still emitted, so the dashboard can say "window too quiet" with the
# window's real timestamp instead of silently showing the previous window).
TRENDING_MIN_COUNT = int(os.getenv("TRENDING_MIN_COUNT", "2"))


def should_emit_trending(counter: "TrendingCounter") -> bool:
    """Whether a window's sketch has enough mass to be worth publishing."""
    return counter.cms.total >= TRENDING_MIN_TOKENS


def _countable(token: str) -> bool:
    """Whether a (lowercased) token may appear in a trending term at all."""
    return (len(token) >= _MIN_TOKEN_LEN and token.isalpha()
            and token not in _TRENDING_STOPWORDS)


def trending_terms(record: dict[str, Any], keyword: str) -> Iterable[str]:
    """Terms of one cleaned record worth counting under one tracked keyword.

    Yields single words plus two-word phrases from tokens that are adjacent
    in the original comment (a stopword between them breaks the phrase, so
    "king of pop" never becomes "king pop"). The bare keyword is excluded as
    a single word - it appears in 100% of its own comments - but may be half
    of a phrase ("apple watch"), which is where the interesting trends live.

    Each distinct term is yielded ONCE per comment (document frequency, not
    raw term frequency): trending should mean "many comments mention X", and
    without this a single spam comment repeating a word ten times counts as
    ten mentions and swamps the window.
    """
    kw = keyword.lower()
    matched = {k.lower() for k in record.get("matched_keywords") or []}
    if kw not in matched:
        return  # trending is scoped to this keyword's conversation
    seen: set[str] = set()  # once per comment, see docstring
    prev: str | None = None  # previous token, if it survived the filter
    for token in record.get("tokens") or []:
        t = token.lower()
        # The tracked keyword is always phrase-eligible, even when it would
        # not be countable on its own (shorter than the minimum, e.g. "ai"):
        # phrases around the keyword are the whole point of the feature.
        if not _countable(t) and t != kw:
            prev = None
            continue
        if t != kw and t not in seen:
            seen.add(t)
            yield t
        if prev is not None:
            phrase = f"{prev} {t}"
            if phrase != kw and phrase not in seen and not (prev == kw and t == kw):
                seen.add(phrase)
                yield phrase
        prev = t


# Verbatim reposts (spam "menus", bot copypasta) would otherwise dominate
# trending: the same comment posted 10x contributes its terms 10x. The filter
# remembers the last `capacity` comment bodies (hashes, LRU) per worker and
# drops repeats. Best-effort by design: with parallelism > 1 each worker has
# its own memory, so a duplicate can slip through on another worker - fine,
# the goal is to stop 10x, not to be exact. 0 disables.
TRENDING_DEDUP_CAPACITY = int(os.getenv("TRENDING_DEDUP_CAPACITY", "4096"))


class RecentDuplicateFilter:
    """Remembers the last `capacity` texts (as 64-bit hashes, LRU-evicted).

    The first occurrence passes (real content trends once); repeats are
    reported as duplicates. Memory is bounded: capacity x ~8-byte keys.
    """

    def __init__(self, capacity: int = 4096):
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self._seen: OrderedDict[int, None] = OrderedDict()

    def is_duplicate(self, text: str) -> bool:
        key = _hash64(text)
        if key in self._seen:
            self._seen.move_to_end(key)
            return True
        self._seen[key] = None
        if len(self._seen) > self.capacity:
            self._seen.popitem(last=False)
        return False


# ---------------------------------------------------------------------------
# result records (dashboard schema, unix seconds like sentiment-results)
# ---------------------------------------------------------------------------


def build_trending_record(counter: TrendingCounter, keyword: str,
                          window_start_ms: int, window_end_ms: int) -> dict[str, Any]:
    return {
        "type": "trending",
        "keyword": keyword,
        "window_start": int(window_start_ms // 1000),
        "window_end": int(window_end_ms // 1000),
        "items": [{"token": t, "count": c, "score": s}
                  for t, c, s in counter.top() if c >= TRENDING_MIN_COUNT],
        "sketch": {
            "kind": "count-min",
            "width": counter.cms.width,
            "depth": counter.cms.depth,
            "stream_total": counter.cms.total,
        },
    }


def build_reach_record(keyword: str, hll: HyperLogLog, comment_count: int,
                       window_start_ms: int, window_end_ms: int) -> dict[str, Any]:
    return {
        "type": "reach",
        "keyword": keyword,
        "window_start": int(window_start_ms // 1000),
        "window_end": int(window_end_ms // 1000),
        "unique_authors": hll.cardinality(),
        "comment_count": comment_count,
        "sketch": {
            "kind": "hyperloglog",
            "precision": hll.precision,
            "std_error": round(hll.std_error, 4),
        },
    }


# ---------------------------------------------------------------------------
# Flink wrappers
# ---------------------------------------------------------------------------

CMS_WIDTH = int(os.getenv("CMS_WIDTH", "2048"))
CMS_DEPTH = int(os.getenv("CMS_DEPTH", "4"))
TRENDING_TOP_K = int(os.getenv("TRENDING_TOP_K", "20"))
HLL_PRECISION = int(os.getenv("HLL_PRECISION", "12"))

try:
    from pyflink.datastream.functions import (
        AggregateFunction,
        FlatMapFunction,
        ProcessWindowFunction,
    )

    class TrendingFanoutFunction(FlatMapFunction):
        """One {keyword, term} pair per trending term per matched keyword.

        Splitting the stream per keyword is what lets the dashboard show (and
        drop) trends per tracked keyword the moment the tracked set changes.
        Verbatim reposts of a recently seen comment body are skipped entirely
        (copypasta/spam suppression - see RecentDuplicateFilter).
        """

        def __init__(self):
            self._dedup: RecentDuplicateFilter | None = None

        def open(self, runtime_context):
            if TRENDING_DEDUP_CAPACITY > 0:
                self._dedup = RecentDuplicateFilter(TRENDING_DEDUP_CAPACITY)

        def flat_map(self, record: dict):
            body = record.get("cleaned_body") or ""
            if body and self._dedup is not None and self._dedup.is_duplicate(body):
                return  # verbatim repost - already counted once
            for kw in record.get("matched_keywords") or []:
                for term in trending_terms(record, kw):
                    yield {"keyword": kw, "term": term}

    class TrendingAggregateFunction(AggregateFunction):
        """Windowed accumulator IS the sketch - Flink merges partial sketches."""

        def create_accumulator(self) -> TrendingCounter:
            return TrendingCounter(width=CMS_WIDTH, depth=CMS_DEPTH,
                                   top_k=TRENDING_TOP_K)

        def add(self, pair: dict, acc: TrendingCounter) -> TrendingCounter:
            acc.add(pair["term"])
            return acc

        def get_result(self, acc: TrendingCounter) -> TrendingCounter:
            return acc

        def merge(self, a: TrendingCounter, b: TrendingCounter) -> TrendingCounter:
            return a.merge(b)

    class TrendingWindowFunction(ProcessWindowFunction):
        """Attach keyword + window bounds to the aggregated sketch."""

        def process(self, key: str, context: "ProcessWindowFunction.Context", counters):
            window = context.window()  # TimeWindow; bounds in ms
            for counter in counters:  # exactly one: the aggregate result
                if should_emit_trending(counter):
                    yield build_trending_record(counter, key, window.start, window.end)

    class AuthorFanoutFunction(FlatMapFunction):
        """One (keyword, author) pair per matched keyword of a comment."""

        def flat_map(self, record: dict):
            author = record.get("author") or ""
            if not author or author == "[deleted]":
                return
            for kw in record.get("matched_keywords") or []:
                yield {"keyword": kw, "author": author}

    class ReachAggregateFunction(AggregateFunction):
        """Accumulator = (HLL of authors, exact comment count) per keyword."""

        def create_accumulator(self) -> tuple[HyperLogLog, int]:
            return (HyperLogLog(precision=HLL_PRECISION), 0)

        def add(self, pair: dict, acc: tuple) -> tuple:
            hll, count = acc
            hll.add(pair["author"])
            return (hll, count + 1)

        def get_result(self, acc: tuple) -> tuple:
            return acc

        def merge(self, a: tuple, b: tuple) -> tuple:
            return (a[0].merge(b[0]), a[1] + b[1])

    class ReachWindowFunction(ProcessWindowFunction):
        """Attach keyword + window bounds to the aggregated (HLL, count)."""

        def process(self, key: str, context: "ProcessWindowFunction.Context", accs):
            window = context.window()
            for hll, count in accs:  # exactly one: the aggregate result
                yield build_reach_record(key, hll, count, window.start, window.end)

except ImportError:
    # Running outside Flink (e.g. unit tests) - pure classes above still work
    TrendingFanoutFunction = None
    TrendingAggregateFunction = None
    TrendingWindowFunction = None
    AuthorFanoutFunction = None
    ReachAggregateFunction = None
    ReachWindowFunction = None
