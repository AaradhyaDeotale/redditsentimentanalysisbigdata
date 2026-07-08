#!/usr/bin/env python3
"""
Stage 0b - throwaway concept-validation probe. NOT production code.

Checks whether "subkeyword similarity" (cosine similarity of a comment's mean
word2vec embedding against a hand-picked sense subkeyword like "technology" or
"fruit") is a viable signal for disambiguating ambiguous keywords like "apple".

Uses the same TextCleaner + tokenize() preprocessing as
ml-model/scripts/train_word2vec_subset.py so tokens land in the same
vocabulary as the trained model.

Usage:
    python ml-model/scripts/probe_subkeyword_sense.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "flink-streaming" / "src"))
sys.path.insert(0, str(_REPO_ROOT / "ml-model" / "src"))

from flink_job.preprocessing.cleaner import TextCleaner  # noqa: E402
from flink_job.preprocessing.tokenizer import tokenize  # noqa: E402
from ml_model.features.word2vec_embedder import Word2VecFeatureExtractor  # noqa: E402

MODEL_PATH = _REPO_ROOT / "ml-model" / "models" / "word2vec_subset" / "word2vec.model"

# Mirrors the preprocessing used to train this model.
_CLEANER = TextCleaner(remove_urls=True, remove_markdown=True, lowercase=False)

CANDIDATE_SUBKEYWORDS = [
    "technology", "company", "fruit", "phone", "food", "stock", "tree", "xjcklsd",
]

TECH_COMMENTS = [
    "my new apple iphone is fast",
    "apple stock jumped today",
    "macbook battery life is great",
]
FRUIT_COMMENTS = [
    "ate a fresh apple from the tree",
    "apple pie recipe",
    "picking apples at the orchard",
]


def cosine_sim(a: np.ndarray, b: np.ndarray) -> float:
    denom = (np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0:
        return 0.0
    return float(np.dot(a, b) / denom)


def main() -> None:
    print(f"Loading model from {MODEL_PATH}")
    extractor = Word2VecFeatureExtractor.load(str(MODEL_PATH))
    wv = extractor._model.wv
    print(f"Vocabulary size: {len(wv.key_to_index):,}\n")

    # 1. Vocabulary check
    print("=" * 70)
    print("1. VOCABULARY CHECK")
    print("=" * 70)
    in_vocab = {}
    for word in CANDIDATE_SUBKEYWORDS:
        present = word in wv.key_to_index
        in_vocab[word] = present
        print(f"  {word!r:15} in vocab: {present}")

    # 2. Similarity sanity check
    print()
    print("=" * 70)
    print("2. SIMILARITY SANITY CHECK (top-10 most_similar)")
    print("=" * 70)
    for word, present in in_vocab.items():
        if not present:
            continue
        print(f"\n  Most similar to {word!r}:")
        for neighbor, score in wv.most_similar(word, topn=10):
            print(f"    {neighbor:20} {score:.4f}")

    # 3. Feature simulation
    print()
    print("=" * 70)
    print("3. FEATURE SIMULATION - apple comments vs 'technology'/'fruit'")
    print("=" * 70)

    if not (in_vocab.get("technology") and in_vocab.get("fruit")):
        print("  'technology' or 'fruit' not in vocab - cannot run simulation.")
        return

    tech_vec = wv["technology"]
    fruit_vec = wv["fruit"]

    all_comments = [("tech", c) for c in TECH_COMMENTS] + [("fruit", c) for c in FRUIT_COMMENTS]
    correct = 0
    for expected_sense, comment in all_comments:
        cleaned = _CLEANER.clean(comment)
        tokens = tokenize(cleaned, remove_stopwords=False, stem=False)
        mean_vec = extractor.embed(tokens)

        sim_tech = cosine_sim(mean_vec, tech_vec)
        sim_fruit = cosine_sim(mean_vec, fruit_vec)
        winner = "technology" if sim_tech > sim_fruit else "fruit"
        is_correct = (winner == "technology" and expected_sense == "tech") or (
            winner == "fruit" and expected_sense == "fruit"
        )
        correct += is_correct

        print(f"\n  comment: {comment!r}")
        print(f"    tokens          : {tokens}")
        print(f"    sim(technology) : {sim_tech:.4f}")
        print(f"    sim(fruit)      : {sim_fruit:.4f}")
        print(f"    winner          : {winner}   (expected: {expected_sense})   {'OK' if is_correct else 'WRONG'}")

    print(f"\n  Score: {correct}/{len(all_comments)} correct")


if __name__ == "__main__":
    main()
