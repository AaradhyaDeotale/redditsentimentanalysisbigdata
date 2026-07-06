"""
Word-sense disambiguation for ambiguous keywords (e.g. "apple" the company
vs. "apple" the fruit).

Pure Python, no Flink/Redis dependencies - fully unit-testable in isolation.
Approach: for each candidate sense, count whole-word/phrase, case-insensitive
hits of that sense's context words in the record's text. The sense with the
strictly-highest count wins; a tie (including 0-0) is reported as "ambiguous"
rather than guessed at.
"""

from __future__ import annotations

import re

AMBIGUOUS_KEYWORDS: dict[str, dict[str, list[str]]] = {
    "apple": {
        "company": [
            "iphone", "ios", "mac", "macbook", "ipad", "stock", "shares",
            "tim cook", "app store", "airpods",
        ],
        "fruit": [
            "eat", "ate", "eating", "tree", "juice", "pie", "fruit",
            "orchard", "picking", "cider",
        ],
    },
}


def resolve_sense(keyword: str, search_text: str) -> str:
    senses = AMBIGUOUS_KEYWORDS.get(keyword.strip().lower())
    if not senses:
        return "ambiguous"

    text = search_text or ""
    counts = {
        sense: sum(
            1
            for word in context_words
            if re.search(rf"\b{re.escape(word)}\b", text, re.IGNORECASE)
        )
        for sense, context_words in senses.items()
    }

    best = max(counts.values())
    winners = [sense for sense, count in counts.items() if count == best]
    if best == 0 or len(winners) > 1:
        return "ambiguous"
    return winners[0]
